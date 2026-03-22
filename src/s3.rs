use anyhow::{bail, Context, Result};
use bytes::Buf;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::Client;

const DEFAULT_BUCKET: &str = "barka";
const DEFAULT_REGION: &str = "us-east-1";

/// S3 configuration. When `endpoint_url` is set, the client targets that
/// endpoint (e.g. LocalStack at `http://localhost:4566`) instead of real AWS.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint_url: Option<String>,
    pub bucket: String,
    pub region: String,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint_url: None,
            bucket: DEFAULT_BUCKET.into(),
            region: DEFAULT_REGION.into(),
        }
    }
}

impl S3Config {
    pub fn from_env() -> Self {
        Self {
            endpoint_url: std::env::var("AWS_ENDPOINT_URL").ok(),
            bucket: std::env::var("BARKA_S3_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.into()),
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| DEFAULT_REGION.into()),
        }
    }
}

/// Build an S3 client. Points at LocalStack when `config.endpoint_url` is set.
#[tracing::instrument(skip_all)]
pub async fn build_client(config: &S3Config) -> Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .load()
        .await;

    let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
    if let Some(ref url) = config.endpoint_url {
        builder = builder.endpoint_url(url).force_path_style(true);
    }

    Client::from_conf(builder.build())
}

/// Ensure the bucket exists, creating it if necessary.
#[tracing::instrument(skip(client), err)]
pub async fn ensure_bucket(client: &Client, bucket: &str) -> Result<()> {
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(_) => {
            client
                .create_bucket()
                .bucket(bucket)
                .send()
                .await
                .context("create bucket")?;
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

const RETRY_MAX: u32 = 4;
const RETRY_BASE_MS: u64 = 50;
const RETRY_CAP_MS: u64 = 2_000;

enum RetryResult<T> {
    Done(T),
    Retry(anyhow::Error),
    Fail(anyhow::Error),
}

#[tracing::instrument(skip_all, fields(op), err)]
async fn retry_with_backoff<T, Fut, F>(op: &str, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RetryResult<T>>,
{
    let mut attempt = 0u32;
    loop {
        let span = tracing::info_span!("retry_attempt", %op, attempt);
        match tracing::Instrument::instrument(f(), span).await {
            RetryResult::Done(v) => return Ok(v),
            RetryResult::Fail(e) => return Err(e),
            RetryResult::Retry(e) => {
                attempt += 1;
                if attempt > RETRY_MAX {
                    return Err(e.context(format!("{op}: exhausted {RETRY_MAX} retries")));
                }
                let delay = backoff_delay(attempt);
                tracing::warn!(
                    attempt,
                    max = RETRY_MAX,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "{op}: retrying",
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn backoff_delay(attempt: u32) -> std::time::Duration {
    let exp = RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(10));
    let jitter = cheap_jitter_ms(exp);
    let ms = (RETRY_BASE_MS + jitter).min(RETRY_CAP_MS);
    std::time::Duration::from_millis(ms)
}

/// Sub-millisecond jitter sourced from the system clock. Only needs to
/// desynchronize concurrent retriers, not be cryptographically random.
fn cheap_jitter_ms(max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    nanos % max
}

fn is_transient_s3_error<E>(err: &SdkError<E>) -> bool {
    match err.raw_response() {
        Some(raw) => matches!(raw.status().as_u16(), 429 | 500 | 502 | 503 | 504),
        None => true,
    }
}

// ---------------------------------------------------------------------------
// Higher-level S3 operations
// ---------------------------------------------------------------------------

/// List objects under `prefix` (single page). Fails if the response is
/// truncated (>1 000 keys).
#[tracing::instrument(skip(client), err)]
pub async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<aws_sdk_s3::types::Object>> {
    let resp = retry_with_backoff("list objects", || {
        let client = client.clone();
        let bucket = bucket.to_owned();
        let prefix = prefix.to_owned();
        async move {
            match client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
            {
                Ok(v) => RetryResult::Done(v),
                Err(e) if is_transient_s3_error(&e) => {
                    RetryResult::Retry(anyhow::anyhow!(e).context("s3: list objects"))
                }
                Err(e) => RetryResult::Fail(anyhow::anyhow!(e).context("s3: list objects")),
            }
        }
    })
    .await?;

    // ListObjectsV2 returns at most 1 000 keys per page. We don't paginate;
    // if the listing is truncated the caller is in an unexpected state.
    if resp.is_truncated.unwrap_or(false) {
        bail!("s3: list under {prefix}: response truncated (too many objects)");
    }
    Ok(resp.contents.unwrap_or_default())
}

/// Fetch an object's body as a reader. The response is buffered in memory
/// but not flattened into a single contiguous allocation.
#[tracing::instrument(skip(client), err)]
pub async fn get_object_reader(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<impl std::io::Read> {
    let body = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("s3: get object")?
        .body
        .collect()
        .await
        .context("s3: read object body")?;
    Ok(body.reader())
}

/// Unconditional put: writes `body` to `key`, overwriting any existing object.
#[tracing::instrument(skip(client, body), err)]
pub async fn put_object(client: &Client, bucket: &str, key: &str, body: String) -> Result<()> {
    retry_with_backoff("put object", || {
        let client = client.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let body = body.clone();
        async move {
            match client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(SdkBody::from(body)))
                .send()
                .await
            {
                Ok(_) => RetryResult::Done(()),
                Err(e) if is_transient_s3_error(&e) => {
                    RetryResult::Retry(anyhow::anyhow!(e).context("s3: put object"))
                }
                Err(e) => RetryResult::Fail(anyhow::anyhow!(e).context("s3: put object")),
            }
        }
    })
    .await
}

/// Batch-delete objects by key. Silently succeeds on an empty list.
/// Keys are capped at 1 000 per the S3 DeleteObjects API (matches our
/// list_objects cap, so callers that list-then-delete are always safe).
#[tracing::instrument(skip(client, keys), fields(count = keys.len()), err)]
pub async fn delete_objects(client: &Client, bucket: &str, keys: Vec<String>) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    retry_with_backoff("delete objects", || {
        let client = client.clone();
        let bucket = bucket.to_owned();
        let keys = keys.clone();
        async move {
            let objects: Vec<_> = match keys
                .into_iter()
                .map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k)
                        .build()
                })
                .collect::<std::result::Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(e) => return RetryResult::Fail(anyhow::anyhow!(e)),
            };
            let delete = match aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build()
            {
                Ok(v) => v,
                Err(e) => return RetryResult::Fail(anyhow::anyhow!(e)),
            };
            match client
                .delete_objects()
                .bucket(&bucket)
                .delete(delete)
                .send()
                .await
            {
                Ok(_) => RetryResult::Done(()),
                Err(e) if is_transient_s3_error(&e) => {
                    RetryResult::Retry(anyhow::anyhow!(e).context("s3: delete objects"))
                }
                Err(e) => RetryResult::Fail(anyhow::anyhow!(e).context("s3: delete objects")),
            }
        }
    })
    .await
}

enum S3CASOutcome {
    AlreadyExists,
    RetryableConflict,
}

/// Returns `None` for errors unrelated to conditional-write semantics
/// (e.g. network failures, auth errors), leaving them for the caller to
/// classify as transient or fatal.
fn classify_put_if_absent_error(err: &SdkError<PutObjectError>) -> Option<S3CASOutcome> {
    let status = err.raw_response().map(|r| r.status().as_u16());
    let code = err.code();

    if status == Some(412) {
        return Some(S3CASOutcome::AlreadyExists);
    }
    if status == Some(409) {
        return Some(S3CASOutcome::RetryableConflict);
    }

    if let Some(c) = code {
        if c == "PreconditionFailed" {
            return Some(S3CASOutcome::AlreadyExists);
        }
        if c == "ConditionalRequestConflict" {
            return Some(S3CASOutcome::RetryableConflict);
        }
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyExists,
}

/// Conditional-put: writes `body` to `key` only if no object exists there
/// (S3 `if-none-match: *`). Retries transient S3 errors and 409 conflicts
/// with jittered exponential backoff (up to 4 attempts).
#[tracing::instrument(skip(client, body), err)]
pub async fn put_if_absent(
    client: &Client,
    bucket: &str,
    key: &str,
    body: bytes::Bytes,
) -> Result<PutOutcome> {
    retry_with_backoff("put if absent", || {
        let client = client.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let body = body.clone();
        async move {
            match client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(SdkBody::from(body)))
                .if_none_match("*")
                .send()
                .await
            {
                Ok(_) => RetryResult::Done(PutOutcome::Created),
                Err(e) => match classify_put_if_absent_error(&e) {
                    Some(S3CASOutcome::AlreadyExists) => {
                        RetryResult::Done(PutOutcome::AlreadyExists)
                    }
                    Some(S3CASOutcome::RetryableConflict) => {
                        RetryResult::Retry(anyhow::anyhow!(e))
                    }
                    None if is_transient_s3_error(&e) => {
                        RetryResult::Retry(anyhow::anyhow!(e).context("s3: put if absent"))
                    }
                    None => RetryResult::Fail(anyhow::anyhow!(e).context("s3: put if absent")),
                },
            }
        }
    })
    .await
}
