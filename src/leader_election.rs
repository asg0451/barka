// based on https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/

use anyhow::{Context, Result, bail};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use bytes::Buf;
use serde::{Deserialize, Serialize};

use crate::s3::{self, S3Config};

#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    /// The timestamp (ms since epoch) until which the lock is valid, unless manually expired.
    valid_until_ms: u64,
    /// True if the lock file was manually expired by the node.
    expired: bool,
    /// The node ID of the leader.
    node_id: u64,
    // epoch is encoded in the file key
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Epoch(u64);

impl Epoch {
    // format: {prefix}/{epoch}.lock
    fn from_key(prefix: &str, key: &str) -> Result<Self> {
        let key = key.strip_prefix(prefix).unwrap();
        // let epoch = key.split('.').last().ok_or()?.parse::<u64>()?;
        // Ok(Self(epoch))
        todo!()
    }
    fn to_key(self, prefix: &str) -> String {
        format!("{}/{}.lock", prefix, self.0)
    }
    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug)]
pub enum TryBecomeLeaderResult {
    NotLeader,
    /// The current node is the leader.
    Leader(LeadershipInfo),
}

#[derive(Debug)]
pub struct LeadershipInfo {
    pub valid_until_ms: u64,
    pub epoch: Epoch,
}
// TODO: methods - extend, release, ..

pub struct LeaderElection {
    node_id: u64,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

const LOCK_FILE_PREFIX: &str = "lock/";
const VALIDITY_MILLIS: u64 = 10_000;

impl LeaderElection {
    pub async fn new<S: AsRef<str>>(node_id: u64, namespace: S, s3_config: S3Config) -> Self {
        let s3_client = s3::build_client(&s3_config).await;
        let prefix = format!("{}/{}/", LOCK_FILE_PREFIX, namespace.as_ref());
        tracing::debug!(
            node_id = node_id,
            namespace = namespace.as_ref(),
            "leader election initialized"
        );
        Self {
            node_id,
            s3_client,
            bucket: s3_config.bucket.clone(),
            prefix,
        }
    }

    // algorithm:
    // 1. List all lock files
    // 2. If there is no lock file, or the latest one has expired:
    //    3. Increment the epoch value by 1 and try to create a new lock file
    //    4. If the lock file could be created:
    //       5. The current node is the leader, start with the actual work
    //    6. Otherwise, go back to 1.
    // 7. Otherwise, another process already is the leader, so do nothing.
    //    Go back to 1. periodically
    // also we probably want a background process deleting old lock files..
    #[tracing::instrument(skip_all, fields(node_id = self.node_id, prefix = self.prefix, bucket = self.bucket))]
    pub async fn try_become_leader(&self) -> Result<TryBecomeLeaderResult> {
        let lock_files = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(LOCK_FILE_PREFIX)
            .send()
            .await
            .context("s3: list lock files")?;

        if lock_files.continuation_token.is_some() {
            bail!("leader election: too many lock files (paginated response)");
        }
        let contents = lock_files.contents.unwrap_or_default();

        let mut last_epoch = None;
        if !contents.is_empty() {
            let newest_lock_file = contents.last().unwrap();
            last_epoch = Some(Epoch::from_key(
                &self.prefix,
                newest_lock_file.key().unwrap(),
            )?);
            let newest_lock_file = self
                .s3_client
                .get_object()
                .bucket(&self.bucket)
                .key(newest_lock_file.key().unwrap())
                .send()
                .await
                .context("s3: get lock file")?
                .body
                .collect()
                .await
                .context("s3: read lock file body")?;
            let newest_lock_file: LockFile = serde_json::from_reader(newest_lock_file.reader())
                .context("deserialize lock file")?;
            // TODO: check if valid_until_ms is in the past too
            if !newest_lock_file.expired {
                tracing::debug!(
                    leader_node_id = newest_lock_file.node_id,
                    "not becoming leader: lock file not expired"
                );
                return Ok(TryBecomeLeaderResult::NotLeader);
            }
        }

        // create new lock file
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let valid_until_ms = now_ms + VALIDITY_MILLIS;

        let lock_file = serde_json::to_string(&LockFile {
            valid_until_ms,
            expired: false,
            node_id: self.node_id,
        })
        .unwrap();

        let epoch = last_epoch.unwrap_or(Epoch(0)).next();
        let lock_file_key = epoch.to_key(&self.prefix);
        let put_res = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(lock_file_key)
            .body(ByteStream::from(SdkBody::from(lock_file)))
            .if_none_match("*") // CAS
            .send()
            .await;
        match put_res {
            Ok(_) => {
                tracing::info!(epoch = epoch.0, "became leader");
                Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                    valid_until_ms,
                    epoch,
                }))
            }
            Err(e) => match classify_put_if_absent_error(&e) {
                Some(S3CASOutcome::AlreadyExists) => {
                    tracing::debug!("lock file already exists, not becoming leader");
                    Ok(TryBecomeLeaderResult::NotLeader)
                }
                Some(S3CASOutcome::RetryableConflict) => {
                    tracing::debug!("retryable CAS conflict, will retry... todo");
                    Ok(TryBecomeLeaderResult::NotLeader)
                }
                None => Err(anyhow::anyhow!(e).context("s3: put lock file")),
            },
        }
    }
}

enum S3CASOutcome {
    AlreadyExists,
    RetryableConflict,
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{self, S3Config};

    fn localstack_config(bucket: &str) -> S3Config {
        S3Config {
            endpoint_url: Some("http://localhost:4566".into()),
            bucket: bucket.into(),
            region: "us-east-1".into(),
        }
    }

    fn unique_name(prefix: &str) -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{ts}")
    }

    #[tokio::test]
    async fn test_become_leader_empty_bucket() {
        let bucket = unique_name("test-leader");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le = LeaderElection::new(1, "test-ns", config).await;
        let result = le.try_become_leader().await.unwrap();

        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(_)),
            "first node should become leader on empty bucket"
        );
        if let TryBecomeLeaderResult::Leader(info) = result {
            assert!(info.valid_until_ms > 0);
        }
    }

    #[tokio::test]
    async fn test_second_node_not_leader() {
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le1 = LeaderElection::new(1, &ns, config.clone()).await;
        let result = le1.try_become_leader().await.unwrap();
        assert!(matches!(result, TryBecomeLeaderResult::Leader(_)));

        let le2 = LeaderElection::new(2, &ns, config).await;
        let result = le2.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::NotLeader),
            "second node should not become leader"
        );
    }
}
