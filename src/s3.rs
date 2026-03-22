use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;

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
pub async fn ensure_bucket(client: &Client, bucket: &str) -> crate::error::Result<()> {
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(_) => {
            client
                .create_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|e| crate::error::Error::Storage {
                    message: format!("create bucket: {e}"),
                    backtrace: std::backtrace::Backtrace::capture(),
                })?;
            Ok(())
        }
    }
}
