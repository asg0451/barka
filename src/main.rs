use barka::node::{Node, NodeConfig};
use barka::s3::{self, S3Config};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = NodeConfig::default();
    let s3_config = S3Config::from_env();

    tracing::info!(
        node_id = config.node_id,
        s3_endpoint = s3_config.endpoint_url.as_deref().unwrap_or("aws"),
        s3_bucket = %s3_config.bucket,
        "starting barka",
    );

    let s3_client = s3::build_client(&s3_config).await;
    s3::ensure_bucket(&s3_client, &s3_config.bucket).await?;

    let node = Node::new(config);
    node.serve().await?;
    Ok(())
}
