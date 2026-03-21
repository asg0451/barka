use barka::node::{Node, NodeConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = NodeConfig::default();
    tracing::info!(node_id = config.node_id, "starting barka");

    let node = Node::new(config);
    node.serve().await?;
    Ok(())
}
