use std::net::{IpAddr, SocketAddr};

use barka::node::{Node, NodeConfig};
use barka::s3::{self, S3Config};
use clap::Parser;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "barka", version, about = "Barka distributed log node")]
struct Cli {
    #[arg(long, env = "BARKA_NODE_ID", default_value_t = 0)]
    node_id: u64,

    #[arg(long, env = "BARKA_RPC_PORT", default_value_t = 9292)]
    rpc_port: u16,

    #[arg(long, env = "BARKA_JEPSEN_GATEWAY_PORT", default_value_t = 9293)]
    jepsen_gateway_port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,

    /// S3 API endpoint (e.g. http://localhost:4566 for LocalStack)
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "BARKA_S3_BUCKET", default_value = "barka")]
    s3_bucket: String,

    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    aws_region: String,
}

impl Cli {
    fn node_config(&self) -> NodeConfig {
        NodeConfig {
            node_id: self.node_id,
            rpc_addr: SocketAddr::new(self.bind, self.rpc_port),
            jepsen_gateway_addr: SocketAddr::new(self.bind, self.jepsen_gateway_port),
            s3_prefix: None,
        }
    }

    fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint_url: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            region: self.aws_region.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();
    let config = cli.node_config();
    let s3_config = cli.s3_config();

    tracing::info!(
        node_id = config.node_id,
        s3_endpoint = s3_config.endpoint_url.as_deref().unwrap_or("aws"),
        s3_bucket = %s3_config.bucket,
        "starting barka",
    );

    let s3_client = s3::build_client(&s3_config).await;
    s3::ensure_bucket(&s3_client, &s3_config.bucket).await?;

    let node = Node::new(config, &s3_config).await?;
    node.serve().await?;
    Ok(())
}
