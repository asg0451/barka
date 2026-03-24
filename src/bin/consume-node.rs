use std::net::{IpAddr, SocketAddr};

use barka::consume_node::{ConsumeNode, ConsumeNodeConfig};
use barka::s3::{self, S3Config};
use clap::Parser;

#[derive(Parser)]
#[command(name = "consume-node", version, about = "Barka consume node")]
struct Cli {
    #[arg(long, env = "BARKA_RPC_PORT", default_value_t = 9392)]
    rpc_port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,

    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "BARKA_S3_BUCKET", default_value = "barka")]
    s3_bucket: String,

    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    aws_region: String,

    #[arg(long, env = "BARKA_S3_PREFIX")]
    s3_prefix: Option<String>,
}

impl Cli {
    fn node_config(&self) -> ConsumeNodeConfig {
        ConsumeNodeConfig {
            rpc_addr: SocketAddr::new(self.bind, self.rpc_port),
            s3_prefix: self.s3_prefix.clone(),
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
    barka::tracing_init::init_tracing();

    let cli = Cli::parse();
    let config = cli.node_config();
    let s3_config = cli.s3_config();

    tracing::info!(
        rpc_addr = %config.rpc_addr,
        s3_endpoint = s3_config.endpoint_url.as_deref().unwrap_or("aws"),
        s3_bucket = %s3_config.bucket,
        "starting consume-node",
    );

    let s3_client = s3::build_client(&s3_config).await;
    s3::ensure_bucket(&s3_client, &s3_config.bucket).await?;

    let node = ConsumeNode::new(config, &s3_config).await?;
    node.serve().await?;
    Ok(())
}
