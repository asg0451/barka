use std::net::SocketAddr;

use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser)]
#[command(name = "jepsen-gateway", version, about = "Barka Jepsen test gateway")]
struct Cli {
    #[arg(
        long,
        env = "BARKA_JEPSEN_LISTEN_ADDR",
        default_value = "127.0.0.1:9293"
    )]
    listen_addr: SocketAddr,

    #[arg(long, env = "BARKA_CONSUME_RPC_ADDR", default_value = "127.0.0.1:9392")]
    consume_rpc_addr: SocketAddr,

    #[arg(long, env = "BARKA_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "BARKA_S3_BUCKET", default_value = "barka")]
    s3_bucket: String,

    #[arg(long, env = "BARKA_AWS_REGION", default_value = "us-east-1")]
    aws_region: String,

    /// Path segment in S3 before `lock/` (must match produce-node's --leader-election-prefix).
    #[arg(long, env = "BARKA_LEADER_ELECTION_PREFIX")]
    leader_election_prefix: Option<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();

    let s3_config = barka::s3::S3Config {
        endpoint_url: cli.s3_endpoint,
        bucket: cli.s3_bucket,
        region: cli.aws_region,
    };

    tracing::info!(
        listen_addr = %cli.listen_addr,
        consume_rpc_addr = %cli.consume_rpc_addr,
        "starting jepsen-gateway",
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(barka::jepsen_gateway::serve(
        cli.consume_rpc_addr,
        cli.listen_addr,
        s3_config,
        cli.leader_election_prefix,
    ))
}
