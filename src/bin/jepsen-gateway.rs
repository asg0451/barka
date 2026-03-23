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

    #[arg(long, env = "BARKA_PRODUCE_RPC_ADDR", default_value = "127.0.0.1:9292")]
    produce_rpc_addr: SocketAddr,

    #[arg(long, env = "BARKA_CONSUME_RPC_ADDR", default_value = "127.0.0.1:9392")]
    consume_rpc_addr: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();

    tracing::info!(
        listen_addr = %cli.listen_addr,
        produce_rpc_addr = %cli.produce_rpc_addr,
        consume_rpc_addr = %cli.consume_rpc_addr,
        "starting jepsen-gateway",
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(barka::jepsen_gateway::serve(
        cli.produce_rpc_addr,
        cli.consume_rpc_addr,
        cli.listen_addr,
    ))
}
