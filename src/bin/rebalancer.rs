use std::time::Duration;

use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser)]
#[command(name = "rebalancer", version, about = "Barka partition rebalancer")]
struct Cli {
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "BARKA_S3_BUCKET", default_value = "barka")]
    s3_bucket: String,

    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    aws_region: String,

    /// Path segment in S3 before `lock/` (must match produce-node's --leader-election-prefix).
    #[arg(long, env = "BARKA_LEADER_ELECTION_PREFIX")]
    leader_election_prefix: Option<String>,

    /// Run one rebalance cycle and exit.
    #[arg(long)]
    once: bool,

    /// Seconds between rebalance cycles in periodic mode.
    #[arg(long, default_value_t = 30)]
    interval_secs: u64,

    /// Max partitions to abdicate per overloaded node per cycle.
    #[arg(long, default_value_t = 1)]
    max_abdications_per_node: usize,

    /// Seconds to wait between individual abdications.
    #[arg(long, default_value_t = 5)]
    settle_delay_secs: u64,

    /// Log the plan but don't actually abdicate.
    #[arg(long)]
    dry_run: bool,
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

    let le_prefix = cli.leader_election_prefix;
    let max_abd = cli.max_abdications_per_node;
    let settle_delay = Duration::from_secs(cli.settle_delay_secs);
    let dry_run = cli.dry_run;
    let once = cli.once;
    let interval = Duration::from_secs(cli.interval_secs);

    tracing::info!(
        once,
        interval_secs = interval.as_secs(),
        max_abdications_per_node = max_abd,
        settle_delay_secs = settle_delay.as_secs(),
        dry_run,
        "starting rebalancer"
    );

    // Cap'n Proto RPC is !Send, so we need a single-threaded runtime with LocalSet.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let s3_client = barka::s3::build_client(&s3_config).await;
                let registry = barka::partition_registry::PartitionRegistry::new(
                    s3_client.clone(),
                    s3_config.bucket.clone(),
                    le_prefix.as_deref(),
                );

                loop {
                    if let Err(e) = barka::rebalancer::run_once(
                        &s3_client,
                        &s3_config.bucket,
                        &registry,
                        le_prefix.as_deref(),
                        max_abd,
                        settle_delay,
                        dry_run,
                    )
                    .await
                    {
                        tracing::error!(error = %e, "rebalance cycle failed");
                    }

                    if once {
                        break;
                    }
                    tokio::time::sleep(interval).await;
                }
            })
            .await;
    });

    Ok(())
}
