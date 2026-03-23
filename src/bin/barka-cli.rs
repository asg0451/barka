use std::io::{self, BufRead, IsTerminal};
use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use barka::log_offset::format_decomposed;
use barka::rpc::client::BarkaClient;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "barka-cli",
    version,
    about = "Cap'n Proto RPC client for barka"
)]
#[group(id = "mode", required = true, args = ["produce", "consume"])]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:9292")]
    addr: SocketAddr,

    #[arg(long, conflicts_with = "consume")]
    produce: bool,

    #[arg(long, conflicts_with = "produce")]
    consume: bool,

    #[arg(short, long)]
    topic: String,

    #[arg(short = 'n', long, default_value_t = 0)]
    partition: u32,

    /// Record payload as UTF-8 (repeat for multiple records). With --produce, if omitted and stdin is piped, one record per line is read.
    #[arg(short, long)]
    value: Vec<String>,

    #[arg(long, default_value_t = 0)]
    offset: u64,

    #[arg(long, default_value_t = 10)]
    max: u32,

    /// Print consume output as JSON Lines (default: pretty text).
    #[arg(long)]
    json: bool,
}

fn collect_produce_values(cli: &Cli) -> Result<Vec<Vec<u8>>> {
    if !cli.value.is_empty() {
        return Ok(cli.value.iter().map(|s| s.as_bytes().to_vec()).collect());
    }

    let stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("--produce needs --value or piped stdin (one record per line)");
    }

    let mut out = Vec::new();
    for line in stdin.lock().lines() {
        out.push(line?.into_bytes());
    }
    if out.is_empty() {
        bail!("stdin had no lines to produce");
    }
    Ok(out)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("tokio runtime")?;

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let client = BarkaClient::connect(cli.addr)
                    .await
                    .with_context(|| format!("connect {}", cli.addr))?;

                if cli.produce {
                    let values = collect_produce_values(&cli)?;
                    let records = client
                        .produce(&cli.topic, cli.partition, values)
                        .await
                        .context("produce RPC")?;
                    if cli.json {
                        for rec in &records {
                            println!(
                                "{}",
                                serde_json::to_string(rec).context("serialize record")?
                            );
                        }
                    } else {
                        for rec in &records {
                            let value = String::from_utf8_lossy(&rec.value);
                            println!(
                                "{} timestamp={} value={value}",
                                format_decomposed(rec.offset),
                                rec.timestamp
                            );
                        }
                    }
                    return Ok(());
                }

                let records = client
                    .consume(&cli.topic, cli.partition, cli.offset, cli.max)
                    .await
                    .context("consume RPC")?;

                if cli.json {
                    for rec in &records {
                        println!(
                            "{}",
                            serde_json::to_string(rec).context("serialize record")?
                        );
                    }
                } else {
                    for rec in records {
                        let value = String::from_utf8_lossy(&rec.value);
                        println!(
                            "{} timestamp={} value={value}",
                            format_decomposed(rec.offset),
                            rec.timestamp
                        );
                    }
                }
                Ok(())
            })
            .await
    })
}
