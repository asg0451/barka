#![recursion_limit = "256"]

use std::io::{self, BufRead, IsTerminal};
use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use barka::leader_election;
use barka::log_offset::format_decomposed;
use barka::node;
use barka::partition_registry::PartitionRegistry;
use barka::rpc::client::ConsumeClient;
use barka::s3;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "barka-cli", version, about = "CLI for barka distributed log")]
struct Cli {
    /// S3 endpoint URL (e.g. http://localhost:4566).
    #[arg(long, global = true)]
    s3_endpoint: Option<String>,

    /// S3 bucket.
    #[arg(long, global = true, default_value = "barka")]
    s3_bucket: String,

    /// AWS region for S3.
    #[arg(long, global = true, default_value = "us-east-1")]
    aws_region: String,

    /// S3 key prefix for leader election lock files.
    #[arg(long, global = true)]
    leader_election_prefix: Option<String>,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    fn s3_config(&self) -> s3::S3Config {
        s3::S3Config {
            endpoint_url: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            region: self.aws_region.clone(),
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Produce records to a topic-partition.
    Produce(ProduceArgs),
    /// Consume records from a topic-partition.
    Consume(ConsumeArgs),
    /// Manage topics and partitions.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
}

#[derive(Args)]
struct ProduceArgs {
    #[arg(short, long)]
    topic: String,

    #[arg(short = 'n', long, default_value_t = 0)]
    partition: u32,

    /// Record payload as UTF-8 (repeat for multiple records). With --produce, if omitted and stdin is piped, one record per line is read.
    #[arg(short, long)]
    value: Vec<String>,

    /// Print output as JSON Lines.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct ConsumeArgs {
    #[arg(long, default_value = "127.0.0.1:9392")]
    consume_addr: SocketAddr,

    #[arg(short, long)]
    topic: String,

    #[arg(short = 'n', long, default_value_t = 0)]
    partition: u32,

    #[arg(long, default_value_t = 0)]
    offset: u64,

    #[arg(long, default_value_t = 10)]
    max: u32,

    /// Print output as JSON Lines.
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
enum TopicCommand {
    /// List all registered topic-partition pairs.
    List {
        /// Print output as JSON Lines.
        #[arg(long)]
        json: bool,
    },
    /// Register a new topic-partition.
    Add {
        #[arg(short, long)]
        topic: String,

        #[arg(short = 'n', long)]
        partition: u32,
    },
    /// Remove a topic-partition from the registry.
    Remove {
        #[arg(short, long)]
        topic: String,

        #[arg(short = 'n', long)]
        partition: u32,
    },
    /// Show partitions for a topic with leader status.
    Describe {
        #[arg(short, long)]
        topic: String,

        /// Print output as JSON Lines.
        #[arg(long)]
        json: bool,
    },
}

fn collect_produce_values(args: &ProduceArgs) -> Result<Vec<Vec<u8>>> {
    if !args.value.is_empty() {
        return Ok(args.value.iter().map(|s| s.as_bytes().to_vec()).collect());
    }

    let stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("produce needs --value or piped stdin (one record per line)");
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

async fn topic_list(
    s3_config: &s3::S3Config,
    leader_election_prefix: Option<&str>,
    json: bool,
) -> Result<()> {
    let s3_client = s3::build_client(s3_config).await;
    let registry =
        PartitionRegistry::new(s3_client, s3_config.bucket.clone(), leader_election_prefix);
    let entries = registry.list().await?;
    if json {
        for entry in &entries {
            println!("{}", serde_json::to_string(entry)?);
        }
    } else if entries.is_empty() {
        println!("No partitions registered.");
    } else {
        for entry in &entries {
            println!("{}\t{}", entry.topic, entry.partition);
        }
    }
    Ok(())
}

async fn topic_add(
    s3_config: &s3::S3Config,
    leader_election_prefix: Option<&str>,
    topic: &str,
    partition: u32,
) -> Result<()> {
    let s3_client = s3::build_client(s3_config).await;
    let registry =
        PartitionRegistry::new(s3_client, s3_config.bucket.clone(), leader_election_prefix);
    let added = registry.add(topic, partition).await?;
    if added {
        println!("Added {topic}/{partition}");
    } else {
        println!("{topic}/{partition} already exists");
    }
    Ok(())
}

async fn topic_remove(
    s3_config: &s3::S3Config,
    leader_election_prefix: Option<&str>,
    topic: &str,
    partition: u32,
) -> Result<()> {
    let s3_client = s3::build_client(s3_config).await;
    let registry =
        PartitionRegistry::new(s3_client, s3_config.bucket.clone(), leader_election_prefix);
    let removed = registry.remove(topic, partition).await?;
    if removed {
        println!("Removed {topic}/{partition}");
    } else {
        println!("{topic}/{partition} not found");
    }
    Ok(())
}

async fn topic_describe(
    s3_config: &s3::S3Config,
    leader_election_prefix: Option<&str>,
    topic: &str,
    json: bool,
) -> Result<()> {
    let s3_client = s3::build_client(s3_config).await;
    let registry = PartitionRegistry::new(
        s3_client.clone(),
        s3_config.bucket.clone(),
        leader_election_prefix,
    );
    let entries = registry.list().await?;
    let mut partitions: Vec<_> = entries.iter().filter(|e| e.topic == topic).collect();
    partitions.sort_by_key(|e| e.partition);

    if partitions.is_empty() {
        if json {
            println!("[]");
        } else {
            println!("No partitions registered for topic '{topic}'.");
        }
        return Ok(());
    }

    for entry in &partitions {
        let namespace = node::leader_namespace(&entry.topic, entry.partition);
        let leader = leader_election::read_current_leader(
            &s3_client,
            &s3_config.bucket,
            &namespace,
            leader_election_prefix,
        )
        .await?;

        if json {
            let obj = serde_json::json!({
                "topic": entry.topic,
                "partition": entry.partition,
                "leader": leader.as_ref().map(|l| serde_json::json!({
                    "node_id": l.node_id,
                    "addr": l.addr.to_string(),
                    "epoch": l.epoch.as_u64(),
                    "valid_until_ms": l.valid_until_ms,
                })),
            });
            println!("{}", serde_json::to_string(&obj)?);
        } else {
            match leader {
                Some(l) => println!(
                    "{topic}/{}\tleader={}  node_id={}  epoch={}  valid_until_ms={}",
                    entry.partition,
                    l.addr,
                    l.node_id,
                    l.epoch.as_u64(),
                    l.valid_until_ms,
                ),
                None => println!("{topic}/{}\tno leader", entry.partition),
            }
        }
    }
    Ok(())
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
                let s3_config = cli.s3_config();
                let le_prefix = cli.leader_election_prefix.as_deref();

                match cli.command {
                    Command::Produce(ref args) => {
                        let values = collect_produce_values(args)?;
                        let mut router = barka::produce_router::ProduceRouter::new(
                            &s3_config,
                            cli.leader_election_prefix.clone(),
                        )
                        .await;
                        let records = router
                            .produce(&args.topic, args.partition, values)
                            .await
                            .context("produce")?;
                        if args.json {
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
                    }
                    Command::Consume(ref args) => {
                        let mut client = ConsumeClient::connect(args.consume_addr)
                            .await
                            .with_context(|| format!("connect {}", args.consume_addr))?;
                        let records = client
                            .consume(&args.topic, args.partition, args.offset, args.max)
                            .await
                            .context("consume RPC")?;

                        if args.json {
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
                    }
                    Command::Topic { ref command } => match command {
                        TopicCommand::List { json } => {
                            topic_list(&s3_config, le_prefix, *json).await?;
                        }
                        TopicCommand::Add { topic, partition } => {
                            topic_add(&s3_config, le_prefix, topic, *partition).await?;
                        }
                        TopicCommand::Remove { topic, partition } => {
                            topic_remove(&s3_config, le_prefix, topic, *partition).await?;
                        }
                        TopicCommand::Describe { topic, json } => {
                            topic_describe(&s3_config, le_prefix, topic, *json).await?;
                        }
                    },
                }
                Ok(())
            })
            .await
    })
}
