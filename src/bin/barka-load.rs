#![recursion_limit = "256"]

use std::path::PathBuf;
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use barka::leader_election;
use barka::node::leader_namespace;
use barka::partition_registry::PartitionRegistry;
use barka::produce_router::ProduceRouter;
use barka::s3::{self, S3Config};
use clap::Parser;

const BASE_PRODUCE_PORT: u16 = 9292;
const CONSUME_PORT: u16 = 9392;

#[derive(Parser)]
#[command(name = "barka-load", version, about = "Barka produce load tester")]
struct Cli {
    /// Number of worker threads.
    #[arg(long, default_value_t = 8)]
    workers: usize,

    /// Number of partitions to create and produce to.
    #[arg(long, default_value_t = 4)]
    partitions: u32,

    /// How long to run the test (seconds).
    #[arg(long, default_value_t = 30)]
    duration: u64,

    /// Topic name.
    #[arg(long, default_value = "load-test")]
    topic: String,

    /// Records per produce call.
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Bytes per record value.
    #[arg(long, default_value_t = 1024)]
    record_size: usize,

    /// Number of produce nodes to start.
    #[arg(long, default_value_t = 3)]
    nodes: u32,

    /// S3 endpoint URL.
    #[arg(long, default_value = "http://localhost:4566")]
    s3_endpoint: String,

    /// S3 bucket name.
    #[arg(long, default_value = "barka")]
    s3_bucket: String,

    /// Skip starting the cluster; requires --s3-prefix.
    #[arg(long)]
    skip_start: bool,

    /// S3 prefix (auto-generated if not provided).
    #[arg(long)]
    s3_prefix: Option<String>,
}

struct WorkerMetrics {
    records_produced: u64,
    bytes_produced: u64,
    errors: u64,
    latencies: Vec<Duration>,
}

/// Find sibling binaries in the same target/{profile}/ directory as ourselves.
fn bin_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("current_exe")?;
    Ok(exe.parent().context("exe has no parent dir")?.to_path_buf())
}

fn start_nodes(cli: &Cli, s3_prefix: &str) -> Result<Vec<Child>> {
    let dir = bin_dir()?;
    let mut children = Vec::new();

    // Produce nodes
    for i in 0..cli.nodes {
        let port = BASE_PRODUCE_PORT + (i as u16);
        println!("  produce-node {i}: rpc=:{port}");
        let child = std::process::Command::new(dir.join("produce-node"))
            .args(["--node-id", &i.to_string()])
            .args(["--rpc-port", &port.to_string()])
            .args(["--s3-endpoint", &cli.s3_endpoint])
            .args(["--s3-bucket", &cli.s3_bucket])
            .args(["--s3-prefix", s3_prefix])
            .args(["--leader-election-prefix", s3_prefix])
            .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_default())
            .env("RUST_BACKTRACE", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn produce-node {i}"))?;
        children.push(child);
    }

    // Consume node
    println!("  consume-node: rpc=:{CONSUME_PORT}");
    let child = std::process::Command::new(dir.join("consume-node"))
        .args(["--rpc-port", &CONSUME_PORT.to_string()])
        .args(["--s3-endpoint", &cli.s3_endpoint])
        .args(["--s3-bucket", &cli.s3_bucket])
        .args(["--s3-prefix", s3_prefix])
        .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_default())
        .env("RUST_BACKTRACE", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .context("spawn consume-node")?;
    children.push(child);

    // Rebalancer
    println!("  rebalancer: interval=30s");
    let child = std::process::Command::new(dir.join("rebalancer"))
        .args(["--s3-endpoint", &cli.s3_endpoint])
        .args(["--s3-bucket", &cli.s3_bucket])
        .args(["--leader-election-prefix", s3_prefix])
        .args(["--interval-secs", "30"])
        .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_default())
        .env("RUST_BACKTRACE", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .context("spawn rebalancer")?;
    children.push(child);

    Ok(children)
}

fn kill_children(children: &mut [Child]) {
    for child in children.iter_mut() {
        let _ = child.kill();
    }
    for child in children.iter_mut() {
        let _ = child.wait();
    }
}

async fn wait_for_leaders(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    topic: &str,
    num_partitions: u32,
    le_prefix: Option<&str>,
) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(3)).await;

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut all_ready = true;
        for p in 0..num_partitions {
            let ns = leader_namespace(topic, p);
            let leader =
                leader_election::read_current_leader(s3_client, bucket, &ns, le_prefix).await?;
            if leader.is_none() {
                all_ready = false;
                break;
            }
        }
        if all_ready {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("timed out waiting for leaders on all {num_partitions} partitions");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn run_worker(
    worker_id: usize,
    partition: u32,
    topic: String,
    s3_config: S3Config,
    le_prefix: Option<String>,
    batch: Vec<Vec<u8>>,
    stop: Arc<AtomicBool>,
) -> WorkerMetrics {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let batch_bytes: u64 = batch.iter().map(|v| v.len() as u64).sum();
    let mut metrics = WorkerMetrics {
        records_produced: 0,
        bytes_produced: 0,
        errors: 0,
        latencies: Vec::with_capacity(4096),
    };

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut router = ProduceRouter::new(&s3_config, le_prefix).await;

                while !stop.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    match router.produce(&topic, partition, batch.clone()).await {
                        Ok(records) => {
                            let elapsed = start.elapsed();
                            metrics.records_produced += records.len() as u64;
                            metrics.bytes_produced += batch_bytes;
                            metrics.latencies.push(elapsed);
                        }
                        Err(e) => {
                            tracing::warn!(worker = worker_id, error = %e, "produce failed");
                            metrics.errors += 1;
                        }
                    }
                }
            })
            .await;
    });

    metrics
}

fn print_metrics(cli: &Cli, all_metrics: Vec<WorkerMetrics>, actual_duration: Duration) {
    let total_records: u64 = all_metrics.iter().map(|m| m.records_produced).sum();
    let total_bytes: u64 = all_metrics.iter().map(|m| m.bytes_produced).sum();
    let total_errors: u64 = all_metrics.iter().map(|m| m.errors).sum();
    let total_batches: usize = all_metrics.iter().map(|m| m.latencies.len()).sum();

    let mut all_latencies: Vec<Duration> =
        all_metrics.into_iter().flat_map(|m| m.latencies).collect();
    all_latencies.sort();

    let secs = actual_duration.as_secs_f64();
    let records_per_sec = total_records as f64 / secs;
    let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;

    println!();
    println!("=== Load Test Results ===");
    println!("Duration:      {:.1}s", secs);
    println!(
        "Workers:       {}  Partitions: {}",
        cli.workers, cli.partitions,
    );
    println!("Batch size:    {} x {}B", cli.batch_size, cli.record_size);
    println!("Records:       {total_records}");
    println!("Batches:       {total_batches}");
    println!("Errors:        {total_errors}");
    println!(
        "Bytes:         {total_bytes} ({:.2} MB)",
        total_bytes as f64 / (1024.0 * 1024.0),
    );
    println!("Throughput:    {records_per_sec:.1} records/sec");
    println!("Bandwidth:     {mb_per_sec:.2} MB/sec");

    if !all_latencies.is_empty() {
        let percentile = |pct: f64| -> Duration {
            let idx = ((pct / 100.0) * (all_latencies.len() - 1) as f64).round() as usize;
            all_latencies[idx.min(all_latencies.len() - 1)]
        };
        println!("Latency (per batch):");
        println!("  p50:   {:?}", percentile(50.0));
        println!("  p95:   {:?}", percentile(95.0));
        println!("  p99:   {:?}", percentile(99.0));
        println!("  max:   {:?}", all_latencies.last().unwrap());
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let s3_prefix = match (&cli.s3_prefix, cli.skip_start) {
        (Some(p), _) => p.clone(),
        (None, true) => bail!("--s3-prefix is required with --skip-start"),
        (None, false) => {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            format!("load-{ts}")
        }
    };

    // -- Start cluster --
    let mut children = if !cli.skip_start {
        println!(
            "starting {} produce-nodes + 1 consume-node + 1 rebalancer (s3-prefix={s3_prefix})",
            cli.nodes,
        );
        start_nodes(&cli, &s3_prefix)?
    } else {
        vec![]
    };

    let s3_config = S3Config {
        endpoint_url: Some(cli.s3_endpoint.clone()),
        bucket: cli.s3_bucket.clone(),
        region: "us-east-1".to_string(),
    };

    // -- Setup: ensure bucket, register partitions, wait for leaders --
    let setup_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("setup runtime")?;

    setup_rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let s3_client = s3::build_client(&s3_config).await;
                s3::ensure_bucket(&s3_client, &s3_config.bucket).await?;

                let registry = PartitionRegistry::new(
                    s3_client.clone(),
                    s3_config.bucket.clone(),
                    Some(s3_prefix.as_str()),
                );
                for p in 0..cli.partitions {
                    registry.add(&cli.topic, p).await?;
                }
                println!(
                    "registered {} partitions for topic '{}'",
                    cli.partitions, cli.topic,
                );

                println!("waiting for leaders...");
                wait_for_leaders(
                    &s3_client,
                    &s3_config.bucket,
                    &cli.topic,
                    cli.partitions,
                    Some(s3_prefix.as_str()),
                )
                .await?;
                println!("all partitions have leaders");

                Ok::<(), anyhow::Error>(())
            })
            .await
    })?;

    // -- Build payload template --
    let record_value: Vec<u8> = (0..cli.record_size).map(|_| rand::random::<u8>()).collect();
    let batch: Vec<Vec<u8>> = vec![record_value; cli.batch_size];

    // -- Run load test --
    let stop = Arc::new(AtomicBool::new(false));

    println!(
        "starting load test: {} workers, {}s duration",
        cli.workers, cli.duration,
    );

    let wall_start = Instant::now();

    // Timer thread
    let stop_timer = stop.clone();
    let duration = cli.duration;
    let timer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(duration));
        stop_timer.store(true, Ordering::Relaxed);
    });

    // Worker threads
    let handles: Vec<_> = (0..cli.workers)
        .map(|i| {
            let partition = (i as u32) % cli.partitions;
            let topic = cli.topic.clone();
            let s3_config = s3_config.clone();
            let le_prefix = Some(s3_prefix.clone());
            let batch = batch.clone();
            let stop = stop.clone();
            std::thread::Builder::new()
                .name(format!("worker-{i}"))
                .spawn(move || run_worker(i, partition, topic, s3_config, le_prefix, batch, stop))
                .expect("spawn worker thread")
        })
        .collect();

    // Join
    let mut all_metrics = Vec::with_capacity(cli.workers);
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(m) => all_metrics.push(m),
            Err(_) => {
                eprintln!("worker {i} panicked");
                all_metrics.push(WorkerMetrics {
                    records_produced: 0,
                    bytes_produced: 0,
                    errors: 0,
                    latencies: vec![],
                });
            }
        }
    }
    timer.join().ok();

    let actual_duration = wall_start.elapsed();
    print_metrics(&cli, all_metrics, actual_duration);

    // -- Cleanup --
    if !children.is_empty() {
        println!("\nshutting down cluster...");
        kill_children(&mut children);
    }

    Ok(())
}
