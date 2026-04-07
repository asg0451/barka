#![recursion_limit = "256"]

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use barka::leader_election;
use barka::node::leader_namespace;
use barka::partition_registry::PartitionRegistry;
use barka::produce_router::ProduceRouter;
use barka::s3::{self, S3Config};
use clap::Parser;

const BASE_PRODUCE_PORT: u16 = 9292;
const CONSUME_PORT: u16 = 9392;

/// Default S3 object prefix and `--leader-election-prefix` for spawned nodes.
const DEFAULT_LOAD_PREFIX: &str = "barka-load";

/// Fixed samply `-o` path (current working directory when `barka-load` runs).
const PROFILE_OUTPUT_GZ: &str = "barka-load-profile.json.gz";

// ---------------------------------------------------------------------------
// Process-group cleanup: every child gets `process_group(0)` so its PGID
// equals its PID.  We record PGIDs here so a signal handler can SIGKILL all
// groups on Ctrl-C / SIGTERM — no orphans regardless of how we exit.
// ---------------------------------------------------------------------------
const MAX_CHILDREN: usize = 16;
#[allow(clippy::declare_interior_mutable_const)]
static CHILD_PGIDS: [AtomicU32; MAX_CHILDREN] = {
    const ZERO: AtomicU32 = AtomicU32::new(0);
    [ZERO; MAX_CHILDREN]
};
static CHILD_COUNT: AtomicUsize = AtomicUsize::new(0);

fn _register_child_pgid(pid: u32) {
    let idx = CHILD_COUNT.fetch_add(1, Ordering::Relaxed);
    if idx < MAX_CHILDREN {
        CHILD_PGIDS[idx].store(pid, Ordering::Relaxed);
    }
}

#[cfg(unix)]
#[allow(clippy::needless_range_loop)]
fn kill_all_child_groups() {
    let count = CHILD_COUNT.load(Ordering::Relaxed).min(MAX_CHILDREN);
    for i in 0..count {
        let pgid = CHILD_PGIDS[i].load(Ordering::Relaxed);
        if pgid > 0 {
            unsafe {
                libc::kill(-(pgid as libc::pid_t), libc::SIGKILL);
            }
        }
    }
}

#[cfg(unix)]
extern "C" fn cleanup_signal_handler(sig: libc::c_int) {
    kill_all_child_groups();
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

#[cfg(unix)]
fn install_signal_handlers() {
    unsafe {
        libc::signal(
            libc::SIGINT,
            cleanup_signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            cleanup_signal_handler as *const () as libc::sighandler_t,
        );
    }
}

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

    /// Concurrent in-flight produce calls per worker thread.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Number of produce nodes to start.
    #[arg(long, default_value_t = 3)]
    nodes: u32,

    /// Run produce-node 0 under `samply record` (writes `barka-load-profile.json.gz` in cwd).
    #[arg(long)]
    profile: bool,

    /// S3 endpoint URL.
    #[arg(long, default_value = "http://localhost:4566")]
    s3_endpoint: String,

    /// S3 bucket name.
    #[arg(long, default_value = "barka")]
    s3_bucket: String,

    /// Skip starting the cluster (use an already-running cluster on the same prefix).
    #[arg(long)]
    skip_start: bool,

    /// S3 data prefix and leader-election namespace override [default: barka-load].
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

/// Spawn a command, putting it in its own process group and registering it for
/// cleanup.  Every child must go through here so the signal handler can reap it.
fn _spawn_in_own_pgroup(cmd: &mut std::process::Command) -> Result<Child> {
    #[cfg(unix)]
    cmd.process_group(0);
    let child = cmd.spawn()?;
    _register_child_pgid(child.id());
    Ok(child)
}

fn start_nodes(
    cli: &Cli,
    s3_prefix: &str,
    _profile_gz: Option<&Path>,
) -> Result<(Vec<Child>, bool)> {
    let dir = bin_dir()?;
    let mut children = Vec::new();
    let samply_wrapped_produce = false;

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

    Ok((children, samply_wrapped_produce))
}

/// Stop the samply wrapper by terminating the profiled child underneath it.
///
/// samply ignores SIGTERM/SIGINT — it only saves the `-o` profile when
/// its child process exits.  We find the child (produce-node) via
/// `pgrep -P`, SIGTERM it, then wait for samply to flush and exit.
/// Falls back to SIGKILL of the process group.
fn terminate_samply_child(child: &mut Child, timeout: Duration) {
    #[cfg(unix)]
    {
        let samply_pid = child.id();
        let grandchildren = child_pids_of(samply_pid);
        if grandchildren.is_empty() {
            // samply already exited — group-kill to clean up any orphan.
            unsafe {
                libc::kill(-(samply_pid as libc::pid_t), libc::SIGKILL);
            }
            let _ = child.wait();
            return;
        }
        for &cpid in &grandchildren {
            unsafe {
                libc::kill(cpid as libc::pid_t, libc::SIGTERM);
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => {
                    if Instant::now() > deadline {
                        unsafe {
                            libc::kill(-(samply_pid as libc::pid_t), libc::SIGKILL);
                        }
                        let _ = child.wait();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    let _ = child.wait();
                    return;
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn child_pids_of(parent: u32) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .args(["-P", &parent.to_string()])
        .output()
        .ok()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter_map(|l| l.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn shutdown_cluster(children: &mut [Child], samply_wrapped_produce: bool) {
    // Orderly samply shutdown: SIGTERM produce-node → samply saves profile → exits.
    if samply_wrapped_produce {
        println!("stopping profiled produce-node (up to 15s for samply profile flush)...");
        terminate_samply_child(&mut children[0], Duration::from_secs(15));
    }
    // SIGTERM remaining children first so they can flush (dhat, etc.), then
    // wait briefly, then SIGKILL anything still alive.
    #[cfg(unix)]
    for (i, child) in children.iter_mut().enumerate() {
        if samply_wrapped_produce && i == 0 {
            continue;
        }
        unsafe {
            libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
        }
    }
    // Give children up to 3s to shut down gracefully.
    let deadline = Instant::now() + Duration::from_secs(3);
    for (i, child) in children.iter_mut().enumerate() {
        if samply_wrapped_produce && i == 0 {
            continue;
        }
        while Instant::now() < deadline {
            if let Ok(Some(_)) = child.try_wait() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // SIGKILL anything still alive.
    for (i, child) in children.iter_mut().enumerate() {
        if samply_wrapped_produce && i == 0 {
            continue;
        }
        let _ = child.kill();
    }
    for (i, child) in children.iter_mut().enumerate() {
        if samply_wrapped_produce && i == 0 {
            continue;
        }
        let _ = child.wait();
    }
    // Safety net: SIGKILL every child process group in case anything leaked.
    #[cfg(unix)]
    kill_all_child_groups();
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

/// Poll the stop flag, completing when it becomes true.
async fn poll_stop(stop: &AtomicBool) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    worker_id: usize,
    partition: u32,
    topic: String,
    s3_config: S3Config,
    le_prefix: Option<String>,
    batch: Vec<Vec<u8>>,
    stop: Arc<AtomicBool>,
    concurrency: usize,
) -> WorkerMetrics {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let batch_bytes: u64 = batch.iter().map(|v| v.len() as u64).sum();

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut handles = Vec::with_capacity(concurrency);
                for _ in 0..concurrency {
                    let topic = topic.clone();
                    let s3_config = s3_config.clone();
                    let le_prefix = le_prefix.clone();
                    let batch = batch.clone();
                    let stop = stop.clone();
                    handles.push(tokio::task::spawn_local(async move {
                        let mut router = ProduceRouter::new(&s3_config, le_prefix).await;
                        let mut m = WorkerMetrics {
                            records_produced: 0,
                            bytes_produced: 0,
                            errors: 0,
                            latencies: Vec::with_capacity(4096),
                        };
                        while !stop.load(Ordering::Relaxed) {
                            let start = Instant::now();
                            // Cancel in-flight produce when the stop flag is set
                            // so workers don't get stuck in retry backoff.
                            let result = tokio::select! {
                                biased;
                                r = router.produce(&topic, partition, batch.clone()) => Some(r),
                                _ = poll_stop(&stop) => None,
                            };
                            match result {
                                Some(Ok(records)) => {
                                    m.records_produced += records.len() as u64;
                                    m.bytes_produced += batch_bytes;
                                    m.latencies.push(start.elapsed());
                                }
                                Some(Err(e)) => {
                                    tracing::warn!(
                                        worker = worker_id,
                                        error = %e,
                                        "produce failed"
                                    );
                                    m.errors += 1;
                                }
                                None => break,
                            }
                        }
                        m
                    }));
                }

                let mut agg = WorkerMetrics {
                    records_produced: 0,
                    bytes_produced: 0,
                    errors: 0,
                    latencies: Vec::new(),
                };
                for handle in handles {
                    match handle.await {
                        Ok(m) => {
                            agg.records_produced += m.records_produced;
                            agg.bytes_produced += m.bytes_produced;
                            agg.errors += m.errors;
                            agg.latencies.extend(m.latencies);
                        }
                        Err(_) => agg.errors += 1,
                    }
                }
                agg
            })
            .await
    })
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
        "Workers:       {} x {} concurrency ({} connections)  Partitions: {}",
        cli.workers,
        cli.concurrency,
        cli.workers * cli.concurrency,
        cli.partitions,
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

    #[cfg(unix)]
    install_signal_handlers();

    let cli = Cli::parse();

    let s3_prefix = cli
        .s3_prefix
        .clone()
        .unwrap_or_else(|| DEFAULT_LOAD_PREFIX.to_string());

    let profile_gz: Option<&Path> = if cli.profile {
        Some(Path::new(PROFILE_OUTPUT_GZ))
    } else {
        None
    };

    // -- Start cluster --
    let (mut children, samply_wrapped_produce) = if !cli.skip_start {
        println!(
            "starting {} produce-nodes + 1 consume-node + 1 rebalancer (s3-prefix={s3_prefix})",
            cli.nodes,
        );
        start_nodes(&cli, &s3_prefix, profile_gz)?
    } else {
        (vec![], false)
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

    let concurrency = cli.concurrency;
    println!(
        "starting load test: {} workers x {} concurrency, {}s duration",
        cli.workers, concurrency, cli.duration,
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
                .spawn(move || {
                    run_worker(
                        i,
                        partition,
                        topic,
                        s3_config,
                        le_prefix,
                        batch,
                        stop,
                        concurrency,
                    )
                })
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
        shutdown_cluster(&mut children, samply_wrapped_produce);
    }

    Ok(())
}
