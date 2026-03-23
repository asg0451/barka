use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::leader_election::{LeaderElection, LeaderElectionConfig, TryBecomeLeaderResult};
use crate::node::{leader_namespace, partition_data_prefix, segment_key_prefix};
use crate::partition_registry::PartitionRegistry;
use crate::producer::PartitionProducer;
use crate::rpc::server::serve_produce_rpc;
use crate::s3::{self, S3Config};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ProducerBatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub linger_ms: u64,
}

impl ProducerBatchLimits {
    pub fn linger(&self) -> Duration {
        Duration::from_millis(self.linger_ms)
    }
}

#[derive(Clone, Debug)]
pub struct TopicConfig {
    pub topic: String,
    pub partitions: u32,
}

#[derive(Clone, Debug)]
pub struct ProduceNodeConfig {
    pub node_id: u64,
    pub rpc_addr: SocketAddr,
    pub s3_prefix: Option<String>,
    pub producer_limits: Option<ProducerBatchLimits>,
    pub leader_election_poll_secs: u64,
    /// Path segment in S3 before `lock/` and the topic-partition namespace.
    pub leader_election_prefix: Option<String>,
    pub topics: Vec<TopicConfig>,
    /// How long to back off from leader election after an abdication (seconds).
    pub abdication_cooldown_secs: u64,
}

impl Default for ProduceNodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            rpc_addr: "127.0.0.1:9292".parse().unwrap(),
            s3_prefix: None,
            producer_limits: None,
            leader_election_poll_secs: 3,
            leader_election_prefix: None,
            topics: vec![TopicConfig {
                topic: "default".into(),
                partitions: 1,
            }],
            abdication_cooldown_secs: 60,
        }
    }
}

#[derive(Clone)]
pub struct PartitionProduceState {
    pub producer: Arc<PartitionProducer>,
    pub leadership: Arc<LeadershipState>,
}

#[derive(Debug, Clone)]
struct LeadershipInner {
    is_leader: bool,
    valid_until_ms: u64,
    epoch: u64,
}

#[derive(Debug)]
pub struct LeadershipState {
    inner: RwLock<LeadershipInner>,
    cooldown_until_ms: AtomicU64,
}

impl Default for LeadershipState {
    fn default() -> Self {
        Self::new()
    }
}

impl LeadershipState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(LeadershipInner {
                is_leader: false,
                valid_until_ms: 0,
                epoch: 0,
            }),
            cooldown_until_ms: AtomicU64::new(0),
        }
    }

    /// Returns `Some(epoch)` if this node is leader and the lease hasn't expired.
    pub fn check_leader(&self) -> Option<u64> {
        let inner = self.inner.read().unwrap();
        if !inner.is_leader {
            return None;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        if now_ms < inner.valid_until_ms {
            Some(inner.epoch)
        } else {
            None
        }
    }

    /// Returns `true` if this call transitioned from not-leader to leader.
    pub fn set_leader(&self, valid_until_ms: u64, epoch: u64) -> bool {
        let mut inner = self.inner.write().unwrap();
        let was_follower = !inner.is_leader;
        inner.is_leader = true;
        inner.valid_until_ms = valid_until_ms;
        inner.epoch = epoch;
        was_follower
    }

    /// Returns the epoch we lost if this call transitioned from leader to not-leader.
    pub fn set_not_leader(&self) -> Option<u64> {
        let mut inner = self.inner.write().unwrap();
        let lost_epoch = inner.is_leader.then_some(inner.epoch);
        inner.is_leader = false;
        inner.valid_until_ms = 0;
        lost_epoch
    }

    /// Prevent this node from competing for leadership of this partition until
    /// `duration` has elapsed.
    pub fn set_cooldown(&self, duration: Duration) {
        let until_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + duration.as_millis() as u64;
        self.cooldown_until_ms.store(until_ms, Ordering::Release);
    }

    /// Returns `true` if this partition is in a cooldown period.
    pub fn in_cooldown(&self) -> bool {
        let until = self.cooldown_until_ms.load(Ordering::Acquire);
        if until == 0 {
            return false;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now_ms < until
    }
}

pub type PartitionMap = Arc<RwLock<HashMap<(String, u32), PartitionProduceState>>>;

pub struct ProduceNode {
    pub config: ProduceNodeConfig,
    pub s3_config: S3Config,
    pub partitions: PartitionMap,
}

impl ProduceNode {
    pub async fn new(config: ProduceNodeConfig, s3_config: &S3Config) -> anyhow::Result<Self> {
        let base_prefix = segment_key_prefix(config.s3_prefix.as_deref());
        let s3_client = s3::build_client(s3_config).await;
        let registry = PartitionRegistry::new(
            s3_client,
            s3_config.bucket.clone(),
            config.leader_election_prefix.as_deref(),
        );

        // Seed the registry from --barka-topics (best-effort, idempotent).
        for tc in &config.topics {
            for p in 0..tc.partitions {
                if let Err(e) = registry.add(&tc.topic, p).await {
                    tracing::warn!(topic = %tc.topic, partition = p, error = %e, "failed to seed registry");
                }
            }
        }

        // Read the registry to discover all partitions (including ones seeded
        // by other nodes).
        let entries = registry.list().await?;
        let init_futs: Vec<_> = entries
            .iter()
            .map(|entry| {
                init_partition_state(
                    s3_config,
                    &base_prefix,
                    &entry.topic,
                    entry.partition,
                    config.producer_limits.as_ref(),
                )
            })
            .collect();
        let states = futures::future::try_join_all(init_futs).await?;
        let partitions: HashMap<_, _> = entries
            .into_iter()
            .zip(states)
            .map(|(entry, state)| ((entry.topic, entry.partition), state))
            .collect();

        Ok(Self {
            config,
            s3_config: s3_config.clone(),
            partitions: Arc::new(RwLock::new(partitions)),
        })
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let rpc_addr = self.config.rpc_addr;
        let partitions = Arc::clone(&self.partitions);

        let abdication_cooldown = Duration::from_secs(self.config.abdication_cooldown_secs);
        let node_id = self.config.node_id;
        let s3_config = self.s3_config.clone();
        let le_prefix = self.config.leader_election_prefix.clone();
        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_produce_rpc(
                partitions,
                rpc_addr,
                abdication_cooldown,
                node_id,
                s3_config,
                le_prefix,
            ))
        });

        let le_poll = Duration::from_secs(self.config.leader_election_poll_secs);

        // Start LE loops for all initial partitions, storing handles by key.
        let le_init_futs: Vec<_> = {
            let map = self.partitions.read().unwrap();
            map.iter()
                .map(|((topic, partition), state)| {
                    let key = (topic.clone(), *partition);
                    let namespace = leader_namespace(topic, *partition);
                    let span_prefix =
                        le_span_prefix(self.config.leader_election_prefix.as_deref(), &namespace);
                    let cfg = LeaderElectionConfig {
                        node_id: self.config.node_id,
                        addr: self.config.rpc_addr,
                        namespace,
                        leader_election_prefix: self.config.leader_election_prefix.clone(),
                        s3_config: self.s3_config.clone(),
                        validity_millis: None,
                    };
                    let leadership = Arc::clone(&state.leadership);
                    let producer = Arc::clone(&state.producer);
                    async move {
                        let le = LeaderElection::new(cfg).await;
                        (key, le, leadership, producer, span_prefix)
                    }
                })
                .collect()
        }; // RwLock guard dropped here, before the await

        let mut le_handles: HashMap<(String, u32), JoinHandle<anyhow::Result<()>>> = HashMap::new();
        let inited = futures::future::join_all(le_init_futs).await;
        for (key, le, leadership, producer, span_prefix) in inited {
            let handle = tokio::spawn(run_leader_loop(
                le,
                leadership,
                producer,
                le_poll,
                span_prefix,
            ));
            le_handles.insert(key, handle);
        }

        // Registry poller: discovers new partitions, tears down removed ones.
        let poller_cfg = RegistryPollerConfig {
            partitions: Arc::clone(&self.partitions),
            s3_config: self.s3_config.clone(),
            base_prefix: segment_key_prefix(self.config.s3_prefix.as_deref()),
            limits: self.config.producer_limits.clone(),
            le_prefix: self.config.leader_election_prefix.clone(),
            node_id: self.config.node_id,
            rpc_addr: self.config.rpc_addr,
            le_poll,
        };
        let registry_poller = tokio::spawn(async move {
            let s3_client = s3::build_client(&poller_cfg.s3_config).await;
            let registry = PartitionRegistry::new(
                s3_client,
                poller_cfg.s3_config.bucket.clone(),
                poller_cfg.le_prefix.as_deref(),
            );
            run_registry_poller(registry, poller_cfg, le_handles).await
        });

        tokio::select! {
            _ = tokio::task::spawn_blocking(move || rpc_handle.join().unwrap()) => {},
            r = registry_poller => { r??; },
        };
        Ok(())
    }
}

async fn init_partition_state(
    s3_config: &S3Config,
    base_prefix: &str,
    topic: &str,
    partition: u32,
    limits: Option<&ProducerBatchLimits>,
) -> anyhow::Result<PartitionProduceState> {
    let pp = partition_data_prefix(base_prefix, topic, partition);
    let leadership = Arc::new(LeadershipState::new());
    let producer = match limits {
        Some(l) => {
            PartitionProducer::with_opts(
                s3_config,
                pp,
                l.max_records,
                l.max_bytes,
                l.linger(),
                Arc::clone(&leadership),
            )
            .await?
        }
        None => PartitionProducer::new(s3_config, pp, Arc::clone(&leadership)).await?,
    };
    tracing::info!(topic, partition, "initialized partition producer");
    Ok(PartitionProduceState {
        producer,
        leadership,
    })
}

fn le_span_prefix(leader_election_prefix: Option<&str>, namespace: &str) -> String {
    match leader_election_prefix {
        Some(prefix) => format!("{}/{}", prefix.trim_matches('/'), namespace),
        None => namespace.to_string(),
    }
}

struct RegistryPollerConfig {
    partitions: PartitionMap,
    s3_config: S3Config,
    base_prefix: String,
    limits: Option<ProducerBatchLimits>,
    le_prefix: Option<String>,
    node_id: u64,
    rpc_addr: SocketAddr,
    le_poll: Duration,
}

async fn run_registry_poller(
    registry: PartitionRegistry,
    cfg: RegistryPollerConfig,
    mut le_handles: HashMap<(String, u32), JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    // Poll slightly faster than leader election so new partitions are picked up promptly.
    let registry_poll = cfg.le_poll.min(Duration::from_secs(2));

    loop {
        tokio::time::sleep(registry_poll).await;

        let entries = match registry.list().await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "registry poll failed");
                continue;
            }
        };

        let registry_set: HashSet<(String, u32)> = entries
            .iter()
            .map(|e| (e.topic.clone(), e.partition))
            .collect();

        let local_set: HashSet<(String, u32)> = {
            let map = cfg.partitions.read().unwrap();
            map.keys().cloned().collect()
        };

        // --- Additions ---
        for entry in &entries {
            let key = (entry.topic.clone(), entry.partition);
            if local_set.contains(&key) {
                continue;
            }
            tracing::info!(topic = %entry.topic, partition = entry.partition, "registry: creating new partition");

            let state = match init_partition_state(
                &cfg.s3_config,
                &cfg.base_prefix,
                &entry.topic,
                entry.partition,
                cfg.limits.as_ref(),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        topic = %entry.topic, partition = entry.partition,
                        error = %e, "failed to create partition from registry"
                    );
                    continue;
                }
            };

            // Immediately attempt leader election so the partition is usable fast.
            let namespace = leader_namespace(&entry.topic, entry.partition);
            let le_cfg = LeaderElectionConfig {
                node_id: cfg.node_id,
                addr: cfg.rpc_addr,
                namespace: namespace.clone(),
                leader_election_prefix: cfg.le_prefix.clone(),
                s3_config: cfg.s3_config.clone(),
                validity_millis: None,
            };
            let le = LeaderElection::new(le_cfg).await;
            match le.try_become_leader().await {
                Ok(TryBecomeLeaderResult::Leader(info)) => {
                    state
                        .leadership
                        .set_leader(info.valid_until_ms, info.epoch.as_u64());
                    tracing::info!(
                        topic = %entry.topic, partition = entry.partition,
                        epoch = info.epoch.as_u64(), "auto-created partition, acquired leadership"
                    );
                }
                Ok(TryBecomeLeaderResult::NotLeader) => {
                    tracing::info!(
                        topic = %entry.topic, partition = entry.partition,
                        "auto-created partition, another node is leader"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %entry.topic, partition = entry.partition,
                        error = %e, "initial leader election failed for auto-created partition"
                    );
                }
            }

            {
                let mut map = cfg.partitions.write().unwrap();
                map.insert(key.clone(), state.clone());
            }

            let span_prefix =
                le_span_prefix(cfg.le_prefix.as_deref(), &leader_namespace(&key.0, key.1));
            let handle = tokio::spawn(run_leader_loop(
                le,
                state.leadership,
                state.producer,
                cfg.le_poll,
                span_prefix,
            ));
            le_handles.insert(key, handle);
        }

        // --- Removals ---
        for key in &local_set {
            if registry_set.contains(key) {
                continue;
            }
            tracing::info!(topic = %key.0, partition = key.1, "registry: removing partition");

            if let Some(handle) = le_handles.remove(key) {
                handle.abort();
            }

            let state = {
                let mut map = cfg.partitions.write().unwrap();
                map.remove(key)
            };
            if let Some(state) = state {
                if let Some(epoch) = state.leadership.set_not_leader() {
                    tracing::info!(topic = %key.0, partition = key.1, epoch, "abdicated leadership (partition removed from registry)");
                }
                state.producer.cancel_pending();
            }
        }

        // Reap crashed LE loops — remove from both handle map and partition
        // map so the next poll cycle re-creates the partition cleanly.
        le_handles.retain(|key, handle| {
            if handle.is_finished() {
                tracing::error!(topic = %key.0, partition = key.1, "leader election loop exited unexpectedly, removing partition for re-creation");
                let state = {
                    let mut map = cfg.partitions.write().unwrap();
                    map.remove(key)
                };
                if let Some(state) = state {
                    state.leadership.set_not_leader();
                    state.producer.cancel_pending();
                }
                false
            } else {
                true
            }
        });
    }
}

#[tracing::instrument(level = "debug", skip_all, fields(prefix = %span_prefix), err)]
async fn run_leader_loop(
    le: LeaderElection,
    state: Arc<LeadershipState>,
    producer: Arc<PartitionProducer>,
    poll_interval: Duration,
    span_prefix: String,
) -> anyhow::Result<()> {
    let mut prev_epoch: Option<u64> = None;
    loop {
        if state.in_cooldown() {
            tracing::debug!(prefix = %span_prefix, "skipping leader election (cooldown active)");
            tokio::time::sleep(poll_interval).await;
            continue;
        }
        match le.try_become_leader().await {
            Ok(TryBecomeLeaderResult::Leader(info)) => {
                let epoch = info.epoch.as_u64();
                if prev_epoch.is_some_and(|e| e != epoch) {
                    producer.cancel_pending();
                }
                if state.set_leader(info.valid_until_ms, epoch) {
                    tracing::info!(epoch, valid_until_ms = info.valid_until_ms, prefix = %span_prefix, "acquired leadership");
                }
                prev_epoch = Some(epoch);
            }
            Ok(TryBecomeLeaderResult::NotLeader) => {
                if prev_epoch.is_some() {
                    producer.cancel_pending();
                }
                if let Some(epoch) = state.set_not_leader() {
                    tracing::info!(epoch, prefix = %span_prefix, "lost leadership");
                }
                prev_epoch = None;
            }
            Err(e) => {
                tracing::warn!(error = %e, prefix = %span_prefix, "leader election error");
                if prev_epoch.is_some() {
                    producer.cancel_pending();
                }
                if let Some(epoch) = state.set_not_leader() {
                    tracing::info!(epoch, prefix = %span_prefix, "lost leadership");
                }
                prev_epoch = None;
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_batch_limits_serde_round_trips() {
        let limits = ProducerBatchLimits {
            max_records: 42,
            max_bytes: 99,
            linger_ms: 500,
        };
        let json = serde_json::to_string(&limits).unwrap();
        let back: ProducerBatchLimits = serde_json::from_str(&json).unwrap();
        assert_eq!(back, limits);
    }
}
