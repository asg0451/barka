use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::leader_election::{LeaderElection, LeaderElectionConfig, TryBecomeLeaderResult};
use crate::node::{leader_namespace, partition_data_prefix, segment_key_prefix};
use crate::producer::PartitionProducer;
use crate::rpc::server::serve_produce_rpc;
use crate::s3::S3Config;

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
    pub topics: Vec<TopicConfig>,
}

impl Default for ProduceNodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            rpc_addr: "127.0.0.1:9292".parse().unwrap(),
            s3_prefix: None,
            producer_limits: None,
            leader_election_poll_secs: 3,
            topics: vec![TopicConfig {
                topic: "default".into(),
                partitions: 1,
            }],
        }
    }
}

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

    pub fn set_leader(&self, valid_until_ms: u64, epoch: u64) {
        let mut inner = self.inner.write().unwrap();
        if !inner.is_leader {
            tracing::info!(epoch, valid_until_ms, "acquired leadership");
        }
        inner.is_leader = true;
        inner.valid_until_ms = valid_until_ms;
        inner.epoch = epoch;
    }

    pub fn set_not_leader(&self) {
        let mut inner = self.inner.write().unwrap();
        if inner.is_leader {
            tracing::info!(epoch = inner.epoch, "lost leadership");
        }
        inner.is_leader = false;
        inner.valid_until_ms = 0;
    }
}

pub type PartitionMap = Arc<HashMap<(String, u32), PartitionProduceState>>;

pub struct ProduceNode {
    pub config: ProduceNodeConfig,
    pub s3_config: S3Config,
    pub partitions: PartitionMap,
}

impl ProduceNode {
    pub async fn new(config: ProduceNodeConfig, s3_config: &S3Config) -> anyhow::Result<Self> {
        let base_prefix = segment_key_prefix(config.s3_prefix.as_deref());
        let mut partitions = HashMap::new();

        for tc in &config.topics {
            for p in 0..tc.partitions {
                let pp = partition_data_prefix(&base_prefix, &tc.topic, p);
                let leadership = Arc::new(LeadershipState::new());
                let producer = match config.producer_limits.as_ref() {
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
                tracing::info!(
                    topic = %tc.topic,
                    partition = p,
                    "initialized partition producer",
                );
                partitions.insert(
                    (tc.topic.clone(), p),
                    PartitionProduceState {
                        producer,
                        leadership,
                    },
                );
            }
        }

        Ok(Self {
            config,
            s3_config: s3_config.clone(),
            partitions: Arc::new(partitions),
        })
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let rpc_addr = self.config.rpc_addr;
        let partitions = Arc::clone(&self.partitions);

        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_produce_rpc(partitions, rpc_addr))
        });

        let poll = Duration::from_secs(self.config.leader_election_poll_secs);
        let mut leader_handles = Vec::new();

        for ((topic, partition), state) in self.partitions.iter() {
            let ns = leader_namespace(topic, *partition);
            let le = LeaderElection::new(LeaderElectionConfig {
                node_id: self.config.node_id,
                namespace: ns,
                s3_config: self.s3_config.clone(),
                validity_millis: None,
            })
            .await;
            let leadership = Arc::clone(&state.leadership);
            let producer = Arc::clone(&state.producer);
            leader_handles.push(tokio::spawn(run_leader_loop(
                le, leadership, producer, poll,
            )));
        }

        tokio::select! {
            _ = tokio::task::spawn_blocking(move || rpc_handle.join().unwrap()) => {},
            r = futures::future::try_join_all(leader_handles) => {
                for result in r? {
                    result?;
                }
            }
        };
        Ok(())
    }
}

async fn run_leader_loop(
    le: LeaderElection,
    state: Arc<LeadershipState>,
    producer: Arc<PartitionProducer>,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    let mut prev_epoch: Option<u64> = None;
    loop {
        match le.try_become_leader().await {
            Ok(TryBecomeLeaderResult::Leader(info)) => {
                let epoch = info.epoch.as_u64();
                if prev_epoch.is_some_and(|e| e != epoch) {
                    producer.cancel_pending();
                }
                state.set_leader(info.valid_until_ms, epoch);
                prev_epoch = Some(epoch);
            }
            Ok(TryBecomeLeaderResult::NotLeader) => {
                if prev_epoch.is_some() {
                    producer.cancel_pending();
                }
                state.set_not_leader();
                prev_epoch = None;
            }
            Err(e) => {
                tracing::warn!(error = %e, "leader election error");
                if prev_epoch.is_some() {
                    producer.cancel_pending();
                }
                state.set_not_leader();
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
