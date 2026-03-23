use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::jepsen_gateway;
use crate::leader_election::{LeaderElection, LeaderElectionConfig, TryBecomeLeaderResult};
use crate::log::partition::Partition;
use crate::producer::PartitionProducer;
use crate::rpc::barka_capnp::{consume_request, consume_response, produce_request};
use crate::rpc::server::serve_rpc;
use crate::s3::S3Config;

/// (topic, partition_id)
pub type TopicPartition = (String, u32);

/// Overrides [`PartitionProducer`](crate::producer::PartitionProducer) batch limits and flush linger.
///
/// Serialized as `linger_ms` (milliseconds) so configs stay JSON-friendly. When omitted from
/// [`NodeConfig`], [`PartitionProducer::new`](crate::producer::PartitionProducer::new) defaults apply.
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

fn default_leader_election_poll_secs() -> u64 {
    3
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub rpc_addr: SocketAddr,
    pub jepsen_gateway_addr: SocketAddr,
    /// Optional path before [`Self::SEGMENT_PREFIX_TAIL`] (`data`). Segment keys use
    /// `{optional}/data/test/0` when set, or `data/test/0` when `None`. Trim slashes;
    /// empty string is treated as unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_limits: Option<ProducerBatchLimits>,
    /// Sleep between leader-election attempts in the run loop.
    #[serde(default = "default_leader_election_poll_secs")]
    pub leader_election_poll_secs: u64,
}

impl NodeConfig {
    pub const SEGMENT_PREFIX_TAIL: &'static str = "data";

    /// Full S3 prefix for segment objects under this node.
    pub fn segment_key_prefix(optional_leading: Option<&str>) -> String {
        let tail = Self::SEGMENT_PREFIX_TAIL;
        match optional_leading {
            None => tail.to_string(),
            Some(s) => {
                let s = s.trim_matches('/');
                if s.is_empty() {
                    tail.to_string()
                } else {
                    format!("{s}/{tail}")
                }
            }
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            rpc_addr: "127.0.0.1:9292".parse().unwrap(),
            jepsen_gateway_addr: "127.0.0.1:9293".parse().unwrap(),
            s3_prefix: None,
            producer_limits: None,
            leader_election_poll_secs: default_leader_election_poll_secs(),
        }
    }
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

#[derive(Clone)]
pub struct Node {
    pub config: NodeConfig,
    pub s3_config: S3Config,
    pub partitions: Arc<Mutex<HashMap<TopicPartition, Partition>>>,
    pub producer: Arc<PartitionProducer>,
    pub leadership: Arc<LeadershipState>,
}

impl Node {
    pub async fn new(config: NodeConfig, s3_config: &S3Config) -> anyhow::Result<Self> {
        let prefix = NodeConfig::segment_key_prefix(config.s3_prefix.as_deref());
        let partition_prefix = format!("{}/test/0", prefix);
        let leadership = Arc::new(LeadershipState::new());
        let producer = match config.producer_limits.as_ref() {
            Some(l) => {
                PartitionProducer::with_opts(
                    s3_config,
                    partition_prefix,
                    l.max_records,
                    l.max_bytes,
                    l.linger(),
                    Some(Arc::clone(&leadership)),
                )
                .await?
            }
            None => {
                PartitionProducer::new(s3_config, partition_prefix, Some(Arc::clone(&leadership)))
                    .await?
            }
        };
        Ok(Self {
            config,
            s3_config: s3_config.clone(),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            producer,
            leadership,
        })
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let rpc_addr = self.config.rpc_addr;
        let jepsen_gateway_addr = self.config.jepsen_gateway_addr;

        let rpc_node = self.clone();

        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_rpc(rpc_node, rpc_addr))
        });

        // Jepsen gateway is a Cap'n Proto client; same `!Send` / LocalSet constraints as `serve_rpc`.
        let jepsen_gateway_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(jepsen_gateway::serve(rpc_addr, jepsen_gateway_addr))
        });

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: self.config.node_id,
            namespace: "test-0".into(),
            s3_config: self.s3_config.clone(),
            validity_millis: None,
        })
        .await;
        let leadership = Arc::clone(&self.leadership);
        let producer = Arc::clone(&self.producer);
        let poll = Duration::from_secs(self.config.leader_election_poll_secs);
        let leader_loop_handle = tokio::spawn(run_leader_loop(le, leadership, producer, poll));

        tokio::select! {
            c = tokio::task::spawn_blocking(move || jepsen_gateway_handle.join()) => {
                c.unwrap().unwrap()?;
            }
            _ = tokio::task::spawn_blocking(move || rpc_handle.join().unwrap()) => {},
            r = leader_loop_handle => {
                r.unwrap()?;
            }
        };
        Ok(())
    }

    /// Apply a Cap'n Proto [`produce_request::Reader`]: read key/value as slices from the
    /// inbound message and copy once per field into partition storage.
    pub fn apply_produce_request(
        &self,
        request: produce_request::Reader<'_>,
    ) -> capnp::Result<u64> {
        let topic = request.get_topic()?.to_string()?;
        let partition = request.get_partition();
        let records = request.get_records()?;

        let tp = (topic, partition);
        let mut partitions = self.partitions.lock().unwrap();
        let part = partitions
            .entry(tp)
            .or_insert_with(|| Partition::new(partition));

        let mut base_offset = None;
        for record in records.iter() {
            let key_slice = record.get_key()?;
            let key = if key_slice.is_empty() {
                None
            } else {
                Some(key_slice.to_vec())
            };
            let value = record.get_value()?.to_vec();
            let ts = record.get_timestamp();
            let off = part.append(key, value, ts);
            if base_offset.is_none() {
                base_offset = Some(off);
            }
        }
        Ok(base_offset.unwrap_or(0))
    }

    /// Apply a Cap'n Proto [`consume_request::Reader`] and fill [`consume_response::Builder`].
    pub fn apply_consume_request(
        &self,
        request: consume_request::Reader<'_>,
        response: consume_response::Builder<'_>,
    ) -> capnp::Result<()> {
        let topic = request.get_topic()?.to_string()?;
        let partition = request.get_partition();
        let offset = request.get_offset();
        let max = request.get_max_records();

        let tp = (topic, partition);
        let partitions = self.partitions.lock().unwrap();
        let records = partitions
            .get(&tp)
            .map(|p| p.read(offset, max))
            .unwrap_or_default();

        let mut list = response.init_records(records.len() as u32);
        for (i, rec) in records.iter().enumerate() {
            let mut entry = list.reborrow().get(i as u32);
            if let Some(ref k) = rec.key {
                entry.set_key(k);
            }
            entry.set_value(&rec.value);
            entry.set_offset(rec.offset);
            entry.set_timestamp(rec.timestamp);
        }
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
    fn node_config_serde_round_trips_producer_limits() {
        let cfg = NodeConfig {
            producer_limits: Some(ProducerBatchLimits {
                max_records: 42,
                max_bytes: 99,
                linger_ms: 500,
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: NodeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.producer_limits, cfg.producer_limits);
    }
}
