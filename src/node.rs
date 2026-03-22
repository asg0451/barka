use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::jepsen_gateway;
use crate::log::partition::Partition;
use crate::log::record::Record;
use crate::rpc::server::serve_rpc;

/// One record to append (shared by Cap'n Proto and the Jepsen JSON gateway).
#[derive(Debug)]
pub struct ProduceInput {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
}

/// (topic, partition_id)
pub type TopicPartition = (String, u32);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub rpc_addr: SocketAddr,
    pub jepsen_gateway_addr: SocketAddr,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            rpc_addr: "127.0.0.1:9292".parse().unwrap(),
            jepsen_gateway_addr: "127.0.0.1:9293".parse().unwrap(),
        }
    }
}

impl NodeConfig {
    pub fn from_env() -> Self {
        let node_id: u64 = std::env::var("BARKA_NODE_ID")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let rpc_port: u16 = std::env::var("BARKA_RPC_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9292);
        let jepsen_gateway_port: u16 = std::env::var("BARKA_JEPSEN_GATEWAY_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9293);
        Self {
            node_id,
            rpc_addr: ([127, 0, 0, 1], rpc_port).into(),
            jepsen_gateway_addr: ([127, 0, 0, 1], jepsen_gateway_port).into(),
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub config: NodeConfig,
    pub partitions: Arc<Mutex<HashMap<TopicPartition, Partition>>>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
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

        tokio::select! {
            c = tokio::task::spawn_blocking(move || jepsen_gateway_handle.join()) => {
                c.unwrap().unwrap()?;
            }
            _ = tokio::task::spawn_blocking(move || rpc_handle.join().unwrap()) => {},
        };
        Ok(())
    }

    /// Append records to `(topic, partition)`; returns base offset of the first record,
    /// or `0` if `records` is empty (matches Cap'n Proto response shape).
    pub fn produce_records(
        &self,
        topic: String,
        partition: u32,
        records: impl IntoIterator<Item = ProduceInput>,
    ) -> u64 {
        let tp = (topic, partition);
        let mut partitions = self.partitions.lock().unwrap();
        let part = partitions
            .entry(tp)
            .or_insert_with(|| Partition::new(partition));

        let mut base_offset = None;
        for r in records {
            let off = part.append(r.key, r.value, r.timestamp);
            if base_offset.is_none() {
                base_offset = Some(off);
            }
        }
        base_offset.unwrap_or(0)
    }

    /// Read up to `max` records from `offset` onward.
    pub fn consume_records(
        &self,
        topic: String,
        partition: u32,
        offset: u64,
        max: u32,
    ) -> Vec<Record> {
        let tp = (topic, partition);
        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&tp)
            .map(|p| p.read(offset, max))
            .unwrap_or_default()
    }
}
