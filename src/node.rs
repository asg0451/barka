use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::jepsen_gateway;
use crate::log::partition::Partition;
use crate::producer::PartitionProducer;
use crate::rpc::barka_capnp::{consume_request, consume_response, produce_request};
use crate::rpc::server::serve_rpc;
use crate::s3::S3Config;

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

#[derive(Clone)]
pub struct Node {
    pub config: NodeConfig,
    pub partitions: Arc<Mutex<HashMap<TopicPartition, Partition>>>,
    pub producer: Arc<PartitionProducer>,
}

impl Node {
    pub async fn new(config: NodeConfig, s3_config: &S3Config) -> Self {
        // TODO: this is a hack around not using up to date sequence numbers yet.
        let prefix = format!(
            "data/test/0/{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let producer = PartitionProducer::new(s3_config, prefix).await;
        Self {
            config,
            partitions: Arc::new(Mutex::new(HashMap::new())),
            producer,
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
