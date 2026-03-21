use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::info;

use crate::log::partition::Partition;
use crate::rpc::server::serve_rpc;

/// (topic, partition_id)
pub type TopicPartition = (String, u32);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub rpc_addr: SocketAddr,
    pub control_addr: SocketAddr,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            rpc_addr: "127.0.0.1:9292".parse().unwrap(),
            control_addr: "127.0.0.1:9293".parse().unwrap(),
        }
    }
}

pub struct NodeInner {
    pub config: NodeConfig,
    pub partitions: Mutex<HashMap<TopicPartition, Partition>>,
}

pub struct Node {
    inner: Arc<NodeInner>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                config,
                partitions: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn serve(self) -> crate::error::Result<()> {
        let rpc_addr = self.inner.config.rpc_addr;
        let control_addr = self.inner.config.control_addr;

        let rpc_inner = self.inner.clone();
        let control_inner = self.inner.clone();

        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_rpc(rpc_inner, rpc_addr))
        });

        let control_handle = tokio::spawn(async move {
            serve_control(control_inner, control_addr).await
        });

        tokio::select! {
            result = control_handle => result.unwrap()?,
            _ = tokio::task::spawn_blocking(move || rpc_handle.join().unwrap()) => {},
        };
        Ok(())
    }
}

/// JSON-over-TCP control API for Jepsen.
///
/// Protocol: newline-delimited JSON.
///   Request:  {"op":"produce","topic":"events","partition":0,"value":"hello"}
///             {"op":"consume","topic":"events","partition":0,"offset":0,"max":10}
///   Response: {"ok":true,"offset":0}
///             {"ok":true,"values":["hello"]}
///             {"ok":false,"error":"..."}
async fn serve_control(inner: Arc<NodeInner>, addr: SocketAddr) -> crate::error::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "control api listening");

    loop {
        let (stream, remote) = listener.accept().await?;
        info!(%remote, "control connection");
        let inner = inner.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let response = handle_control_request(&inner, &line);
                let mut out = serde_json::to_string(&response).unwrap();
                out.push('\n');
                if writer.write_all(out.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}

#[derive(Deserialize)]
struct ControlRequest {
    op: String,
    #[serde(default = "default_topic")]
    topic: String,
    #[serde(default)]
    partition: u32,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_max")]
    max: u32,
}

fn default_topic() -> String {
    "default".into()
}

fn default_max() -> u32 {
    10
}

#[derive(Serialize)]
struct ControlResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn handle_control_request(inner: &NodeInner, line: &str) -> ControlResponse {
    let req: ControlRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return ControlResponse {
                ok: false,
                offset: None,
                values: None,
                error: Some(format!("bad request: {e}")),
            };
        }
    };

    let tp = (req.topic.clone(), req.partition);

    match req.op.as_str() {
        "produce" => {
            let value = req.value.unwrap_or_default();
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let mut partitions = inner.partitions.lock().unwrap();
            let part = partitions
                .entry(tp)
                .or_insert_with(|| Partition::new(req.partition));
            let offset = part.append(None, value.into_bytes(), ts);
            ControlResponse {
                ok: true,
                offset: Some(offset),
                values: None,
                error: None,
            }
        }
        "consume" => {
            let partitions = inner.partitions.lock().unwrap();
            let records = partitions
                .get(&tp)
                .map(|p| p.read(req.offset, req.max))
                .unwrap_or_default();
            let values: Vec<String> = records
                .iter()
                .map(|r| String::from_utf8_lossy(&r.value).to_string())
                .collect();
            ControlResponse {
                ok: true,
                offset: None,
                values: Some(values),
                error: None,
            }
        }
        other => ControlResponse {
            ok: false,
            offset: None,
            values: None,
            error: Some(format!("unknown op: {other}")),
        },
    }
}
