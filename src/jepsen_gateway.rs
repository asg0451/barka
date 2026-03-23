//! Jepsen API gateway: newline-delimited JSON over TCP → local Cap'n Proto RPC.
//!
//! Translates each JSON line into [`crate::rpc::client::BarkaClient`] calls to the
//! node's RPC address. All data path logic lives in `serve_rpc` + [`crate::node::Node`].
//!
//! Runs on a dedicated `current_thread` Tokio runtime with a top-level [`LocalSet`]
//! (same constraints as the Cap'n Proto RPC stack: `!Send` futures, `spawn_local`).

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::rpc::client::BarkaClient;

/// Max time for a single produce/consume RPC before returning `{"ok":false,"error":"..."}`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Protocol: newline-delimited JSON.
///
/// Request:  `{"op":"produce","topic":"events","partition":0,"value":"hello"}`
///           `{"op":"consume","topic":"events","partition":0,"offset":0,"max":10}`
/// Response: `{"ok":true,"offset":0}`
///           `{"ok":true,"values":["hello"]}`
///           `{"ok":false,"error":"..."}`
///
/// Must be driven by a **`current_thread`** runtime (see [`crate::node::Node::serve`]).
pub async fn serve(rpc_addr: SocketAddr, listen_addr: SocketAddr) -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let listener = TcpListener::bind(listen_addr).await?;
            info!(%listen_addr, %rpc_addr, "jepsen gateway listening (capnp client to local rpc)");

            loop {
                let (stream, remote) = listener.accept().await?;
                info!(%remote, "jepsen gateway connection");

                tokio::task::spawn_local(async move {
                    if let Err(e) = run_session(stream, rpc_addr).await {
                        error!(%e, "jepsen gateway session ended with error");
                    }
                });
            }
        })
        .await
}

async fn run_session(stream: tokio::net::TcpStream, rpc_addr: SocketAddr) -> anyhow::Result<()> {
    let client = connect_barka_with_retry(rpc_addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = handle_json_line(&client, &line).await;
        let mut out = serde_json::to_string(&response).unwrap();
        out.push('\n');
        if writer.write_all(out.as_bytes()).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn connect_barka_with_retry(rpc_addr: SocketAddr) -> anyhow::Result<BarkaClient> {
    const MAX_ATTEMPTS: u32 = 100;
    const DELAY: Duration = Duration::from_millis(25);

    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match BarkaClient::connect(rpc_addr).await {
            Ok(c) => {
                if attempt > 0 {
                    info!(%rpc_addr, attempt, "jepsen gateway: connected to capnp rpc");
                }
                return Ok(c);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(DELAY).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("connect failed")))
}

#[derive(Deserialize)]
struct JsonlRequest {
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
struct JsonlResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn handle_json_line(client: &BarkaClient, line: &str) -> JsonlResponse {
    let req: JsonlRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return JsonlResponse {
                ok: false,
                offset: None,
                values: None,
                error: Some(format!("bad request: {e}")),
            };
        }
    };

    match req.op.as_str() {
        "produce" => {
            let value = req.value.unwrap_or_default().into_bytes();
            match tokio::time::timeout(
                REQUEST_TIMEOUT,
                client.produce(&req.topic, req.partition, vec![value]),
            )
            .await
            {
                Ok(Ok(records)) => JsonlResponse {
                    ok: true,
                    offset: records.first().map(|r| r.offset),
                    values: None,
                    error: None,
                },
                Ok(Err(e)) => JsonlResponse {
                    ok: false,
                    offset: None,
                    values: None,
                    error: Some(e.to_string()),
                },
                Err(_elapsed) => {
                    warn!(
                        topic = %req.topic,
                        partition = req.partition,
                        ?REQUEST_TIMEOUT,
                        "jepsen gateway produce timed out"
                    );
                    JsonlResponse {
                        ok: false,
                        offset: None,
                        values: None,
                        error: Some(format!("request timed out after {:?}", REQUEST_TIMEOUT)),
                    }
                }
            }
        }
        "consume" => {
            match tokio::time::timeout(
                REQUEST_TIMEOUT,
                client.consume(&req.topic, req.partition, req.offset, req.max),
            )
            .await
            {
                Ok(Ok(records)) => {
                    let values: Vec<String> = records
                        .iter()
                        .map(|r| String::from_utf8_lossy(&r.value).to_string())
                        .collect();
                    JsonlResponse {
                        ok: true,
                        offset: None,
                        values: Some(values),
                        error: None,
                    }
                }
                Ok(Err(e)) => JsonlResponse {
                    ok: false,
                    offset: None,
                    values: None,
                    error: Some(e.to_string()),
                },
                Err(_elapsed) => {
                    warn!(
                        topic = %req.topic,
                        partition = req.partition,
                        ?REQUEST_TIMEOUT,
                        "jepsen gateway consume timed out"
                    );
                    JsonlResponse {
                        ok: false,
                        offset: None,
                        values: None,
                        error: Some(format!("request timed out after {:?}", REQUEST_TIMEOUT)),
                    }
                }
            }
        }
        other => JsonlResponse {
            ok: false,
            offset: None,
            values: None,
            error: Some(format!("unknown op: {other}")),
        },
    }
}
