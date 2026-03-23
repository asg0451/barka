//! Jepsen API gateway: newline-delimited JSON over TCP, routing produce ops to
//! a produce-node and consume ops to a consume-node via Cap'n Proto RPC.
//!
//! Runs on a dedicated `current_thread` Tokio runtime with a top-level [`LocalSet`]
//! (same constraints as the Cap'n Proto RPC stack: `!Send` futures, `spawn_local`).

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::rpc::client::{ConsumeClient, ProduceClient};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const PRODUCE_MAX_RETRIES: u32 = 5;
const PRODUCE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Newline-delimited JSON gateway.
///
/// Request:  `{"op":"produce","topic":"events","partition":0,"value":"hello"}`
///           `{"op":"consume","topic":"events","partition":0,"offset":0,"max":10}`
/// Response: `{"ok":true,"offset":0}`
///           `{"ok":true,"values":["hello"]}`
///           `{"ok":false,"error":"..."}`
pub async fn serve(
    produce_rpc_addr: SocketAddr,
    consume_rpc_addr: SocketAddr,
    listen_addr: SocketAddr,
) -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let listener = TcpListener::bind(listen_addr).await?;
            info!(
                %listen_addr,
                %produce_rpc_addr,
                %consume_rpc_addr,
                "jepsen gateway listening",
            );

            loop {
                let (stream, remote) = listener.accept().await?;
                info!(%remote, "jepsen gateway connection");

                tokio::task::spawn_local(async move {
                    if let Err(e) =
                        run_session(stream, produce_rpc_addr, consume_rpc_addr).await
                    {
                        error!(%e, "jepsen gateway session ended with error");
                    }
                });
            }
        })
        .await
}

async fn run_session(
    stream: tokio::net::TcpStream,
    produce_rpc_addr: SocketAddr,
    consume_rpc_addr: SocketAddr,
) -> anyhow::Result<()> {
    let produce_client = connect_produce_with_retry(produce_rpc_addr).await?;
    let consume_client = connect_consume_with_retry(consume_rpc_addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = handle_json_line(&produce_client, &consume_client, &line).await;
        let mut out = serde_json::to_string(&response).unwrap();
        out.push('\n');
        if writer.write_all(out.as_bytes()).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn connect_produce_with_retry(addr: SocketAddr) -> anyhow::Result<ProduceClient> {
    const MAX_ATTEMPTS: u32 = 100;
    const DELAY: Duration = Duration::from_millis(25);

    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match ProduceClient::connect(addr).await {
            Ok(c) => {
                if attempt > 0 {
                    info!(%addr, attempt, "jepsen gateway: connected to produce rpc");
                }
                return Ok(c);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(DELAY).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("produce connect failed")))
}

async fn connect_consume_with_retry(addr: SocketAddr) -> anyhow::Result<ConsumeClient> {
    const MAX_ATTEMPTS: u32 = 100;
    const DELAY: Duration = Duration::from_millis(25);

    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match ConsumeClient::connect(addr).await {
            Ok(c) => {
                if attempt > 0 {
                    info!(%addr, attempt, "jepsen gateway: connected to consume rpc");
                }
                return Ok(c);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(DELAY).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("consume connect failed")))
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
    next_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn is_transient_produce_error(msg: &str) -> bool {
    msg.contains("not leader")
        || msg.contains("leadership lost")
        || msg.contains("batch cancelled")
        || msg.contains("epoch changed")
}

async fn handle_json_line(
    produce_client: &ProduceClient,
    consume_client: &ConsumeClient,
    line: &str,
) -> JsonlResponse {
    let req: JsonlRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return JsonlResponse {
                ok: false,
                offset: None,
                next_offset: None,
                values: None,
                error: Some(format!("bad request: {e}")),
            };
        }
    };

    match req.op.as_str() {
        "produce" => {
            let value = req.value.unwrap_or_default().into_bytes();
            let mut last_err = String::new();
            for attempt in 0..=PRODUCE_MAX_RETRIES {
                if attempt > 0 {
                    tokio::time::sleep(PRODUCE_RETRY_DELAY).await;
                }
                match tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    produce_client.produce(&req.topic, req.partition, vec![value.clone()]),
                )
                .await
                {
                    Ok(Ok(records)) => {
                        return JsonlResponse {
                            ok: true,
                            offset: records.first().map(|r| r.offset),
                            next_offset: None,
                            values: None,
                            error: None,
                        };
                    }
                    Ok(Err(e)) => {
                        let msg = e.to_string();
                        if is_transient_produce_error(&msg) && attempt < PRODUCE_MAX_RETRIES {
                            warn!(
                                topic = %req.topic,
                                attempt,
                                error = %msg,
                                "jepsen gateway produce retrying",
                            );
                            last_err = msg;
                            continue;
                        }
                        return JsonlResponse {
                            ok: false,
                            offset: None,
                            next_offset: None,
                            values: None,
                            error: Some(msg),
                        };
                    }
                    Err(_elapsed) => {
                        warn!(
                            topic = %req.topic,
                            partition = req.partition,
                            ?REQUEST_TIMEOUT,
                            "jepsen gateway produce timed out"
                        );
                        return JsonlResponse {
                            ok: false,
                            offset: None,
                            next_offset: None,
                            values: None,
                            error: Some(format!("request timed out after {:?}", REQUEST_TIMEOUT)),
                        };
                    }
                }
            }
            JsonlResponse {
                ok: false,
                offset: None,
                next_offset: None,
                values: None,
                error: Some(format!("produce failed after retries: {last_err}")),
            }
        }
        "consume" => {
            match tokio::time::timeout(
                REQUEST_TIMEOUT,
                consume_client.consume(&req.topic, req.partition, req.offset, req.max),
            )
            .await
            {
                Ok(Ok(records)) => {
                    let next_offset = records.last().map(|r| r.offset + 1);
                    let values: Vec<String> = records
                        .iter()
                        .map(|r| String::from_utf8_lossy(&r.value).to_string())
                        .collect();
                    JsonlResponse {
                        ok: true,
                        offset: None,
                        next_offset,
                        values: Some(values),
                        error: None,
                    }
                }
                Ok(Err(e)) => JsonlResponse {
                    ok: false,
                    offset: None,
                    next_offset: None,
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
                        next_offset: None,
                        values: None,
                        error: Some(format!("request timed out after {:?}", REQUEST_TIMEOUT)),
                    }
                }
            }
        }
        other => JsonlResponse {
            ok: false,
            offset: None,
            next_offset: None,
            values: None,
            error: Some(format!("unknown op: {other}")),
        },
    }
}
