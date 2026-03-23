//! Jepsen API gateway: newline-delimited JSON over TCP, routing produce ops
//! through S3-based leader discovery and consume ops to a consume-node via
//! Cap'n Proto RPC.
//!
//! Runs on a dedicated `current_thread` Tokio runtime with a top-level [`LocalSet`]
//! (same constraints as the Cap'n Proto RPC stack: `!Send` futures, `spawn_local`).

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::produce_router::ProduceRouter;
use crate::rpc::client::ConsumeClient;
use crate::s3::S3Config;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Newline-delimited JSON gateway.
///
/// Request:  `{"op":"produce","topic":"events","partition":0,"value":"hello"}`
///           `{"op":"consume","topic":"events","partition":0,"offset":0,"max":10}`
/// Response: `{"ok":true,"offset":0}`
///           `{"ok":true,"values":["hello"]}`
///           `{"ok":false,"error":"..."}`
pub async fn serve(
    consume_rpc_addr: SocketAddr,
    listen_addr: SocketAddr,
    s3_config: S3Config,
) -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let listener = TcpListener::bind(listen_addr).await?;
            info!(
                %listen_addr,
                %consume_rpc_addr,
                "jepsen gateway listening",
            );

            loop {
                let (stream, remote) = listener.accept().await?;
                info!(%remote, "jepsen gateway connection");

                let s3_config = s3_config.clone();
                tokio::task::spawn_local(async move {
                    if let Err(e) = run_session(stream, consume_rpc_addr, s3_config).await {
                        error!(%e, "jepsen gateway session ended with error");
                    }
                });
            }
        })
        .await
}

async fn run_session(
    stream: tokio::net::TcpStream,
    consume_rpc_addr: SocketAddr,
    s3_config: S3Config,
) -> anyhow::Result<()> {
    let mut produce = ProduceRouter::new(&s3_config, None).await;
    let mut consume_client = connect_consume_with_retry(consume_rpc_addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = handle_json_line(&mut produce, &mut consume_client, &line).await;
        let mut out = serde_json::to_string(&response).unwrap();
        out.push('\n');
        if writer.write_all(out.as_bytes()).await.is_err() {
            break;
        }
    }
    Ok(())
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

async fn handle_json_line(
    produce: &mut ProduceRouter,
    consume_client: &mut ConsumeClient,
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
            match tokio::time::timeout(
                REQUEST_TIMEOUT,
                produce.produce(&req.topic, req.partition, vec![value]),
            )
            .await
            {
                Ok(Ok(records)) => JsonlResponse {
                    ok: true,
                    offset: records.first().map(|r| r.offset),
                    next_offset: None,
                    values: None,
                    error: None,
                },
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
                        "jepsen gateway produce timed out"
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
