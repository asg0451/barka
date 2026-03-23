use std::net::SocketAddr;

use anyhow::{Context, Result};

use crate::leader_election;
use crate::log::record::Record;
use crate::node::leader_namespace;
use crate::rpc::client::ProduceClient;
use crate::s3::S3Config;

const MAX_RETRIES: u32 = 5;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(200);

pub struct ProduceRouter {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    leader_election_prefix: Option<String>,
    cached: Option<CachedLeader>,
}

struct CachedLeader {
    namespace: String,
    addr: SocketAddr,
    valid_until_ms: u64,
    client: ProduceClient,
}

impl ProduceRouter {
    pub async fn new(s3_config: &S3Config, leader_election_prefix: Option<String>) -> Self {
        let s3_client = crate::s3::build_client(s3_config).await;
        Self {
            s3_client,
            bucket: s3_config.bucket.clone(),
            leader_election_prefix,
            cached: None,
        }
    }

    pub async fn produce(
        &mut self,
        topic: &str,
        partition: u32,
        mut values: Vec<Vec<u8>>,
    ) -> Result<Vec<Record>> {
        let namespace = leader_namespace(topic, partition);

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(RETRY_DELAY).await;
                self.cached = None;
            }

            if !self.cache_valid_for(&namespace) {
                let leader = match leader_election::read_current_leader(
                    &self.s3_client,
                    &self.bucket,
                    &namespace,
                    self.leader_election_prefix.as_deref(),
                )
                .await
                {
                    Ok(Some(l)) => l,
                    Ok(None) => {
                        tracing::warn!(attempt, %namespace, "no leader found");
                        continue;
                    }
                    Err(e) if attempt < MAX_RETRIES => {
                        tracing::warn!(attempt, error = %e, "leader discovery failed, retrying");
                        continue;
                    }
                    Err(e) => return Err(e).context("read current leader"),
                };

                let need_reconnect = self.cached.as_ref().is_none_or(|c| c.addr != leader.addr);

                if need_reconnect {
                    let client = match ProduceClient::connect(leader.addr).await {
                        Ok(c) => c,
                        Err(e) if attempt < MAX_RETRIES => {
                            tracing::warn!(attempt, error = %e, addr = %leader.addr, "connect failed, retrying");
                            continue;
                        }
                        Err(e) => {
                            return Err(e).context(format!("connect to leader at {}", leader.addr));
                        }
                    };
                    self.cached = Some(CachedLeader {
                        namespace: namespace.clone(),
                        addr: leader.addr,
                        valid_until_ms: leader.valid_until_ms,
                        client,
                    });
                } else if let Some(ref mut c) = self.cached {
                    c.valid_until_ms = leader.valid_until_ms;
                }
            }

            let cached = self.cached.as_ref().unwrap();
            let v = if attempt < MAX_RETRIES {
                values.clone()
            } else {
                std::mem::take(&mut values)
            };
            match cached.client.produce(topic, partition, v).await {
                Ok(records) => return Ok(records),
                Err(e) => {
                    let msg = e.to_string();
                    if is_retriable(&msg) && attempt < MAX_RETRIES {
                        tracing::warn!(attempt, error = %msg, "produce router retrying");
                        continue;
                    }
                    return Err(e).context("produce via router");
                }
            }
        }

        anyhow::bail!("produce router: exhausted {MAX_RETRIES} retries");
    }

    fn cache_valid_for(&self, namespace: &str) -> bool {
        match &self.cached {
            None => false,
            Some(c) => {
                if c.namespace != namespace {
                    return false;
                }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                now_ms < c.valid_until_ms
            }
        }
    }
}

fn is_retriable(msg: &str) -> bool {
    msg.contains("not leader")
        || msg.contains("leadership lost")
        || msg.contains("batch cancelled")
        || msg.contains("epoch changed")
        || msg.contains("connection refused")
        || msg.contains("broken pipe")
}
