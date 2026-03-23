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
    cached: Option<CachedLeader>,
}

struct CachedLeader {
    addr: SocketAddr,
    valid_until_ms: u64,
    client: ProduceClient,
}

impl ProduceRouter {
    pub async fn new(s3_config: &S3Config) -> Self {
        let s3_client = crate::s3::build_client(s3_config).await;
        Self {
            s3_client,
            bucket: s3_config.bucket.clone(),
            cached: None,
        }
    }

    pub async fn produce(
        &mut self,
        topic: &str,
        partition: u32,
        values: Vec<Vec<u8>>,
    ) -> Result<Vec<Record>> {
        let namespace = leader_namespace(topic, partition);

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(RETRY_DELAY).await;
                self.cached = None;
            }

            if !self.cache_valid() {
                let leader = leader_election::read_current_leader(
                    &self.s3_client,
                    &self.bucket,
                    &namespace,
                    None,
                )
                .await
                .context("read current leader")?;

                let leader = match leader {
                    Some(l) => l,
                    None => {
                        tracing::warn!(attempt, %namespace, "no leader found");
                        continue;
                    }
                };

                let need_reconnect = self.cached.as_ref().is_none_or(|c| c.addr != leader.addr);

                if need_reconnect {
                    let client = ProduceClient::connect(leader.addr)
                        .await
                        .with_context(|| format!("connect to leader at {}", leader.addr))?;
                    self.cached = Some(CachedLeader {
                        addr: leader.addr,
                        valid_until_ms: leader.valid_until_ms,
                        client,
                    });
                } else if let Some(ref mut c) = self.cached {
                    c.valid_until_ms = leader.valid_until_ms;
                }
            }

            let cached = self.cached.as_ref().unwrap();
            match cached
                .client
                .produce(topic, partition, values.clone())
                .await
            {
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

    fn cache_valid(&self) -> bool {
        match &self.cached {
            None => false,
            Some(c) => {
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
