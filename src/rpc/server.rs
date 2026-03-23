use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use capnp_rpc::RpcSystem;
use capnp_rpc::rpc_twoparty_capnp;
use futures::AsyncReadExt;
use lru::LruCache;
use tokio::net::TcpListener;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{error, info};

use crate::consumer::{ConsumerConfig, PartitionConsumer};
use crate::node::partition_data_prefix;
use crate::produce_node::PartitionMap;
use crate::rpc::barka_capnp::{consume_svc, produce_svc};
use crate::rpc::bytes_transport::{BytesVatNetwork, MessageBytesQueue};
use crate::s3::S3Config;

const MAX_CACHED_CONSUMERS: usize = 256;

type ConsumerCache = Rc<RefCell<LruCache<(String, u32), Arc<PartitionConsumer>>>>;

pub async fn serve_produce_rpc(partitions: PartitionMap, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "produce-rpc listening");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            loop {
                let (stream, remote) = match listener.accept().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!(%e, "accept error");
                        continue;
                    }
                };
                info!(%remote, "produce-rpc connection");
                let partitions = Arc::clone(&partitions);

                tokio::task::spawn_local(async move {
                    let stream = stream.compat();
                    let (reader, writer) = stream.split();
                    let call_bytes_queue: MessageBytesQueue =
                        Rc::new(RefCell::new(VecDeque::new()));
                    let network = BytesVatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                        call_bytes_queue.clone(),
                    );
                    let per_conn = PerConnectionProduceNode {
                        partitions,
                        msg_bytes: call_bytes_queue,
                    };
                    let client: produce_svc::Client = capnp_rpc::new_client(per_conn);
                    let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                    if let Err(e) = rpc.await {
                        error!(%e, "produce rpc session error");
                    }
                });
            }
        })
        .await;
    Ok(())
}

struct PerConnectionProduceNode {
    partitions: PartitionMap,
    msg_bytes: MessageBytesQueue,
}

impl produce_svc::Server for PerConnectionProduceNode {
    async fn produce(
        self: Rc<Self>,
        params: produce_svc::ProduceParams,
        mut results: produce_svc::ProduceResults,
    ) -> Result<(), capnp::Error> {
        let raw = self
            .msg_bytes
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| capnp::Error::failed("missing message bytes".into()))?;
        let req = params.get()?.get_request()?;
        let key = (req.get_topic()?.to_string()?, req.get_partition());

        let state = self.partitions.get(&key).ok_or_else(|| {
            capnp::Error::failed(format!(
                "partition {}/{} not configured on this node",
                key.0, key.1
            ))
        })?;

        let epoch = state.leadership.check_leader().ok_or_else(|| {
            capnp::Error::failed(format!("not leader for partition {}/{}", key.0, key.1))
        })?;

        let produced = state
            .producer
            .apply_produce_request(raw, req, epoch)
            .await
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let resp = results.get().get_response()?;
        let mut out = resp.init_records(produced.len() as u32);
        for (i, p) in produced.iter().enumerate() {
            let mut dst = out.reborrow().get(i as u32);
            dst.set_offset(p.offset);
            dst.set_timestamp(p.timestamp);
        }
        Ok(())
    }
}

pub async fn serve_consume_rpc(
    s3_config: S3Config,
    base_prefix: String,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "consume-rpc listening");

    let consumers: ConsumerCache = Rc::new(RefCell::new(LruCache::new(
        NonZeroUsize::new(MAX_CACHED_CONSUMERS).unwrap(),
    )));

    let s3_config = Rc::new(s3_config);
    let base_prefix = Rc::new(base_prefix);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            loop {
                let (stream, remote) = match listener.accept().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!(%e, "accept error");
                        continue;
                    }
                };
                info!(%remote, "consume-rpc connection");
                let consumers = Rc::clone(&consumers);
                let s3_config = Rc::clone(&s3_config);
                let base_prefix = Rc::clone(&base_prefix);

                tokio::task::spawn_local(async move {
                    let stream = stream.compat();
                    let (reader, writer) = stream.split();
                    let network = capnp_rpc::twoparty::VatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );
                    let per_conn = PerConnectionConsumeNode {
                        consumers,
                        s3_config,
                        base_prefix,
                    };
                    let client: consume_svc::Client = capnp_rpc::new_client(per_conn);
                    let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                    if let Err(e) = rpc.await {
                        error!(%e, "consume rpc session error");
                    }
                });
            }
        })
        .await;
    Ok(())
}

struct PerConnectionConsumeNode {
    consumers: ConsumerCache,
    s3_config: Rc<S3Config>,
    base_prefix: Rc<String>,
}

impl PerConnectionConsumeNode {
    async fn get_or_create_consumer(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Arc<PartitionConsumer>, capnp::Error> {
        let key = (topic.to_string(), partition);
        {
            let mut cache = self.consumers.borrow_mut();
            if let Some(c) = cache.get(&key) {
                return Ok(Arc::clone(c));
            }
        }
        let prefix = partition_data_prefix(&self.base_prefix, topic, partition);
        let cache_dir = std::env::temp_dir()
            .join("barka-segment-cache")
            .join(prefix.replace('/', "-"));
        let consumer = PartitionConsumer::new(
            &self.s3_config,
            prefix,
            ConsumerConfig {
                cache_dir,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| capnp::Error::failed(e.to_string()))?;
        // Re-check after await: another task may have inserted while we yielded.
        let mut cache = self.consumers.borrow_mut();
        if let Some(existing) = cache.get(&key) {
            return Ok(Arc::clone(existing));
        }
        cache.put(key, Arc::clone(&consumer));
        Ok(consumer)
    }
}

impl consume_svc::Server for PerConnectionConsumeNode {
    async fn consume(
        self: Rc<Self>,
        params: consume_svc::ConsumeParams,
        mut results: consume_svc::ConsumeResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_request()?;
        let topic = req.get_topic()?.to_string()?;
        let partition = req.get_partition();
        let offset = req.get_offset();
        let max = req.get_max_records();

        let consumer = self.get_or_create_consumer(&topic, partition).await?;

        let records = consumer
            .consume(offset, max)
            .await
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let resp = results.get().get_response()?;
        let mut list = resp.init_records(records.len() as u32);
        for (i, rec) in records.iter().enumerate() {
            let mut entry = list.reborrow().get(i as u32);
            if !rec.key.is_empty() {
                entry.set_key(&rec.key);
            }
            entry.set_value(&rec.value);
            entry.set_offset(rec.offset);
            entry.set_timestamp(rec.timestamp);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::log_offset::compose;
    use crate::node::segment_key_prefix;
    use crate::produce_node::{LeadershipState, PartitionProduceState, ProducerBatchLimits};
    use crate::producer::{self, PartitionProducer};
    use crate::rpc::client::{ConsumeClient, ProduceClient};

    fn test_s3_config(bucket: &str) -> S3Config {
        S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: bucket.into(),
            region: "us-east-1".into(),
        }
    }

    fn unique_prefix(label: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{label}/{nanos}")
    }

    /// Build a single-topic partition map for test use.
    /// Returns the map plus the leadership handle so tests can call `set_leader`.
    async fn make_produce_partition_map(
        s3_config: &S3Config,
        s3_prefix: &str,
        topic: &str,
        partition: u32,
        limits: Option<ProducerBatchLimits>,
    ) -> (PartitionMap, Arc<LeadershipState>) {
        let prefix = segment_key_prefix(Some(s3_prefix));
        let pp = partition_data_prefix(&prefix, topic, partition);
        let leadership = Arc::new(LeadershipState::new());
        let producer = match limits {
            Some(l) => PartitionProducer::with_opts(
                s3_config,
                pp,
                l.max_records,
                l.max_bytes,
                l.linger(),
                Arc::clone(&leadership),
            )
            .await
            .unwrap(),
            None => PartitionProducer::new(s3_config, pp, Arc::clone(&leadership))
                .await
                .unwrap(),
        };
        let mut map = HashMap::new();
        map.insert(
            (topic.to_string(), partition),
            PartitionProduceState {
                producer,
                leadership: Arc::clone(&leadership),
            },
        );
        (Arc::new(map), leadership)
    }

    fn make_consume_node(s3_config: &S3Config, s3_prefix: &str) -> PerConnectionConsumeNode {
        let base_prefix = segment_key_prefix(Some(s3_prefix));
        PerConnectionConsumeNode {
            consumers: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(MAX_CACHED_CONSUMERS).unwrap(),
            ))),
            s3_config: Rc::new(s3_config.clone()),
            base_prefix: Rc::new(base_prefix),
        }
    }

    static STRESS_BUCKET_SEQ: AtomicU64 = AtomicU64::new(0);

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_produce_consume_round_trip() {
        let produce_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let produce_addr = produce_listener.local_addr().unwrap();
        let consume_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let consume_addr = consume_listener.local_addr().unwrap();

        let s3_config = test_s3_config("test-rpc");
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let s3_prefix = unique_prefix("rpc-round-trip");
        let (partitions, leadership) = make_produce_partition_map(
            &s3_config,
            &s3_prefix,
            "test-topic",
            0,
            Some(ProducerBatchLimits {
                max_records: 2,
                max_bytes: 1024 * 1024,
                linger_ms: 100,
            }),
        )
        .await;
        leadership.set_leader(u64::MAX, 0);

        let pm = Arc::clone(&partitions);
        let produce_server = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let (stream, _) = produce_listener.accept().await.unwrap();
                        let stream = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream);
                        let (reader, writer) = futures::AsyncReadExt::split(stream);
                        let call_bytes_queue: MessageBytesQueue =
                            Rc::new(RefCell::new(VecDeque::new()));
                        let network = BytesVatNetwork::new(
                            futures::io::BufReader::new(reader),
                            futures::io::BufWriter::new(writer),
                            rpc_twoparty_capnp::Side::Server,
                            Default::default(),
                            call_bytes_queue.clone(),
                        );
                        let per_conn = PerConnectionProduceNode {
                            partitions: pm,
                            msg_bytes: call_bytes_queue,
                        };
                        let client: produce_svc::Client = capnp_rpc::new_client(per_conn);
                        let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                        rpc.await.unwrap();
                    })
                    .await;
            });
        });

        let cs3 = s3_config.clone();
        let cpfx = s3_prefix.clone();
        let consume_server = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let (stream, _) = consume_listener.accept().await.unwrap();
                        let stream = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream);
                        let (reader, writer) = futures::AsyncReadExt::split(stream);
                        let network = capnp_rpc::twoparty::VatNetwork::new(
                            futures::io::BufReader::new(reader),
                            futures::io::BufWriter::new(writer),
                            rpc_twoparty_capnp::Side::Server,
                            Default::default(),
                        );
                        let per_conn = make_consume_node(&cs3, &cpfx);
                        let client: consume_svc::Client = capnp_rpc::new_client(per_conn);
                        let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                        rpc.await.unwrap();
                    })
                    .await;
            });
        });

        let client = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let produce_client = ProduceClient::connect(produce_addr).await.unwrap();
                        let consume_client = ConsumeClient::connect(consume_addr).await.unwrap();

                        let recs = produce_client
                            .produce("test-topic", 0, vec![b"hello".to_vec(), b"world".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(recs.len(), 2);
                        assert_eq!(recs[0].offset, compose(0, 0));
                        assert_eq!(recs[0].value, b"hello");
                        assert_eq!(recs[1].offset, compose(0, 1));
                        assert_eq!(recs[1].value, b"world");

                        let recs2 = produce_client
                            .produce("test-topic", 0, vec![b"third".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(recs2.len(), 1);
                        assert_eq!(recs2[0].offset, compose(1, 0));
                        assert_eq!(recs2[0].value, b"third");

                        let consumed = consume_client
                            .consume("test-topic", 0, compose(0, 0), 10)
                            .await
                            .unwrap();
                        assert_eq!(
                            consumed.len(),
                            3,
                            "should get all 3 records across both segments"
                        );
                        assert_eq!(consumed[0].value, b"hello");
                        assert_eq!(consumed[0].offset, compose(0, 0));
                        assert_eq!(consumed[1].value, b"world");
                        assert_eq!(consumed[1].offset, compose(0, 1));
                        assert_eq!(consumed[2].value, b"third");
                        assert_eq!(consumed[2].offset, compose(1, 0));

                        let consumed2 = consume_client
                            .consume("test-topic", 0, compose(0, 1), 10)
                            .await
                            .unwrap();
                        assert_eq!(consumed2.len(), 2, "should skip first record");
                        assert_eq!(consumed2[0].value, b"world");
                        assert_eq!(consumed2[1].value, b"third");

                        let consumed3 = consume_client
                            .consume("test-topic", 0, compose(1, 0), 10)
                            .await
                            .unwrap();
                        assert_eq!(consumed3.len(), 1);
                        assert_eq!(consumed3[0].value, b"third");

                        let consumed4 = consume_client
                            .consume("test-topic", 0, compose(2, 0), 10)
                            .await
                            .unwrap();
                        assert!(consumed4.is_empty(), "no segment 2 exists");
                    })
                    .await;
            });
        });

        let (c, ps, cs) = tokio::join!(client, produce_server, consume_server);
        c.unwrap();
        ps.unwrap();
        cs.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_default_producer_limits_single_large_produce() {
        const N: usize = 101;
        const _: () = assert!(N > 100 && N <= producer::DEFAULT_MAX_RECORDS);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let s3_config = test_s3_config("test-rpc");
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let (partitions, leadership) = make_produce_partition_map(
            &s3_config,
            &unique_prefix("rpc-default-limits"),
            "default-limits-topic",
            0,
            None,
        )
        .await;
        leadership.set_leader(u64::MAX, 0);

        let pm = Arc::clone(&partitions);
        let server = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let (stream, _) = listener.accept().await.unwrap();
                        let stream = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream);
                        let (reader, writer) = futures::AsyncReadExt::split(stream);
                        let call_bytes_queue: MessageBytesQueue =
                            Rc::new(RefCell::new(VecDeque::new()));
                        let network = BytesVatNetwork::new(
                            futures::io::BufReader::new(reader),
                            futures::io::BufWriter::new(writer),
                            rpc_twoparty_capnp::Side::Server,
                            Default::default(),
                            call_bytes_queue.clone(),
                        );
                        let per_conn = PerConnectionProduceNode {
                            partitions: pm,
                            msg_bytes: call_bytes_queue,
                        };
                        let client: produce_svc::Client = capnp_rpc::new_client(per_conn);
                        let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                        rpc.await.unwrap();
                    })
                    .await;
            });
        });

        let client = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let client = ProduceClient::connect(addr).await.unwrap();
                        let values: Vec<Vec<u8>> =
                            (0..N).map(|i| format!("rec-{i}").into_bytes()).collect();
                        let recs = client
                            .produce("default-limits-topic", 0, values.clone())
                            .await
                            .unwrap();
                        assert_eq!(recs.len(), N);
                        for (i, r) in recs.iter().enumerate() {
                            assert_eq!(r.offset, compose(0, i as u64));
                            assert_eq!(r.value, values[i]);
                        }
                    })
                    .await;
            });
        });

        let (c, s) = tokio::join!(client, server);
        c.unwrap();
        s.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_produce_consume_load_stress() {
        const NUM_CLIENTS: usize = 6;
        const RUN_SECS: u64 = 4;
        const BATCH: usize = 40;
        const MIN_TOTAL_RECORDS: u64 = 400;
        const TOPIC: &str = "stress-topic";

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let bucket = format!(
            "test-rpc-stress-{}",
            STRESS_BUCKET_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let s3_config = test_s3_config(&bucket);
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let (partitions, leadership) = make_produce_partition_map(
            &s3_config,
            &unique_prefix(&format!("rpc-stress/{bucket}")),
            TOPIC,
            0,
            Some(ProducerBatchLimits {
                max_records: 100,
                max_bytes: 1024 * 1024,
                linger_ms: 1000,
            }),
        )
        .await;
        leadership.set_leader(u64::MAX, 0);

        let pm = Arc::clone(&partitions);
        let server = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async move {
                        let mut shutdown_rx = pin!(shutdown_rx);
                        loop {
                            tokio::select! {
                                biased;
                                _ = &mut shutdown_rx => {
                                    break;
                                }
                                accept_res = listener.accept() => {
                                    let (stream, _) = accept_res.unwrap();
                                    let stream =
                                        tokio_util::compat::TokioAsyncReadCompatExt::compat(stream);
                                    let (reader, writer) = futures::AsyncReadExt::split(stream);
                                    let call_bytes_queue: MessageBytesQueue =
                                        Rc::new(RefCell::new(VecDeque::new()));
                                    let network = BytesVatNetwork::new(
                                        futures::io::BufReader::new(reader),
                                        futures::io::BufWriter::new(writer),
                                        rpc_twoparty_capnp::Side::Server,
                                        Default::default(),
                                        call_bytes_queue.clone(),
                                    );
                                    let per_conn = PerConnectionProduceNode {
                                        partitions: Arc::clone(&pm),
                                        msg_bytes: call_bytes_queue,
                                    };
                                    let client: produce_svc::Client = capnp_rpc::new_client(per_conn);
                                    let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                                    tokio::task::spawn_local(async move {
                                        let _ = rpc.await;
                                    });
                                }
                            }
                        }
                    })
                    .await;
            });
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
        for client_id in 0..NUM_CLIENTS {
            client_handles.push(tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let local = tokio::task::LocalSet::new();
                    local
                        .run_until(async {
                            let client = ProduceClient::connect(addr).await.unwrap();
                            let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
                            let mut produce_count: u64 = 0;
                            let mut prev_first_offset: Option<u64> = None;

                            while Instant::now() < deadline {
                                let batch: Vec<Vec<u8>> = (0..BATCH)
                                    .map(|i| {
                                        format!("c{client_id}-{}", produce_count + i as u64)
                                            .into_bytes()
                                    })
                                    .collect();
                                let recs = client.produce(TOPIC, 0, batch).await.unwrap();
                                assert_eq!(recs.len(), BATCH);
                                let first_offset = recs[0].offset;
                                if let Some(prev) = prev_first_offset {
                                    assert!(
                                        first_offset > prev,
                                        "first_offset must increase: prev={prev:#x} cur={first_offset:#x}"
                                    );
                                }
                                for (i, r) in recs.iter().enumerate() {
                                    let want =
                                        format!("c{client_id}-{}", produce_count + i as u64)
                                            .into_bytes();
                                    assert_eq!(r.value, want);
                                }
                                prev_first_offset = Some(first_offset);
                                produce_count += BATCH as u64;
                            }

                            assert!(
                                produce_count >= MIN_TOTAL_RECORDS,
                                "client {client_id} produced only {produce_count} records in {RUN_SECS}s"
                            );
                        })
                        .await;
                });
            }));
        }

        for h in client_handles {
            h.await.unwrap();
        }

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_produce_rejected_when_not_leader() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let s3_config = test_s3_config("test-rpc");
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let (partitions, _leadership) = make_produce_partition_map(
            &s3_config,
            &unique_prefix("rpc-not-leader"),
            "test-topic",
            0,
            None,
        )
        .await;
        // Deliberately NOT calling set_leader — node should reject produce.

        let pm = Arc::clone(&partitions);
        let server = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let (stream, _) = listener.accept().await.unwrap();
                        let stream = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream);
                        let (reader, writer) = futures::AsyncReadExt::split(stream);
                        let call_bytes_queue: MessageBytesQueue =
                            Rc::new(RefCell::new(VecDeque::new()));
                        let network = BytesVatNetwork::new(
                            futures::io::BufReader::new(reader),
                            futures::io::BufWriter::new(writer),
                            rpc_twoparty_capnp::Side::Server,
                            Default::default(),
                            call_bytes_queue.clone(),
                        );
                        let per_conn = PerConnectionProduceNode {
                            partitions: pm,
                            msg_bytes: call_bytes_queue,
                        };
                        let client: produce_svc::Client = capnp_rpc::new_client(per_conn);
                        let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                        rpc.await.unwrap();
                    })
                    .await;
            });
        });

        let client = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async {
                        let client = ProduceClient::connect(addr).await.unwrap();
                        let err = client
                            .produce("test-topic", 0, vec![b"should-fail".to_vec()])
                            .await
                            .unwrap_err();
                        let msg = err.to_string();
                        assert!(
                            msg.contains("not leader"),
                            "expected 'not leader' error, got: {msg}"
                        );
                    })
                    .await;
            });
        });

        let (c, s) = tokio::join!(client, server);
        c.unwrap();
        s.unwrap();
    }
}
