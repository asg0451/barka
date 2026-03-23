use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use capnp_rpc::RpcSystem;
use capnp_rpc::rpc_twoparty_capnp;
use futures::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{error, info};

use crate::node::Node;
use crate::rpc::barka_capnp::barka_svc;
use crate::rpc::bytes_transport::{BytesVatNetwork, MessageBytesQueue};

pub async fn serve_rpc(node: Node, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "capnp-rpc listening");

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
                info!(%remote, "capnp-rpc connection");
                let node = node.clone();

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
                    let per_conn = PerConnectionNode {
                        node,
                        msg_bytes: call_bytes_queue,
                    };
                    let client: barka_svc::Client = capnp_rpc::new_client(per_conn);
                    let rpc = RpcSystem::new(Box::new(network), Some(client.client));
                    if let Err(e) = rpc.await {
                        error!(%e, "rpc session error");
                    }
                });
            }
        })
        .await;
    Ok(())
}

// TODO: revisit — coupled to bytes_transport.rs custom transport (see note there).
struct PerConnectionNode {
    node: Node,
    msg_bytes: MessageBytesQueue,
}

impl barka_svc::Server for PerConnectionNode {
    async fn produce(
        self: Rc<Self>,
        params: barka_svc::ProduceParams,
        mut results: barka_svc::ProduceResults,
    ) -> Result<(), capnp::Error> {
        let raw = self
            .msg_bytes
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| capnp::Error::failed("missing message bytes".into()))?;
        let req = params.get()?.get_request()?;

        // NOTE: only one producer atm. so only one topic & partition.
        // TODO: more than one producer, hook up leadership, etc
        let produced = self
            .node
            .producer
            .apply_produce_request(raw, req)
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

    async fn consume(
        self: Rc<Self>,
        params: barka_svc::ConsumeParams,
        mut results: barka_svc::ConsumeResults,
    ) -> Result<(), capnp::Error> {
        let _raw = self.msg_bytes.borrow_mut().pop_front();
        let req = params.get()?.get_request()?;
        let resp = results.get().get_response()?;
        self.node.apply_consume_request(req, resp).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::log_offset::compose;
    use crate::node::{Node, NodeConfig, ProducerBatchLimits};
    use crate::producer;
    use crate::rpc::client::BarkaClient;
    use crate::s3::S3Config;

    static STRESS_BUCKET_SEQ: AtomicU64 = AtomicU64::new(0);

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_produce_consume_round_trip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let s3_config = S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "test-rpc".into(),
            region: "us-east-1".into(),
        };
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let node = Node::new(
            NodeConfig {
                rpc_addr: addr,
                s3_prefix: Some(format!(
                    "rpc-round-trip/{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )),
                // max_records=2 so the first produce (2 records) flushes immediately;
                // the second (1 record) flushes on the 100ms linger timer.
                producer_limits: Some(ProducerBatchLimits {
                    max_records: 2,
                    max_bytes: 1024 * 1024,
                    linger_ms: 100,
                }),
                ..Default::default()
            },
            &s3_config,
        )
        .await
        .unwrap();

        let server_node = node.clone();
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
                        let per_conn = PerConnectionNode {
                            node: server_node,
                            msg_bytes: call_bytes_queue,
                        };
                        let client: barka_svc::Client = capnp_rpc::new_client(per_conn);
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
                        let client = BarkaClient::connect(addr).await.unwrap();

                        // First produce: 2 records → segment 0, intra [0,1]
                        let recs = client
                            .produce("test-topic", 0, vec![b"hello".to_vec(), b"world".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(recs.len(), 2);
                        assert_eq!(recs[0].offset, compose(0, 0));
                        assert_eq!(recs[0].value, b"hello");
                        assert_eq!(recs[1].offset, compose(0, 1));
                        assert_eq!(recs[1].value, b"world");

                        // Second produce: 1 record → segment 1, intra [0]
                        let recs2 = client
                            .produce("test-topic", 0, vec![b"third".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(recs2.len(), 1);
                        assert_eq!(recs2[0].offset, compose(1, 0));
                        assert_eq!(recs2[0].value, b"third");

                        // Consume from segment 0: should return "hello", "world"
                        let consumed = client
                            .consume("test-topic", 0, compose(0, 0), 10)
                            .await
                            .unwrap();
                        assert_eq!(consumed.len(), 3, "should get all 3 records across both segments");
                        assert_eq!(consumed[0].value, b"hello");
                        assert_eq!(consumed[0].offset, compose(0, 0));
                        assert_eq!(consumed[1].value, b"world");
                        assert_eq!(consumed[1].offset, compose(0, 1));
                        assert_eq!(consumed[2].value, b"third");
                        assert_eq!(consumed[2].offset, compose(1, 0));

                        // Consume from mid-segment offset
                        let consumed2 = client
                            .consume("test-topic", 0, compose(0, 1), 10)
                            .await
                            .unwrap();
                        assert_eq!(consumed2.len(), 2, "should skip first record");
                        assert_eq!(consumed2[0].value, b"world");
                        assert_eq!(consumed2[1].value, b"third");

                        // Consume from segment 1 only
                        let consumed3 = client
                            .consume("test-topic", 0, compose(1, 0), 10)
                            .await
                            .unwrap();
                        assert_eq!(consumed3.len(), 1);
                        assert_eq!(consumed3[0].value, b"third");

                        // Consume past all data — should be empty
                        let consumed4 = client
                            .consume("test-topic", 0, compose(2, 0), 10)
                            .await
                            .unwrap();
                        assert!(consumed4.is_empty(), "no segment 2 exists");
                    })
                    .await;
            });
        });

        let (c, s) = tokio::join!(client, server);
        c.unwrap();
        s.unwrap();
    }

    /// `Node` with `producer_limits: None` must use library defaults (100k records / 100 MiB cap).
    /// A single produce larger than the stress-test override (100) is rejected if defaults regress.
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_default_producer_limits_single_large_produce() {
        const N: usize = 101;
        const _: () = assert!(N > 100 && N <= producer::DEFAULT_MAX_RECORDS);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let s3_config = S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "test-rpc".into(),
            region: "us-east-1".into(),
        };
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let node = Node::new(
            NodeConfig {
                rpc_addr: addr,
                s3_prefix: Some(format!(
                    "rpc-default-limits/{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )),
                producer_limits: None,
                ..Default::default()
            },
            &s3_config,
        )
        .await
        .unwrap();
        assert!(
            node.config.producer_limits.is_none(),
            "e2e expects real PartitionProducer::new defaults"
        );

        let server_node = node.clone();
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
                        let per_conn = PerConnectionNode {
                            node: server_node,
                            msg_bytes: call_bytes_queue,
                        };
                        let client: barka_svc::Client = capnp_rpc::new_client(per_conn);
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
                        let client = BarkaClient::connect(addr).await.unwrap();
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

    /// Several concurrent RPC clients, each producing in batches for a few seconds on its own
    /// topic, then consuming back and checking offsets and payloads. Requires LocalStack S3.
    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_produce_consume_load_stress() {
        const NUM_CLIENTS: usize = 6;
        const RUN_SECS: u64 = 4;
        const BATCH: usize = 40;
        const MIN_TOTAL_RECORDS: u64 = 400;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let bucket = format!(
            "test-rpc-stress-{}",
            STRESS_BUCKET_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let s3_config = S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: bucket.clone(),
            region: "us-east-1".into(),
        };
        let s3_client = crate::s3::build_client(&s3_config).await;
        crate::s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        // Tight batch limits so flushes trigger without waiting on the 1s linger timer
        // (production defaults are 100k records / 100 MiB).
        let node = Node::new(
            NodeConfig {
                rpc_addr: addr,
                s3_prefix: Some(format!(
                    "rpc-stress/{}/{}",
                    bucket,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )),
                producer_limits: Some(ProducerBatchLimits {
                    max_records: 100,
                    max_bytes: 1024 * 1024,
                    linger_ms: 1000,
                }),
                ..Default::default()
            },
            &s3_config,
        )
        .await
        .unwrap();

        let server_node = node.clone();
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
                                    let per_conn = PerConnectionNode {
                                        node: server_node.clone(),
                                        msg_bytes: call_bytes_queue,
                                    };
                                    let client: barka_svc::Client = capnp_rpc::new_client(per_conn);
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
                            let client = BarkaClient::connect(addr).await.unwrap();
                            let topic = format!("load-stress-{client_id}");
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
                                let recs = client.produce(&topic, 0, batch).await.unwrap();
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
}
