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
        self.node
            .producer
            .apply_produce_request(raw, req)
            .await
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let base_offset = self.node.apply_produce_request(req)?;
        results.get().get_response()?.set_base_offset(base_offset);
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
        self.node.apply_consume_request(req, resp)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::node::{Node, NodeConfig};
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

                        let base = client
                            .produce("test-topic", 0, vec![b"hello".to_vec(), b"world".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(base, 0);

                        let base2 = client
                            .produce("test-topic", 0, vec![b"third".to_vec()])
                            .await
                            .unwrap();
                        assert_eq!(base2, 2);

                        let records = client.consume("test-topic", 0, 0, 10).await.unwrap();
                        assert_eq!(records.len(), 3);
                        assert_eq!(records[0].value, b"hello");
                        assert_eq!(records[1].value, b"world");
                        assert_eq!(records[2].value, b"third");
                        assert_eq!(records[0].offset, 0);
                        assert_eq!(records[1].offset, 1);
                        assert_eq!(records[2].offset, 2);

                        let slice = client.consume("test-topic", 0, 1, 1).await.unwrap();
                        assert_eq!(slice.len(), 1);
                        assert_eq!(slice[0].value, b"world");
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

        let node = Node::new(
            NodeConfig {
                rpc_addr: addr,
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
                            let mut next_seq: u64 = 0;
                            let mut expected_base: u64 = 0;

                            while Instant::now() < deadline {
                                let batch: Vec<Vec<u8>> = (0..BATCH)
                                    .map(|i| {
                                        format!("c{client_id}-{}", next_seq + i as u64).into_bytes()
                                    })
                                    .collect();
                                let base = client.produce(&topic, 0, batch).await.unwrap();
                                assert_eq!(base, expected_base);
                                next_seq += BATCH as u64;
                                expected_base += BATCH as u64;
                            }

                            assert!(
                                next_seq >= MIN_TOTAL_RECORDS,
                                "client {client_id} produced only {next_seq} records in {RUN_SECS}s"
                            );

                            let records =
                                client.consume(&topic, 0, 0, next_seq as u32).await.unwrap();
                            assert_eq!(records.len() as u64, next_seq);
                            for (i, r) in records.iter().enumerate() {
                                assert_eq!(r.offset, i as u64);
                                let want = format!("c{client_id}-{}", i as u64).into_bytes();
                                assert_eq!(r.value, want);
                            }
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
