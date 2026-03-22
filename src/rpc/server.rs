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
        let _raw = self.msg_bytes.borrow_mut().pop_front();
        let req = params.get()?.get_request()?;
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
