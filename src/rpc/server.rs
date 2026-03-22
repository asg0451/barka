use std::net::SocketAddr;
use std::rc::Rc;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{error, info};

use crate::node::Node;
use crate::rpc::barka_capnp::barka_svc;

pub async fn serve_rpc(node: Node, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "capnp-rpc listening");

    let client: barka_svc::Client = capnp_rpc::new_client(node);

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
                let client = client.clone();

                tokio::task::spawn_local(async move {
                    let stream = stream.compat();
                    let (reader, writer) = stream.split();
                    let network = twoparty::VatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );
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

impl barka_svc::Server for Node {
    async fn produce(
        self: Rc<Self>,
        params: barka_svc::ProduceParams,
        mut results: barka_svc::ProduceResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_request()?;
        let topic = req.get_topic()?.to_string()?;
        let partition = req.get_partition();
        let records = req.get_records()?;

        let tp = (topic, partition);
        let mut partitions = self.partitions.lock().unwrap();
        let part = partitions
            .entry(tp)
            .or_insert_with(|| crate::log::partition::Partition::new(partition));

        let mut base_offset = None;
        for record in records.iter() {
            let value = record.get_value()?.to_vec();
            let key = {
                let k = record.get_key()?;
                if k.is_empty() { None } else { Some(k.to_vec()) }
            };
            let ts = record.get_timestamp();
            let off = part.append(key, value, ts);
            if base_offset.is_none() {
                base_offset = Some(off);
            }
        }

        results
            .get()
            .get_response()?
            .set_base_offset(base_offset.unwrap_or(0));
        Ok(())
    }

    async fn consume(
        self: Rc<Self>,
        params: barka_svc::ConsumeParams,
        mut results: barka_svc::ConsumeResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_request()?;
        let topic = req.get_topic()?.to_string()?;
        let partition = req.get_partition();
        let offset = req.get_offset();
        let max = req.get_max_records();

        let tp = (topic, partition);
        let partitions = self.partitions.lock().unwrap();
        let records = partitions
            .get(&tp)
            .map(|p| p.read(offset, max))
            .unwrap_or_default();

        let resp = results.get().get_response()?;
        let mut list = resp.init_records(records.len() as u32);
        for (i, rec) in records.iter().enumerate() {
            let mut entry = list.reborrow().get(i as u32);
            if let Some(ref k) = rec.key {
                entry.set_key(k);
            }
            entry.set_value(&rec.value);
            entry.set_offset(rec.offset);
            entry.set_timestamp(rec.timestamp);
        }
        Ok(())
    }
}
