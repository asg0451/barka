use std::net::SocketAddr;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::rpc::barka_capnp::barka_svc;

/// Cap'n Proto RPC client. Must be used within a tokio LocalSet.
pub struct BarkaClient {
    client: barka_svc::Client,
}

impl BarkaClient {
    pub async fn connect(addr: SocketAddr) -> crate::error::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let stream = stream.compat();
        let (reader, writer) = stream.split();

        let network = twoparty::VatNetwork::new(
            futures::io::BufReader::new(reader),
            futures::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        );

        let mut rpc = RpcSystem::new(Box::new(network), None);
        let client: barka_svc::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(async move {
            if let Err(e) = rpc.await {
                tracing::error!(%e, "rpc client error");
            }
        });

        Ok(Self { client })
    }

    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        values: Vec<Vec<u8>>,
    ) -> crate::error::Result<u64> {
        let mut req = self.client.produce_request();
        let mut builder = req.get().get_request()?;
        builder.set_topic(topic);
        builder.set_partition(partition);
        let mut records = builder.init_records(values.len() as u32);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for (i, val) in values.iter().enumerate() {
            let mut rec = records.reborrow().get(i as u32);
            rec.set_value(val);
            rec.set_timestamp(ts);
        }
        let response = req.send().promise.await?;
        Ok(response.get()?.get_response()?.get_base_offset())
    }

    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_records: u32,
    ) -> crate::error::Result<Vec<crate::log::record::Record>> {
        let mut req = self.client.consume_request();
        let mut builder = req.get().get_request()?;
        builder.set_topic(topic);
        builder.set_partition(partition);
        builder.set_offset(offset);
        builder.set_max_records(max_records);
        let response = req.send().promise.await?;
        let records = response.get()?.get_response()?.get_records()?;
        let mut out = Vec::with_capacity(records.len() as usize);
        for r in records.iter() {
            out.push(crate::log::record::Record {
                key: {
                    let k = r.get_key()?;
                    if k.is_empty() { None } else { Some(k.to_vec()) }
                },
                value: r.get_value()?.to_vec(),
                offset: r.get_offset(),
                timestamp: r.get_timestamp(),
            });
        }
        Ok(out)
    }
}
