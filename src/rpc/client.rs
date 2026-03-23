use std::net::SocketAddr;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::rpc::barka_capnp::{consume_svc, produce_svc};

/// Cap'n Proto RPC client for produce operations. Must be used within a tokio LocalSet.
pub struct ProduceClient {
    client: produce_svc::Client,
}

impl ProduceClient {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
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
        let client: produce_svc::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(async move {
            if let Err(e) = rpc.await {
                tracing::error!(%e, "produce rpc client error");
            }
        });

        Ok(Self { client })
    }

    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        values: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<crate::log::record::Record>> {
        let mut req = self.client.produce_request();
        let mut builder = req.get().get_request()?;
        builder.set_topic(topic);
        builder.set_partition(partition);
        let mut records = builder.init_records(values.len() as u32);
        for (i, val) in values.iter().enumerate() {
            let mut rec = records.reborrow().get(i as u32);
            rec.set_value(val);
        }
        let response = req.send().promise.await?;
        let resp_records = response.get()?.get_response()?.get_records()?;
        let n = resp_records.len() as usize;
        if n != values.len() {
            anyhow::bail!(
                "produce response record count mismatch: got {n}, sent {}",
                values.len()
            );
        }
        let mut out = Vec::with_capacity(n);
        for (i, value) in values.into_iter().enumerate() {
            let m = resp_records.get(i as u32);
            out.push(crate::log::record::Record {
                key: None,
                value,
                offset: m.get_offset(),
                timestamp: m.get_timestamp(),
            });
        }
        Ok(out)
    }
}

/// Cap'n Proto RPC client for consume operations. Must be used within a tokio LocalSet.
pub struct ConsumeClient {
    client: consume_svc::Client,
}

impl ConsumeClient {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
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
        let client: consume_svc::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(async move {
            if let Err(e) = rpc.await {
                tracing::error!(%e, "consume rpc client error");
            }
        });

        Ok(Self { client })
    }

    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_records: u32,
    ) -> anyhow::Result<Vec<crate::log::record::Record>> {
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
