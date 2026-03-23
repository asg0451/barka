use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use bytes::Bytes;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::rpc::barka_capnp::{consume_svc, produce_svc};
use crate::rpc::bytes_transport::{BytesVatNetwork, MessageBytesQueue};
use crate::segment::RecordData;

/// Zero-copy `slice_ref` with fallback: if `subset` doesn't point into `buf`
/// (e.g. capnp-rpc copied the response), fall back to a memcpy instead of
/// panicking.
fn slice_ref_or_copy(buf: &Bytes, subset: &[u8]) -> Bytes {
    let buf_start = buf.as_ptr() as usize;
    let buf_end = buf_start + buf.len();
    let sub_start = subset.as_ptr() as usize;
    let sub_end = sub_start + subset.len();
    if sub_start >= buf_start && sub_end <= buf_end {
        buf.slice_ref(subset)
    } else {
        Bytes::copy_from_slice(subset)
    }
}

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

/// Cap'n Proto RPC client for consume operations. Uses [`BytesVatNetwork`] so
/// that response key/value data can be zero-copy `slice_ref`'d into `Bytes`,
/// mirroring the produce server's approach for incoming requests.
///
/// Must be used within a tokio LocalSet.
pub struct ConsumeClient {
    client: consume_svc::Client,
    return_queue: MessageBytesQueue,
}

impl ConsumeClient {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let stream = stream.compat();
        let (reader, writer) = stream.split();

        let return_queue: MessageBytesQueue = Rc::new(RefCell::new(VecDeque::new()));
        let network = BytesVatNetwork::new(
            futures::io::BufReader::new(reader),
            futures::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
            None,
            Some(return_queue.clone()),
        );

        let mut rpc = RpcSystem::new(Box::new(network), None);
        let client: consume_svc::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);

        tokio::task::spawn_local(async move {
            if let Err(e) = rpc.await {
                tracing::error!(%e, "consume rpc client error");
            }
        });

        Ok(Self {
            client,
            return_queue,
        })
    }

    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_records: u32,
    ) -> anyhow::Result<Vec<RecordData>> {
        let mut req = self.client.consume_request();
        let mut builder = req.get().get_request()?;
        builder.set_topic(topic);
        builder.set_partition(partition);
        builder.set_offset(offset);
        builder.set_max_records(max_records);

        // Drain Returns from prior RPCs (e.g. the bootstrap handshake) so
        // that after the await the queue contains exactly our response.
        self.return_queue.borrow_mut().clear();
        let response = req.send().promise.await?;

        let raw = self
            .return_queue
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("missing return message bytes"))?;

        let records = response.get()?.get_response()?.get_records()?;
        let mut out = Vec::with_capacity(records.len() as usize);
        for r in records.iter() {
            let key_slice = r.get_key()?;
            let value_slice = r.get_value()?;
            out.push(RecordData {
                offset: r.get_offset(),
                timestamp: r.get_timestamp(),
                key: if key_slice.is_empty() {
                    Bytes::new()
                } else {
                    slice_ref_or_copy(&raw, key_slice)
                },
                value: if value_slice.is_empty() {
                    Bytes::new()
                } else {
                    slice_ref_or_copy(&raw, value_slice)
                },
            });
        }
        Ok(out)
    }
}
