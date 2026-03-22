use anyhow::Result;
use aws_sdk_s3::Client;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};

use crate::{
    rpc::barka_capnp::{produce_request, segment_data},
    s3::{self, S3Config},
};

struct ProducerInner {
    pending_records: usize,
    pending_bytes: usize,
    max_records: usize,
    max_bytes: usize,
    waiters: Vec<oneshot::Sender<Arc<FlushRound>>>,
}

impl ProducerInner {
    fn new(max_records: usize, max_bytes: usize) -> Self {
        Self {
            pending_records: 0,
            pending_bytes: 0,
            max_records,
            max_bytes,
            waiters: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.pending_records >= self.max_records || self.pending_bytes >= self.max_bytes
    }
}

/// Coordinates a single flush across all pending callers.
///
/// The round pre-allocates a capnp `SegmentData` builder with room for all
/// records. Each participant calls [`contribute`] with its still-live capnp
/// reader, writing records directly into the shared builder — the first and
/// only copy before the bytes hit the network. The last contributor is
/// elected flush leader and uploads the serialized message to S3.
struct FlushRound {
    segment: Mutex<capnp::message::Builder<capnp::message::HeapAllocator>>,
    cursor: AtomicUsize,
    remaining: AtomicUsize,
    done: watch::Sender<Option<Result<(), String>>>,
    bucket: String,
    key: String,
}

impl FlushRound {
    fn new(
        participants: usize,
        total_records: usize,
        epoch: u64,
        bucket: String,
        key: String,
    ) -> Self {
        let (done, _) = watch::channel(None);

        let mut builder = capnp::message::Builder::new_default();
        {
            let mut seg = builder.init_root::<segment_data::Builder>();
            seg.set_epoch(epoch);
            seg.init_records(total_records as u32);
        }

        Self {
            segment: Mutex::new(builder),
            cursor: AtomicUsize::new(0),
            remaining: AtomicUsize::new(participants),
            done,
            bucket,
            key,
        }
    }

    /// Write this caller's records directly into the shared [`SegmentData`]
    /// builder. Each record is read zero-copy from the caller's still-live
    /// RPC message buffer and written straight into the output message — the
    /// only copy, and it's building the bytes that will go on the wire.
    ///
    /// Returns `true` if this was the last contributor (flush leader).
    fn contribute(&self, request: produce_request::Reader<'_>) -> Result<bool> {
        let records = request.get_records()?;
        let count = records.len() as usize;
        let start = self.cursor.fetch_add(count, Ordering::AcqRel);

        {
            let mut builder = self.segment.lock().unwrap();
            let seg = builder.get_root::<segment_data::Builder>()?;
            let mut out = seg.get_records()?;
            for (i, rec) in records.iter().enumerate() {
                let mut entry = out.reborrow().get((start + i) as u32);
                entry.set_key(rec.get_key()?);
                entry.set_value(rec.get_value()?);
                entry.set_offset(rec.get_offset());
                entry.set_timestamp(rec.get_timestamp());
            }
        }

        Ok(self.remaining.fetch_sub(1, Ordering::AcqRel) == 1)
    }

    /// Serialize the completed [`SegmentData`] and upload to S3 with
    /// `if-none-match: *`.
    async fn do_flush(&self, s3_client: &Client) -> Result<()> {
        let bytes = {
            let builder = self.segment.lock().unwrap();
            capnp::serialize::write_message_to_words(&*builder)
        };

        let outcome = s3::put_if_absent(s3_client, &self.bucket, &self.key, bytes.into()).await?;

        if outcome == s3::PutOutcome::AlreadyExists {
            anyhow::bail!("segment {} already exists — sequence collision", self.key);
        }
        Ok(())
    }
}

async fn await_flush_done(mut rx: watch::Receiver<Option<Result<(), String>>>) -> Result<()> {
    rx.wait_for(|v| v.is_some())
        .await
        .map_err(|_| anyhow::anyhow!("flush round dropped"))?;
    match rx.borrow().as_ref() {
        Some(Ok(())) => Ok(()),
        Some(Err(e)) => Err(anyhow::anyhow!("{}", e)),
        None => unreachable!(),
    }
}

pub struct PartitionProducer {
    s3_client: Client,
    bucket: String,
    prefix: String,
    next_sequence: AtomicU64,
    inner: Mutex<ProducerInner>,
}

impl PartitionProducer {
    pub async fn new(s3_config: &S3Config, prefix: String) -> Self {
        let s3_client = crate::s3::build_client(s3_config).await;
        Self {
            bucket: s3_config.bucket.clone(),
            s3_client,
            prefix,
            next_sequence: AtomicU64::new(0),
            inner: Mutex::new(ProducerInner::new(100, 1024 * 1024)),
        }
    }

    fn create_flush_round(&self, participants: usize, total_records: usize) -> Arc<FlushRound> {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let key = format!("{}/{:020}", self.prefix, seq);
        Arc::new(FlushRound::new(
            participants,
            total_records,
            0, // epoch TBD
            self.bucket.clone(),
            key,
        ))
    }

    /// Add records from `request` to the current batch. Resolves once every
    /// record has been durably flushed to S3.
    ///
    /// No data is copied at call time — the batch tracks only metadata.
    /// When the batch is full (or [`flush`] is called), a [`FlushRound`]
    /// coordinates all pending callers: each writes its records directly
    /// from its still-live capnp reader into a shared [`SegmentData`]
    /// builder, then the leader serializes and uploads to S3.
    pub async fn apply_produce_request(&self, request: produce_request::Reader<'_>) -> Result<()> {
        let record_count = request.get_records()?.len() as usize;
        if record_count == 0 {
            return Ok(());
        }
        let byte_size = (request.total_size()?.word_count as usize) * 8;

        let round = {
            let mut inner = self.inner.lock().unwrap();
            inner.pending_records += record_count;
            inner.pending_bytes += byte_size;

            if inner.is_full() {
                let total_records = inner.pending_records;
                let participants = inner.waiters.len() + 1;
                let round = self.create_flush_round(participants, total_records);
                for tx in inner.waiters.drain(..) {
                    let _ = tx.send(Arc::clone(&round));
                }
                inner.pending_records = 0;
                inner.pending_bytes = 0;
                round
            } else {
                let (tx, rx) = oneshot::channel();
                inner.waiters.push(tx);
                drop(inner);
                rx.await
                    .map_err(|_| anyhow::anyhow!("producer dropped before flush"))?
            }
        };

        // Subscribe BEFORE contributing so we never miss the leader's signal.
        let done_rx = round.done.subscribe();
        let is_leader = round.contribute(request)?;

        if is_leader {
            let result = round.do_flush(&self.s3_client).await;
            let _ = round
                .done
                .send(Some(result.as_ref().map(|_| ()).map_err(|e| e.to_string())));
            result
        } else {
            await_flush_done(done_rx).await
        }
    }

    /// Flush the active batch immediately (e.g. on a timer or shutdown).
    /// Creates a [`FlushRound`] and waits for the participants to complete it.
    pub async fn flush(&self) -> Result<()> {
        let done_rx = {
            let mut inner = self.inner.lock().unwrap();
            if inner.waiters.is_empty() {
                return Ok(());
            }
            let total_records = inner.pending_records;
            let participants = inner.waiters.len();
            let round = self.create_flush_round(participants, total_records);
            let done_rx = round.done.subscribe();
            for tx in inner.waiters.drain(..) {
                let _ = tx.send(Arc::clone(&round));
            }
            inner.pending_records = 0;
            inner.pending_bytes = 0;
            done_rx
        };

        await_flush_done(done_rx).await
    }
}

// see
// https://github.com/slatedb/slatedb/blob/main/rfcs/0001-manifest.md#writer-protocol
// for inspo.
//
// when writing a file: condwrite in prefix/seqnum.dat. if we lost the race,
// that means we're split-brained. check the winner's file for the epoch number
// via range read, compare to ours, and kill ourselves if we're the lower.
//
// how do we know what sequence number to start with? for now let's just list
// them all to find the latest. if that one is taken by an older writer, try
// again at the next seq num. see the above link for scenarios
//
// TODO: in future we'll want to add a manifest file that tracks the latest
// sequence number, but that's a whole other can of worms.

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(
        n_records: usize,
        value_size: usize,
    ) -> capnp::message::Builder<capnp::message::HeapAllocator> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut req = message.init_root::<produce_request::Builder>();
            req.set_topic("test");
            req.set_partition(0);
            let mut records = req.init_records(n_records as u32);
            for i in 0..n_records {
                let mut r = records.reborrow().get(i as u32);
                r.set_key(format!("k{i}").as_bytes());
                r.set_value(&vec![b'x'; value_size]);
                r.set_timestamp(i as i64);
            }
        }
        message
    }

    async fn test_producer(max_records: usize, max_bytes: usize) -> PartitionProducer {
        let config = S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "test-producer".into(),
            region: "us-east-1".into(),
        };
        let s3_client = crate::s3::build_client(&config).await;
        s3::ensure_bucket(&s3_client, &config.bucket).await.unwrap();
        PartitionProducer {
            s3_client,
            bucket: config.bucket,
            prefix: format!(
                "test/{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            next_sequence: AtomicU64::new(0),
            inner: Mutex::new(ProducerInner::new(max_records, max_bytes)),
        }
    }

    /// Read back a segment from S3 and verify its record count and epoch.
    async fn read_segment(
        s3_client: &Client,
        bucket: &str,
        key: &str,
    ) -> capnp::Result<(u64, usize)> {
        let mut reader = s3::get_object_reader(s3_client, bucket, key).await.unwrap();
        let msg =
            capnp::serialize::read_message(&mut reader, capnp::message::ReaderOptions::default())?;
        let seg = msg.get_root::<segment_data::Reader>()?;
        Ok((seg.get_epoch(), seg.get_records()?.len() as usize))
    }

    #[tokio::test]
    async fn batch_full_flushes_immediately() {
        let producer = test_producer(3, 1024 * 1024).await;
        let msg = make_request(3, 10);
        let reader = msg.get_root_as_reader::<produce_request::Reader>().unwrap();
        producer.apply_produce_request(reader).await.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (epoch, count) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(epoch, 0);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn oversized_request_flushes_immediately() {
        let producer = test_producer(3, 1024 * 1024).await;
        let msg = make_request(5, 10);
        let reader = msg.get_root_as_reader::<produce_request::Reader>().unwrap();
        producer.apply_produce_request(reader).await.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, count) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn explicit_flush_unblocks_pending() {
        let producer = test_producer(5, 1024 * 1024).await;
        let msg = make_request(3, 10);
        let reader = msg.get_root_as_reader::<produce_request::Reader>().unwrap();
        let (a, b) = tokio::join!(producer.apply_produce_request(reader), producer.flush(),);
        a.unwrap();
        b.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, count) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn later_request_flushes_earlier_waiter() {
        let producer = test_producer(3, 1024 * 1024).await;

        let msg1 = make_request(2, 10);
        let r1 = msg1
            .get_root_as_reader::<produce_request::Reader>()
            .unwrap();
        let msg2 = make_request(2, 10);
        let r2 = msg2
            .get_root_as_reader::<produce_request::Reader>()
            .unwrap();

        let (a, b) = tokio::join!(
            producer.apply_produce_request(r1),
            producer.apply_produce_request(r2),
        );
        a.unwrap();
        b.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, count) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn sequence_increments_across_flushes() {
        let producer = test_producer(2, 1024 * 1024).await;

        let msg1 = make_request(2, 10);
        let r1 = msg1
            .get_root_as_reader::<produce_request::Reader>()
            .unwrap();
        producer.apply_produce_request(r1).await.unwrap();

        let msg2 = make_request(2, 10);
        let r2 = msg2
            .get_root_as_reader::<produce_request::Reader>()
            .unwrap();
        producer.apply_produce_request(r2).await.unwrap();

        let key0 = format!("{}/{:020}", producer.prefix, 0);
        let key1 = format!("{}/{:020}", producer.prefix, 1);
        let (_, c0) = read_segment(&producer.s3_client, &producer.bucket, &key0)
            .await
            .unwrap();
        let (_, c1) = read_segment(&producer.s3_client, &producer.bucket, &key1)
            .await
            .unwrap();
        assert_eq!(c0, 2);
        assert_eq!(c1, 2);
    }

    #[tokio::test]
    async fn empty_request_resolves_immediately() {
        let producer = test_producer(5, 1024 * 1024).await;
        let msg = make_request(0, 0);
        let reader = msg.get_root_as_reader::<produce_request::Reader>().unwrap();
        producer.apply_produce_request(reader).await.unwrap();
    }
}
