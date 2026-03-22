use anyhow::Result;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tracing::warn;

use crate::{
    rpc::barka_capnp::produce_request,
    s3::{self, S3Config},
    segment::{self, RecordData},
};

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
//
// TODO: handle offsets and timestamps in the records. offset could be the sequence number + record number inside it?

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
/// Each participant calls [`contribute`] with both the raw `Bytes` backing
/// the RPC message and the parsed capnp reader. Key/value data is extracted
/// as zero-copy `Bytes` sub-slices via `Bytes::slice_ref` — no copies at
/// all until the bytes hit S3. The last contributor is elected flush leader
/// and uploads via scatter-gather.
struct FlushRound {
    records: Mutex<Vec<RecordData>>,
    remaining: AtomicUsize,
    done: watch::Sender<Option<Result<(), String>>>,
    epoch: u64,
    bucket: String,
    key: String,
}

impl FlushRound {
    fn new(participants: usize, epoch: u64, bucket: String, key: String) -> Self {
        let (done, _) = watch::channel(None);
        Self {
            records: Mutex::new(Vec::new()),
            remaining: AtomicUsize::new(participants),
            done,
            epoch,
            bucket,
            key,
        }
    }

    /// Extract zero-copy `Bytes` sub-slices for each record's key/value data
    /// and collect them into the shared record list.
    ///
    /// Returns `true` if this was the last contributor (flush leader).
    #[tracing::instrument(skip(self, message_bytes, request), fields(key = %self.key))]
    fn contribute(
        &self,
        message_bytes: &Bytes,
        request: produce_request::Reader<'_>,
    ) -> Result<bool> {
        let records = request.get_records()?;
        let mut batch = Vec::with_capacity(records.len() as usize);

        for rec in records.iter() {
            let key_slice: &[u8] = rec.get_key()?;
            let value_slice: &[u8] = rec.get_value()?;

            let key = if key_slice.is_empty() {
                Bytes::new()
            } else {
                message_bytes.slice_ref(key_slice)
            };
            let value = if value_slice.is_empty() {
                Bytes::new()
            } else {
                message_bytes.slice_ref(value_slice)
            };

            batch.push(RecordData {
                offset: rec.get_offset(),
                timestamp: rec.get_timestamp(),
                key,
                value,
            });
        }

        {
            let mut all = self.records.lock().unwrap();
            all.extend(batch);
        }

        Ok(self.remaining.fetch_sub(1, Ordering::AcqRel) == 1)
    }

    /// Encode the collected records as a binary segment and upload to S3 via
    /// scatter-gather (zero additional copies).
    #[tracing::instrument(skip(self, s3_client), fields(key = %self.key, epoch = self.epoch))]
    async fn do_flush(&self, s3_client: &Client) -> Result<()> {
        let records = {
            let mut guard = self.records.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        let (chunks, total_len) = segment::encode_gather(self.epoch, &records);
        tracing::debug!(
            key = %self.key,
            bucket = %self.bucket,
            epoch = self.epoch,
            records = records.len(),
            chunks = chunks.len(),
            total_bytes = total_len,
            "uploading segment to S3",
        );
        let outcome =
            s3::put_if_absent_stream(s3_client, &self.bucket, &self.key, chunks, total_len).await?;

        if outcome == s3::PutOutcome::AlreadyExists {
            anyhow::bail!("segment {} already exists — sequence collision", self.key);
        }
        tracing::debug!(key = %self.key, "segment uploaded successfully");
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

const DEFAULT_MAX_RECORDS: usize = 100;
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_LINGER: Duration = Duration::from_millis(100);

pub struct PartitionProducer {
    s3_client: Client,
    bucket: String,
    prefix: String,
    next_sequence: AtomicU64,
    inner: Mutex<ProducerInner>,
    linger: Duration,
}

impl PartitionProducer {
    pub async fn new(s3_config: &S3Config, prefix: String) -> Arc<Self> {
        Self::with_opts(
            s3_config,
            prefix,
            DEFAULT_MAX_RECORDS,
            DEFAULT_MAX_BYTES,
            DEFAULT_LINGER,
        )
        .await
    }

    pub async fn with_opts(
        s3_config: &S3Config,
        prefix: String,
        max_records: usize,
        max_bytes: usize,
        linger: Duration,
    ) -> Arc<Self> {
        let s3_client = crate::s3::build_client(s3_config).await;
        let producer = Arc::new(Self {
            bucket: s3_config.bucket.clone(),
            s3_client,
            prefix,
            next_sequence: AtomicU64::new(0), // TODO: get from S3. add epoch too
            inner: Mutex::new(ProducerInner::new(max_records, max_bytes)),
            linger,
        });
        producer.spawn_flush_timer();
        producer
    }

    fn spawn_flush_timer(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let linger = self.linger;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(linger).await;
                let Some(producer) = weak.upgrade() else {
                    break;
                };
                if let Err(e) = producer.flush().await {
                    warn!(error = %e, "flush timer: flush failed");
                }
            }
        });
    }

    fn create_flush_round(&self, participants: usize) -> Arc<FlushRound> {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let key = format!("{}/{:020}", self.prefix, seq);
        Arc::new(FlushRound::new(
            participants,
            0, // epoch TBD
            self.bucket.clone(),
            key,
        ))
    }

    /// Add records from `request` to the current batch. Resolves once every
    /// record has been durably flushed to S3.
    ///
    /// `message_bytes` is the raw `Bytes` buffer backing the capnp reader.
    /// Key/value data is extracted as zero-copy sub-slices via
    /// `Bytes::slice_ref` — no copies between the TCP read and S3 upload.
    #[tracing::instrument(skip(self, message_bytes, request), fields(prefix = %self.prefix))]
    pub async fn apply_produce_request(
        &self,
        message_bytes: Bytes,
        request: produce_request::Reader<'_>,
    ) -> Result<()> {
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
                let participants = inner.waiters.len() + 1;
                let round = self.create_flush_round(participants);
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

        let done_rx = round.done.subscribe();
        let is_leader = round.contribute(&message_bytes, request)?;

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

    #[tracing::instrument(skip(self), fields(prefix = %self.prefix))]
    pub async fn flush(&self) -> Result<()> {
        let done_rx = {
            let mut inner = self.inner.lock().unwrap();
            if inner.waiters.is_empty() {
                return Ok(());
            }
            let participants = inner.waiters.len();
            let round = self.create_flush_round(participants);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// Holds a `Bytes`-backed capnp message reader, keeping the parsed segments
    /// alive so we can borrow a `produce_request::Reader` from it.
    struct TestMessage {
        bytes: Bytes,
        reader: capnp::message::Reader<capnp::serialize::BufferSegments<Bytes>>,
    }

    impl TestMessage {
        fn new(n_records: usize, value_size: usize) -> Self {
            let mut builder = capnp::message::Builder::new_default();
            {
                let mut req = builder.init_root::<produce_request::Builder>();
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
            let words = capnp::serialize::write_message_to_words(&builder);
            let bytes = Bytes::from(words);
            let segments =
                capnp::serialize::BufferSegments::new(bytes.clone(), Default::default()).unwrap();
            let reader = capnp::message::Reader::new(segments, Default::default());
            Self { bytes, reader }
        }

        fn bytes(&self) -> &Bytes {
            &self.bytes
        }

        fn request(&self) -> produce_request::Reader<'_> {
            self.reader
                .get_root::<produce_request::Reader<'_>>()
                .unwrap()
        }
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
                "test/{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
            ),
            next_sequence: AtomicU64::new(0),
            inner: Mutex::new(ProducerInner::new(max_records, max_bytes)),
            linger: DEFAULT_LINGER,
        }
    }

    async fn read_segment(
        s3_client: &Client,
        bucket: &str,
        key: &str,
    ) -> Result<(u64, Vec<RecordData>)> {
        use std::io::Read;
        let mut reader = s3::get_object_reader(s3_client, bucket, key).await?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        segment::decode(&buf)
    }

    #[tokio::test]
    async fn batch_full_flushes_immediately() {
        let producer = test_producer(3, 1024 * 1024).await;
        let msg = TestMessage::new(3, 10);
        producer
            .apply_produce_request(msg.bytes().clone(), msg.request())
            .await
            .unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (epoch, records) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(epoch, 0);
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn oversized_request_flushes_immediately() {
        let producer = test_producer(3, 1024 * 1024).await;
        let msg = TestMessage::new(5, 10);
        producer
            .apply_produce_request(msg.bytes().clone(), msg.request())
            .await
            .unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, records) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(records.len(), 5);
    }

    #[tokio::test]
    async fn explicit_flush_unblocks_pending() {
        let producer = test_producer(5, 1024 * 1024).await;
        let msg = TestMessage::new(3, 10);
        let (a, b) = tokio::join!(
            producer.apply_produce_request(msg.bytes().clone(), msg.request()),
            producer.flush(),
        );
        a.unwrap();
        b.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, records) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn later_request_flushes_earlier_waiter() {
        let producer = test_producer(3, 1024 * 1024).await;

        let m1 = TestMessage::new(2, 10);
        let m2 = TestMessage::new(2, 10);

        let (a, b) = tokio::join!(
            producer.apply_produce_request(m1.bytes().clone(), m1.request()),
            producer.apply_produce_request(m2.bytes().clone(), m2.request()),
        );
        a.unwrap();
        b.unwrap();

        let key = format!("{}/{:020}", producer.prefix, 0);
        let (_, records) = read_segment(&producer.s3_client, &producer.bucket, &key)
            .await
            .unwrap();
        assert_eq!(records.len(), 4);
    }

    #[tokio::test]
    async fn sequence_increments_across_flushes() {
        let producer = test_producer(2, 1024 * 1024).await;

        let m1 = TestMessage::new(2, 10);
        producer
            .apply_produce_request(m1.bytes().clone(), m1.request())
            .await
            .unwrap();

        let m2 = TestMessage::new(2, 10);
        producer
            .apply_produce_request(m2.bytes().clone(), m2.request())
            .await
            .unwrap();

        let key0 = format!("{}/{:020}", producer.prefix, 0);
        let key1 = format!("{}/{:020}", producer.prefix, 1);
        let (_, r0) = read_segment(&producer.s3_client, &producer.bucket, &key0)
            .await
            .unwrap();
        let (_, r1) = read_segment(&producer.s3_client, &producer.bucket, &key1)
            .await
            .unwrap();
        assert_eq!(r0.len(), 2);
        assert_eq!(r1.len(), 2);
    }

    #[tokio::test]
    async fn empty_request_resolves_immediately() {
        let producer = test_producer(5, 1024 * 1024).await;
        let msg = TestMessage::new(0, 0);
        producer
            .apply_produce_request(msg.bytes().clone(), msg.request())
            .await
            .unwrap();
    }
}
