//! S3-backed segment consumer with a two-tier (memory + disk) LRU cache.
//!
//! All S3 and disk I/O runs on a dedicated background thread with its own
//! tokio runtime, keeping the `!Send` capnp-rpc `LocalSet` non-blocking.
//! The RPC handler communicates with the I/O thread via channels.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use lru::LruCache;
use tokio::sync::{mpsc, oneshot};
use tracing::{Instrument, debug, info, warn};

use crate::log_offset;
use crate::s3::{self, S3Config};
use crate::segment::{self, RecordData};

/// Configuration knobs for [`PartitionConsumer`].
#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    /// Max segments kept decoded in the memory tier.
    pub mem_cache_max_segments: usize,
    /// Max segments kept as files in the disk tier.
    pub disk_cache_max_segments: usize,
    /// Segment byte-size threshold: segments larger than this are cached on
    /// disk instead of in memory.
    pub disk_threshold_bytes: usize,
    /// Directory for disk-cached segment files.
    pub cache_dir: PathBuf,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            mem_cache_max_segments: 256,
            disk_cache_max_segments: 64,
            disk_threshold_bytes: 1024 * 1024, // 1 MiB
            cache_dir: std::env::temp_dir().join("barka-segment-cache"),
        }
    }
}

// ---------------------------------------------------------------------------
// I/O thread request/response protocol
// ---------------------------------------------------------------------------

type FetchResult = Result<Option<Arc<Vec<RecordData>>>>;

struct FetchRequest {
    segment_seq: u64,
    /// `None` for fire-and-forget prefetch requests.
    reply: Option<oneshot::Sender<FetchResult>>,
}

// ---------------------------------------------------------------------------
// PartitionConsumer (public handle)
// ---------------------------------------------------------------------------

/// Backpressure limit on outstanding fetch requests to the I/O thread.
const FETCH_CHANNEL_CAPACITY: usize = 64;

/// Max number of consecutive missing segments to probe past before assuming
/// the consumer has caught up to the producer. Failed flushes (e.g. leadership
/// changes) burn a segment sequence number without writing to S3, creating gaps.
const MAX_SEGMENT_GAP: u64 = 64;

pub struct PartitionConsumer {
    /// Wrapped in `Option` so `Drop` can close the channel before joining the thread.
    request_tx: Option<mpsc::Sender<FetchRequest>>,
    io_thread: Option<std::thread::JoinHandle<()>>,
}

impl PartitionConsumer {
    #[tracing::instrument(skip(s3_config), fields(%prefix))]
    pub async fn new(
        s3_config: &S3Config,
        prefix: String,
        config: ConsumerConfig,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.cache_dir)?;

        let s3_config = s3_config.clone();
        let (tx, rx) = mpsc::channel(FETCH_CHANNEL_CAPACITY);

        let io_thread = std::thread::Builder::new()
            .name(format!("consumer-io-{prefix}"))
            .spawn({
                let prefix = prefix.clone();
                move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("consumer I/O runtime");
                    rt.block_on(
                        io_loop(s3_config, prefix, config, rx)
                            .instrument(tracing::info_span!("consumer_io")),
                    );
                }
            })?;

        info!(%prefix, "partition consumer started");
        Ok(Arc::new(Self {
            request_tx: Some(tx),
            io_thread: Some(io_thread),
        }))
    }

    /// Consume up to `max_records` starting from the composite `offset`.
    ///
    /// Reads across segment boundaries as needed. Returns fewer records (or
    /// an empty vec) when the consumer is ahead of the producer.
    #[tracing::instrument(skip(self), fields(offset_hex = format!("{offset:#x}")))]
    pub async fn consume(&self, offset: u64, max_records: u32) -> Result<Vec<RecordData>> {
        let mut seg_seq = log_offset::segment(offset);
        let mut intra = log_offset::intra(offset) as usize;
        let max = max_records as usize;
        let mut result = Vec::with_capacity(max.min(1024));

        while result.len() < max {
            let records = match self.fetch_segment(seg_seq).await? {
                Some(r) => r,
                None => {
                    // Probe ahead with exponential steps: failed flushes can skip
                    // segment sequence numbers, creating gaps.
                    let mut found = None;
                    let mut probe = 1u64;
                    while probe <= MAX_SEGMENT_GAP {
                        if let Some(r) = self.fetch_segment(seg_seq + probe).await? {
                            debug!(seg_seq, gap = probe, "skipping segment gap");
                            seg_seq += probe;
                            intra = 0;
                            found = Some(r);
                            break;
                        }
                        probe *= 2;
                    }
                    match found {
                        Some(r) => r,
                        None => {
                            debug!(seg_seq, "no segments found — caught up to producer");
                            break;
                        }
                    }
                }
            };

            if intra >= records.len() {
                // Offset past end of this segment; try next.
                seg_seq += 1;
                intra = 0;
                continue;
            }

            let remaining = max - result.len();
            let end = (intra + remaining).min(records.len());
            result.extend_from_slice(&records[intra..end]);

            // Prefetch heuristic: if we read past 75% of the segment, warm up the next one.
            if end * 4 >= records.len() * 3 {
                self.prefetch(seg_seq + 1);
            }

            if end >= records.len() && result.len() < max {
                seg_seq += 1;
                intra = 0;
            } else {
                break;
            }
        }

        debug!(
            returned = result.len(),
            start_seg = log_offset::segment(offset),
            end_seg = seg_seq,
            "consume complete",
        );
        Ok(result)
    }

    /// Request a segment from the I/O thread and await the result.
    async fn fetch_segment(&self, seg_seq: u64) -> Result<Option<Arc<Vec<RecordData>>>> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("consumer shut down"))?
            .send(FetchRequest {
                segment_seq: seg_seq,
                reply: Some(tx),
            })
            .await
            .map_err(|_| anyhow::anyhow!("consumer I/O thread gone"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("consumer I/O thread dropped reply"))?
    }

    /// Fire-and-forget prefetch. The I/O thread fetches and caches the segment
    /// but discards the result (no reply channel).
    fn prefetch(&self, seg_seq: u64) {
        if let Some(tx) = self.request_tx.as_ref() {
            let _ = tx.try_send(FetchRequest {
                segment_seq: seg_seq,
                reply: None,
            });
        }
    }
}

impl Drop for PartitionConsumer {
    fn drop(&mut self) {
        // Close the channel first so the I/O loop sees `None` from `recv()` and exits.
        self.request_tx.take();
        if let Some(handle) = self.io_thread.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// I/O thread internals
// ---------------------------------------------------------------------------

struct IoState {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    config: ConsumerConfig,
    mem_cache: LruCache<u64, Arc<Vec<RecordData>>>,
    disk_cache: LruCache<u64, PathBuf>,
}

impl IoState {
    fn new(
        s3_client: aws_sdk_s3::Client,
        bucket: String,
        prefix: String,
        config: ConsumerConfig,
    ) -> Self {
        let mem_cap = NonZeroUsize::new(config.mem_cache_max_segments.max(1)).unwrap();
        let disk_cap = NonZeroUsize::new(config.disk_cache_max_segments.max(1)).unwrap();
        Self {
            s3_client,
            bucket,
            prefix,
            config,
            mem_cache: LruCache::new(mem_cap),
            disk_cache: LruCache::new(disk_cap),
        }
    }

    #[tracing::instrument(skip(self), fields(seg_seq))]
    async fn handle_fetch(&mut self, seg_seq: u64) -> Result<Option<Arc<Vec<RecordData>>>> {
        // Tier 1: memory cache hit
        if let Some(records) = self.mem_cache.get(&seg_seq) {
            debug!(seg_seq, "memory cache hit");
            return Ok(Some(Arc::clone(records)));
        }

        // Tier 2: disk cache hit — read from file and decode.
        // We intentionally skip promoting to the memory tier: these segments exceeded
        // `disk_threshold_bytes`, so keeping decoded `Vec<RecordData>` in RAM would
        // defeat the point of spilling. The decode cost is paid on every hit; the
        // kernel page cache absorbs most of the I/O cost for repeat reads.
        if let Some(path) = self.disk_cache.get(&seg_seq).cloned() {
            if path.exists() {
                debug!(seg_seq, path = %path.display(), "disk cache hit");
                let buf = tokio::fs::read(&path).await?;
                let (_epoch, records) = segment::decode(Bytes::from(buf))?;
                return Ok(Some(Arc::new(records)));
            }
            // Stale entry — file was removed externally.
            self.disk_cache.pop(&seg_seq);
        }

        // Cache miss: fetch from S3
        let key = format!("{}/{:020}.dat", self.prefix, seg_seq);
        debug!(seg_seq, %key, "S3 fetch");

        let buf = s3::get_object_bytes_if_present(&self.s3_client, &self.bucket, &key).await?;
        let Some(buf) = buf else {
            return Ok(None);
        };
        let raw_len = buf.len();

        let (_epoch, records) = segment::decode(buf.clone())?;
        let records = Arc::new(records);

        // Classify by size and cache appropriately
        if raw_len <= self.config.disk_threshold_bytes {
            debug!(seg_seq, raw_len, "caching in memory tier");
            self.mem_cache.push(seg_seq, Arc::clone(&records));
        } else {
            debug!(seg_seq, raw_len, "caching in disk tier");
            let path = self.config.cache_dir.join(format!("{:020}.dat", seg_seq));
            tokio::fs::write(&path, &buf).await?;
            if let Some((_evicted_seq, evicted_path)) = self.disk_cache.push(seg_seq, path)
                && let Err(e) = tokio::fs::remove_file(&evicted_path).await
            {
                warn!(path = %evicted_path.display(), error = %e, "failed to remove evicted segment file");
            }
        }

        Ok(Some(records))
    }
}

impl Drop for IoState {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.config.cache_dir);
    }
}

#[tracing::instrument(skip_all, fields(%prefix))]
async fn io_loop(
    s3_config: S3Config,
    prefix: String,
    config: ConsumerConfig,
    mut rx: mpsc::Receiver<FetchRequest>,
) {
    let s3_client = s3::build_client(&s3_config).await;
    let bucket = s3_config.bucket.clone();
    let mut state = IoState::new(s3_client, bucket, prefix, config);

    info!("consumer I/O loop started");

    while let Some(req) = rx.recv().await {
        let seg_seq = req.segment_seq;
        let result = state
            .handle_fetch(seg_seq)
            .instrument(tracing::debug_span!("fetch_segment", seg_seq))
            .await;

        if let Some(reply) = req.reply {
            let _ = reply.send(result);
        }
    }

    info!("consumer I/O loop exiting");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_offset::compose;
    use crate::produce_node::LeadershipState;
    use crate::producer::PartitionProducer;
    use crate::test_util::TestMessage;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn test_s3_config() -> S3Config {
        S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "test-consumer".into(),
            region: "us-east-1".into(),
        }
    }

    fn unique_prefix() -> String {
        format!(
            "test-consumer/{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        )
    }

    fn test_consumer_config() -> ConsumerConfig {
        ConsumerConfig {
            mem_cache_max_segments: 4,
            disk_cache_max_segments: 2,
            disk_threshold_bytes: 1024 * 1024,
            cache_dir: std::env::temp_dir().join("barka-test-cache").join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
            )),
        }
    }

    async fn produce_segment(
        producer: &PartitionProducer,
        n_records: usize,
        value_prefix: &str,
    ) -> Vec<crate::producer::ProducedRecord> {
        let msg = TestMessage::new(n_records, value_prefix);
        producer
            .apply_produce_request(msg.bytes().clone(), msg.request(), 0)
            .await
            .unwrap()
    }

    async fn setup() -> (Arc<PartitionProducer>, Arc<PartitionConsumer>, String) {
        let s3_config = test_s3_config();
        let s3_client = s3::build_client(&s3_config).await;
        s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let prefix = unique_prefix();
        let leadership = Arc::new(LeadershipState::new());
        leadership.set_leader(u64::MAX, 0);
        let producer = PartitionProducer::with_opts(
            &s3_config,
            prefix.clone(),
            3, // max_records = 3 so each produce fills a segment
            1024 * 1024,
            std::time::Duration::from_secs(60),
            Arc::clone(&leadership),
        )
        .await
        .unwrap();

        let consumer = PartitionConsumer::new(&s3_config, prefix.clone(), test_consumer_config())
            .await
            .unwrap();

        (producer, consumer, prefix)
    }

    #[tokio::test]
    async fn consume_single_segment() {
        let (producer, consumer, _) = setup().await;
        let produced = produce_segment(&producer, 3, "val").await;
        assert_eq!(produced.len(), 3);

        let records = consumer.consume(compose(0, 0), 10).await.unwrap();
        assert_eq!(records.len(), 3);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.offset, compose(0, i as u64));
            assert_eq!(std::str::from_utf8(&rec.value).unwrap(), format!("val-{i}"));
        }
    }

    #[tokio::test]
    async fn consume_across_segments() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "seg0").await;
        produce_segment(&producer, 3, "seg1").await;

        // Consume starting from segment 0, requesting all 6 records
        let records = consumer.consume(compose(0, 0), 10).await.unwrap();
        assert_eq!(records.len(), 6);
        assert_eq!(std::str::from_utf8(&records[0].value).unwrap(), "seg0-0");
        assert_eq!(std::str::from_utf8(&records[3].value).unwrap(), "seg1-0");
    }

    #[tokio::test]
    async fn consume_ahead_of_producer() {
        let (_producer, consumer, _) = setup().await;
        let records = consumer.consume(compose(99, 0), 10).await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn consume_with_intra_offset() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "val").await;

        // Start from intra=1, should get records 1 and 2
        let records = consumer.consume(compose(0, 1), 10).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(std::str::from_utf8(&records[0].value).unwrap(), "val-1");
        assert_eq!(std::str::from_utf8(&records[1].value).unwrap(), "val-2");
    }

    #[tokio::test]
    async fn consume_respects_max_records() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "val").await;

        let records = consumer.consume(compose(0, 0), 2).await.unwrap();
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn consume_across_segment_boundary_with_intra() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "seg0").await;
        produce_segment(&producer, 3, "seg1").await;

        // Start from last record of segment 0, read 3 records → should cross into segment 1
        let records = consumer.consume(compose(0, 2), 3).await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(std::str::from_utf8(&records[0].value).unwrap(), "seg0-2");
        assert_eq!(std::str::from_utf8(&records[1].value).unwrap(), "seg1-0");
        assert_eq!(std::str::from_utf8(&records[2].value).unwrap(), "seg1-1");
    }

    #[tokio::test]
    async fn disk_cache_eviction_deletes_files() {
        let s3_config = test_s3_config();
        let s3_client = s3::build_client(&s3_config).await;
        s3::ensure_bucket(&s3_client, &s3_config.bucket)
            .await
            .unwrap();

        let prefix = unique_prefix();
        let leadership = Arc::new(LeadershipState::new());
        leadership.set_leader(u64::MAX, 0);
        let producer = PartitionProducer::with_opts(
            &s3_config,
            prefix.clone(),
            3,
            1024 * 1024,
            std::time::Duration::from_secs(60),
            Arc::clone(&leadership),
        )
        .await
        .unwrap();

        let mut config = test_consumer_config();
        // Force everything to disk tier by setting threshold to 0.
        // disk_cache=2 means segment 0's file gets evicted when segment 2 arrives.
        config.disk_threshold_bytes = 0;
        config.disk_cache_max_segments = 2;
        config.mem_cache_max_segments = 1;

        let consumer = PartitionConsumer::new(&s3_config, prefix.clone(), config.clone())
            .await
            .unwrap();

        // Produce 3 segments
        produce_segment(&producer, 3, "s0").await;
        produce_segment(&producer, 3, "s1").await;
        produce_segment(&producer, 3, "s2").await;

        // Consume each segment individually so they each go through handle_fetch
        consumer.consume(compose(0, 0), 3).await.unwrap();
        consumer.consume(compose(1, 0), 3).await.unwrap();
        consumer.consume(compose(2, 0), 3).await.unwrap();

        let seg0_path = config.cache_dir.join(format!("{:020}.dat", 0));
        let seg1_path = config.cache_dir.join(format!("{:020}.dat", 1));
        let seg2_path = config.cache_dir.join(format!("{:020}.dat", 2));

        // Segments 1 and 2 should still be on disk (disk cache capacity = 2)
        assert!(
            seg1_path.exists(),
            "segment 1 should still be cached on disk"
        );
        assert!(
            seg2_path.exists(),
            "segment 2 should still be cached on disk"
        );

        // Segment 0 was evicted from disk cache when segment 2 was inserted
        assert!(
            !seg0_path.exists(),
            "segment 0 should have been evicted and its file deleted"
        );
    }

    #[tokio::test]
    async fn consume_intra_past_segment_end_snaps_forward() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "seg0").await;
        produce_segment(&producer, 3, "seg1").await;

        // intra=100 is way past segment 0's 3 records — should skip to segment 1
        let records = consumer.consume(compose(0, 100), 10).await.unwrap();
        assert_eq!(records.len(), 3, "should get all of segment 1");
        assert_eq!(std::str::from_utf8(&records[0].value).unwrap(), "seg1-0");
        assert_eq!(std::str::from_utf8(&records[1].value).unwrap(), "seg1-1");
        assert_eq!(std::str::from_utf8(&records[2].value).unwrap(), "seg1-2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn consume_concurrent_calls() {
        let (producer, consumer, _) = setup().await;
        produce_segment(&producer, 3, "seg0").await;
        produce_segment(&producer, 3, "seg1").await;

        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = Arc::clone(&consumer);
            handles.push(tokio::spawn(async move {
                c.consume(compose(0, 0), 10).await.unwrap()
            }));
        }

        for h in handles {
            let records = h.await.unwrap();
            assert_eq!(records.len(), 6);
            assert_eq!(std::str::from_utf8(&records[0].value).unwrap(), "seg0-0");
            assert_eq!(std::str::from_utf8(&records[5].value).unwrap(), "seg1-2");
        }
    }

    #[tokio::test]
    async fn prefetch_nonexistent_segment_is_harmless() {
        let (producer, consumer, _) = setup().await;
        // Only one segment exists; consuming past 75% triggers prefetch of segment 1
        // which doesn't exist. This must not error.
        produce_segment(&producer, 3, "val").await;

        let records = consumer.consume(compose(0, 0), 10).await.unwrap();
        assert_eq!(records.len(), 3);

        // Explicitly prefetch a segment that definitely doesn't exist
        consumer.prefetch(999);

        // A subsequent normal consume still works fine
        let records = consumer.consume(compose(0, 0), 10).await.unwrap();
        assert_eq!(records.len(), 3);
    }
}
