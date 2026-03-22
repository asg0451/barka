// based on https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::s3::{self, PutOutcome, S3Config};

#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    /// The timestamp (ms since epoch) until which the lock is valid, unless manually expired.
    valid_until_ms: u64,
    /// True if the lock file was manually expired by the node.
    expired: bool,
    /// The node ID of the leader.
    node_id: u64,
    // epoch is encoded in the file key
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Epoch(u64);

impl Epoch {
    // format: {prefix}/{epoch}.lock
    fn from_key(key: &str) -> Result<Self> {
        let epoch = key
            .split('/')
            .last()
            .unwrap()
            .split('.')
            .next()
            .ok_or_else(|| anyhow::anyhow!("no epoch in key: {key}"))?
            .parse::<u64>()
            .with_context(|| anyhow::anyhow!("invalid epoch in key: {key}"))?;
        Ok(Self(epoch))
    }
    fn to_key(self, prefix: &str) -> String {
        format!("{}/{:010}.lock", prefix.trim_end_matches("/"), self.0)
    }
    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug)]
pub enum TryBecomeLeaderResult {
    NotLeader,
    /// The current node is the leader.
    Leader(LeadershipInfo),
}

#[derive(Debug)]
pub struct LeadershipInfo {
    pub valid_until_ms: u64,
    pub epoch: Epoch,
}

#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    pub node_id: u64,
    pub namespace: String,
    pub s3_config: S3Config,
    pub validity_millis: Option<u64>,
}

pub struct LeaderElection {
    node_id: u64,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

const LOCK_FILE_PREFIX: &str = "lock/";
const VALIDITY_MILLIS: u64 = 10_000;

impl LeaderElection {
    pub async fn new(config: LeaderElectionConfig) -> Self {
        let s3_client = s3::build_client(&config.s3_config).await;
        let prefix = format!(
            "{}/{}/",
            LOCK_FILE_PREFIX.trim_end_matches("/"),
            &config.namespace,
        );
        tracing::debug!(
            node_id = config.node_id,
            namespace = %config.namespace,
            "leader election initialized"
        );
        Self {
            node_id: config.node_id,
            s3_client,
            bucket: config.s3_config.bucket.clone(),
            prefix,
        }
    }

    // algorithm:
    // 1. List all lock files
    // 2. If there is no lock file, or the latest one has expired:
    //    3. Increment the epoch value by 1 and try to create a new lock file
    //    4. If the lock file could be created:
    //       5. The current node is the leader, start with the actual work
    //    6. Otherwise, go back to 1.
    // 7. Otherwise, another process already is the leader, so do nothing.
    //    Go back to 1. periodically
    // also we probably want a background process deleting old lock files..
    #[tracing::instrument(skip_all, fields(node_id = self.node_id, prefix = self.prefix, bucket = self.bucket), err, ret)]
    pub async fn try_become_leader(&self) -> Result<TryBecomeLeaderResult> {
        let contents = s3::list_objects(&self.s3_client, &self.bucket, &self.prefix).await?;

        let mut last_epoch = None;
        if !contents.is_empty() {
            let newest = contents.last().unwrap();
            let key = newest.key().unwrap();
            last_epoch = Some(Epoch::from_key(key)?);
            let reader = s3::get_object_reader(&self.s3_client, &self.bucket, key).await?;
            let lock_file: LockFile =
                serde_json::from_reader(reader).context("deserialize lock file")?;
            // TODO: check if valid_until_ms is in the past too
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if !lock_file.expired && lock_file.valid_until_ms > now_ms {
                if lock_file.node_id == self.node_id {
                    tracing::debug!(
                        valid_until_ms = lock_file.valid_until_ms,
                        now_ms,
                        "still leader"
                    );
                    return Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                        valid_until_ms: lock_file.valid_until_ms,
                        epoch: last_epoch.unwrap(),
                    }));
                }
                tracing::debug!(
                    leader_node_id = lock_file.node_id,
                    valid_until_ms = lock_file.valid_until_ms,
                    now_ms,
                    "not becoming leader: lock file not expired"
                );
                return Ok(TryBecomeLeaderResult::NotLeader);
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let valid_until_ms = now_ms + VALIDITY_MILLIS;

        let lock_file_body = serde_json::to_string(&LockFile {
            valid_until_ms,
            expired: false,
            node_id: self.node_id,
        })
        .unwrap();

        let epoch = last_epoch.unwrap_or(Epoch(0)).next();
        let key = epoch.to_key(&self.prefix);

        let old_keys: Vec<String> = contents
            .iter()
            .filter_map(|obj| obj.key().map(str::to_owned))
            .collect();

        match s3::put_if_absent(&self.s3_client, &self.bucket, &key, lock_file_body).await? {
            PutOutcome::Created => {
                tracing::info!(epoch = epoch.0, "became leader");
                // clean up old lock files
                if !old_keys.is_empty() {
                    let client = self.s3_client.clone();
                    let bucket = self.bucket.clone();
                    tokio::spawn(async move {
                        if let Err(e) = s3::delete_objects(&client, &bucket, old_keys).await {
                            tracing::warn!(error = %e, "failed to clean up old lock files");
                        }
                    });
                }
                Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                    valid_until_ms,
                    epoch,
                }))
            }
            PutOutcome::AlreadyExists => {
                tracing::debug!("lock file already exists, not becoming leader");
                Ok(TryBecomeLeaderResult::NotLeader)
            }
        }
    }

    /// Release leadership by marking the lock file as expired. Other nodes will
    /// see the expired flag and can immediately start a new election instead of
    /// waiting for `valid_until_ms` to pass. It is not safe to call abdicate if
    /// you do not think you are the leader.
    #[tracing::instrument(skip_all, fields(node_id = self.node_id, epoch = epoch.0), err)]
    pub async fn abdicate(&self, epoch: Epoch) -> Result<()> {
        let key = epoch.to_key(&self.prefix);
        let body = serde_json::to_string(&LockFile {
            valid_until_ms: 0,
            expired: true,
            node_id: self.node_id,
        })
        .unwrap();
        s3::put_object(&self.s3_client, &self.bucket, &key, body).await?;
        tracing::info!("released leadership");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{self, S3Config};

    const LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";

    async fn require_localstack() {
        static CHECKED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        if CHECKED.get().is_some() {
            return;
        }
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::net::TcpStream::connect("127.0.0.1:4566"),
        )
        .await
        {
            Ok(Ok(_)) => {
                CHECKED.get_or_init(|| ());
            }
            _ => panic!(
                "LocalStack not reachable at {LOCALSTACK_ENDPOINT}. \
                 Start it with: docker run -d -p 4566:4566 localstack/localstack"
            ),
        }
    }

    fn localstack_config(bucket: &str) -> S3Config {
        S3Config {
            endpoint_url: Some(LOCALSTACK_ENDPOINT.into()),
            bucket: bucket.into(),
            region: "us-east-1".into(),
        }
    }

    fn unique_name(prefix: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{n}")
    }

    #[tokio::test]
    async fn test_become_leader_empty_bucket() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            namespace: "test-ns".into(),
            s3_config: config.clone(),
            validity_millis: Some(10_000),
        })
        .await;
        let result = le.try_become_leader().await.unwrap();

        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(_)),
            "first node should become leader on empty bucket"
        );
        if let TryBecomeLeaderResult::Leader(info) = result {
            assert!(info.valid_until_ms > 0);
        }
    }

    #[tokio::test]
    async fn test_second_node_not_leader() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le1 = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            namespace: ns.clone(),
            s3_config: config.clone(),
            validity_millis: None,
        })
        .await;
        let result = le1.try_become_leader().await.unwrap();
        assert!(matches!(result, TryBecomeLeaderResult::Leader(_)));

        let le2 = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            namespace: ns.clone(),
            s3_config: config.clone(),
            validity_millis: None,
        })
        .await;
        let result = le2.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::NotLeader),
            "second node should not become leader"
        );
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn lock_prefix(namespace: &str) -> String {
        format!("{}{}/", LOCK_FILE_PREFIX, namespace)
    }

    /// Unconditionally write a lock file to S3, bypassing the leader election
    /// protocol. Used to set up preconditions (e.g. expired locks).
    async fn write_lock_file_direct(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        namespace: &str,
        epoch: Epoch,
        lock_file: &LockFile,
    ) {
        use aws_sdk_s3::primitives::{ByteStream, SdkBody};
        let key = epoch.to_key(&lock_prefix(namespace));
        let body = serde_json::to_string(lock_file).unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(SdkBody::from(body)))
            .send()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_same_leader_not_reacquired_while_valid() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(matches!(result, TryBecomeLeaderResult::Leader(_)));

        let result = le.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(_)),
            "same node should still be recognized as leader: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_leader_acquired_after_time_expiry() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(1),
            &LockFile {
                valid_until_ms: now_ms() - 60_000,
                expired: false,
                node_id: 42,
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(ref info) if info.epoch == Epoch(2)),
            "should become leader at epoch 2 after time-based expiry: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_leader_acquired_after_manual_expiry() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(5),
            &LockFile {
                valid_until_ms: now_ms() + 600_000,
                expired: true,
                node_id: 42,
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 3,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(ref info) if info.epoch == Epoch(6)),
            "should become leader at epoch 6 after manual expiry: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_epoch_advances_past_highest_lock() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        for e in 1..=4u64 {
            write_lock_file_direct(
                &client,
                &bucket,
                &ns,
                Epoch(e),
                &LockFile {
                    valid_until_ms: now_ms() - 1_000,
                    expired: false,
                    node_id: 99,
                },
            )
            .await;
        }

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 7,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(ref info) if info.epoch == Epoch(5)),
            "should create epoch 5 (one past the highest existing lock): {result:?}"
        );
    }

    #[tokio::test]
    async fn test_abdicate_expires_lock() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le1 = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            namespace: ns.clone(),
            s3_config: config.clone(),
            validity_millis: None,
        })
        .await;

        let info = match le1.try_become_leader().await.unwrap() {
            TryBecomeLeaderResult::Leader(info) => info,
            other => panic!("expected Leader, got {other:?}"),
        };

        le1.abdicate(info.epoch).await.unwrap();

        // Verify the on-disk lock file has expired=true
        let key = info.epoch.to_key(&lock_prefix(&ns));
        let reader = s3::get_object_reader(&client, &bucket, &key).await.unwrap();
        let lock_file: LockFile = serde_json::from_reader(reader).unwrap();
        assert!(lock_file.expired, "lock file should be marked expired");

        // Another node should be able to take over immediately
        let le2 = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le2.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(ref i) if i.epoch == info.epoch.next()),
            "node 2 should become leader at next epoch after release: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_old_lock_files_cleaned_up_after_election() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        for e in 1..=3u64 {
            write_lock_file_direct(
                &client,
                &bucket,
                &ns,
                Epoch(e),
                &LockFile {
                    valid_until_ms: now_ms() - 60_000,
                    expired: false,
                    node_id: 99,
                },
            )
            .await;
        }

        let prefix = lock_prefix(&ns);
        let before = s3::list_objects(&client, &bucket, &prefix).await.unwrap();
        assert_eq!(before.len(), 3, "precondition: 3 old lock files");

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            namespace: ns,
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(matches!(result, TryBecomeLeaderResult::Leader(_)));

        // Cleanup is fire-and-forget; poll until old files are gone.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let remaining = s3::list_objects(&client, &bucket, &prefix).await.unwrap();
            if remaining.len() == 1 {
                let key = remaining[0].key().unwrap();
                assert!(
                    key.contains("/0000000004.lock"),
                    "only the new epoch should remain: {key}"
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "old lock files were not cleaned up in time, {} remain",
                remaining.len(),
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_racing_nodes_one_leader_empty_bucket() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        const NUM_NODES: usize = 5;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(NUM_NODES));

        let mut handles = Vec::with_capacity(NUM_NODES);
        for i in 0..NUM_NODES {
            let config = config.clone();
            let ns = ns.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let le = LeaderElection::new(LeaderElectionConfig {
                    node_id: i as u64,
                    namespace: ns,
                    s3_config: config,
                    validity_millis: None,
                })
                .await;
                barrier.wait().await;
                le.try_become_leader().await.unwrap()
            }));
        }

        let mut leaders = 0usize;
        for handle in handles {
            if matches!(handle.await.unwrap(), TryBecomeLeaderResult::Leader(_)) {
                leaders += 1;
            }
        }
        assert_eq!(leaders, 1, "exactly one node should become leader");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_racing_nodes_one_leader_after_expiry() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(3),
            &LockFile {
                valid_until_ms: now_ms() - 60_000,
                expired: false,
                node_id: 99,
            },
        )
        .await;

        const NUM_NODES: usize = 5;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(NUM_NODES));

        let mut handles = Vec::with_capacity(NUM_NODES);
        for i in 0..NUM_NODES {
            let config = config.clone();
            let ns = ns.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let le = LeaderElection::new(LeaderElectionConfig {
                    node_id: i as u64,
                    namespace: ns,
                    s3_config: config,
                    validity_millis: None,
                })
                .await;
                barrier.wait().await;
                le.try_become_leader().await.unwrap()
            }));
        }

        let mut leader_epochs = vec![];
        for handle in handles {
            if let TryBecomeLeaderResult::Leader(info) = handle.await.unwrap() {
                leader_epochs.push(info.epoch);
            }
        }
        assert_eq!(
            leader_epochs.len(),
            1,
            "exactly one node should become leader"
        );
        assert_eq!(
            leader_epochs[0],
            Epoch(4),
            "new epoch should be one past the expired lock"
        );
    }
}
