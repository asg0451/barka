// based on https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/

use std::net::SocketAddr;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::s3::{self, PutOutcome, S3Config};

#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    valid_until_ms: u64,
    expired: bool,
    node_id: u64,
    addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Epoch(u64);

impl Epoch {
    pub fn as_u64(self) -> u64 {
        self.0
    }

    // format: {prefix}/{epoch}.lock
    fn from_key(key: &str) -> Result<Self> {
        let epoch = key
            .split('/')
            .next_back()
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
    pub addr: SocketAddr,
    pub namespace: String,
    /// Optional path segment before `lock/`; keys are `{prefix}/lock/{namespace}/` when set.
    pub leader_election_prefix: Option<String>,
    pub s3_config: S3Config,
    pub validity_millis: Option<u64>,
}

pub struct LeaderElection {
    node_id: u64,
    addr: SocketAddr,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    validity_millis: u64,
}

const LOCK_FILE_PREFIX: &str = "lock/";
const VALIDITY_MILLIS: u64 = 10_000;
/// Restart from `list_objects` when a listed lock key is deleted before `get_object` (e.g. winner cleanup).
const TRY_BECOME_LEADER_MAX_LIST_GET_RACES: u32 = 64;

/// S3 key prefix for lock files: `{leader_election_prefix}/lock/{namespace}/` when a prefix is set,
/// otherwise `lock/{namespace}/`. The namespace is the topic-partition identifier.
fn leader_lock_s3_prefix(leader_election_prefix: Option<&str>, namespace: &str) -> String {
    let lock = LOCK_FILE_PREFIX.trim_end_matches('/');
    let extra = leader_election_prefix
        .map(str::trim)
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty());
    match extra {
        Some(ep) => format!("{}/{}/{}/", ep, lock, namespace),
        None => format!("{}/{}/", lock, namespace),
    }
}

impl LeaderElection {
    pub async fn new(config: LeaderElectionConfig) -> Self {
        let s3_client = s3::build_client(&config.s3_config).await;
        let prefix =
            leader_lock_s3_prefix(config.leader_election_prefix.as_deref(), &config.namespace);
        tracing::debug!(
            node_id = config.node_id,
            namespace = %config.namespace,
            leader_election_prefix = ?config.leader_election_prefix,
            "leader election initialized"
        );
        Self {
            node_id: config.node_id,
            addr: config.addr,
            s3_client,
            bucket: config.s3_config.bucket.clone(),
            prefix,
            validity_millis: config.validity_millis.unwrap_or(VALIDITY_MILLIS),
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
    #[tracing::instrument(level = "debug", skip_all, fields(node_id = self.node_id, prefix = self.prefix, bucket = self.bucket), err, ret)]
    pub async fn try_become_leader(&self) -> Result<TryBecomeLeaderResult> {
        for _ in 0..TRY_BECOME_LEADER_MAX_LIST_GET_RACES {
            let contents = s3::list_objects(&self.s3_client, &self.bucket, &self.prefix).await?;

            let mut last_epoch = None;
            if !contents.is_empty() {
                let newest = contents.last().unwrap();
                let key = newest.key().context("S3 object missing key")?;
                last_epoch = Some(Epoch::from_key(key)?);
                let reader =
                    match s3::get_object_reader_if_present(&self.s3_client, &self.bucket, key)
                        .await?
                    {
                        Some(r) => r,
                        None => {
                            tracing::debug!("lock file removed between list and get, retrying");
                            continue;
                        }
                    };
                let lock_file: LockFile =
                    serde_json::from_reader(reader).context("deserialize lock file")?;
                // TODO: check if valid_until_ms is in the past too
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if !lock_file.expired && lock_file.valid_until_ms > now_ms {
                    if lock_file.node_id == self.node_id {
                        let remaining_ms = lock_file.valid_until_ms - now_ms;
                        let epoch = last_epoch.unwrap();

                        if remaining_ms < self.validity_millis / 2 {
                            // Renew the lease before it expires
                            return self.renew_lease(key, epoch, lock_file.valid_until_ms).await;
                        }

                        tracing::debug!(
                            valid_until_ms = lock_file.valid_until_ms,
                            remaining_ms,
                            "still leader"
                        );
                        return Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                            valid_until_ms: lock_file.valid_until_ms,
                            epoch,
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
            let valid_until_ms = now_ms + self.validity_millis;

            let lock_file_body = serde_json::to_string(&LockFile {
                valid_until_ms,
                expired: false,
                node_id: self.node_id,
                addr: self.addr,
            })
            .unwrap();

            let epoch = last_epoch.unwrap_or(Epoch(0)).next();
            let key = epoch.to_key(&self.prefix);

            let old_keys: Vec<String> = contents
                .iter()
                .filter_map(|obj| obj.key().map(str::to_owned))
                .collect();

            return match s3::put_if_absent(
                &self.s3_client,
                &self.bucket,
                &key,
                lock_file_body.into(),
            )
            .await?
            {
                PutOutcome::Created => {
                    tracing::info!(epoch = epoch.0, prefix = %self.prefix, "became leader");
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
            };
        }

        anyhow::bail!("leader election: exhausted retries after concurrent lock cleanup");
    }

    /// Overwrite our lock file with a fresh TTL, then re-list to verify no newer epoch appeared.
    async fn renew_lease(
        &self,
        key: &str,
        epoch: Epoch,
        original_valid_until_ms: u64,
    ) -> Result<TryBecomeLeaderResult> {
        const RENEW_ATTEMPTS: u32 = 3;

        for attempt in 0..RENEW_ATTEMPTS {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let new_valid_until = now + self.validity_millis;

            let body = serde_json::to_string(&LockFile {
                valid_until_ms: new_valid_until,
                expired: false,
                node_id: self.node_id,
                addr: self.addr,
            })
            .unwrap();

            if let Err(e) = s3::put_object(&self.s3_client, &self.bucket, key, body).await {
                tracing::warn!(attempt, error = %e, "lease renewal put failed, retrying");
                continue;
            }

            match s3::list_objects(&self.s3_client, &self.bucket, &self.prefix).await {
                Ok(fresh_contents) => {
                    let newest_key = fresh_contents.last().and_then(|o| o.key());
                    match newest_key {
                        Some(newest_key) => {
                            let fresh_epoch = Epoch::from_key(newest_key)?;
                            if fresh_epoch != epoch {
                                tracing::warn!(
                                    our_epoch = epoch.0,
                                    fresh_epoch = fresh_epoch.0,
                                    "newer epoch appeared during renewal"
                                );
                                return Ok(TryBecomeLeaderResult::NotLeader);
                            }
                        }
                        None => {
                            tracing::warn!("empty lock listing after renewal put, retrying");
                            continue;
                        }
                    }
                    tracing::debug!(epoch = epoch.0, new_valid_until, "renewed lease");
                    return Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                        valid_until_ms: new_valid_until,
                        epoch,
                    }));
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "lease renewal verify-list failed, retrying");
                    continue;
                }
            }
        }

        // All renewal attempts failed -- still within TTL, next poll will retry
        tracing::warn!("lease renewal failed after {RENEW_ATTEMPTS} attempts, using remaining TTL");
        Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
            valid_until_ms: original_valid_until_ms,
            epoch,
        }))
    }

    /// Release leadership by marking the lock file as expired. Other nodes will
    /// see the expired flag and can immediately start a new election instead of
    /// waiting for `valid_until_ms` to pass. It is not safe to call abdicate if
    /// you do not think you are the leader.
    #[tracing::instrument(level = "debug", skip_all, fields(node_id = self.node_id, epoch = epoch.0), err)]
    pub async fn abdicate(&self, epoch: Epoch) -> Result<()> {
        let key = epoch.to_key(&self.prefix);
        let body = serde_json::to_string(&LockFile {
            valid_until_ms: 0,
            expired: true,
            node_id: self.node_id,
            addr: self.addr,
        })
        .unwrap();
        s3::put_object(&self.s3_client, &self.bucket, &key, body).await?;
        tracing::info!("released leadership");
        Ok(())
    }
}

#[derive(Debug)]
pub struct CurrentLeader {
    pub node_id: u64,
    pub addr: SocketAddr,
    pub valid_until_ms: u64,
    pub epoch: Epoch,
}

pub async fn read_current_leader(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    namespace: &str,
    leader_election_prefix: Option<&str>,
) -> Result<Option<CurrentLeader>> {
    let prefix = leader_lock_s3_prefix(leader_election_prefix, namespace);
    let contents = s3::list_objects(s3_client, bucket, &prefix).await?;

    let newest = match contents.last() {
        Some(obj) => obj,
        None => return Ok(None),
    };

    let key = newest.key().context("S3 object missing key")?;
    let epoch = Epoch::from_key(key)?;

    let reader = match s3::get_object_reader_if_present(s3_client, bucket, key).await? {
        Some(r) => r,
        None => return Ok(None),
    };

    let lock_file: LockFile = serde_json::from_reader(reader).context("deserialize lock file")?;

    if lock_file.expired {
        return Ok(None);
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if lock_file.valid_until_ms <= now_ms {
        return Ok(None);
    }

    Ok(Some(CurrentLeader {
        node_id: lock_file.node_id,
        addr: lock_file.addr,
        valid_until_ms: lock_file.valid_until_ms,
        epoch,
    }))
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

    #[test]
    fn leader_lock_s3_prefix_formatting() {
        assert_eq!(leader_lock_s3_prefix(None, "topic-0"), "lock/topic-0/");
        assert_eq!(
            leader_lock_s3_prefix(Some("cluster-a"), "topic-0"),
            "cluster-a/lock/topic-0/"
        );
        assert_eq!(
            leader_lock_s3_prefix(Some("  staging/ "), "topic-0"),
            "staging/lock/topic-0/"
        );
        assert_eq!(leader_lock_s3_prefix(Some(""), "topic-0"), "lock/topic-0/");
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
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: "test-ns".into(),
            leader_election_prefix: None,
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
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.clone(),
            leader_election_prefix: None,
            s3_config: config.clone(),
            validity_millis: None,
        })
        .await;
        let result = le1.try_become_leader().await.unwrap();
        assert!(matches!(result, TryBecomeLeaderResult::Leader(_)));

        let le2 = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.clone(),
            leader_election_prefix: None,
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
        leader_lock_s3_prefix(None, namespace)
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
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 3,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
                    addr: "127.0.0.1:0".parse().unwrap(),
                },
            )
            .await;
        }

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 7,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.clone(),
            leader_election_prefix: None,
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
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
                    addr: "127.0.0.1:0".parse().unwrap(),
                },
            )
            .await;
        }

        let prefix = lock_prefix(&ns);
        let before = s3::list_objects(&client, &bucket, &prefix).await.unwrap();
        assert_eq!(before.len(), 3, "precondition: 3 old lock files");

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
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
                    addr: "127.0.0.1:0".parse().unwrap(),
                    namespace: ns,
                    leader_election_prefix: None,
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

    #[tokio::test]
    async fn test_election_prefix_isolates_same_namespace() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = "shared-topic-partition";
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let le_a = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.into(),
            leader_election_prefix: Some("cluster-a".into()),
            s3_config: config.clone(),
            validity_millis: None,
        })
        .await;
        let le_b = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.into(),
            leader_election_prefix: Some("cluster-b".into()),
            s3_config: config,
            validity_millis: None,
        })
        .await;

        let r_a = le_a.try_become_leader().await.unwrap();
        let r_b = le_b.try_become_leader().await.unwrap();
        assert!(
            matches!(r_a, TryBecomeLeaderResult::Leader(_)),
            "cluster-a should elect a leader: {r_a:?}"
        );
        assert!(
            matches!(r_b, TryBecomeLeaderResult::Leader(_)),
            "cluster-b should elect independently: {r_b:?}"
        );
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
                addr: "127.0.0.1:0".parse().unwrap(),
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
                    addr: "127.0.0.1:0".parse().unwrap(),
                    namespace: ns,
                    leader_election_prefix: None,
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

    #[tokio::test]
    async fn test_read_current_leader() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let result = super::read_current_leader(&client, &bucket, &ns, None)
            .await
            .unwrap();
        assert!(result.is_none(), "should be None when no lock files exist");

        let addr: SocketAddr = "127.0.0.1:9292".parse().unwrap();
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(1),
            &LockFile {
                valid_until_ms: now_ms() + 60_000,
                expired: false,
                node_id: 1,
                addr,
            },
        )
        .await;

        let leader = super::read_current_leader(&client, &bucket, &ns, None)
            .await
            .unwrap()
            .expect("should find leader");
        assert_eq!(leader.node_id, 1);
        assert_eq!(leader.addr, addr);
        assert_eq!(leader.epoch, Epoch(1));

        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(2),
            &LockFile {
                valid_until_ms: now_ms() - 1_000,
                expired: false,
                node_id: 2,
                addr: "127.0.0.1:9393".parse().unwrap(),
            },
        )
        .await;

        let result = super::read_current_leader(&client, &bucket, &ns, None)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "should be None when lock is expired by time"
        );
    }

    #[tokio::test]
    async fn test_leader_renews_before_expiry() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let validity_ms: u64 = 10_000;
        // Seed a lock with less than half the TTL remaining (2s out of 10s)
        let original_valid_until = now_ms() + 2_000;
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(5),
            &LockFile {
                valid_until_ms: original_valid_until,
                expired: false,
                node_id: 1,
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
            s3_config: config,
            validity_millis: Some(validity_ms),
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        match result {
            TryBecomeLeaderResult::Leader(info) => {
                assert_eq!(info.epoch, Epoch(5), "epoch should stay the same");
                assert!(
                    info.valid_until_ms > original_valid_until,
                    "valid_until_ms should be extended: {} > {}",
                    info.valid_until_ms,
                    original_valid_until,
                );
            }
            other => panic!("expected Leader, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_no_renewal_with_ample_remaining() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let validity_ms: u64 = 10_000;
        // Seed a lock with plenty of TTL remaining (8s out of 10s)
        let original_valid_until = now_ms() + 8_000;
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(3),
            &LockFile {
                valid_until_ms: original_valid_until,
                expired: false,
                node_id: 1,
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
            s3_config: config,
            validity_millis: Some(validity_ms),
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        match result {
            TryBecomeLeaderResult::Leader(info) => {
                assert_eq!(info.epoch, Epoch(3));
                assert_eq!(
                    info.valid_until_ms, original_valid_until,
                    "should return original valid_until_ms without renewal"
                );
            }
            other => panic!("expected Leader, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_renewal_detects_newer_epoch() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let validity_ms: u64 = 10_000;
        // Seed our lock near expiry
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(5),
            &LockFile {
                valid_until_ms: now_ms() + 2_000,
                expired: false,
                node_id: 1,
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;
        // Pre-create epoch 6 for another node
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(6),
            &LockFile {
                valid_until_ms: now_ms() + 60_000,
                expired: false,
                node_id: 2,
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        let le = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
            s3_config: config,
            validity_millis: Some(validity_ms),
        })
        .await;

        let result = le.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::NotLeader),
            "should detect newer epoch and return NotLeader: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_renewed_lease_blocks_other_nodes() {
        require_localstack().await;
        let bucket = unique_name("test-leader");
        let ns = unique_name("ns");
        let config = localstack_config(&bucket);
        let client = s3::build_client(&config).await;
        s3::ensure_bucket(&client, &bucket).await.unwrap();

        let validity_ms: u64 = 10_000;
        // Seed a lock near expiry for node 1
        write_lock_file_direct(
            &client,
            &bucket,
            &ns,
            Epoch(3),
            &LockFile {
                valid_until_ms: now_ms() + 2_000,
                expired: false,
                node_id: 1,
                addr: "127.0.0.1:0".parse().unwrap(),
            },
        )
        .await;

        // Node 1 renews
        let le1 = LeaderElection::new(LeaderElectionConfig {
            node_id: 1,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns.clone(),
            leader_election_prefix: None,
            s3_config: config.clone(),
            validity_millis: Some(validity_ms),
        })
        .await;
        let result = le1.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::Leader(ref info) if info.epoch == Epoch(3)),
            "node 1 should renew: {result:?}"
        );

        // Node 2 polls and should see the renewed lease
        let le2 = LeaderElection::new(LeaderElectionConfig {
            node_id: 2,
            addr: "127.0.0.1:0".parse().unwrap(),
            namespace: ns,
            leader_election_prefix: None,
            s3_config: config,
            validity_millis: Some(validity_ms),
        })
        .await;
        let result = le2.try_become_leader().await.unwrap();
        assert!(
            matches!(result, TryBecomeLeaderResult::NotLeader),
            "node 2 should see renewed lease and return NotLeader: {result:?}"
        );
    }
}
