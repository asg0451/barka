// based on https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use bytes::Buf;
use serde::{Deserialize, Serialize};

use crate::error::{Error, LeaderElectionError, Result};
use crate::s3::{self, S3Config};

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
    fn from_key(prefix: &str, key: &str) -> Result<Self> {
        let key = key.strip_prefix(prefix).unwrap();
        // let epoch = key.split('.').last().ok_or()?.parse::<u64>()?;
        // Ok(Self(epoch))
        todo!()
    }
    fn to_key(self, prefix: &str) -> String {
        format!("{}/{}.lock", prefix, self.0)
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
// TODO: methods - extend, release, ..

pub struct LeaderElection {
    node_id: u64,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

const LOCK_FILE_PREFIX: &str = "lock/";
const VALIDITY_MILLIS: u64 = 10_000;

impl LeaderElection {
    pub async fn new<S: AsRef<str>>(node_id: u64, namespace: S, s3_config: S3Config) -> Self {
        let s3_client = s3::build_client(&s3_config).await;
        let prefix = format!("{}/{}/", LOCK_FILE_PREFIX, namespace.as_ref());
        tracing::debug!(
            node_id = node_id,
            namespace = namespace.as_ref(),
            "leader election initialized"
        );
        Self {
            node_id,
            s3_client,
            bucket: s3_config.bucket.clone(),
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
    pub async fn try_become_leader(&self) -> Result<TryBecomeLeaderResult> {
        let lock_files = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(LOCK_FILE_PREFIX)
            .send()
            .await
            .map_err(|e| Self::map_s3_err(e))?;

        if lock_files.continuation_token.is_some() {
            return Err(Error::from(LeaderElectionError::Fatal(
                "too many lock files (paginated response)".into(),
            )));
        }
        let contents = lock_files.contents.unwrap_or_default();

        let mut last_epoch = None;
        if !contents.is_empty() {
            let newest_lock_file = contents.last().unwrap();
            last_epoch = Some(Epoch::from_key(
                &self.prefix,
                newest_lock_file.key().unwrap(),
            )?);
            let newest_lock_file = self
                .s3_client
                .get_object()
                .bucket(&self.bucket)
                .key(newest_lock_file.key().unwrap())
                .send()
                .await
                .map_err(|e| Self::map_s3_err(e))?
                .body
                .collect()
                .await
                .map_err(|e| Self::map_s3_err(e))?;
            let newest_lock_file: LockFile = serde_json::from_reader(newest_lock_file.reader())
                .map_err(|e| Error::from(LeaderElectionError::Serde(e)))?;
            if !newest_lock_file.expired {
                tracing::debug!(
                    leader_node_id = newest_lock_file.node_id,
                    "not becoming leader: lock file not expired"
                );
                return Ok(TryBecomeLeaderResult::NotLeader);
            }
        }

        // create new lock file
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let valid_until_ms = now_ms + VALIDITY_MILLIS;

        let lock_file = serde_json::to_string(&LockFile {
            valid_until_ms,
            expired: false,
            node_id: self.node_id,
        })
        .unwrap();

        let epoch = last_epoch.unwrap_or(Epoch(0)).next();
        let lock_file_key = epoch.to_key(&self.prefix);
        let put_res = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(lock_file_key)
            .body(ByteStream::from(SdkBody::from(lock_file)))
            .if_none_match("*") // CAS
            .send()
            .await;
        match put_res {
            // TODO is that the right error code?
            Err(SdkError::ServiceError(e))
                if e.err().meta().code() == Some("PreconditionFailed") =>
            {
                return Ok(TryBecomeLeaderResult::NotLeader);
            }
            Err(e) => {
                return Err(Self::map_s3_err(e));
            }
            Ok(_) => {
                tracing::info!(epoch = epoch.0, "became leader");
                return Ok(TryBecomeLeaderResult::Leader(LeadershipInfo {
                    valid_until_ms,
                    epoch,
                }));
            }
        }
    }

    fn map_s3_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> Error {
        Error::LeaderElection {
            source: LeaderElectionError::S3(Box::new(e)),
            backtrace: std::backtrace::Backtrace::capture(),
        }
    }
}
