use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::s3::{self, PutIfMatchOutcome, PutOutcome};

const MAX_CAS_ATTEMPTS: u32 = 8;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionEntry {
    pub topic: String,
    pub partition: u32,
}

pub struct PartitionRegistry {
    s3_client: Client,
    bucket: String,
    key: String,
}

/// Derive the S3 key for the registry file from an optional leader-election prefix.
fn registry_key(leader_election_prefix: Option<&str>) -> String {
    match leader_election_prefix {
        Some(prefix) => {
            let prefix = prefix.trim_matches('/');
            if prefix.is_empty() {
                "partitions.json".to_string()
            } else {
                format!("{prefix}/partitions.json")
            }
        }
        None => "partitions.json".to_string(),
    }
}

impl PartitionRegistry {
    pub fn new(s3_client: Client, bucket: String, leader_election_prefix: Option<&str>) -> Self {
        Self {
            s3_client,
            bucket,
            key: registry_key(leader_election_prefix),
        }
    }

    /// Read the current partition list from S3. Returns an empty vec if the
    /// registry file does not exist yet.
    pub async fn list(&self) -> Result<Vec<PartitionEntry>> {
        let obj = s3::get_object_with_etag(&self.s3_client, &self.bucket, &self.key).await?;
        match obj {
            None => Ok(Vec::new()),
            Some(o) => {
                let entries: Vec<PartitionEntry> =
                    serde_json::from_slice(&o.body).context("parse partition registry")?;
                Ok(entries)
            }
        }
    }

    /// Add a partition to the registry using ETag-based CAS. Returns `true` if
    /// the entry was added, `false` if it was already present.
    pub async fn add(&self, topic: &str, partition: u32) -> Result<bool> {
        let entry = PartitionEntry {
            topic: topic.to_string(),
            partition,
        };

        for attempt in 0..MAX_CAS_ATTEMPTS {
            let obj = s3::get_object_with_etag(&self.s3_client, &self.bucket, &self.key).await?;

            match obj {
                Some(existing) => {
                    let mut entries: Vec<PartitionEntry> = serde_json::from_slice(&existing.body)
                        .context("parse partition registry")?;

                    if entries.contains(&entry) {
                        return Ok(false);
                    }
                    entries.push(entry.clone());
                    let body = Bytes::from(serde_json::to_vec(&entries)?);

                    match s3::put_if_match(
                        &self.s3_client,
                        &self.bucket,
                        &self.key,
                        body,
                        &existing.etag,
                    )
                    .await?
                    {
                        PutIfMatchOutcome::Updated => return Ok(true),
                        PutIfMatchOutcome::Conflict => {
                            tracing::debug!(attempt, "registry add CAS conflict, retrying");
                            continue;
                        }
                    }
                }
                None => {
                    let entries = vec![entry.clone()];
                    let body = Bytes::from(serde_json::to_vec(&entries)?);

                    match s3::put_if_absent(&self.s3_client, &self.bucket, &self.key, body).await? {
                        PutOutcome::Created => return Ok(true),
                        PutOutcome::AlreadyExists => {
                            tracing::debug!(attempt, "registry create race, retrying");
                            continue;
                        }
                    }
                }
            }
        }

        anyhow::bail!("registry add: exhausted {MAX_CAS_ATTEMPTS} CAS attempts")
    }

    /// Remove a partition from the registry using ETag-based CAS. Returns `true`
    /// if the entry was removed, `false` if it was not present.
    pub async fn remove(&self, topic: &str, partition: u32) -> Result<bool> {
        let target = PartitionEntry {
            topic: topic.to_string(),
            partition,
        };

        for attempt in 0..MAX_CAS_ATTEMPTS {
            let obj = s3::get_object_with_etag(&self.s3_client, &self.bucket, &self.key).await?;

            match obj {
                Some(existing) => {
                    let mut entries: Vec<PartitionEntry> = serde_json::from_slice(&existing.body)
                        .context("parse partition registry")?;

                    let before_len = entries.len();
                    entries.retain(|e| e != &target);
                    if entries.len() == before_len {
                        return Ok(false);
                    }

                    let body = Bytes::from(serde_json::to_vec(&entries)?);

                    match s3::put_if_match(
                        &self.s3_client,
                        &self.bucket,
                        &self.key,
                        body,
                        &existing.etag,
                    )
                    .await?
                    {
                        PutIfMatchOutcome::Updated => return Ok(true),
                        PutIfMatchOutcome::Conflict => {
                            tracing::debug!(attempt, "registry remove CAS conflict, retrying");
                            continue;
                        }
                    }
                }
                None => return Ok(false),
            }
        }

        anyhow::bail!("registry remove: exhausted {MAX_CAS_ATTEMPTS} CAS attempts")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::S3Config;

    fn test_s3_config() -> S3Config {
        S3Config {
            endpoint_url: Some("http://localhost:4566".to_string()),
            bucket: "test-registry".into(),
            region: "us-east-1".into(),
        }
    }

    async fn make_registry(prefix: &str) -> PartitionRegistry {
        let s3_config = test_s3_config();
        let client = s3::build_client(&s3_config).await;
        s3::ensure_bucket(&client, &s3_config.bucket).await.unwrap();
        PartitionRegistry::new(client, s3_config.bucket, Some(prefix))
    }

    fn unique_prefix(label: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("test-reg-{label}/{nanos}")
    }

    #[test]
    fn registry_key_no_prefix() {
        assert_eq!(registry_key(None), "partitions.json");
    }

    #[test]
    fn registry_key_with_prefix() {
        assert_eq!(registry_key(Some("cluster-a")), "cluster-a/partitions.json");
    }

    #[test]
    fn registry_key_trims_slashes() {
        assert_eq!(
            registry_key(Some("/cluster-a/")),
            "cluster-a/partitions.json"
        );
    }

    #[tokio::test]
    async fn list_empty_when_no_file() {
        let reg = make_registry(&unique_prefix("list-empty")).await;
        let entries = reg.list().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn add_creates_and_is_idempotent() {
        let reg = make_registry(&unique_prefix("add-idem")).await;

        assert!(reg.add("events", 0).await.unwrap());
        assert!(!reg.add("events", 0).await.unwrap());

        let entries = reg.list().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].topic, "events");
        assert_eq!(entries[0].partition, 0);
    }

    #[tokio::test]
    async fn add_multiple_partitions() {
        let reg = make_registry(&unique_prefix("add-multi")).await;

        reg.add("events", 0).await.unwrap();
        reg.add("events", 1).await.unwrap();
        reg.add("orders", 0).await.unwrap();

        let entries = reg.list().await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn remove_returns_false_when_empty() {
        let reg = make_registry(&unique_prefix("rm-empty")).await;
        assert!(!reg.remove("events", 0).await.unwrap());
    }

    #[tokio::test]
    async fn remove_returns_false_when_not_present() {
        let reg = make_registry(&unique_prefix("rm-absent")).await;
        reg.add("events", 0).await.unwrap();
        assert!(!reg.remove("orders", 0).await.unwrap());
    }

    #[tokio::test]
    async fn remove_removes_entry() {
        let reg = make_registry(&unique_prefix("rm-entry")).await;
        reg.add("events", 0).await.unwrap();
        reg.add("events", 1).await.unwrap();

        assert!(reg.remove("events", 0).await.unwrap());

        let entries = reg.list().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].partition, 1);
    }
}
