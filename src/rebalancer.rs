use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;

use crate::leader_election;
use crate::node::leader_namespace;
use crate::partition_registry::{PartitionEntry, PartitionRegistry};
use crate::rpc::client::ProduceClient;

#[derive(Debug, Clone)]
pub struct PartitionLeadership {
    pub topic: String,
    pub partition: u32,
    pub leader: Option<LeaderSummary>,
}

#[derive(Debug, Clone)]
pub struct LeaderSummary {
    pub node_id: u64,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub abdications: Vec<Abdication>,
    pub distribution: HashMap<u64, usize>,
    pub node_count: usize,
    pub leaderless: Vec<(String, u32)>,
}

#[derive(Debug, Clone)]
pub struct Abdication {
    pub topic: String,
    pub partition: u32,
    pub from_node_id: u64,
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub struct RebalanceResult {
    pub succeeded: usize,
    pub failed: usize,
}

pub async fn snapshot_leadership(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    partitions: &[PartitionEntry],
    leader_election_prefix: Option<&str>,
) -> Result<Vec<PartitionLeadership>> {
    let futs: Vec<_> = partitions
        .iter()
        .map(|entry| {
            let namespace = leader_namespace(&entry.topic, entry.partition);
            let topic = entry.topic.clone();
            let partition = entry.partition;
            async move {
                let leader = leader_election::read_current_leader(
                    s3_client,
                    bucket,
                    &namespace,
                    leader_election_prefix,
                )
                .await?;
                Ok(PartitionLeadership {
                    topic,
                    partition,
                    leader: leader.map(|l| LeaderSummary {
                        node_id: l.node_id,
                        addr: l.addr,
                    }),
                })
            }
        })
        .collect();
    futures::future::try_join_all(futs).await
}

pub fn compute_plan(
    snapshot: &[PartitionLeadership],
    max_abdications_per_node: usize,
) -> RebalancePlan {
    let mut by_node: HashMap<u64, Vec<&PartitionLeadership>> = HashMap::new();
    let mut leaderless = Vec::new();

    for pl in snapshot {
        match &pl.leader {
            Some(leader) => by_node.entry(leader.node_id).or_default().push(pl),
            None => leaderless.push((pl.topic.clone(), pl.partition)),
        }
    }

    let distribution: HashMap<u64, usize> = by_node
        .iter()
        .map(|(&nid, parts)| (nid, parts.len()))
        .collect();
    let node_count = by_node.len();

    if node_count <= 1 {
        return RebalancePlan {
            abdications: Vec::new(),
            distribution,
            node_count,
            leaderless,
        };
    }

    let total_led: usize = by_node.values().map(|v| v.len()).sum();
    let target = total_led.div_ceil(node_count);

    let mut abdications = Vec::new();
    for (&_node_id, parts) in &by_node {
        if parts.len() <= target {
            continue;
        }
        let excess = parts.len() - target;
        let to_shed = excess.min(max_abdications_per_node);

        // Sort deterministically, shed from the end.
        let mut sorted: Vec<&PartitionLeadership> = parts.clone();
        sorted.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));

        for pl in sorted.iter().rev().take(to_shed) {
            let leader = pl.leader.as_ref().unwrap();
            abdications.push(Abdication {
                topic: pl.topic.clone(),
                partition: pl.partition,
                from_node_id: leader.node_id,
                addr: leader.addr,
            });
        }
    }

    // Sort abdications for deterministic execution order.
    abdications.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));

    RebalancePlan {
        abdications,
        distribution,
        node_count,
        leaderless,
    }
}

pub async fn execute_plan(
    plan: &RebalancePlan,
    settle_delay: Duration,
    dry_run: bool,
) -> Result<RebalanceResult> {
    let mut succeeded = 0;
    let mut failed = 0;

    for (i, abd) in plan.abdications.iter().enumerate() {
        if dry_run {
            tracing::info!(
                topic = %abd.topic, partition = abd.partition,
                from_node_id = abd.from_node_id, addr = %abd.addr,
                "[dry-run] would abdicate"
            );
            succeeded += 1;
            continue;
        }

        tracing::info!(
            topic = %abd.topic, partition = abd.partition,
            from_node_id = abd.from_node_id, addr = %abd.addr,
            "sending abdicate request"
        );

        match ProduceClient::connect(abd.addr).await {
            Ok(client) => match client.abdicate(&abd.topic, abd.partition).await {
                Ok(true) => {
                    tracing::info!(
                        topic = %abd.topic, partition = abd.partition,
                        "abdication succeeded"
                    );
                    succeeded += 1;
                }
                Ok(false) => {
                    tracing::warn!(
                        topic = %abd.topic, partition = abd.partition,
                        "abdication returned false (partition not found on node)"
                    );
                    failed += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %abd.topic, partition = abd.partition,
                        error = %e, "abdicate RPC failed"
                    );
                    failed += 1;
                }
            },
            Err(e) => {
                tracing::warn!(
                    addr = %abd.addr, error = %e,
                    "failed to connect to produce-node"
                );
                failed += 1;
            }
        }

        if i + 1 < plan.abdications.len() {
            tokio::time::sleep(settle_delay).await;
        }
    }

    Ok(RebalanceResult { succeeded, failed })
}

pub async fn run_once(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    registry: &PartitionRegistry,
    leader_election_prefix: Option<&str>,
    max_abdications_per_node: usize,
    settle_delay: Duration,
    dry_run: bool,
) -> Result<RebalancePlan> {
    let entries = registry.list().await?;
    if entries.is_empty() {
        tracing::info!("no partitions registered");
        return Ok(RebalancePlan {
            abdications: Vec::new(),
            distribution: HashMap::new(),
            node_count: 0,
            leaderless: Vec::new(),
        });
    }
    tracing::info!(partition_count = entries.len(), "snapshotting leadership");

    let snapshot = snapshot_leadership(s3_client, bucket, &entries, leader_election_prefix).await?;

    let plan = compute_plan(&snapshot, max_abdications_per_node);

    tracing::info!(
        node_count = plan.node_count,
        leaderless = plan.leaderless.len(),
        abdications = plan.abdications.len(),
        distribution = ?plan.distribution,
        "computed rebalance plan"
    );

    if plan.abdications.is_empty() {
        tracing::info!("cluster is balanced, nothing to do");
    } else {
        let result = execute_plan(&plan, settle_delay, dry_run).await?;
        tracing::info!(
            succeeded = result.succeeded,
            failed = result.failed,
            "rebalance cycle complete"
        );
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Assignment<'a> = (&'a str, u32, Option<(u64, &'a str)>);

    fn make_snapshot(assignments: &[Assignment<'_>]) -> Vec<PartitionLeadership> {
        assignments
            .iter()
            .map(|(topic, partition, leader)| PartitionLeadership {
                topic: topic.to_string(),
                partition: *partition,
                leader: leader.map(|(node_id, addr)| LeaderSummary {
                    node_id,
                    addr: addr.parse().unwrap(),
                }),
            })
            .collect()
    }

    #[test]
    fn balanced_cluster_no_abdications() {
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, Some((2, "127.0.0.1:9002"))),
            ("events", 2, Some((3, "127.0.0.1:9003"))),
        ]);
        let plan = compute_plan(&snapshot, 1);
        assert!(plan.abdications.is_empty());
        assert_eq!(plan.node_count, 3);
    }

    #[test]
    fn single_node_no_abdications() {
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, Some((1, "127.0.0.1:9001"))),
            ("events", 2, Some((1, "127.0.0.1:9001"))),
        ]);
        let plan = compute_plan(&snapshot, 1);
        assert!(plan.abdications.is_empty());
        assert_eq!(plan.node_count, 1);
    }

    #[test]
    fn imbalanced_sheds_excess() {
        // Node 1 has 4 partitions, nodes 2 and 3 have 1 each.
        // Total = 6, nodes = 3, target = ceil(6/3) = 2.
        // Node 1 excess = 4 - 2 = 2, but max_abdications_per_node = 1.
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, Some((1, "127.0.0.1:9001"))),
            ("events", 2, Some((1, "127.0.0.1:9001"))),
            ("events", 3, Some((1, "127.0.0.1:9001"))),
            ("orders", 0, Some((2, "127.0.0.1:9002"))),
            ("orders", 1, Some((3, "127.0.0.1:9003"))),
        ]);
        let plan = compute_plan(&snapshot, 1);
        assert_eq!(plan.abdications.len(), 1);
        assert_eq!(plan.abdications[0].from_node_id, 1);
    }

    #[test]
    fn imbalanced_sheds_all_excess_when_max_allows() {
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, Some((1, "127.0.0.1:9001"))),
            ("events", 2, Some((1, "127.0.0.1:9001"))),
            ("events", 3, Some((1, "127.0.0.1:9001"))),
            ("orders", 0, Some((2, "127.0.0.1:9002"))),
            ("orders", 1, Some((3, "127.0.0.1:9003"))),
        ]);
        let plan = compute_plan(&snapshot, 10);
        assert_eq!(plan.abdications.len(), 2);
        for abd in &plan.abdications {
            assert_eq!(abd.from_node_id, 1);
        }
    }

    #[test]
    fn leaderless_partitions_excluded() {
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, None),
            ("events", 2, Some((2, "127.0.0.1:9002"))),
        ]);
        let plan = compute_plan(&snapshot, 1);
        assert!(plan.abdications.is_empty());
        assert_eq!(plan.leaderless.len(), 1);
        assert_eq!(plan.leaderless[0], ("events".to_string(), 1));
    }

    #[test]
    fn deterministic_selection() {
        let snapshot = make_snapshot(&[
            ("a", 0, Some((1, "127.0.0.1:9001"))),
            ("b", 0, Some((1, "127.0.0.1:9001"))),
            ("c", 0, Some((1, "127.0.0.1:9001"))),
            ("d", 0, Some((2, "127.0.0.1:9002"))),
        ]);
        // Run twice, should get same result.
        let plan1 = compute_plan(&snapshot, 1);
        let plan2 = compute_plan(&snapshot, 1);
        assert_eq!(plan1.abdications.len(), 1);
        assert_eq!(plan1.abdications[0].topic, plan2.abdications[0].topic);
        assert_eq!(
            plan1.abdications[0].partition,
            plan2.abdications[0].partition
        );
        // Sheds from the end of sorted list: "c" is last.
        assert_eq!(plan1.abdications[0].topic, "c");
    }

    #[test]
    fn two_nodes_shed_one() {
        // Node 1 has 3, node 2 has 1. Target = ceil(4/2) = 2. Shed 1.
        let snapshot = make_snapshot(&[
            ("events", 0, Some((1, "127.0.0.1:9001"))),
            ("events", 1, Some((1, "127.0.0.1:9001"))),
            ("events", 2, Some((1, "127.0.0.1:9001"))),
            ("events", 3, Some((2, "127.0.0.1:9002"))),
        ]);
        let plan = compute_plan(&snapshot, 1);
        assert_eq!(plan.abdications.len(), 1);
        assert_eq!(plan.abdications[0].from_node_id, 1);
    }

    #[test]
    fn empty_snapshot() {
        let plan = compute_plan(&[], 1);
        assert!(plan.abdications.is_empty());
        assert_eq!(plan.node_count, 0);
    }

    #[test]
    fn all_leaderless() {
        let snapshot = make_snapshot(&[("events", 0, None), ("events", 1, None)]);
        let plan = compute_plan(&snapshot, 1);
        assert!(plan.abdications.is_empty());
        assert_eq!(plan.leaderless.len(), 2);
    }
}
