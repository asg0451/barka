use std::net::{IpAddr, SocketAddr};

use barka::produce_node::{ProduceNode, ProduceNodeConfig, ProducerBatchLimits, TopicConfig};
use barka::producer;
use barka::s3::{self, S3Config};
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

fn parse_topics(s: &str) -> Result<Vec<TopicConfig>, String> {
    let mut out = Vec::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (topic, count) = entry
            .rsplit_once(':')
            .ok_or_else(|| format!("invalid topic spec '{entry}', expected TOPIC:NUM_PARTITIONS"))?;
        let partitions: u32 = count
            .parse()
            .map_err(|_| format!("invalid partition count '{count}' in '{entry}'"))?;
        if partitions == 0 {
            return Err(format!("partition count must be >= 1 in '{entry}'"));
        }
        out.push(TopicConfig {
            topic: topic.to_string(),
            partitions,
        });
    }
    if out.is_empty() {
        return Err("at least one topic must be specified".into());
    }
    Ok(out)
}

#[derive(Parser)]
#[command(name = "produce-node", version, about = "Barka produce node")]
struct Cli {
    #[arg(long, env = "BARKA_NODE_ID", default_value_t = 0)]
    node_id: u64,

    #[arg(long, env = "BARKA_RPC_PORT", default_value_t = 9292)]
    rpc_port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,

    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "BARKA_S3_BUCKET", default_value = "barka")]
    s3_bucket: String,

    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    aws_region: String,

    #[arg(long, env = "BARKA_S3_PREFIX")]
    s3_prefix: Option<String>,

    #[arg(long, env = "BARKA_PRODUCER_MAX_RECORDS")]
    producer_max_records: Option<usize>,

    #[arg(long, env = "BARKA_PRODUCER_MAX_BYTES")]
    producer_max_bytes: Option<usize>,

    #[arg(long, env = "BARKA_PRODUCER_LINGER_MS")]
    producer_linger_ms: Option<u64>,

    #[arg(long, env = "BARKA_LEADER_ELECTION_POLL_SECS", default_value_t = 3)]
    leader_election_poll_secs: u64,

    /// Topic configuration: TOPIC:NUM_PARTITIONS[,TOPIC:NUM_PARTITIONS,...]
    #[arg(long, env = "BARKA_TOPICS", default_value = "default:1", value_parser = parse_topics)]
    topics: Vec<TopicConfig>,
}

impl Cli {
    fn node_config(&self) -> ProduceNodeConfig {
        let producer_limits = if self.producer_max_records.is_some()
            || self.producer_max_bytes.is_some()
            || self.producer_linger_ms.is_some()
        {
            Some(ProducerBatchLimits {
                max_records: self
                    .producer_max_records
                    .unwrap_or(producer::DEFAULT_MAX_RECORDS),
                max_bytes: self
                    .producer_max_bytes
                    .unwrap_or(producer::DEFAULT_MAX_BYTES),
                linger_ms: self
                    .producer_linger_ms
                    .unwrap_or(producer::DEFAULT_LINGER_MS),
            })
        } else {
            None
        };
        ProduceNodeConfig {
            node_id: self.node_id,
            rpc_addr: SocketAddr::new(self.bind, self.rpc_port),
            s3_prefix: self.s3_prefix.clone(),
            producer_limits,
            leader_election_poll_secs: self.leader_election_poll_secs,
            topics: self.topics.clone(),
        }
    }

    fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint_url: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            region: self.aws_region.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();
    let config = cli.node_config();
    let s3_config = cli.s3_config();

    tracing::info!(
        node_id = config.node_id,
        rpc_addr = %config.rpc_addr,
        topics = ?config.topics,
        s3_endpoint = s3_config.endpoint_url.as_deref().unwrap_or("aws"),
        s3_bucket = %s3_config.bucket,
        "starting produce-node",
    );

    let s3_client = s3::build_client(&s3_config).await;
    s3::ensure_bucket(&s3_client, &s3_config.bucket).await?;

    let node = ProduceNode::new(config, &s3_config).await?;
    node.serve().await?;
    Ok(())
}
