use std::net::SocketAddr;
use std::sync::Arc;

use crate::consumer::{ConsumerConfig, PartitionConsumer};
use crate::node::segment_key_prefix;
use crate::rpc::server::serve_consume_rpc;
use crate::s3::S3Config;

#[derive(Clone, Debug)]
pub struct ConsumeNodeConfig {
    pub rpc_addr: SocketAddr,
    pub s3_prefix: Option<String>,
}

impl Default for ConsumeNodeConfig {
    fn default() -> Self {
        Self {
            rpc_addr: "127.0.0.1:9392".parse().unwrap(),
            s3_prefix: None,
        }
    }
}

pub struct ConsumeNode {
    pub config: ConsumeNodeConfig,
    pub consumer: Arc<PartitionConsumer>,
}

impl ConsumeNode {
    pub async fn new(config: ConsumeNodeConfig, s3_config: &S3Config) -> anyhow::Result<Self> {
        let prefix = segment_key_prefix(config.s3_prefix.as_deref());
        let partition_prefix = format!("{prefix}/test/0");
        let cache_dir = std::env::temp_dir()
            .join("barka-segment-cache")
            .join(partition_prefix.replace('/', "-"));
        let consumer = PartitionConsumer::new(
            s3_config,
            partition_prefix,
            ConsumerConfig {
                cache_dir,
                ..Default::default()
            },
        )
        .await?;

        Ok(Self { config, consumer })
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let rpc_addr = self.config.rpc_addr;
        let consumer = Arc::clone(&self.consumer);

        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_consume_rpc(consumer, rpc_addr))
        });

        tokio::task::spawn_blocking(move || rpc_handle.join().unwrap())
            .await
            .unwrap()?;
        Ok(())
    }
}
