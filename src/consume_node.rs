use std::net::SocketAddr;

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
    pub s3_config: S3Config,
    pub base_prefix: String,
}

impl ConsumeNode {
    pub async fn new(config: ConsumeNodeConfig, s3_config: &S3Config) -> anyhow::Result<Self> {
        let base_prefix = segment_key_prefix(config.s3_prefix.as_deref());
        Ok(Self {
            config,
            s3_config: s3_config.clone(),
            base_prefix,
        })
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let rpc_addr = self.config.rpc_addr;
        let s3_config = self.s3_config.clone();
        let base_prefix = self.base_prefix.clone();

        let rpc_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve_consume_rpc(s3_config, base_prefix, rpc_addr))
        });

        tokio::task::spawn_blocking(move || rpc_handle.join().unwrap())
            .await
            .unwrap()?;
        Ok(())
    }
}
