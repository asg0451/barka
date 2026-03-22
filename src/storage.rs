use anyhow::Result;

/// Opaque storage backend. The implementation details (S3, segments, etc.)
/// are left to the implementor.
pub trait Storage: Send + Sync + 'static {
    fn put(&self, partition: u32, data: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn get(&self, partition: u32, offset: u64) -> impl Future<Output = Result<Vec<u8>>> + Send;
}

use std::future::Future;
