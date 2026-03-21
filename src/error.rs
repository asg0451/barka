use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("capnp: {0}")]
    Capnp(#[from] capnp::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("not leader for partition {partition}")]
    NotLeader { partition: u32 },

    #[error("partition {partition} not found")]
    UnknownPartition { partition: u32 },

    #[error("storage: {0}")]
    Storage(String),

    #[error("lease: {0}")]
    Lease(String),
}

pub type Result<T> = std::result::Result<T, Error>;
