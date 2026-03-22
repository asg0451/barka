use std::backtrace::Backtrace;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {source}")]
    Io {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[error("capnp: {source}")]
    Capnp {
        #[from]
        source: capnp::Error,
        backtrace: Backtrace,
    },

    #[error("json: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[error("not leader for partition {partition}")]
    NotLeader {
        partition: u32,
        backtrace: Backtrace,
    },

    #[error("partition {partition} not found")]
    UnknownPartition {
        partition: u32,
        backtrace: Backtrace,
    },

    #[error("storage: {message}")]
    Storage {
        message: String,
        backtrace: Backtrace,
    },

    #[error("lease: {message}")]
    Lease {
        message: String,
        backtrace: Backtrace,
    },

    #[error("leader election: {source}")]
    LeaderElection {
        #[from]
        source: LeaderElectionError,
        backtrace: Backtrace,
    },
}

#[derive(Debug, Error)]
pub enum LeaderElectionError {
    #[error("s3: {0}")]
    S3(Box<dyn std::error::Error + Send + Sync>),

    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("fatal: {0}")]
    Fatal(String),
}

pub type Result<T> = std::result::Result<T, Error>;
