use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Not a leader")]
    NotLeader,

    #[error("Node is not running")]
    NotRunning,

    #[error("Invalid term: {0}")]
    InvalidTerm(u64),

    #[error("Invalid log index: {0}")]
    InvalidLogIndex(u64),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Timeout")]
    Timeout,

    #[error("Consensus failed")]
    ConsensusFailed,
}
