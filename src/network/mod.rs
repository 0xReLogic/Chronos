// Temporarily simplified network module for testing
#[derive(Debug)]
pub enum NetworkError {
    ConnectionError(String),
    RpcError(String),
    TransportError(String),
}

impl From<tonic::transport::Error> for NetworkError {
    fn from(err: tonic::transport::Error) -> Self {
        NetworkError::TransportError(err.to_string())
    }
}

impl From<tonic::Status> for NetworkError {
    fn from(status: tonic::Status) -> Self {
        NetworkError::RpcError(status.to_string())
    }
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::ConnectionError(e) => write!(f, "Connection error: {e}"),
            NetworkError::RpcError(e) => write!(f, "RPC error: {e}"),
            NetworkError::TransportError(e) => write!(f, "Transport error: {e}"),
        }
    }
}

impl std::error::Error for NetworkError {}

pub mod client;
pub mod server;
pub mod connectivity;
pub mod offline_queue;
pub mod sync_worker;

pub use connectivity::*;
pub use offline_queue::*;
pub use server::{HealthServer, SyncServer};
pub use sync_worker::SyncWorker;

pub mod proto {
    tonic::include_proto!("raft");
}

pub use client::{RaftClient, SqlClient, SyncClient};
pub use server::{RaftServer, SqlServer};