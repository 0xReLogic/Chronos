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
pub mod connectivity;
pub mod http_admin;
pub mod ingest;
pub mod metrics;
pub mod offline_queue;
pub mod server;
pub mod sync_status;
pub mod sync_worker;

pub use connectivity::*;
pub use ingest::{run_ingest_worker, IngestRequest};
pub use offline_queue::*;
pub use server::{HealthServer, SyncServer, SyncStatusServer};
pub use sync_status::{SharedSyncStatus, SyncStatusState};
pub use sync_worker::SyncWorker;

pub mod proto {
    tonic::include_proto!("raft");
}

pub use client::{RaftClient, SqlClient, SyncClient};
pub use server::{RaftServer, SqlServer};

#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn display_includes_error_kind() {
        let conn = NetworkError::ConnectionError("boom".to_string());
        assert!(format!("{conn}").contains("Connection error"));

        let rpc = NetworkError::RpcError("bad".to_string());
        assert!(format!("{rpc}").contains("RPC error"));

        let transport = NetworkError::TransportError("down".to_string());
        assert!(format!("{transport}").contains("Transport error"));
    }

    #[test]
    fn from_tonic_status_maps_to_rpc_error() {
        let status = Status::invalid_argument("oops");
        let err: NetworkError = status.into();
        match err {
            NetworkError::RpcError(msg) => {
                assert!(msg.contains("oops"));
            }
            other => panic!("expected RpcError, got {other:?}"),
        }
    }
}
