use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;


use crate::raft::{RaftMessage, LogEntry};

use crate::network::proto::raft_service_client::RaftServiceClient;
use crate::network::proto::sql_service_client::SqlServiceClient;
use crate::network::proto::sync_service_client::SyncServiceClient;
use crate::network::proto::{
    AppendEntriesRequest,
    RequestVoteRequest,
    SqlRequest,
    SyncOperation,
    SyncRequest,
    SyncResponse,
};

use super::proto::*;
use super::NetworkError;

pub struct RaftClient {
    address: String,
    client: Option<RaftServiceClient<Channel>>,
}

impl RaftClient {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            client: None,
        }
    }
    
    pub async fn connect(&mut self) -> Result<(), NetworkError> {
        let endpoint = Endpoint::from_shared(format!("http://{}", self.address))
            .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
        
        let channel = endpoint
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await?;
        
        self.client = Some(RaftServiceClient::new(channel));
        
        Ok(())
    }
    
    pub async fn request_vote(
        &mut self,
        term: u64,
        candidate_id: &str,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Result<RaftMessage, NetworkError> {
        if self.client.is_none() {
            self.connect().await?;
        }
        
        let request = RequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index,
            last_log_term,
        };
        
        let response = self.client.as_mut()
            .ok_or_else(|| NetworkError::ConnectionError("Client not connected".to_string()))?
            .request_vote(Request::new(request))
            .await?
            .into_inner();
        
        Ok(RaftMessage::RequestVoteResponse {
            term: response.term,
            vote_granted: response.vote_granted,
        })
    }
    
    pub async fn append_entries(
        &mut self,
        term: u64,
        leader_id: &str,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<RaftMessage, NetworkError> {
        if self.client.is_none() {
            self.connect().await?;
        }
        
        // Convert entries to proto format
        let proto_entries = entries.into_iter()
            .map(|e| super::proto::LogEntry {
                term: e.term,
                command: e.command,
            })
            .collect();
        
        let request = AppendEntriesRequest {
            term,
            leader_id: leader_id.to_string(),
            prev_log_index,
            prev_log_term,
            entries: proto_entries,
            leader_commit,
        };
        
        let response = self.client.as_mut()
            .ok_or_else(|| NetworkError::ConnectionError("Client not connected".to_string()))?
            .append_entries(Request::new(request))
            .await?
            .into_inner();
        
        Ok(RaftMessage::AppendEntriesResponse {
            term: response.term,
            success: response.success,
            match_index: response.match_index,
        })
    }
}

pub struct SqlClient {
    address: String,
    client: Option<SqlServiceClient<Channel>>,
}

impl SqlClient {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            client: None,
        }
    }
    
    pub async fn connect(&mut self) -> Result<(), NetworkError> {
        let endpoint = Endpoint::from_shared(format!("http://{}", self.address))
            .map_err(|e| NetworkError::ConnectionError(e.to_string()))?
            .connect_timeout(Duration::from_secs(5));
        
        let channel = endpoint
            .connect()
            .await?;
        
        self.client = Some(SqlServiceClient::new(channel));
        
        Ok(())
    }
    
    pub async fn execute_sql(&mut self, sql: &str) -> Result<SqlResponse, NetworkError> {
        if self.client.is_none() {
            self.connect().await?;
        }
        
        let request = SqlRequest {
            sql: sql.to_string(),
        };
        
        let response = self.client.as_mut()
            .ok_or_else(|| NetworkError::ConnectionError("Client not connected".to_string()))?
            .execute_sql(Request::new(request))
            .await?
            .into_inner();
        
        Ok(response)
    }
}

pub struct SyncClient {
    address: String,
    client: Option<SyncServiceClient<Channel>>,
}

impl SyncClient {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            client: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), NetworkError> {
        let endpoint = Endpoint::from_shared(self.address.clone())
            .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;

        let channel = endpoint
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await?;

        self.client = Some(SyncServiceClient::new(channel));

        Ok(())
    }

    pub async fn sync_operations(
        &mut self,
        edge_id: &str,
        operations: Vec<crate::storage::offline_queue::PersistentQueuedOperation>,
    ) -> Result<u64, NetworkError> {
        if self.client.is_none() {
            self.connect().await?;
        }

        let ops: Vec<SyncOperation> = operations
            .into_iter()
            .map(|op| {
                let bytes = bincode::serde::encode_to_vec(&op, bincode::config::standard())
                    .expect("failed to encode PersistentQueuedOperation for sync");
                SyncOperation {
                    id: op.id,
                    data: bytes,
                }
            })
            .collect();

        let request = SyncRequest { edge_id: edge_id.to_string(), operations: ops };

        let response: SyncResponse = self
            .client
            .as_mut()
            .ok_or_else(|| NetworkError::ConnectionError("Client not connected".to_string()))?
            .sync(Request::new(request))
            .await?
            .into_inner();

        Ok(response.applied)
    }
}