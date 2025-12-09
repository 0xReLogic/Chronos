use std::time::Duration;
use std::env;
use std::fs;
use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, ClientTlsConfig, Certificate, Identity};
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

fn tls_env_present() -> bool {
    env::var("CHRONOS_TLS_CERT").is_ok()
        && env::var("CHRONOS_TLS_KEY").is_ok()
        && env::var("CHRONOS_TLS_CA_CERT").is_ok()
}

fn configure_tls(endpoint: Endpoint) -> Result<Endpoint, NetworkError> {
    let cert_path = env::var("CHRONOS_TLS_CERT")
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
    let key_path = env::var("CHRONOS_TLS_KEY")
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
    let ca_path = env::var("CHRONOS_TLS_CA_CERT")
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;

    let cert = fs::read(cert_path)
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
    let key = fs::read(key_path)
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
    let ca = fs::read(ca_path)
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;

    let identity = Identity::from_pem(cert, key);
    let ca_cert = Certificate::from_pem(ca);

    let domain = env::var("CHRONOS_TLS_DOMAIN").unwrap_or_else(|_| "localhost".to_string());

    let tls = ClientTlsConfig::new()
        .identity(identity)
        .ca_certificate(ca_cert)
        .domain_name(domain);

    endpoint
        .tls_config(tls)
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))
}

fn build_endpoint(address: &str) -> Result<Endpoint, NetworkError> {
    let use_tls = tls_env_present();

    let uri = if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else if use_tls {
        format!("https://{}", address)
    } else {
        format!("http://{}", address)
    };

    let endpoint = Endpoint::from_shared(uri)
        .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;

    if use_tls {
        configure_tls(endpoint)
    } else {
        Ok(endpoint)
    }
}

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
        let endpoint = build_endpoint(&self.address)?;

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
    auth_token: Option<String>,
}

impl SqlClient {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            client: None,
            auth_token: None,
        }
    }

    /// Configure this client to send a bearer token for authentication.
    /// The token will be attached as an `authorization: Bearer <token>`
    /// header on each gRPC request.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
    
    pub async fn connect(&mut self) -> Result<(), NetworkError> {
        let endpoint = build_endpoint(&self.address)?
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
        
        let mut request = Request::new(SqlRequest {
            sql: sql.to_string(),
        });

        if let Some(token) = &self.auth_token {
            let header_val = format!("Bearer {}", token);
            let meta_val = MetadataValue::from_str(&header_val)
                .map_err(|e| NetworkError::ConnectionError(e.to_string()))?;
            request.metadata_mut().insert("authorization", meta_val);
        }

        let response = self
            .client
            .as_mut()
            .ok_or_else(|| NetworkError::ConnectionError("Client not connected".to_string()))?
            .execute_sql(request)
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
        let endpoint = build_endpoint(&self.address)?;

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