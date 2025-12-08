use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use log::{info, error, debug};

use crate::raft::{RaftNode, RaftMessage};
use crate::executor::Executor;
use crate::parser::Parser;
use crate::network::proto::raft_service_server::RaftService;
use crate::network::proto::sql_service_server::SqlService;
use crate::network::proto::health_service_server::HealthService;
use crate::network::proto::sync_service_server::SyncService;
use crate::network::proto::sync_status_service_server::SyncStatusService;
use crate::common::timestamp::HybridTimestamp;
use crate::storage::wal::Operation as WalOperation;

use super::proto::*;
use crate::network::ConnectivityState;
use crate::network::sync_status::SharedSyncStatus;

type LwwStateMap = HashMap<(String, Vec<u8>), HybridTimestamp>;


pub struct RaftServer {
    node: Arc<Mutex<RaftNode>>,
}

impl RaftServer {
    pub fn new(node: Arc<Mutex<RaftNode>>) -> Self {
        Self { node }
    }
}

pub struct SyncStatusServer {
    state: SharedSyncStatus,
}

impl SyncStatusServer {
    pub fn new(state: SharedSyncStatus) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl SyncStatusService for SyncStatusServer {
    async fn get_sync_status(
        &self,
        _request: Request<SyncStatusRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        let status = self.state.lock().await;
        Ok(Response::new(SyncStatusResponse {
            last_sync_ts_ms: status.last_sync_ts_ms,
            last_sync_applied: status.last_sync_applied,
            pending_ops: status.pending_ops,
            last_error: status.last_error.clone().unwrap_or_default(),
        }))
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        debug!("Received RequestVote: {:?}", req);
        
        let mut node = self.node.lock().await;
        
        // Convert to internal message format
        let message = RaftMessage::RequestVote {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };
        
        // Process the message
        let mut response = RequestVoteResponse {
            term: node.state().current_term,
            vote_granted: false,
        };
        
        match node.handle_message(message) {
            Ok(reply) => {
                if let Some(RaftMessage::RequestVoteResponse { term, vote_granted }) = reply {
                    response.term = term;
                    response.vote_granted = vote_granted;
                }
            },
            Err(e) => {
                error!("Error handling RequestVote: {e}");
                return Err(Status::internal(format!("Internal error: {e}")));
            }
        }
        
        Ok(Response::new(response))
    }
    
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        debug!("Received AppendEntries: term={}, leader={}, entries={}", 
               req.term, req.leader_id, req.entries.len());
        
        let mut node = self.node.lock().await;
        
        // Convert entries to internal format
        let entries = req.entries.into_iter()
            .map(|e| crate::raft::LogEntry {
                term: e.term,
                command: e.command,
            })
            .collect();
        
        // Convert to internal message format
        let message = RaftMessage::AppendEntries {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries,
            leader_commit: req.leader_commit,
        };
        
        // Process the message
        let mut response = AppendEntriesResponse {
            term: node.state().current_term,
            success: false,
            match_index: 0,
        };
        
        match node.handle_message(message) {
            Ok(reply) => {
                if let Some(RaftMessage::AppendEntriesResponse { term, success, match_index }) = reply {
                    response.term = term;
                    response.success = success;
                    response.match_index = match_index;
                }
            },
            Err(e) => {
                error!("Error handling AppendEntries: {e}");
                return Err(Status::internal(format!("Internal error: {e}")));
            }
        }
        
        Ok(Response::new(response))
    }
}

pub struct HealthServer {
    state: Arc<RwLock<ConnectivityState>>,
}

impl HealthServer {
    pub fn new(state: Arc<RwLock<ConnectivityState>>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl HealthService for HealthServer {
    async fn get_connectivity(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let state = *self.state.read().await;
        let state_str = match state {
            ConnectivityState::Connected => "Connected",
            ConnectivityState::Disconnected => "Disconnected",
            ConnectivityState::Reconnecting => "Reconnecting",
        };
        Ok(Response::new(HealthResponse { state: state_str.to_string() }))
    }
}

pub struct SyncServer {
    executor: Arc<Mutex<Executor>>,
    lww_state: Arc<Mutex<LwwStateMap>>,
}

impl SyncServer {
    pub fn new(executor: Arc<Mutex<Executor>>) -> Self {
        Self {
            executor,
            lww_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl SyncService for SyncServer {
    async fn sync(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<SyncResponse>, Status> {
        let req = request.into_inner();
        let mut applied: u64 = 0;

        let mut executor = self.executor.lock().await;
        let mut lww = self.lww_state.lock().await;

        for op in req.operations {
            let data = op.data;
            let decoded: Result<
                (
                    crate::storage::offline_queue::PersistentQueuedOperation,
                    usize,
                ),
                _,
            > = bincode::serde::decode_from_slice(&data, bincode::config::standard());

            match decoded {
                Ok((entry, _len)) => {
                    // Derive LWW key from the underlying operation
                    let (table, key_bytes) = match &entry.operation {
                        WalOperation::Insert { table, key, .. } => (table.clone(), key.clone()),
                        WalOperation::Update { table, key, .. } => (table.clone(), key.clone()),
                        WalOperation::Delete { table, key } => (table.clone(), key.clone()),
                    };
                    let map_key = (table, key_bytes);

                    // Last-Write-Wins: skip if we have a newer or equal timestamp
                    let ts = entry.timestamp;
                    let should_apply = match lww.get(&map_key) {
                        Some(prev_ts) if *prev_ts >= ts => {
                            info!(
                                "Skipping stale synced operation id={} due to LWW (prev_ts >= new_ts)",
                                entry.id
                            );
                            false
                        }
                        _ => {
                            lww.insert(map_key, ts);
                            true
                        }
                    };

                    if !should_apply {
                        continue;
                    }

                    if let Err(e) = executor
                        .apply_storage_operation(entry.operation.clone())
                        .await
                    {
                        error!("Failed to apply synced operation id={}: {}", entry.id, e);
                        continue;
                    }
                    applied += 1;
                }
                Err(e) => {
                    error!("Failed to decode SyncOperation id={}: {}", op.id, e);
                }
            }
        }

        info!("SyncServer applied {} operations from edge", applied);

        Ok(Response::new(SyncResponse { applied }))
    }
}

pub struct SqlServer {
    node: Arc<Mutex<RaftNode>>,
    executor: Arc<Mutex<Executor>>,
}

impl SqlServer {
    pub fn new(node: Arc<Mutex<RaftNode>>, executor: Arc<Mutex<Executor>>) -> Self {
        Self { node, executor }
    }
}

#[tonic::async_trait]
impl SqlService for SqlServer {
    async fn execute_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<Response<SqlResponse>, Status> {
        info!("SqlServer::execute_sql: ENTER");
        let req = request.into_inner();
        info!("SqlServer::execute_sql: Received SQL: {}", req.sql);
        
        // Parse the SQL
        let ast = match Parser::parse(&req.sql) {
            Ok(ast) => {
                info!("SqlServer::execute_sql: parse OK");
                ast
            }
            Err(e) => {
                error!("SQL parse error: {e}");
                return Ok(Response::new(SqlResponse {
                    success: false,
                    error: format!("Parse error: {e}"),
                    columns: vec![],
                    rows: vec![],
                }));
            }
        };
        
        // Check if this node is the leader
        let is_leader = {
            let node = self.node.lock().await;
            let is_leader = node.is_leader();
            debug!("SqlServer::execute_sql: is_leader={}", is_leader);
            is_leader
        };
        
        if !is_leader {
            info!("SqlServer::execute_sql: Not the leader, returning error");
            return Ok(Response::new(SqlResponse {
                success: false,
                error: "Not the leader".to_string(),
                columns: vec![],
                rows: vec![],
            }));
        }
        
        // Submit the command to Raft
        let command = bincode::serde::encode_to_vec(&ast, bincode::config::standard())
            .map_err(|e| Status::internal(format!("Serialization error: {e}")))?;
        
        {
            let mut node = self.node.lock().await;
            info!("SqlServer::execute_sql: submitting command to Raft");
            node.submit_command(command)
                .map_err(|e| {
                    error!("SqlServer::execute_sql: Raft submit_command error: {e}");
                    Status::internal(format!("Raft error: {e}"))
                })?;
            debug!("SqlServer::execute_sql: Raft submit_command OK");
        }
        
        // For now, we'll just execute the command directly
        // In a real implementation, we would wait for the command to be committed
        
        // Use tokio Mutex for async-safe locking
        let mut executor = self.executor.lock().await;
        info!("SqlServer::execute_sql: calling Executor::execute");
        let result = executor.execute(ast).await
            .map_err(|e| {
                error!("SqlServer::execute_sql: Executor::execute error: {e}");
                Status::internal(format!("Execution error: {e}"))
            })?;
        info!("SqlServer::execute_sql: Executor::execute done");
        
        // Convert the result to the response format
        let mut response = SqlResponse {
            success: true,
            error: "".to_string(),
            columns: result.columns,
            rows: vec![],
        };
        
        // Convert rows
        for row in result.rows {
            let mut proto_row = Row { values: vec![] };
            
            for value in row {
                let proto_value = match value {
                    crate::parser::Value::String(s) => Value {
                        value: Some(super::proto::value::Value::StringValue(s)),
                    },
                    crate::parser::Value::Integer(i) => Value {
                        value: Some(super::proto::value::Value::IntValue(i)),
                    },
                    crate::parser::Value::Float(f) => Value {
                        value: Some(super::proto::value::Value::FloatValue(f)),
                    },
                    crate::parser::Value::Boolean(b) => Value {
                        value: Some(super::proto::value::Value::BoolValue(b)),
                    },
                    crate::parser::Value::Null => Value {
                        value: Some(super::proto::value::Value::NullValue(true)),
                    },
                };
                
                proto_row.values.push(proto_value);
            }
            
            response.rows.push(proto_row);
        }
        
        info!("SqlServer::execute_sql: returning SqlResponse (success={})", response.success);
        Ok(Response::new(response))
    }
}