use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use log::{info, error, debug};

use crate::raft::{RaftNode, RaftMessage};
use crate::executor::Executor;
use crate::parser::Parser;
use crate::network::proto::raft_service_server::RaftService;
use crate::network::proto::sql_service_server::SqlService;

use super::proto::*;


pub struct RaftServer {
    node: Arc<Mutex<RaftNode>>,
}

impl RaftServer {
    pub fn new(node: Arc<Mutex<RaftNode>>) -> Self {
        Self { node }
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
        let req = request.into_inner();
        info!("Received SQL: {}", req.sql);
        
        // Parse the SQL
        let ast = match Parser::parse(&req.sql) {
            Ok(ast) => ast,
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
            node.is_leader()
        };
        
        if !is_leader {
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
            node.submit_command(command)
                .map_err(|e| Status::internal(format!("Raft error: {e}")))?;
        }
        
        // For now, we'll just execute the command directly
        // In a real implementation, we would wait for the command to be committed
        
        // Use tokio Mutex for async-safe locking
        let mut executor = self.executor.lock().await;
        let result = executor.execute(ast).await
            .map_err(|e| Status::internal(format!("Execution error: {e}")))?;
        
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
        
        Ok(Response::new(response))
    }
}