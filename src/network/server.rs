use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};

use crate::common::timestamp::HybridTimestamp;
use crate::executor::Executor;
use crate::network::proto::health_service_server::HealthService;
use crate::network::proto::raft_service_server::RaftService;
use crate::network::proto::sql_service_server::SqlService;
use crate::network::proto::sync_service_server::SyncService;
use crate::network::proto::sync_status_service_server::SyncStatusService;
use crate::parser::Parser;
use crate::raft::{RaftMessage, RaftNode};
use crate::storage::wal::Operation as WalOperation;
use crate::storage::StorageError;
use uhlc::HLC;

use super::proto::*;
use crate::network::metrics;
use crate::network::sync_status::SharedSyncStatus;
use crate::network::ConnectivityState;

type LwwStateMap = HashMap<(String, Vec<u8>), HybridTimestamp>;

#[derive(Debug, Clone, Copy)]
enum Role {
    Admin,
    ReadOnly,
}

#[derive(Debug, Clone)]
struct AuthContext {
    user: String,
    role: Option<Role>,
}

fn load_auth_tokens_from_env() -> (Option<String>, Option<String>) {
    let admin = std::env::var("CHRONOS_AUTH_TOKEN_ADMIN").ok();
    let readonly = std::env::var("CHRONOS_AUTH_TOKEN_READONLY").ok();
    (admin, readonly)
}

#[allow(clippy::result_large_err)]
fn authenticate_request(
    metadata: &tonic::metadata::MetadataMap,
    is_read_query: bool,
) -> Result<AuthContext, Status> {
    let (admin_token, readonly_token) = load_auth_tokens_from_env();
    let require_auth = admin_token.is_some() || readonly_token.is_some();

    let user = metadata
        .get("x-chronos-user")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();

    if !require_auth {
        return Ok(AuthContext { user, role: None });
    }

    let raw = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
    let raw_str = raw
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid authorization header"))?;

    const PREFIX: &str = "Bearer ";
    if !raw_str.starts_with(PREFIX) {
        return Err(Status::unauthenticated("invalid authorization scheme"));
    }
    let token = &raw_str[PREFIX.len()..];

    let role = if let Some(ref admin) = admin_token {
        if token == admin {
            Role::Admin
        } else if let Some(ref ro) = readonly_token {
            if token == ro {
                Role::ReadOnly
            } else {
                return Err(Status::unauthenticated("invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("invalid token"));
        }
    } else if let Some(ref ro) = readonly_token {
        if token == ro {
            Role::ReadOnly
        } else {
            return Err(Status::unauthenticated("invalid token"));
        }
    } else {
        // Should not happen if require_auth is false above, but keep a safe default.
        return Ok(AuthContext { user, role: None });
    };

    if let Role::ReadOnly = role {
        if !is_read_query {
            return Err(Status::permission_denied(
                "write operations are not allowed for read-only role",
            ));
        }
    }

    Ok(AuthContext {
        user,
        role: Some(role),
    })
}

#[derive(Default, Debug)]
struct SyncStats {
    /// Total number of operations successfully applied via SyncServer.
    total_applied: u64,
    /// Total number of operations skipped due to LWW (stale timestamps).
    total_skipped_lww: u64,
}

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
            }
            Err(e) => {
                error!("Error handling RequestVote: {e}");
                return Err(Status::internal(format!("Internal error: {e}")));
            }
        }

        Ok(Response::new(response))
    }

    async fn pre_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        debug!("Received PreVote: {:?}", req);

        let mut node = self.node.lock().await;

        // Convert to internal message format
        let message = RaftMessage::PreVote {
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
                if let Some(RaftMessage::PreVoteResponse { term, vote_granted }) = reply {
                    response.term = term;
                    response.vote_granted = vote_granted;
                }
            }
            Err(e) => {
                error!("Error handling PreVote: {e}");
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
        debug!(
            "Received AppendEntries: term={}, leader={}, entries={}",
            req.term,
            req.leader_id,
            req.entries.len()
        );

        let mut node = self.node.lock().await;

        // Convert entries to internal format
        let entries = req
            .entries
            .into_iter()
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
                if let Some(RaftMessage::AppendEntriesResponse {
                    term,
                    success,
                    match_index,
                }) = reply
                {
                    response.term = term;
                    response.success = success;
                    response.match_index = match_index;
                }
            }
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
        Ok(Response::new(HealthResponse {
            state: state_str.to_string(),
        }))
    }
}

pub struct SyncServer {
    executor: Arc<Mutex<Executor>>,
    lww_state: Arc<Mutex<LwwStateMap>>,
    stats: Arc<Mutex<SyncStats>>,
}

impl SyncServer {
    pub fn new(executor: Arc<Mutex<Executor>>) -> Self {
        Self {
            executor,
            lww_state: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(SyncStats::default())),
        }
    }

    /// Load the last applied sync ID for a given edge_id from the underlying
    /// storage engine. This is implemented via a dedicated sled tree in
    /// SledEngine; here we just delegate through the Executor.
    async fn get_last_applied_id(
        &self,
        executor: &Executor,
        edge_id: &str,
    ) -> Result<u64, StorageError> {
        // For now, we store the cursor in the same LWW timestamp tree using a
        // reserved key prefix, keeping the complexity minimal. This can be
        // refactored into a dedicated metadata tree if needed.
        // We use an empty key and interpret the HybridTimestamp node_id
        // field as the last applied ID.
        if let Some(ts) = executor
            .get_lww_timestamp(edge_id, &[])
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))?
        {
            Ok(ts.node_id)
        } else {
            Ok(0)
        }
    }

    async fn set_last_applied_id(
        &self,
        executor: &mut Executor,
        edge_id: &str,
        id: u64,
    ) -> Result<(), StorageError> {
        let ts = HybridTimestamp {
            ts: HLC::default().new_timestamp(),
            node_id: id,
        };
        executor
            .set_lww_timestamp(edge_id, &[], ts)
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))
    }
}

#[tonic::async_trait]
impl SyncService for SyncServer {
    async fn sync(&self, request: Request<SyncRequest>) -> Result<Response<SyncResponse>, Status> {
        let req = request.into_inner();
        let mut applied: u64 = 0;
        let mut skipped_lww: u64 = 0;

        let mut executor = self.executor.lock().await;
        let mut lww = self.lww_state.lock().await;

        // Determine the last applied ID for this edge (delta sync cursor).
        let edge_id = req.edge_id.clone();
        let mut max_seen_id: u64 = 0;
        let last_applied_id = match self.get_last_applied_id(&executor, &edge_id).await {
            Ok(id) => id,
            Err(e) => {
                error!("Failed to get last_applied_id for edge {}: {}", edge_id, e);
                0
            }
        };

        for op in req.operations {
            // Skip operations whose IDs are already at or below the cursor
            // for this edge. This avoids re-processing old batches after
            // restarts.
            if op.id <= last_applied_id {
                continue;
            }

            if op.id > max_seen_id {
                max_seen_id = op.id;
            }
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
                    let map_key = (table.clone(), key_bytes.clone());

                    // Last-Write-Wins: consult in-memory cache first, then
                    // persisted LWW metadata if needed. Skip if we have a
                    // newer or equal timestamp.
                    let ts = entry.timestamp;

                    let prev_ts = match lww.get(&map_key) {
                        Some(prev) => Some(*prev),
                        None => match executor.get_lww_timestamp(&table, &key_bytes).await {
                            Ok(opt_ts) => {
                                if let Some(persisted) = opt_ts {
                                    lww.insert(map_key.clone(), persisted);
                                    Some(persisted)
                                } else {
                                    None
                                }
                            }
                            Err(e) => {
                                error!(
                                    "LWW get_lww_timestamp error for table={} id={}: {}",
                                    table, entry.id, e
                                );
                                None
                            }
                        },
                    };

                    let should_apply = match prev_ts {
                        Some(prev_ts) if prev_ts >= ts => {
                            info!(
                                "Skipping stale synced operation id={} due to LWW (prev_ts >= new_ts)",
                                entry.id
                            );
                            skipped_lww += 1;
                            false
                        }
                        _ => {
                            lww.insert(map_key.clone(), ts);
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
                    // Best-effort persist of the new LWW timestamp; if it
                    // fails we still keep the in-memory view and applied
                    // data, and will recompute on next sync.
                    if let Err(e) = executor.set_lww_timestamp(&table, &key_bytes, ts).await {
                        error!(
                            "Failed to persist LWW timestamp for table={} id={}: {}",
                            table, entry.id, e
                        );
                    }
                    applied += 1;
                }
                Err(e) => {
                    error!("Failed to decode SyncOperation id={}: {}", op.id, e);
                }
            }
        }

        {
            let mut stats = self.stats.lock().await;
            stats.total_applied = stats.total_applied.saturating_add(applied);
            stats.total_skipped_lww = stats.total_skipped_lww.saturating_add(skipped_lww);
            info!(
                "SyncServer applied {} operations from edge (skipped_lww={}, total_applied={}, total_skipped_lww={})",
                applied,
                skipped_lww,
                stats.total_applied,
                stats.total_skipped_lww,
            );
        }

        // Update the per-edge cursor if we observed any new IDs.
        if max_seen_id > last_applied_id {
            if let Err(e) = self
                .set_last_applied_id(&mut executor, &edge_id, max_seen_id)
                .await
            {
                error!(
                    "Failed to persist last_applied_id for edge {} to {}: {}",
                    edge_id, max_seen_id, e
                );
            }
        }

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

        let sql = request.get_ref().sql.clone();
        info!("SqlServer::execute_sql: Received SQL: {}", sql);
        let is_read_query = sql.trim_start().to_uppercase().starts_with("SELECT");

        let auth = authenticate_request(request.metadata(), is_read_query)?;
        let role_label = match auth.role {
            Some(Role::Admin) => "Admin",
            Some(Role::ReadOnly) => "ReadOnly",
            None => "None",
        };
        info!(
            "audit_sql user={} role={} sql={}",
            auth.user, role_label, sql
        );

        // Parse the SQL
        let ast = match Parser::parse(&sql) {
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

        // Use tokio Mutex for async-safe locking
        let mut executor = self.executor.lock().await;
        info!("SqlServer::execute_sql: calling Executor::execute");
        let result = executor.execute(ast).await.map_err(|e| {
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

        if response.success {
            metrics::record_sql(is_read_query);
        }
        info!(
            "SqlServer::execute_sql: returning SqlResponse (success={})",
            response.success
        );
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Mutex as TokioMutex, RwLock};
    use tonic::metadata::{AsciiMetadataValue, MetadataMap};
    use tonic::{Code, Request};
    use crate::network::SyncStatusState;

    fn make_metadata(user: &str, auth_header: Option<&str>) -> MetadataMap {
        let mut meta = MetadataMap::new();
        meta.insert(
            "x-chronos-user",
            AsciiMetadataValue::try_from(user).expect("user meta"),
        );
        if let Some(h) = auth_header {
            meta.insert(
                "authorization",
                AsciiMetadataValue::try_from(h).expect("auth meta"),
            );
        }
        meta
    }

    #[test]
    fn authenticate_request_allows_anonymous_when_no_tokens_set() {
        std::env::remove_var("CHRONOS_AUTH_TOKEN_ADMIN");
        std::env::remove_var("CHRONOS_AUTH_TOKEN_READONLY");

        let meta = make_metadata("alice", None);
        let ctx = authenticate_request(&meta, false).expect("auth should succeed");
        assert_eq!(ctx.user, "alice");
        assert!(ctx.role.is_none());
    }

    #[test]
    fn authenticate_request_admin_token_allows_write() {
        std::env::set_var("CHRONOS_AUTH_TOKEN_ADMIN", "secret-admin");
        std::env::remove_var("CHRONOS_AUTH_TOKEN_READONLY");

        let meta = make_metadata("bob", Some("Bearer secret-admin"));
        let ctx = authenticate_request(&meta, false).expect("auth should succeed");
        assert_eq!(ctx.user, "bob");
        matches!(ctx.role, Some(Role::Admin));
    }

    #[test]
    fn authenticate_request_readonly_rejects_write_queries() {
        std::env::remove_var("CHRONOS_AUTH_TOKEN_ADMIN");
        std::env::set_var("CHRONOS_AUTH_TOKEN_READONLY", "ro-token");

        let meta = make_metadata("carol", Some("Bearer ro-token"));
        let err = authenticate_request(&meta, false).unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    #[tokio::test]
    async fn health_server_reports_connectivity_state() {
        let state = Arc::new(RwLock::new(ConnectivityState::Reconnecting));
        let server = HealthServer::new(Arc::clone(&state));

        let req = Request::new(HealthRequest {});
        let resp = server.get_connectivity(req).await.expect("health OK");
        let inner = resp.into_inner();
        assert_eq!(inner.state, "Reconnecting");
    }

    #[tokio::test]
    async fn sync_status_server_returns_current_status() {
        let status = SyncStatusState {
            pending_ops: 5,
            last_sync_applied: 10,
            last_sync_ts_ms: 1234,
            last_error: Some("boom".to_string()),
        };
        let shared: SharedSyncStatus = Arc::new(TokioMutex::new(status));
        let server = SyncStatusServer::new(Arc::clone(&shared));

        let req = Request::new(SyncStatusRequest {});
        let resp = server.get_sync_status(req).await.expect("sync_status OK");
        let inner = resp.into_inner();

        assert_eq!(inner.pending_ops, 5);
        assert_eq!(inner.last_sync_applied, 10);
        assert_eq!(inner.last_sync_ts_ms, 1234);
        assert_eq!(inner.last_error, "boom".to_string());
    }
}
