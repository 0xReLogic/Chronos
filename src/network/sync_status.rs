use std::sync::Arc;
use tokio::sync::Mutex;

/// In-memory state for sync status metrics on a single node.
#[derive(Debug, Clone, Default)]
pub struct SyncStatusState {
    /// Approximate number of pending operations in the offline queue
    /// at the time of the last sync attempt.
    pub pending_ops: u64,
    /// Number of operations successfully applied in the last sync batch.
    pub last_sync_applied: u64,
    /// Timestamp of the last sync attempt, in milliseconds since UNIX_EPOCH.
    pub last_sync_ts_ms: u64,
    /// Last sync error message, if any (empty/None if last attempt succeeded).
    pub last_error: Option<String>,
}

pub type SharedSyncStatus = Arc<Mutex<SyncStatusState>>;
