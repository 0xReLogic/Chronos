pub mod sled_engine;
pub mod error;
pub mod wal;
pub mod offline_queue;
pub mod snapshot;

pub use sled_engine::SledEngine;
pub use error::StorageError;

use crate::parser::Value;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::common::timestamp::HybridTimestamp;
use crate::storage::offline_queue::PersistentQueuedOperation;
use crate::storage::wal::Operation;

/// Storage engine abstraction for pluggable backends
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    /// Initialize storage (create dirs, open connections, etc)
    async fn init(&mut self) -> Result<()>;
    
    /// Create a new table with schema
    async fn create_table(&mut self, table_name: &str, schema: TableSchema) -> Result<()>;
    
    /// Insert rows into table
    async fn insert(&mut self, table_name: &str, rows: Vec<Row>) -> Result<()>;
    
    /// Query rows with optional filter
    async fn query(&self, table_name: &str, filter: Option<Filter>) -> Result<Vec<Row>>;
    
    /// Delete rows matching filter
    async fn delete(&mut self, table_name: &str, filter: Filter) -> Result<usize>;

    /// Cleanup expired rows based on TTL metadata (if supported). Returns number of deleted rows.
    async fn cleanup_expired(&mut self, _now_secs: u64, _limit: usize) -> Result<u64> {
        Ok(0)
    }
    
    /// Create index on column
    async fn create_index(&mut self, table_name: &str, column: &str) -> Result<()>;
    
    /// Get table metadata
    async fn get_schema(&self, table_name: &str) -> Result<Option<TableSchema>>;
    
    /// List all tables
    async fn list_tables(&self) -> Result<Vec<String>>;
    
    /// Checkpoint/flush to disk
    async fn checkpoint(&mut self) -> Result<()>;
    
    /// Close storage gracefully
    async fn close(&mut self) -> Result<()>;

    /// Apply a low-level storage operation (used by sync replication).
    /// Default implementation returns an error for engines that do not
    /// support this.
    async fn apply_operation(&mut self, _op: Operation) -> Result<()> {
        Err(anyhow::anyhow!("apply_operation not supported for this storage engine"))
    }

    /// Drain at most `limit` operations from the offline queue. Default
    /// implementation returns an empty vector for engines that do not
    /// support offline queueing.
    async fn drain_offline_queue(&self, _limit: usize) -> Result<Vec<PersistentQueuedOperation>> {
        Ok(Vec::new())
    }

    /// Requeue operations back into the offline queue. Default implementation
    /// is a no-op for engines without offline queue support.
    async fn requeue_offline_ops(&self, _ops: Vec<PersistentQueuedOperation>) -> Result<()> {
        Ok(())
    }

    /// Get the last-seen HybridTimestamp for a given logical key (table, key)
    /// used by the edge->cloud sync LWW conflict resolution. Default
    /// implementation returns None for engines that do not support it.
    async fn get_lww_ts(
        &self,
        _table: &str,
        _key: &[u8],
    ) -> Result<Option<HybridTimestamp>> {
        Ok(None)
    }

    /// Persist the last-seen HybridTimestamp for a given logical key
    /// (table, key) used by the edge->cloud sync LWW conflict resolution.
    /// Default implementation is a no-op for engines that do not support it.
    async fn set_lww_ts(
        &mut self,
        _table: &str,
        _key: &[u8],
        _ts: HybridTimestamp,
    ) -> Result<()> {
        Ok(())
    }
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub ttl_seconds: Option<u64>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// Supported data types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
pub enum DataType {
    Int,
    Float,
    String,
    Boolean,
    Timestamp,
}

/// Row representation
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct Row {
    pub values: HashMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
    
    pub fn insert(&mut self, column: String, value: Value) {
        self.values.insert(column, value);
    }
    
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

/// Query filter for WHERE clauses
#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub op: FilterOp,
    pub value: Value,
}

/// Filter operations
#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq,
    Ne,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    Lte,
    Gte,
    Between(Value),
    Like,
}

impl Filter {
    pub fn matches(&self, row: &Row) -> bool {
        let row_value = match row.get(&self.column) {
            Some(v) => v,
            None => return false,
        };
        
        match &self.op {
            FilterOp::Eq => row_value == &self.value,
            FilterOp::Ne | FilterOp::NotEq => row_value != &self.value,
            FilterOp::Lt => self.compare_lt(row_value, &self.value),
            FilterOp::Le | FilterOp::Lte => !self.compare_gt(row_value, &self.value),
            FilterOp::Gt => self.compare_gt(row_value, &self.value),
            FilterOp::Ge | FilterOp::Gte => !self.compare_lt(row_value, &self.value),
            FilterOp::Like => self.matches_like(row_value, &self.value),
            FilterOp::Between(high) => {
                !self.compare_lt(row_value, &self.value) && 
                !self.compare_gt(row_value, high)
            }
        }
    }
    
    fn compare_lt(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(av), Value::Integer(bv)) => av < bv,
            (Value::Float(av), Value::Float(bv)) => av < bv,
            (Value::String(av), Value::String(bv)) => av < bv,
            _ => false,
        }
    }
    
    fn compare_gt(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(av), Value::Integer(bv)) => av > bv,
            (Value::Float(av), Value::Float(bv)) => av > bv,
            (Value::String(av), Value::String(bv)) => av > bv,
            _ => false,
        }
    }
    
    fn matches_like(&self, row_value: &Value, pattern: &Value) -> bool {
        match (row_value, pattern) {
            (Value::String(s), Value::String(p)) => {
                // Simple LIKE implementation (% = wildcard)
                let pattern = p.replace('%', ".*");
                regex::Regex::new(&pattern)
                    .map(|re| re.is_match(s))
                    .unwrap_or(false)
            }
            _ => false,
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone)]
pub enum StorageConfig {
    Sled { data_dir: String },
}

/// Factory for creating storage engines
pub fn create_storage_engine(config: StorageConfig) -> Result<Box<dyn StorageEngine>> {
    match config {
        StorageConfig::Sled { data_dir } => {
            Ok(Box::new(sled_engine::SledEngine::new(&data_dir)?))
        }
    }
}