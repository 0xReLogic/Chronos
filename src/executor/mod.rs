mod error;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::timestamp::HybridTimestamp;
use crate::parser::{
    Ast, ColumnDefinition, Condition, DataType as ParserDataType, Operator, Statement, TtlSpec,
    Value,
};
use crate::storage::offline_queue::PersistentQueuedOperation;
use crate::storage::{
    create_storage_engine, Column, DataType, Filter, FilterOp, Row, StorageConfig, StorageEngine,
    TableSchema,
};
use tokio::sync::Mutex;

pub use self::error::ExecutorError;

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
struct AggBucket {
    // Identifier for the time bucket (epoch minute index).
    start_minute: u64,
    sum: f64,
    count: u64,
}

impl AggBucket {
    fn new() -> Self {
        Self {
            start_minute: 0,
            sum: 0.0,
            count: 0,
        }
    }
}

#[derive(Debug)]
struct ColumnAggState {
    // Fixed-size ring buffer of 60 one-minute buckets for a 1-hour window.
    minute_buckets: Vec<AggBucket>,
    // Fixed-size ring buffer of 24 one-hour buckets for a 24-hour window.
    hour_buckets: Vec<AggBucket>,
    // Fixed-size ring buffer of 7 one-day buckets for a 7-day window.
    day_buckets: Vec<AggBucket>,
}

impl ColumnAggState {
    fn new() -> Self {
        Self {
            minute_buckets: vec![AggBucket::new(); 60],
            hour_buckets: vec![AggBucket::new(); 24],
            day_buckets: vec![AggBucket::new(); 7],
        }
    }

    fn update(&mut self, now_secs: u64, value: f64) {
        Self::update_buckets(&mut self.minute_buckets, now_secs, value, 60);
        Self::update_buckets(&mut self.hour_buckets, now_secs, value, 60 * 60);
        Self::update_buckets(&mut self.day_buckets, now_secs, value, 24 * 60 * 60);
    }

    fn avg_last_1h(&self, now_secs: u64) -> Option<f64> {
        Self::avg_over_window(&self.minute_buckets, now_secs, 60, 60)
    }

    fn avg_last_24h(&self, now_secs: u64) -> Option<f64> {
        Self::avg_over_window(&self.hour_buckets, now_secs, 24, 60 * 60)
    }

    fn avg_last_7d(&self, now_secs: u64) -> Option<f64> {
        Self::avg_over_window(&self.day_buckets, now_secs, 7, 24 * 60 * 60)
    }

    fn update_buckets(buckets: &mut [AggBucket], now_secs: u64, value: f64, bucket_span_secs: u64) {
        let bucket_index = now_secs / bucket_span_secs;
        let len = buckets.len() as u64;
        if len == 0 {
            return;
        }
        let idx = (bucket_index % len) as usize;
        let bucket = &mut buckets[idx];

        if bucket.start_minute != bucket_index {
            bucket.start_minute = bucket_index;
            bucket.sum = 0.0;
            bucket.count = 0;
        }

        bucket.sum += value;
        bucket.count += 1;
    }

    fn avg_over_window(
        buckets: &[AggBucket],
        now_secs: u64,
        window_buckets: u64,
        bucket_span_secs: u64,
    ) -> Option<f64> {
        if buckets.is_empty() || window_buckets == 0 {
            return None;
        }

        let current_index = now_secs / bucket_span_secs;
        let len = buckets.len() as u64;
        let mut sum = 0.0;
        let mut count = 0u64;

        for offset in 0..window_buckets {
            let index = current_index.saturating_sub(offset);
            let idx = (index % len) as usize;
            let bucket = &buckets[idx];
            if bucket.start_minute == index {
                sum += bucket.sum;
                count = count.saturating_add(bucket.count);
            }
        }

        if count == 0 {
            None
        } else {
            Some(sum / count as f64)
        }
    }
}

#[derive(Debug)]
struct TableAggState {
    columns: HashMap<String, ColumnAggState>,
}

impl TableAggState {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    fn update_row(&mut self, now_secs: u64, row: &Row) {
        for (col_name, value) in &row.values {
            if let Value::Float(f) = value {
                let entry = self
                    .columns
                    .entry(col_name.clone())
                    .or_insert_with(ColumnAggState::new);
                entry.update(now_secs, *f);
            }
        }
    }

    fn avg_last_1h(&self, now_secs: u64, column: &str) -> Option<f64> {
        let state = self.columns.get(column)?;
        state.avg_last_1h(now_secs)
    }

    fn avg_last_24h(&self, now_secs: u64, column: &str) -> Option<f64> {
        let state = self.columns.get(column)?;
        state.avg_last_24h(now_secs)
    }

    fn avg_last_7d(&self, now_secs: u64, column: &str) -> Option<f64> {
        let state = self.columns.get(column)?;
        state.avg_last_7d(now_secs)
    }
}

#[derive(Debug)]
struct WindowAggState {
    tables: HashMap<String, TableAggState>,
}

impl WindowAggState {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn update_row(&mut self, table: &str, now_secs: u64, row: &Row) {
        let table_state = self
            .tables
            .entry(table.to_string())
            .or_insert_with(TableAggState::new);
        table_state.update_row(now_secs, row);
    }

    fn avg_last_1h(&self, table: &str, column: &str, now_secs: u64) -> Option<f64> {
        let table_state = self.tables.get(table)?;
        table_state.avg_last_1h(now_secs, column)
    }

    fn avg_last_24h(&self, table: &str, column: &str, now_secs: u64) -> Option<f64> {
        let table_state = self.tables.get(table)?;
        table_state.avg_last_24h(now_secs, column)
    }

    fn avg_last_7d(&self, table: &str, column: &str, now_secs: u64) -> Option<f64> {
        let table_state = self.tables.get(table)?;
        table_state.avg_last_7d(now_secs, column)
    }
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

pub struct Executor {
    storage: Box<dyn StorageEngine>,
    raft_node: Option<std::sync::Weak<Mutex<crate::raft::RaftNode>>>,
    in_transaction: bool,
    transaction_buffer: Vec<Statement>,
    agg_state: WindowAggState,
    agg_tree: sled::Tree,
}

impl Executor {
    pub async fn new(data_dir: &str) -> Self {
        let config = StorageConfig::Sled {
            data_dir: data_dir.to_string(),
        };
        let mut storage = create_storage_engine(config).expect("Failed to create storage engine");

        // Initialize storage
        storage.init().await.expect("Failed to initialize storage");

        // Open a dedicated sled tree for aggregation state snapshots.
        let agg_db = sled::open(format!("{}/agg_state", data_dir))
            .expect("Failed to open aggregation state DB");
        let agg_tree = agg_db
            .open_tree("__agg_state__")
            .expect("Failed to open aggregation state tree");

        let mut this = Self {
            storage,
            raft_node: None,
            in_transaction: false,
            transaction_buffer: Vec::new(),
            agg_state: WindowAggState::new(),
            agg_tree,
        };

        this.load_agg_state_from_sled();

        this
    }

    pub fn set_raft_node(&mut self, node: std::sync::Weak<Mutex<crate::raft::RaftNode>>) {
        self.raft_node = Some(node);
    }

    pub fn avg_last_1h(&self, table: &str, column: &str) -> Option<f64> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.agg_state.avg_last_1h(table, column, now_secs)
    }

    pub fn avg_last_24h(&self, table: &str, column: &str) -> Option<f64> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.agg_state.avg_last_24h(table, column, now_secs)
    }

    pub fn avg_last_7d(&self, table: &str, column: &str) -> Option<f64> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.agg_state.avg_last_7d(table, column, now_secs)
    }

    pub async fn execute(&mut self, ast: Ast) -> Result<QueryResult, ExecutorError> {
        // In a distributed setting, we should check if this is a read-only query
        // Read-only queries can be executed directly, but write queries should go through Raft
        let is_read_only = matches!(
            &ast,
            Ast::Statement(
                Statement::Select { .. }
                    | Statement::SelectAgg1h { .. }
                    | Statement::SelectAgg24h { .. }
                    | Statement::SelectAgg7d { .. }
            )
        );

        if is_read_only {
            // Read-only queries: if in distributed mode, prefer serving on the leader.
            if let Some(node_weak) = &self.raft_node {
                if let Some(node) = node_weak.upgrade() {
                    let (is_leader, lease_ok) = {
                        let node = node.lock().await;
                        (node.is_leader(), node.can_serve_read_locally())
                    };
                    if !is_leader {
                        return Err(ExecutorError::ExecutionError("Not the leader".to_string()));
                    }
                    // If leader: lease_ok can be used for observability; reads proceed regardless.
                    let _ = lease_ok;
                }
            }

            match ast {
                Ast::Statement(stmt) => self.execute_statement(stmt).await,
            }
        } else {
            // Write queries should go through Raft
            if let Some(node_weak) = &self.raft_node {
                if let Some(node) = node_weak.upgrade() {
                    let is_leader = {
                        let node = node.lock().await;
                        node.is_leader()
                    };

                    // Check if this node is the leader
                    if !is_leader {
                        return Err(ExecutorError::ExecutionError("Not the leader".to_string()));
                    }

                    // Serialize the command
                    let command = bincode::encode_to_vec(&ast, bincode::config::standard())
                        .map_err(|e| {
                            ExecutorError::ExecutionError(format!("Serialization error: {e}"))
                        })?;

                    // Submit to Raft and wait for the log entry to be
                    // committed and applied locally on the leader. This
                    // ensures the write is durably replicated before we
                    // return to the client.
                    let rx = {
                        let mut node = node.lock().await;
                        node.submit_command(command).map_err(|e| {
                            ExecutorError::ExecutionError(format!("Raft error: {e}"))
                        })?
                    };

                    rx.await.map_err(|e| {
                        ExecutorError::ExecutionError(format!("Raft commit wait error: {e}"))
                    })?;

                    // Once committed and applied via the Raft state machine,
                    // we can execute a read-only view if needed. For now,
                    // return an empty result for pure writes.
                    Ok(QueryResult::empty())
                } else {
                    // Raft node has been dropped
                    Err(ExecutorError::ExecutionError(
                        "Raft node not available".to_string(),
                    ))
                }
            } else {
                // No Raft node, execute directly (single-node mode)
                match ast {
                    Ast::Statement(stmt) => self.execute_statement(stmt).await,
                }
            }
        }
    }

    // This method is called by the Raft state machine when a command is committed
    pub async fn apply_command(&mut self, command: &[u8]) -> Result<(), ExecutorError> {
        // Deserialize the command
        let (ast, _): (Ast, usize) =
            bincode::decode_from_slice(command, bincode::config::standard()).map_err(|e| {
                ExecutorError::ExecutionError(format!("Deserialization error: {e}"))
            })?;
        if let Ast::Statement(Statement::CreateTable { table_name, .. }) = &ast {
            let schema_exists = self
                .storage
                .get_schema(table_name)
                .await
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            if schema_exists.is_some() {
                return Ok(());
            }
        }

        match ast {
            Ast::Statement(stmt) => {
                self.execute_statement(stmt).await?;
                Ok(())
            }
        }
    }

    async fn execute_statement(&mut self, stmt: Statement) -> Result<QueryResult, ExecutorError> {
        match stmt {
            Statement::Begin => {
                if self.in_transaction {
                    return Err(ExecutorError::ExecutionError(
                        "Already in a transaction".to_string(),
                    ));
                }
                self.in_transaction = true;
                self.transaction_buffer.clear();
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String("Transaction started".to_string())]],
                })
            }
            Statement::Commit => {
                if !self.in_transaction {
                    return Err(ExecutorError::ExecutionError(
                        "Not in a transaction".to_string(),
                    ));
                }

                // To avoid borrow checker issues (E0499), we first collect the statements
                // into a new Vec. This releases the mutable borrow on `self.transaction_buffer`
                // before we start calling `self.execute_immediate` which borrows `self` again.
                let stmts_to_execute: Vec<Statement> = self.transaction_buffer.drain(..).collect();

                for buffered_stmt in stmts_to_execute {
                    // Note: This is a simplified commit. A real implementation would need a way to roll back
                    // partially applied changes if one of the statements fails.
                    self.execute_immediate(buffered_stmt).await?;
                }

                self.in_transaction = false;
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String("Transaction committed".to_string())]],
                })
            }
            Statement::Rollback => {
                if !self.in_transaction {
                    return Err(ExecutorError::ExecutionError(
                        "Not in a transaction".to_string(),
                    ));
                }
                self.in_transaction = false;
                self.transaction_buffer.clear();
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String("Transaction rolled back".to_string())]],
                })
            }
            // For any other statement
            _ => {
                if self.in_transaction {
                    // Buffer the statement instead of executing it
                    self.transaction_buffer.push(stmt);
                    Ok(QueryResult {
                        columns: vec!["result".to_string()],
                        rows: vec![vec![Value::String("Statement buffered".to_string())]],
                    })
                } else {
                    // If not in a transaction, execute immediately
                    self.execute_immediate(stmt).await
                }
            }
        }
    }

    /// Executes a statement directly against the storage engine, bypassing transaction logic.
    async fn execute_immediate(&mut self, stmt: Statement) -> Result<QueryResult, ExecutorError> {
        match stmt {
            Statement::CreateTable {
                table_name,
                columns,
                ttl,
            } => {
                let schema = self.convert_to_table_schema(&table_name, &columns, ttl);

                self.storage
                    .create_table(&table_name, schema)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Table {table_name} created"))]],
                })
            }
            Statement::Insert {
                table_name,
                columns,
                values,
            } => {
                let row = self
                    .convert_to_row(&table_name, columns.as_deref(), &values)
                    .await?;
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                self.agg_state.update_row(&table_name, now_secs, &row);
                self.persist_agg_buckets(&table_name, now_secs, &row);

                self.storage
                    .insert(&table_name, vec![row])
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Inserted into {table_name}"))]],
                })
            }
            Statement::Select {
                table_name,
                columns,
                conditions,
            } => {
                let filter = self.convert_to_filter(conditions.as_ref());

                let rows = self
                    .storage
                    .query(&table_name, filter)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                self.convert_rows_to_query_result(&columns, rows)
            }
            Statement::Update {
                table_name,
                assignments,
                conditions,
            } => {
                // Simplified: fetch, modify, delete old, insert new
                let filter = self.convert_to_filter(conditions.as_ref());

                let mut rows = self
                    .storage
                    .query(&table_name, filter.clone())
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                // Apply assignments
                for row in &mut rows {
                    for assignment in &assignments {
                        row.values
                            .insert(assignment.column_name.clone(), assignment.value.clone());
                    }
                }

                // Delete old rows if filter exists
                if let Some(f) = filter {
                    self.storage
                        .delete(&table_name, f)
                        .await
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                }

                // Insert updated rows
                self.storage
                    .insert(&table_name, rows)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Updated rows in {table_name}"))]],
                })
            }
            Statement::Delete {
                table_name,
                conditions,
            } => {
                let filter = self.convert_to_filter(conditions.as_ref()).ok_or_else(|| {
                    ExecutorError::ExecutionError("DELETE requires WHERE clause".to_string())
                })?;

                let deleted = self
                    .storage
                    .delete(&table_name, filter)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!(
                        "Deleted {deleted} rows from {table_name}"
                    ))]],
                })
            }
            Statement::CreateIndex {
                index_name: _,
                table_name,
                column_name,
            } => {
                self.storage
                    .create_index(&table_name, &column_name)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!(
                        "Index created on {table_name}.{column_name}"
                    ))]],
                })
            }

            Statement::SelectAgg1h {
                table_name,
                column_name,
            } => {
                let col_label = format!("avg_1h({})", column_name);
                let value = self
                    .avg_last_1h(&table_name, &column_name)
                    .map(Value::Float)
                    .unwrap_or(Value::Null);

                Ok(QueryResult {
                    columns: vec![col_label],
                    rows: vec![vec![value]],
                })
            }

            Statement::SelectAgg24h {
                table_name,
                column_name,
            } => {
                let col_label = format!("avg_24h({})", column_name);
                let value = self
                    .avg_last_24h(&table_name, &column_name)
                    .map(Value::Float)
                    .unwrap_or(Value::Null);

                Ok(QueryResult {
                    columns: vec![col_label],
                    rows: vec![vec![value]],
                })
            }

            Statement::SelectAgg7d {
                table_name,
                column_name,
            } => {
                let col_label = format!("avg_7d({})", column_name);
                let value = self
                    .avg_last_7d(&table_name, &column_name)
                    .map(Value::Float)
                    .unwrap_or(Value::Null);

                Ok(QueryResult {
                    columns: vec![col_label],
                    rows: vec![vec![value]],
                })
            }

            // Transaction statements should not reach here
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Err(ExecutorError::ExecutionError(
                    "Cannot execute transaction control statements directly".to_string(),
                ))
            }
        }
    }

    // Helper conversion methods
    fn convert_to_table_schema(
        &self,
        table_name: &str,
        columns: &[ColumnDefinition],
        ttl: Option<TtlSpec>,
    ) -> TableSchema {
        TableSchema {
            name: table_name.to_string(),
            columns: columns
                .iter()
                .map(|col| Column {
                    name: col.name.clone(),
                    data_type: match col.data_type {
                        ParserDataType::Int => DataType::Int,
                        ParserDataType::Text | ParserDataType::String => DataType::String,
                        ParserDataType::Float => DataType::Float,
                        ParserDataType::Boolean => DataType::Boolean,
                    },
                })
                .collect(),
            ttl_seconds: ttl.map(|t| t.seconds),
        }
    }

    async fn convert_to_row(
        &self,
        table_name: &str,
        columns: Option<&[String]>,
        values: &[Value],
    ) -> Result<Row, ExecutorError> {
        let schema = self
            .storage
            .get_schema(table_name)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or_else(|| {
                ExecutorError::ExecutionError(format!("Table {table_name} not found"))
            })?;

        let mut row = Row::new();

        if let Some(cols) = columns {
            for (i, col_name) in cols.iter().enumerate() {
                if i < values.len() {
                    row.insert(col_name.clone(), values[i].clone());
                }
            }
        } else {
            for (i, col) in schema.columns.iter().enumerate() {
                if i < values.len() {
                    row.insert(col.name.clone(), values[i].clone());
                }
            }
        }

        Ok(row)
    }

    fn convert_to_filter(&self, conditions: Option<&Vec<Condition>>) -> Option<Filter> {
        conditions?.first().map(|cond| Filter {
            column: cond.column_name.clone(),
            op: match cond.operator {
                Operator::Equals => FilterOp::Eq,
                Operator::NotEquals => FilterOp::Ne,
                Operator::LessThan => FilterOp::Lt,
                Operator::LessThanOrEqual => FilterOp::Le,
                Operator::GreaterThan => FilterOp::Gt,
                Operator::GreaterThanOrEqual => FilterOp::Ge,
            },
            value: cond.value.clone(),
        })
    }

    fn convert_rows_to_query_result(
        &self,
        columns: &[String],
        rows: Vec<Row>,
    ) -> Result<QueryResult, ExecutorError> {
        let col_names = if columns.contains(&"*".to_string()) || columns.is_empty() {
            if let Some(first_row) = rows.first() {
                first_row.values.keys().cloned().collect()
            } else {
                vec![]
            }
        } else {
            columns.to_vec()
        };

        let result_rows: Vec<Vec<Value>> = rows
            .iter()
            .map(|row| {
                col_names
                    .iter()
                    .filter_map(|col| row.get(col).cloned())
                    .collect()
            })
            .collect();

        Ok(QueryResult {
            columns: col_names,
            rows: result_rows,
        })
    }

    pub async fn drain_offline_queue(
        &self,
        limit: usize,
    ) -> Result<Vec<PersistentQueuedOperation>, ExecutorError> {
        self.storage
            .drain_offline_queue(limit)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn requeue_offline_ops(
        &self,
        ops: Vec<PersistentQueuedOperation>,
    ) -> Result<(), ExecutorError> {
        self.storage
            .requeue_offline_ops(ops)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn apply_storage_operation(
        &mut self,
        op: crate::storage::wal::Operation,
    ) -> Result<(), ExecutorError> {
        self.storage
            .apply_operation(op)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn cleanup_expired(
        &mut self,
        now_secs: u64,
        limit: usize,
    ) -> Result<u64, ExecutorError> {
        self.storage
            .cleanup_expired(now_secs, limit)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn get_lww_timestamp(
        &self,
        table: &str,
        key: &[u8],
    ) -> Result<Option<HybridTimestamp>, ExecutorError> {
        self.storage
            .get_lww_ts(table, key)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn set_lww_timestamp(
        &mut self,
        table: &str,
        key: &[u8],
        ts: HybridTimestamp,
    ) -> Result<(), ExecutorError> {
        self.storage
            .set_lww_ts(table, key, ts)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    /// Apply a Raft log command that encodes an AST via bincode. This is
    /// used by the Raft state machine apply worker to bring followers'
    /// storage in sync with the leader.
    pub async fn apply_raft_command(&mut self, cmd: Vec<u8>) -> Result<(), ExecutorError> {
        let (ast, _): (Ast, usize) =
            bincode::serde::decode_from_slice(&cmd, bincode::config::standard())
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Special-case CREATE TABLE so that Raft apply is idempotent on
        // followers and on nodes that might have executed the statement
        // previously while acting as leader. If the table already exists,
        // we treat this as a no-op instead of an error.
        if let Ast::Statement(Statement::CreateTable { table_name, .. }) = &ast {
            let schema_exists = self
                .storage
                .get_schema(table_name)
                .await
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            if schema_exists.is_some() {
                return Ok(());
            }
        }

        // We are only interested in the side effects of executing this
        // statement (writes applied to storage, aggregates updated, etc.),
        // not in returning rows to a client. Drop the QueryResult.
        let _ = self.execute(ast).await?;
        Ok(())
    }
}

impl Executor {
    fn load_agg_state_from_sled(&mut self) {
        for item in self.agg_tree.iter() {
            let Ok((key, value)) = item else { continue };
            let key_str = match std::str::from_utf8(&key) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let mut parts = key_str.splitn(4, ':');
            let table = match parts.next() {
                Some(t) => t,
                None => continue,
            };
            let column = match parts.next() {
                Some(c) => c,
                None => continue,
            };
            let window = match parts.next() {
                Some(w) => w,
                None => continue,
            };
            let index_str = match parts.next() {
                Some(i) => i,
                None => continue,
            };

            let bucket_index: u64 = match index_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            let Ok((bucket, _)): Result<(AggBucket, usize), _> =
                bincode::decode_from_slice(&value, bincode::config::standard())
            else {
                continue;
            };

            let table_state = self
                .agg_state
                .tables
                .entry(table.to_string())
                .or_insert_with(TableAggState::new);
            let col_state = table_state
                .columns
                .entry(column.to_string())
                .or_insert_with(ColumnAggState::new);

            match window {
                "1h" => Self::restore_bucket(&mut col_state.minute_buckets, bucket_index, bucket),
                "24h" => Self::restore_bucket(&mut col_state.hour_buckets, bucket_index, bucket),
                "7d" => Self::restore_bucket(&mut col_state.day_buckets, bucket_index, bucket),
                _ => {}
            }
        }
    }

    fn restore_bucket(buckets: &mut [AggBucket], bucket_index: u64, bucket: AggBucket) {
        let len = buckets.len() as u64;
        if len == 0 {
            return;
        }
        let idx = (bucket_index % len) as usize;
        buckets[idx] = bucket;
    }

    fn persist_agg_buckets(&self, table: &str, now_secs: u64, row: &Row) {
        let table_state = match self.agg_state.tables.get(table) {
            Some(t) => t,
            None => return,
        };

        for (col_name, value) in &row.values {
            if let Value::Float(_) = value {
                let Some(col_state) = table_state.columns.get(col_name) else {
                    continue;
                };

                Self::persist_window(
                    &self.agg_tree,
                    table,
                    col_name,
                    "1h",
                    now_secs,
                    60,
                    &col_state.minute_buckets,
                );
                Self::persist_window(
                    &self.agg_tree,
                    table,
                    col_name,
                    "24h",
                    now_secs,
                    60 * 60,
                    &col_state.hour_buckets,
                );
                Self::persist_window(
                    &self.agg_tree,
                    table,
                    col_name,
                    "7d",
                    now_secs,
                    24 * 60 * 60,
                    &col_state.day_buckets,
                );
            }
        }
    }

    fn persist_window(
        tree: &sled::Tree,
        table: &str,
        column: &str,
        window: &str,
        now_secs: u64,
        bucket_span_secs: u64,
        buckets: &[AggBucket],
    ) {
        let len = buckets.len() as u64;
        if len == 0 {
            return;
        }

        let bucket_index = now_secs / bucket_span_secs;
        let idx = (bucket_index % len) as usize;
        let bucket = buckets[idx];

        let key = format!("{}:{}:{}:{}", table, column, window, bucket_index);
        if let Ok(bytes) = bincode::encode_to_vec(bucket, bincode::config::standard()) {
            let _ = tree.insert(key.as_bytes(), bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_agg_basic_avg_last_1h() {
        let mut state = WindowAggState::new();

        let base_secs: u64 = 3_600_000;

        // Insert first row at base time with value 10.0
        let mut row1 = Row::new();
        row1.insert("temp".to_string(), Value::Float(10.0));
        state.update_row("sensors", base_secs, &row1);

        // Insert second row one minute later with value 20.0
        let mut row2 = Row::new();
        row2.insert("temp".to_string(), Value::Float(20.0));
        state.update_row("sensors", base_secs + 60, &row2);

        let now_secs = base_secs + 60;
        let avg = state
            .avg_last_1h("sensors", "temp", now_secs)
            .expect("expected non-empty avg");

        assert!((avg - 15.0).abs() < 1e-9);
    }

    #[test]
    fn window_agg_ignores_data_older_than_1h() {
        let mut state = WindowAggState::new();

        let base_secs: u64 = 3_600_000;

        // Very old row: 2 hours before base
        let mut old_row = Row::new();
        old_row.insert("temp".to_string(), Value::Float(100.0));
        state.update_row("sensors", base_secs.saturating_sub(2 * 3600), &old_row);

        // Recent row at base time with value 10.0
        let mut recent_row = Row::new();
        recent_row.insert("temp".to_string(), Value::Float(10.0));
        state.update_row("sensors", base_secs, &recent_row);

        let now_secs = base_secs;
        let avg = state
            .avg_last_1h("sensors", "temp", now_secs)
            .expect("expected non-empty avg");

        // Old row should have fallen out of the 1h window, so only 10.0 remains
        assert!((avg - 10.0).abs() < 1e-9);
    }
}
