mod error;

use tokio::sync::Mutex;
use crate::parser::{Ast, Statement, Value, ColumnDefinition, DataType as ParserDataType, Condition, Operator, TtlSpec};
use crate::storage::{create_storage_engine, StorageEngine, StorageConfig, TableSchema, Column, DataType, Row, Filter, FilterOp};
use crate::storage::offline_queue::PersistentQueuedOperation;

pub use self::error::ExecutorError;

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
}

impl Executor {
    pub async fn new(data_dir: &str) -> Self {
        let config = StorageConfig::Sled { data_dir: data_dir.to_string() };
        let mut storage = create_storage_engine(config).expect("Failed to create storage engine");
        
        // Initialize storage
        storage.init().await.expect("Failed to initialize storage");
        
        Self {
            storage,
            raft_node: None,
            in_transaction: false,
            transaction_buffer: Vec::new(),
        }
    }
    
    pub fn set_raft_node(&mut self, node: std::sync::Weak<Mutex<crate::raft::RaftNode>>) {
        self.raft_node = Some(node);
    }
    
    pub async fn execute(&mut self, ast: Ast) -> Result<QueryResult, ExecutorError> {
        // In a distributed setting, we should check if this is a read-only query
        // Read-only queries can be executed directly, but write queries should go through Raft
        let is_read_only = matches!(&ast, Ast::Statement(Statement::Select { .. }));
        
        if is_read_only {
            // Read-only queries can be executed directly
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
                        .map_err(|e| ExecutorError::ExecutionError(format!("Serialization error: {e}")))?;
                    
                    // Submit to Raft (release lock after this)
                    {
                        let mut node = node.lock().await;
                        node.submit_command(command)
                            .map_err(|e| ExecutorError::ExecutionError(format!("Raft error: {e}")))?;
                    }
                    
                    // For now, we'll just execute the command directly
                    // In a real implementation, we would wait for the command to be committed
                    match ast {
                        Ast::Statement(stmt) => self.execute_statement(stmt).await,
                    }
                } else {
                    // Raft node has been dropped
                    Err(ExecutorError::ExecutionError("Raft node not available".to_string()))
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
        let (ast, _): (Ast, usize) = bincode::decode_from_slice(command, bincode::config::standard())
            .map_err(|e| ExecutorError::ExecutionError(format!("Deserialization error: {e}")))?;
        
        // Execute the command
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
                    return Err(ExecutorError::ExecutionError("Already in a transaction".to_string()));
                }
                self.in_transaction = true;
                self.transaction_buffer.clear();
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String("Transaction started".to_string())]],
                })
            },
            Statement::Commit => {
                if !self.in_transaction {
                    return Err(ExecutorError::ExecutionError("Not in a transaction".to_string()));
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
            },
            Statement::Rollback => {
                if !self.in_transaction {
                    return Err(ExecutorError::ExecutionError("Not in a transaction".to_string()));
                }
                self.in_transaction = false;
                self.transaction_buffer.clear();
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String("Transaction rolled back".to_string())]],
                })
            },
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
            Statement::CreateTable { table_name, columns, ttl } => {
                let schema = self.convert_to_table_schema(&table_name, &columns, ttl);
                
                self.storage.create_table(&table_name, schema).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Table {table_name} created"))]],
                })
            },
            Statement::Insert { table_name, columns, values } => {
                let row = self.convert_to_row(&table_name, columns.as_deref(), &values).await?;
                
                self.storage.insert(&table_name, vec![row]).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Inserted into {table_name}"))]],
                })
            },
            Statement::Select { table_name, columns, conditions } => {
                let filter = self.convert_to_filter(conditions.as_ref());
                
                let rows = self.storage.query(&table_name, filter).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                self.convert_rows_to_query_result(&columns, rows)
            },
            Statement::Update { table_name, assignments, conditions } => {
                // Simplified: fetch, modify, delete old, insert new
                let filter = self.convert_to_filter(conditions.as_ref());
                
                let mut rows = self.storage.query(&table_name, filter.clone()).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                // Apply assignments
                for row in &mut rows {
                    for assignment in &assignments {
                        row.values.insert(assignment.column_name.clone(), assignment.value.clone());
                    }
                }
                
                // Delete old rows if filter exists
                if let Some(f) = filter {
                    self.storage.delete(&table_name, f).await
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                }
                
                // Insert updated rows
                self.storage.insert(&table_name, rows).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Updated rows in {table_name}"))]],
                })
            },
            Statement::Delete { table_name, conditions } => {
                let filter = self.convert_to_filter(conditions.as_ref())
                    .ok_or_else(|| ExecutorError::ExecutionError("DELETE requires WHERE clause".to_string()))?;
                
                let deleted = self.storage.delete(&table_name, filter).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Deleted {deleted} rows from {table_name}"))]],
                })
            },
            Statement::CreateIndex { index_name: _, table_name, column_name } => {
                self.storage.create_index(&table_name, &column_name).await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Index created on {table_name}.{column_name}"))]],
                })
            },

            // Transaction statements should not reach here
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Err(ExecutorError::ExecutionError("Cannot execute transaction control statements directly".to_string()))
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

    async fn convert_to_row(&self, table_name: &str, columns: Option<&[String]>, values: &[Value]) -> Result<Row, ExecutorError> {
        let schema = self.storage.get_schema(table_name).await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or_else(|| ExecutorError::ExecutionError(format!("Table {table_name} not found")))?;
        
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
    
    fn convert_rows_to_query_result(&self, columns: &[String], rows: Vec<Row>) -> Result<QueryResult, ExecutorError> {
        let col_names = if columns.contains(&"*".to_string()) || columns.is_empty() {
            if let Some(first_row) = rows.first() {
                first_row.values.keys().cloned().collect()
            } else {
                vec![]
            }
        } else {
            columns.to_vec()
        };
        
        let result_rows: Vec<Vec<Value>> = rows.iter().map(|row| {
            col_names.iter().filter_map(|col| {
                row.get(col).cloned()
            }).collect()
        }).collect();
        
        Ok(QueryResult {
            columns: col_names,
            rows: result_rows,
        })
    }

    pub async fn drain_offline_queue(&self, limit: usize) -> Result<Vec<PersistentQueuedOperation>, ExecutorError> {
        self.storage
            .drain_offline_queue(limit)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }

    pub async fn requeue_offline_ops(&self, ops: Vec<PersistentQueuedOperation>) -> Result<(), ExecutorError> {
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

    pub async fn cleanup_expired(&mut self, now_secs: u64, limit: usize) -> Result<u64, ExecutorError> {
        self.storage
            .cleanup_expired(now_secs, limit)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))
    }
}