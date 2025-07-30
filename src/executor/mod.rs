mod error;

use std::sync::Mutex;
use crate::parser::{Ast, Statement, Value};
use crate::storage::Storage;

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
    storage: Storage,
    raft_node: Option<std::sync::Weak<Mutex<crate::raft::RaftNode>>>,
    in_transaction: bool,
    transaction_buffer: Vec<Statement>,
}

impl Executor {
    pub fn new(data_dir: &str) -> Self {
        Self {
            storage: Storage::new(data_dir),
            raft_node: None,
            in_transaction: false,
            transaction_buffer: Vec::new(),
        }
    }
    
    pub fn set_raft_node(&mut self, node: std::sync::Weak<Mutex<crate::raft::RaftNode>>) {
        self.raft_node = Some(node);
    }
    
    pub fn execute(&mut self, ast: Ast) -> Result<QueryResult, ExecutorError> {
        // In a distributed setting, we should check if this is a read-only query
        // Read-only queries can be executed directly, but write queries should go through Raft
        let is_read_only = match &ast {
            Ast::Statement(Statement::Select { .. }) => true,
            _ => false,
        };
        
        if is_read_only {
            // Read-only queries can be executed directly
            match ast {
                Ast::Statement(stmt) => self.execute_statement(stmt),
            }
        } else {
            // Write queries should go through Raft
            if let Some(node_weak) = &self.raft_node {
                if let Some(node) = node_weak.upgrade() {
                    let mut node = node.lock().unwrap();
                    
                    // Check if this node is the leader
                    if !node.is_leader() {
                        return Err(ExecutorError::ExecutionError("Not the leader".to_string()));
                    }
                    
                    // Serialize the command
                    let command = bincode::encode_to_vec(&ast, bincode::config::standard())
                        .map_err(|e| ExecutorError::ExecutionError(format!("Serialization error: {}", e)))?;
                    
                    // Submit to Raft
                    node.submit_command(command)
                        .map_err(|e| ExecutorError::ExecutionError(format!("Raft error: {}", e)))?;
                    
                    // For now, we'll just execute the command directly
                    // In a real implementation, we would wait for the command to be committed
                    match ast {
                        Ast::Statement(stmt) => self.execute_statement(stmt),
                    }
                } else {
                    // Raft node has been dropped
                    Err(ExecutorError::ExecutionError("Raft node not available".to_string()))
                }
            } else {
                // No Raft node, execute directly (single-node mode)
                match ast {
                    Ast::Statement(stmt) => self.execute_statement(stmt),
                }
            }
        }
    }
    
    // This method is called by the Raft state machine when a command is committed
    pub fn apply_command(&mut self, command: &[u8]) -> Result<(), ExecutorError> {
        // Deserialize the command
        let (ast, _): (Ast, usize) = bincode::decode_from_slice(command, bincode::config::standard())
            .map_err(|e| ExecutorError::ExecutionError(format!("Deserialization error: {}", e)))?;
        
        // Execute the command
        match ast {
            Ast::Statement(stmt) => {
                self.execute_statement(stmt)?;
                Ok(())
            }
        }
    }
    
        fn execute_statement(&mut self, stmt: Statement) -> Result<QueryResult, ExecutorError> {
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
                    self.execute_immediate(buffered_stmt)?;
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
                    self.execute_immediate(stmt)
                }
            }
        }
    }

    /// Executes a statement directly against the storage engine, bypassing transaction logic.
    fn execute_immediate(&mut self, stmt: Statement) -> Result<QueryResult, ExecutorError> {
        match stmt {
            Statement::CreateTable { table_name, columns } => {
                self.storage.create_table(&table_name, &columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Table {} created", table_name))]],
                })
            },
            Statement::Insert { table_name, columns, values } => {
                self.storage.insert(&table_name, columns.as_deref(), &values)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Inserted into {}", table_name))]],
                })
            },
            Statement::Select { table_name, columns, conditions } => {
                let result = self.storage.select(&table_name, &columns, &conditions)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(result)
            },
            Statement::Update { table_name, assignments, conditions } => {
                self.storage.update(&table_name, &assignments, &conditions)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Updated rows in {}", table_name))]],
                })
            },
            Statement::Delete { table_name, conditions } => {
                self.storage.delete(&table_name, &conditions)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Deleted rows from {}", table_name))]],
                })
            },
            Statement::CreateIndex { index_name, table_name, column_name } => {
                self.storage.create_index(&index_name, &table_name, &column_name)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                Ok(QueryResult {
                    columns: vec!["result".to_string()],
                    rows: vec![vec![Value::String(format!("Index {} created on table {}", index_name, table_name))]],
                })
            },

            // Transaction statements should not reach here
                        Statement::Begin | Statement::Commit | Statement::Rollback => {
                Err(ExecutorError::ExecutionError("Cannot execute transaction control statements directly".to_string()))
            }
        }
    }
}