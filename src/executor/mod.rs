mod error;

use std::sync::Mutex;
use crate::parser::{Ast, Statement, Value};
use crate::storage::Storage;

pub use self::error::ExecutorError;

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub struct Executor {
    storage: Storage,
    raft_node: Option<std::sync::Weak<Mutex<crate::raft::RaftNode>>>,
}

impl Executor {
    pub fn new(data_dir: &str) -> Self {
        Self {
            storage: Storage::new(data_dir),
            raft_node: None,
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
                let result = self.storage.select(&table_name, &columns, conditions.as_deref())
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                
                Ok(result)
            },
        }
    }
}