use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Execution error: {0}")]
    ExecutionError(String),
}