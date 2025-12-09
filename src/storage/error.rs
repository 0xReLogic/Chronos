use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Sled error: {0}")]
    SledError(String),

    #[error("Invalid filter operation")]
    InvalidFilter,

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Unsupported storage version: {0}")]
    UnsupportedStorageVersion(u32),
}
