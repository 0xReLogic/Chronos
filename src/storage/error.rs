use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    #[error("Value error: {0}")]
    ValueError(String),
}