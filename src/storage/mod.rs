mod error;
mod csv_storage;

use std::path::Path;
use crate::parser::{ColumnDefinition, Value, Condition};
use crate::executor::QueryResult;

pub use self::error::StorageError;
use self::csv_storage::CsvStorage;

pub struct Storage {
    inner: CsvStorage,
}

impl Storage {
    pub fn new(data_dir: &str) -> Self {
        let path = Path::new(data_dir);
        if !path.exists() {
            std::fs::create_dir_all(path).expect("Failed to create data directory");
        }
        
        Self {
            inner: CsvStorage::new(data_dir),
        }
    }
    
    pub fn create_table(&mut self, table_name: &str, columns: &[ColumnDefinition]) -> Result<(), StorageError> {
        self.inner.create_table(table_name, columns)
    }
    
    pub fn insert(&self, table_name: &str, columns: Option<&[String]>, values: &[Value]) -> Result<(), StorageError> {
        self.inner.insert(table_name, columns, values)
    }
    
    pub fn select(&self, table_name: &str, columns: &[String], conditions: Option<&[Condition]>) -> Result<QueryResult, StorageError> {
        self.inner.select(table_name, columns, conditions)
    }
}