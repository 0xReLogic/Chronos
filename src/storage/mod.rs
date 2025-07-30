mod error;
mod csv_storage;

use std::path::Path;
use crate::parser::ast::{Assignment, ColumnDefinition, Value, Condition};
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
    
    pub fn insert(&mut self, table_name: &str, columns: Option<&[String]>, values: &[Value]) -> Result<(), StorageError> {
        self.inner.insert(table_name, columns, values)
    }
    
    pub fn select(&self, table_name: &str, columns: &[String], conditions: &Option<Vec<Condition>>) -> Result<QueryResult, StorageError> {
        self.inner.select(table_name, columns, conditions)
    }

    pub fn update(&self, table_name: &str, assignments: &[Assignment], conditions: &Option<Vec<Condition>>) -> Result<(), StorageError> {
        self.inner.update(table_name, assignments, conditions)
    }

    pub fn delete(&self, table_name: &str, conditions: &Option<Vec<Condition>>) -> Result<(), StorageError> {
        self.inner.delete(table_name, conditions)
    }

    pub fn create_index(&mut self, index_name: &str, table_name: &str, column_name: &str) -> Result<(), StorageError> {
        self.inner.create_index(index_name, table_name, column_name)
    }
}