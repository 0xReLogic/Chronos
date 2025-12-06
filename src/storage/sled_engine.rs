// New sled-based storage engine implementation

use super::{
    error::StorageError, Filter, Row, StorageEngine, TableSchema,
};
use anyhow::Result;
use std::path::PathBuf;

const SCHEMA_TREE: &str = "__schemas__";

#[allow(dead_code)]
pub struct SledEngine {
    db: sled::Db,
    path: PathBuf,
}

impl SledEngine {
    pub fn new(data_dir: &str) -> Result<Self> {
        let path = PathBuf::from(data_dir);
        std::fs::create_dir_all(&path)?;
        
        let db = sled::open(&path)
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::info!("Sled storage engine initialized at {:?}", path);
        
        Ok(Self { db, path })
    }
    
    fn table_tree(&self, table_name: &str) -> Result<sled::Tree> {
        self.db
            .open_tree(format!("table:{}", table_name))
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }
    
    fn schema_tree(&self) -> Result<sled::Tree> {
        self.db
            .open_tree(SCHEMA_TREE)
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }
    
    fn index_tree(&self, table_name: &str, column: &str) -> Result<sled::Tree> {
        self.db
            .open_tree(format!("index:{}:{}", table_name, column))
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }
    
    fn generate_row_key(&self) -> String {
        format!(
            "{:020}:{:08x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            rand::random::<u32>()
        )
    }
}

#[async_trait::async_trait]
impl StorageEngine for SledEngine {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
    
    async fn create_table(&mut self, table_name: &str, schema: TableSchema) -> Result<()> {
        let schema_tree = self.schema_tree()?;
        
        if schema_tree.contains_key(table_name.as_bytes())? {
            return Err(StorageError::TableAlreadyExists(table_name.to_string()).into());
        }
        
        let schema_bytes = bincode::encode_to_vec(&schema, bincode::config::standard())?;
        schema_tree.insert(table_name.as_bytes(), schema_bytes)?;
        
        self.table_tree(table_name)?;
        
        log::info!("Created table '{}' with {} columns", table_name, schema.columns.len());
        
        Ok(())
    }
    
    async fn insert(&mut self, table_name: &str, rows: Vec<Row>) -> Result<()> {
        let tree = self.table_tree(table_name)?;
        
        let schema = self.get_schema(table_name).await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        let row_count = rows.len();
        for row in rows {
            for col in &schema.columns {
                if !row.values.contains_key(&col.name) {
                    return Err(StorageError::ColumnNotFound(
                        col.name.clone(),
                        table_name.to_string(),
                    ).into());
                }
            }
            
            let key = self.generate_row_key();
            let value = bincode::encode_to_vec(&row, bincode::config::standard())?;
            tree.insert(key.as_bytes(), value)?;
        }
        
        tree.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::debug!("Inserted {} rows into table '{}'", row_count, table_name);
        
        Ok(())
    }
    
    async fn query(&self, table_name: &str, filter: Option<Filter>) -> Result<Vec<Row>> {
        let tree = self.table_tree(table_name)?;
        let mut results = Vec::new();
        
        for item in tree.iter() {
            let (_key, value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let (row, _): (Row, usize) = bincode::decode_from_slice(&value, bincode::config::standard())?;
            
            if let Some(ref f) = filter {
                if !f.matches(&row) {
                    continue;
                }
            }
            
            results.push(row);
        }
        
        log::debug!("Query returned {} rows from table '{}'", results.len(), table_name);
        
        Ok(results)
    }
    
    async fn delete(&mut self, table_name: &str, filter: Filter) -> Result<usize> {
        let tree = self.table_tree(table_name)?;
        let mut deleted = 0;
        
        let mut to_delete = Vec::new();
        for item in tree.iter() {
            let (key, value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let (row, _): (Row, usize) = bincode::decode_from_slice(&value, bincode::config::standard())?;
            
            if filter.matches(&row) {
                to_delete.push(key);
            }
        }
        
        for key in to_delete {
            tree.remove(key)?;
            deleted += 1;
        }
        
        tree.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::info!("Deleted {} rows from table '{}'", deleted, table_name);
        
        Ok(deleted)
    }
    
    async fn create_index(&mut self, table_name: &str, column: &str) -> Result<()> {
        let table_tree = self.table_tree(table_name)?;
        let index_tree = self.index_tree(table_name, column)?;
        
        for item in table_tree.iter() {
            let (row_key, value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let (row, _): (Row, usize) = bincode::decode_from_slice(&value, bincode::config::standard())?;
            
            if let Some(col_value) = row.get(column) {
                let index_key = format!("{}:{}", 
                    serde_json::to_string(col_value)?,
                    String::from_utf8_lossy(&row_key)
                );
                index_tree.insert(index_key.as_bytes(), row_key)?;
            }
        }
        
        index_tree.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::info!("Created index on column '{}' for table '{}'", column, table_name);
        
        Ok(())
    }
    
    async fn get_schema(&self, table_name: &str) -> Result<Option<TableSchema>> {
        let schema_tree = self.schema_tree()?;
        
        match schema_tree.get(table_name.as_bytes())? {
            Some(schema_bytes) => {
                let (schema, _): (TableSchema, usize) = bincode::decode_from_slice(&schema_bytes, bincode::config::standard())?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }
    
    async fn list_tables(&self) -> Result<Vec<String>> {
        let schema_tree = self.schema_tree()?;
        let mut tables = Vec::new();
        
        for item in schema_tree.iter() {
            let (key, _) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let table_name = String::from_utf8_lossy(&key).to_string();
            tables.push(table_name);
        }
        
        Ok(tables)
    }
    
    async fn checkpoint(&mut self) -> Result<()> {
        self.db.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::debug!("Checkpoint completed for sled storage");
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        self.db.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::info!("Sled storage engine closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Value;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_sled_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap().to_string()).unwrap();
        
        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
            ],
        };
        
        engine.create_table("users", schema).await.unwrap();
        
        let retrieved = engine.get_schema("users").await.unwrap().unwrap();
        assert_eq!(retrieved.columns.len(), 2);
    }
    
    #[tokio::test]
    async fn test_sled_insert_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap().to_string()).unwrap();
        
        let schema = TableSchema {
            name: "sensors".to_string(),
            columns: vec![
                Column {
                    name: "sensor_id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "temperature".to_string(),
                    data_type: DataType::Float,
                },
            ],
        };
        
        engine.create_table("sensors", schema).await.unwrap();
        
        let mut row = Row::new();
        row.insert("sensor_id".to_string(), Value::Int(1));
        row.insert("temperature".to_string(), Value::Float(25.5));
        
        engine.insert("sensors", vec![row]).await.unwrap();
        
        let results = engine.query("sensors", None).await.unwrap();
        assert_eq!(results.len(), 1);
    }
}
