use super::{Filter, Row, StorageEngine, TableSchema};
use anyhow::Result;

#[allow(dead_code)]
pub struct CsvEngine {
    data_dir: String,
}

impl CsvEngine {
    pub fn new(data_dir: &str) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        Ok(Self { data_dir: data_dir.to_string() })
    }
}

#[async_trait::async_trait]
impl StorageEngine for CsvEngine {
    async fn init(&mut self) -> Result<()> {
        log::warn!("CSV storage engine is deprecated, please migrate to sled");
        Ok(())
    }
    
    async fn create_table(&mut self, _table_name: &str, _schema: TableSchema) -> Result<()> {
        unimplemented!("CSV engine not yet migrated")
    }
    
    async fn insert(&mut self, _table_name: &str, _rows: Vec<Row>) -> Result<()> {
        unimplemented!("CSV engine not yet migrated")
    }
    
    async fn query(&self, _table_name: &str, _filter: Option<Filter>) -> Result<Vec<Row>> {
        unimplemented!("CSV engine not yet migrated")
    }
    
    async fn delete(&mut self, _table_name: &str, _filter: Filter) -> Result<usize> {
        unimplemented!("CSV engine not yet migrated")
    }
    
    async fn create_index(&mut self, _table_name: &str, _column: &str) -> Result<()> {
        Ok(())
    }
    
    async fn get_schema(&self, _table_name: &str) -> Result<Option<TableSchema>> {
        Ok(None)
    }
    
    async fn list_tables(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
    
    async fn checkpoint(&mut self) -> Result<()> {
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
