use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};

use crate::parser::ast::{Assignment, ColumnDefinition, DataType, Value, Condition, Operator};
use crate::executor::QueryResult;
use super::StorageError;

#[derive(Debug, Serialize, Deserialize)]
struct TableSchema {
    columns: Vec<ColumnDefinition>,
}

type IndexData = HashMap<String, Vec<u64>>;

#[derive(serde::Serialize, serde::Deserialize, Encode, Decode)]
struct IndexMetadata {
    table_name: String,
    column_name: String,
    data: IndexData,
}

pub struct CsvStorage {
    data_dir: PathBuf,
    schemas: HashMap<String, TableSchema>,
    indices: HashMap<String, IndexMetadata>,
}

impl CsvStorage {
    pub fn new(data_dir: &str) -> Self {
        let data_dir = PathBuf::from(data_dir);
        let mut storage = Self {
            data_dir,
            schemas: HashMap::new(),
            indices: HashMap::new(),
        };

        if let Err(e) = storage.load_schemas() {
            eprintln!("Warning: Could not load schemas: {}", e);
        }

        if let Err(e) = storage.load_indices() {
            eprintln!("Warning: Could not load indices: {}", e);
        }

        storage
    }

    fn load_schemas(&mut self) -> Result<(), StorageError> {
        let schema_dir = self.data_dir.join("schemas");
        if !schema_dir.exists() {
            std::fs::create_dir_all(&schema_dir)?;
            return Ok(());
        }

        for entry in std::fs::read_dir(schema_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                let table_name = path.file_stem().unwrap().to_string_lossy().to_string();
                let mut file = File::open(&path)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                let schema: TableSchema = serde_json::from_str(&contents)
                    .map_err(|e| StorageError::SchemaError(e.to_string()))?;

                self.schemas.insert(table_name, schema);
            }
        }

        Ok(())
    }

    fn save_schema(&self, table_name: &str, schema: &TableSchema) -> Result<(), StorageError> {
        let schema_dir = self.data_dir.join("schemas");
        if !schema_dir.exists() {
            std::fs::create_dir_all(&schema_dir)?;
        }

        let schema_path = schema_dir.join(format!("{}.json", table_name));
        let schema_json = serde_json::to_string_pretty(schema)
            .map_err(|e| StorageError::SchemaError(e.to_string()))?;

        let mut file = File::create(schema_path)?;
        file.write_all(schema_json.as_bytes())?;

        Ok(())
    }

    fn table_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.csv", table_name))
    }

    fn index_path(&self, index_name: &str) -> PathBuf {
        let index_dir = self.data_dir.join("indices");
        index_dir.join(format!("{}.idx", index_name))
    }

    fn get_schema_and_path(&self, table_name: &str) -> Result<(&TableSchema, PathBuf), StorageError> {
        let table_path = self.table_path(table_name);
        if !table_path.exists() {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        let schema = self.schemas.get(table_name)
            .ok_or_else(|| StorageError::SchemaError(format!("Schema not found for table {}", table_name)))?;
        Ok((schema, table_path))
    }

    fn convert_str_to_value(&self, s: &str, data_type: &DataType) -> Result<Value, StorageError> {
        if s == "NULL" {
            return Ok(Value::Null);
        }
        match data_type {
            DataType::Int => s.parse::<i64>().map(Value::Integer).map_err(|_| StorageError::ValueError(format!("Invalid integer: {}", s))),
            DataType::Float => s.parse::<f64>().map(Value::Float).map_err(|_| StorageError::ValueError(format!("Invalid float: {}", s))),
            DataType::Boolean => s.parse::<bool>().map(Value::Boolean).map_err(|_| StorageError::ValueError(format!("Invalid boolean: {}", s))),
            DataType::Text | DataType::String => Ok(Value::String(s.to_string())),
        }
    }

    pub fn create_table(&mut self, table_name: &str, columns: &[ColumnDefinition]) -> Result<(), StorageError> {
        if self.schemas.contains_key(table_name) {
            return Err(StorageError::SchemaError(format!("Table {} already exists", table_name)));
        }

        let table_path = self.table_path(table_name);
        let mut wtr = csv::Writer::from_path(&table_path)?;
        let headers: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
        wtr.write_record(&headers)?;
        wtr.flush()?;

        let schema = TableSchema { columns: columns.to_vec() };
        self.save_schema(table_name, &schema)?;
        self.schemas.insert(table_name.to_string(), schema);

        Ok(())
    }

    pub fn insert(&mut self, table_name: &str, _columns: Option<&[String]>, values: &[Value]) -> Result<(), StorageError> {
        let (schema, table_path) = self.get_schema_and_path(table_name)?;

        if schema.columns.len() != values.len() {
            return Err(StorageError::SchemaError(format!("Schema column count ({}) does not match value count ({})", schema.columns.len(), values.len())));
        }

        let record_pos_start = std::fs::metadata(&table_path)?.len();

        let file = OpenOptions::new().append(true).open(&table_path)?;
        let mut wtr = csv::WriterBuilder::new().has_headers(false).from_writer(file);
        let values_str: Vec<String> = values.iter().map(|v| format!("{}", v)).collect();
        wtr.write_record(&values_str)?;
        wtr.flush()?;

        // --- Index Update Logic ---
        // Phase 1: Collect updates to avoid borrow checker conflicts.
        let mut updates_to_perform = Vec::new();
        for (index_name, metadata) in &self.indices {
            if metadata.table_name == table_name {
                if let Some(column_index) = schema.columns.iter().position(|c| c.name == metadata.column_name) {
                    if let Some(value_to_index) = values.get(column_index) {
                        updates_to_perform.push((index_name.clone(), value_to_index.to_string()));
                    }
                }
            }
        }

        // Phase 2: Apply the collected updates.
        for (index_name, key) in updates_to_perform {
            // Get the path first (immutable borrow)
            let index_path = self.index_path(&index_name);

            // Now get the mutable reference to update the data
            if let Some(metadata) = self.indices.get_mut(&index_name) {
                metadata.data.entry(key).or_default().push(record_pos_start);

                // Persist the updated index back to disk
                let encoded = bincode::encode_to_vec(&*metadata, bincode::config::standard())
                    .map_err(|e| StorageError::IndexError(format!("Failed to serialize updated index: {}", e)))?;
                let mut file = File::create(&index_path)?;
                file.write_all(&encoded)?;
            }
        }

        Ok(())
    }

    pub fn select(&self, table_name: &str, columns: &[String], conditions: &Option<Vec<Condition>>) -> Result<QueryResult, StorageError> {
        let (schema, file_path) = self.get_schema_and_path(table_name)?;
        let mut rdr = csv::Reader::from_path(file_path)?;
        let headers = rdr.headers()?.clone();

        let final_selected_columns = if columns.is_empty() || (columns.len() == 1 && columns[0] == "*") {
            headers.iter().map(|h| h.to_string()).collect::<Vec<String>>()
        } else {
            columns.to_vec()
        };

        let column_indices: HashMap<_, _> = headers.iter().enumerate().map(|(i, h)| (h.to_string(), i)).collect();
        let column_indices_for_conditions: HashMap<_, _> = headers.iter().enumerate().map(|(i, h)| (h, i)).collect();

        let mut rows = Vec::new();

        for result in rdr.records() {
            let record = result?;

            let should_include = match conditions {
                Some(conds) if !conds.is_empty() => self.check_conditions(&record, schema, &column_indices_for_conditions, conds)?,
                _ => true,
            };

            if should_include {
                let mut row_values = Vec::new();
                for col_name in &final_selected_columns {
                    let col_idx = *column_indices.get(col_name).ok_or_else(|| StorageError::ColumnNotFound(col_name.clone()))?;
                    let cell_value = record.get(col_idx).ok_or_else(|| StorageError::ValueError(format!("Invalid column index: {}", col_idx)))?;
                    let column_def = schema.columns.iter().find(|c| &c.name == col_name).ok_or_else(|| StorageError::ColumnNotFound(col_name.clone()))?;
                    let value = self.convert_str_to_value(cell_value, &column_def.data_type)?;
                    row_values.push(value);
                }
                rows.push(row_values);
            }
        }

        Ok(QueryResult { columns: final_selected_columns, rows })
    }

    fn check_conditions(&self, record: &csv::StringRecord, schema: &TableSchema, column_indices: &HashMap<&str, usize>, conditions: &[Condition]) -> Result<bool, StorageError> {
        for condition in conditions {
            let column_index = *column_indices.get(condition.column_name.as_str())
                .ok_or_else(|| StorageError::ColumnNotFound(condition.column_name.clone()))?;
            let cell_value_str = record.get(column_index).ok_or_else(|| StorageError::ValueError(format!("Record missing value for index {}", column_index)))?;
            let record_value = self.convert_str_to_value(cell_value_str, &schema.columns[column_index].data_type)?;

            let matches = match condition.operator {
                Operator::Equals => record_value == condition.value,
                Operator::NotEquals => record_value != condition.value,
                Operator::LessThan => record_value < condition.value,
                Operator::GreaterThan => record_value > condition.value,
                Operator::LessThanOrEqual => record_value <= condition.value,
                Operator::GreaterThanOrEqual => record_value >= condition.value,
            };

            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn update(&self, table_name: &str, assignments: &[Assignment], conditions: &Option<Vec<Condition>>) -> Result<(), StorageError> {
        let (schema, table_path) = self.get_schema_and_path(table_name)?;
        let mut rdr = csv::Reader::from_path(&table_path)?;
        let headers = rdr.headers()?.clone();
        let records: Vec<csv::StringRecord> = rdr.records().collect::<Result<_, _>>()?;
        let mut updated_records = Vec::new();

        let column_indices: HashMap<_, _> = headers.iter().enumerate().map(|(i, h)| (h, i)).collect();

        for record in records {
            let mut record_values: Vec<String> = record.iter().map(String::from).collect();

            let should_update = match &conditions {
                Some(conds) if !conds.is_empty() => self.check_conditions(&record, &schema, &column_indices, conds)?,
                _ => true, // If no conditions, update all rows
            };

            if should_update {
                for assignment in assignments {
                    let col_index = *column_indices.get(assignment.column_name.as_str()).ok_or_else(|| StorageError::ColumnNotFound(assignment.column_name.clone()))?;
                    let new_value_str = format!("{}", assignment.value);
                    record_values[col_index] = new_value_str;
                }
            }
            updated_records.push(record_values);
        }

        let mut wtr = csv::Writer::from_path(&table_path)?;
        wtr.write_record(&headers)?;
        for record_values in updated_records {
            wtr.write_record(&record_values)?;
        }
        wtr.flush()?;

        Ok(())
    }

    pub fn delete(&self, table_name: &str, conditions: &Option<Vec<Condition>>) -> Result<(), StorageError> {
        let (schema, table_path) = self.get_schema_and_path(table_name)?;
        let mut rdr = csv::Reader::from_path(&table_path)?;
        let headers = rdr.headers()?.clone();
        let all_records: Vec<csv::StringRecord> = rdr.records().collect::<Result<_, _>>()?;
        let mut kept_records = Vec::new();

        let column_indices: HashMap<_, _> = headers.iter().enumerate().map(|(i, h)| (h, i)).collect();

        for record in all_records {
            let should_delete = match &conditions {
                Some(conds) if !conds.is_empty() => self.check_conditions(&record, &schema, &column_indices, conds)?,
                _ => true, // If no conditions, delete all rows
            };

            if !should_delete {
                kept_records.push(record);
            }
        }

        let mut wtr = csv::Writer::from_path(&table_path)?;
        wtr.write_record(&headers)?;
        for record in kept_records {
            wtr.write_record(&record)?;
        }
        wtr.flush()?;

        Ok(())
    }

    fn load_indices(&mut self) -> Result<(), StorageError> {
        let index_dir = self.data_dir.join("indices");
        if !index_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(index_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("idx") {
                let index_name = path.file_stem().unwrap().to_str().unwrap().to_string();
                let file_content = std::fs::read(&path)?;
                let (metadata, _): (IndexMetadata, usize) = bincode::decode_from_slice(&file_content, bincode::config::standard())
                    .map_err(|e| StorageError::IndexError(format!("Failed to deserialize index '{}': {}", index_name, e)))?;
                self.indices.insert(index_name, metadata);
            }
        }

        Ok(())
    }

    pub fn create_index(&mut self, index_name: &str, table_name: &str, column_name: &str) -> Result<(), StorageError> {
        let schema = self.schemas.get(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let column_index_in_schema = schema.columns.iter().position(|c| c.name == column_name)
            .ok_or_else(|| StorageError::ColumnNotFound(column_name.to_string()))?;

        let index_path = self.index_path(index_name);
        if self.indices.contains_key(index_name) || index_path.exists() {
            return Err(StorageError::IndexExists(index_name.to_string()));
        }

        let table_path = self.table_path(table_name);
        let mut rdr = csv::Reader::from_path(table_path)?;
        let mut index_data: IndexData = HashMap::new();

        for result in rdr.records() {
            let record = result?;
            if let Some(value) = record.get(column_index_in_schema) {
                let record_pos = record.position().unwrap().byte();
                index_data.entry(value.to_string()).or_default().push(record_pos);
            }
        }

        let metadata = IndexMetadata {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            data: index_data,
        };

        let index_dir = self.data_dir.join("indices");
        if !index_dir.exists() {
            std::fs::create_dir_all(&index_dir)?;
        }

        let encoded = bincode::encode_to_vec(&metadata, bincode::config::standard())
            .map_err(|e| StorageError::IndexError(format!("Failed to serialize index: {}", e)))?;
        
        let mut file = File::create(&index_path)?;
        file.write_all(&encoded)?;

        self.indices.insert(index_name.to_string(), metadata);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{ColumnDefinition, DataType, Value, Operator, Condition, Assignment};
    use tempfile::TempDir;

    // Helper function to set up a clean storage for each test
    fn setup_test_storage() -> (CsvStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = CsvStorage::new(temp_dir.path().to_str().unwrap());
        (storage, temp_dir)
    }

    #[test]
    fn test_create_and_load_index() {
        let (mut storage, _temp_dir) = setup_test_storage();

        // Create a table and insert data
        let table_name = "users";
        let columns = vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, primary_key: true },
            ColumnDefinition { name: "name".to_string(), data_type: DataType::Text, primary_key: false },
        ];
        storage.create_table(table_name, &columns).unwrap();
        storage.insert(table_name, None, &[Value::Integer(1), Value::String("Alice".to_string())]).unwrap();
        storage.insert(table_name, None, &[Value::Integer(2), Value::String("Bob".to_string())]).unwrap();

        // Create an index on the 'name' column
        let index_name = "idx_users_name";
        let column_name = "name";
        storage.create_index(index_name, table_name, column_name).unwrap();

        // 1. Verify the index file was created on disk
        let index_path = storage.index_path(index_name);
        assert!(index_path.exists(), "Index file should be created on disk.");

        // 2. Verify the index is present in the current instance's memory
        assert!(storage.indices.contains_key(index_name), "Index should be in memory after creation.");
        let index_in_mem = storage.indices.get(index_name).unwrap();
        assert_eq!(index_in_mem.table_name, table_name);
        assert_eq!(index_in_mem.column_name, column_name);
        assert_eq!(index_in_mem.data.len(), 2, "Index should contain 2 entries");
        assert!(index_in_mem.data.contains_key("Alice"), "Index should contain key 'Alice'");
        assert!(index_in_mem.data.contains_key("Bob"), "Index should contain key 'Bob'");

        // 3. Insert a new row and verify the index is updated
        storage.insert(table_name, None, &[Value::Integer(3), Value::String("Charlie".to_string())]).unwrap();
        let updated_index_in_mem = storage.indices.get(index_name).unwrap();
        assert_eq!(updated_index_in_mem.data.len(), 3, "Index should now contain 3 entries");
        assert!(updated_index_in_mem.data.contains_key("Charlie"), "Index should contain key 'Charlie'");

        // 4. Verify persistence by creating a new storage instance pointing to the same directory
        let data_dir_path = storage.data_dir.clone();
        drop(storage); // Release file handles
        let new_storage = CsvStorage::new(data_dir_path.to_str().unwrap());
        assert!(new_storage.indices.contains_key(index_name), "Index should be loaded from disk into a new instance.");
        let loaded_index = new_storage.indices.get(index_name).unwrap();
        assert_eq!(loaded_index.data.len(), 3, "Loaded index should have 3 entries");
        assert!(loaded_index.data.contains_key("Charlie"), "Loaded index should contain key 'Charlie'");
    }

    // Setup function for the existing tests
    fn setup() -> (CsvStorage, String, tempfile::TempDir) {
        let (mut storage, dir) = setup_test_storage();
        let table_name = "users".to_string();

        let columns = vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, primary_key: true },
            ColumnDefinition { name: "name".to_string(), data_type: DataType::Text, primary_key: false },
            ColumnDefinition { name: "age".to_string(), data_type: DataType::Int, primary_key: false },
        ];

        storage.create_table(&table_name, &columns).unwrap();
        storage.insert(&table_name, None, &[Value::Integer(1), Value::String("Alice".to_string()), Value::Integer(30)]).unwrap();
        storage.insert(&table_name, None, &[Value::Integer(2), Value::String("Bob".to_string()), Value::Integer(25)]).unwrap();
        storage.insert(&table_name, None, &[Value::Integer(3), Value::String("Charlie".to_string()), Value::Integer(35)]).unwrap();

        (storage, table_name, dir)
    }

    #[test]
    fn test_update_rows() {
        let (storage, table_name, _dir) = setup();

        let assignments = vec![Assignment {
            column_name: "age".to_string(),
            value: Value::Integer(26),
        }];
        let conditions = Some(vec![Condition {
            column_name: "name".to_string(),
            operator: Operator::Equals,
            value: Value::String("Bob".to_string()),
        }]);

        storage.update(&table_name, &assignments, &conditions).unwrap();

        let results = storage.select(&table_name, &vec!["*".to_string()], &None).unwrap();
        assert_eq!(results.rows.len(), 3);

        let bob_row = results.rows.iter().find(|row| row[1] == Value::String("Bob".to_string())).unwrap();
        assert_eq!(bob_row[2], Value::Integer(26));
    }

    #[test]
    fn test_delete_rows() {
        let (storage, table_name, _dir) = setup();

        let conditions = Some(vec![Condition {
            column_name: "name".to_string(),
            operator: Operator::Equals,
            value: Value::String("Charlie".to_string()),
        }]);

        storage.delete(&table_name, &conditions).unwrap();

        let results = storage.select(&table_name, &vec!["*".to_string()], &None).unwrap();
        assert_eq!(results.rows.len(), 2);

        let charlie_row = results.rows.iter().find(|row| row[1] == Value::String("Charlie".to_string()));
        assert!(charlie_row.is_none());
    }
}