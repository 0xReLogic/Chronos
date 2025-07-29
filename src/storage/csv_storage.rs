use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

use crate::parser::{ColumnDefinition, DataType, Value, Condition};
use crate::executor::QueryResult;
use super::StorageError;

#[derive(Debug, Serialize, Deserialize)]
struct TableSchema {
    columns: Vec<ColumnDefinition>,
}

pub struct CsvStorage {
    data_dir: PathBuf,
    schemas: HashMap<String, TableSchema>,
}

impl CsvStorage {
    pub fn new(data_dir: &str) -> Self {
        let data_dir = PathBuf::from(data_dir);
        let mut storage = Self {
            data_dir,
            schemas: HashMap::new(),
        };
        
        storage.load_schemas().unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load schemas: {}", e);
        });
        
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
    
    pub fn create_table(&mut self, table_name: &str, columns: &[ColumnDefinition]) -> Result<(), StorageError> {
        let table_path = self.table_path(table_name);
        if table_path.exists() {
            return Err(StorageError::SchemaError(format!("Table {} already exists", table_name)));
        }
        
        // Create an empty CSV file with headers
        let mut wtr = csv::Writer::from_path(&table_path)?;
        
        // Write headers
        let headers: Vec<String> = columns.iter()
            .map(|col| col.name.clone())
            .collect();
        
        wtr.write_record(&headers)?;
        wtr.flush()?;
        
        // Save schema
        let schema = TableSchema {
            columns: columns.to_vec(),
        };
        
        self.save_schema(table_name, &schema)?;

        // Also update the in-memory schema cache
        self.schemas.insert(table_name.to_string(), schema);

        
        Ok(())
    }
    
    pub fn insert(&self, table_name: &str, columns: Option<&[String]>, values: &[Value]) -> Result<(), StorageError> {
        let table_path = self.table_path(table_name);
        if !table_path.exists() {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        
        // Get schema
        let schema = self.schemas.get(table_name)
            .ok_or_else(|| StorageError::SchemaError(format!("Schema for table {} not found", table_name)))?;
        
        // Validate columns and values
        if let Some(cols) = columns {
            if cols.len() != values.len() {
                return Err(StorageError::SchemaError(
                    format!("Column count ({}) does not match value count ({})", cols.len(), values.len())
                ));
            }
            
            // Check if all columns exist in schema
            for col in cols {
                if !schema.columns.iter().any(|c| &c.name == col) {
                    return Err(StorageError::ColumnNotFound(col.clone()));
                }
            }
        } else {
            // If no columns specified, use all columns in order
            if schema.columns.len() != values.len() {
                return Err(StorageError::SchemaError(
                    format!("Value count ({}) does not match column count ({})", values.len(), schema.columns.len())
                ));
            }
        }
        
        // Open file in append mode
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&table_path)?;
        
        let mut wtr = csv::Writer::from_writer(file);
        
        // Convert values to strings
        let record: Vec<String> = values.iter()
            .map(|v| match v {
                Value::String(s) => s.clone(),
                Value::Integer(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
            })
            .collect();
        
        wtr.write_record(&record)?;
        wtr.flush()?;
        
        Ok(())
    }
    
    pub fn select(&self, table_name: &str, columns: &[String], conditions: Option<&[Condition]>) -> Result<QueryResult, StorageError> {
        let table_path = self.table_path(table_name);
        if !table_path.exists() {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        
        // Get schema
        let schema = self.schemas.get(table_name)
            .ok_or_else(|| StorageError::SchemaError(format!("Schema for table {} not found", table_name)))?;
        
        // Determine which columns to select
        let selected_columns = if columns.len() == 1 && columns[0] == "*" {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            // Validate that all requested columns exist
            for col in columns {
                if col != "*" && !schema.columns.iter().any(|c| &c.name == col) {
                    return Err(StorageError::ColumnNotFound(col.clone()));
                }
            }
            columns.to_vec()
        };
        
        // Read CSV file
        let mut rdr = csv::Reader::from_path(&table_path)?;
        let headers = rdr.headers()?.clone();
        
        // Map column names to indices
        let mut column_indices = HashMap::new();
        for (i, header) in headers.iter().enumerate() {
            column_indices.insert(header.to_string(), i);
        }
        
        // Process each row
        let mut rows = Vec::new();
        for result in rdr.records() {
            let record = result?;
            
            // Check if this row matches the conditions
            if let Some(conds) = conditions {
                let mut matches = true;
                
                for condition in conds {
                    let column_name = match condition {
                        Condition::Equals(col, _) => col,
                        Condition::NotEquals(col, _) => col,
                        Condition::LessThan(col, _) => col,
                        Condition::GreaterThan(col, _) => col,
                        Condition::LessThanOrEqual(col, _) => col,
                        Condition::GreaterThanOrEqual(col, _) => col,
                    };
                    
                    let column_index = column_indices.get(column_name)
                        .ok_or_else(|| StorageError::ColumnNotFound(column_name.clone()))?;
                    
                    let cell_value = record.get(*column_index)
                        .ok_or_else(|| StorageError::ValueError(format!("Invalid column index: {}", column_index)))?;
                    
                    // Find the column definition to determine the data type
                    let column_def = schema.columns.iter()
                        .find(|c| &c.name == column_name)
                        .ok_or_else(|| StorageError::ColumnNotFound(column_name.clone()))?;
                    
                    // Convert cell value to the appropriate type
                    let typed_value = match column_def.data_type {
                        DataType::Int => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let i = cell_value.parse::<i64>()
                                    .map_err(|_| StorageError::ValueError(format!("Invalid integer: {}", cell_value)))?;
                                Value::Integer(i)
                            }
                        },
                        DataType::Float => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let f = cell_value.parse::<f64>()
                                    .map_err(|_| StorageError::ValueError(format!("Invalid float: {}", cell_value)))?;
                                Value::Float(f)
                            }
                        },
                        DataType::Boolean => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let b = cell_value.to_lowercase() == "true";
                                Value::Boolean(b)
                            }
                        },
                        DataType::Text | DataType::String => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                Value::String(cell_value.to_string())
                            }
                        },
                    };
                    
                    // Check if the condition is satisfied
                    let condition_satisfied = match condition {
                        Condition::Equals(_, value) => &typed_value == value,
                        Condition::NotEquals(_, value) => &typed_value != value,
                        Condition::LessThan(_, value) => {
                            match (&typed_value, value) {
                                (Value::Integer(a), Value::Integer(b)) => a < b,
                                (Value::Float(a), Value::Float(b)) => a < b,
                                _ => false,
                            }
                        },
                        Condition::GreaterThan(_, value) => {
                            match (&typed_value, value) {
                                (Value::Integer(a), Value::Integer(b)) => a > b,
                                (Value::Float(a), Value::Float(b)) => a > b,
                                _ => false,
                            }
                        },
                        Condition::LessThanOrEqual(_, value) => {
                            match (&typed_value, value) {
                                (Value::Integer(a), Value::Integer(b)) => a <= b,
                                (Value::Float(a), Value::Float(b)) => a <= b,
                                _ => false,
                            }
                        },
                        Condition::GreaterThanOrEqual(_, value) => {
                            match (&typed_value, value) {
                                (Value::Integer(a), Value::Integer(b)) => a >= b,
                                (Value::Float(a), Value::Float(b)) => a >= b,
                                _ => false,
                            }
                        },
                    };
                    
                    if !condition_satisfied {
                        matches = false;
                        break;
                    }
                }
                
                if !matches {
                    continue;
                }
            }
            
            // Extract selected columns
            let mut row_values = Vec::new();
            for col in &selected_columns {
                if col == "*" {
                    // Add all columns
                    for i in 0..record.len() {
                        let cell_value = record.get(i)
                            .ok_or_else(|| StorageError::ValueError(format!("Invalid column index: {}", i)))?;
                        
                        // Find the column definition
                        let column_name = headers.get(i)
                            .ok_or_else(|| StorageError::ValueError(format!("Invalid header index: {}", i)))?;
                        
                        let column_def = schema.columns.iter()
                            .find(|c| c.name == column_name)
                            .ok_or_else(|| StorageError::ColumnNotFound(column_name.to_string()))?;
                        
                        // Convert to the appropriate type
                        let value = match column_def.data_type {
                            DataType::Int => {
                                if cell_value == "NULL" {
                                    Value::Null
                                } else {
                                    let i = cell_value.parse::<i64>()
                                        .map_err(|_| StorageError::ValueError(format!("Invalid integer: {}", cell_value)))?;
                                    Value::Integer(i)
                                }
                            },
                            DataType::Float => {
                                if cell_value == "NULL" {
                                    Value::Null
                                } else {
                                    let f = cell_value.parse::<f64>()
                                        .map_err(|_| StorageError::ValueError(format!("Invalid float: {}", cell_value)))?;
                                    Value::Float(f)
                                }
                            },
                            DataType::Boolean => {
                                if cell_value == "NULL" {
                                    Value::Null
                                } else {
                                    let b = cell_value.to_lowercase() == "true";
                                    Value::Boolean(b)
                                }
                            },
                            DataType::Text | DataType::String => {
                                if cell_value == "NULL" {
                                    Value::Null
                                } else {
                                    Value::String(cell_value.to_string())
                                }
                            },
                        };
                        
                        row_values.push(value);
                    }
                } else {
                    let column_index = column_indices.get(col)
                        .ok_or_else(|| StorageError::ColumnNotFound(col.clone()))?;
                    
                    let cell_value = record.get(*column_index)
                        .ok_or_else(|| StorageError::ValueError(format!("Invalid column index: {}", column_index)))?;
                    
                    // Find the column definition
                    let column_def = schema.columns.iter()
                        .find(|c| &c.name == col)
                        .ok_or_else(|| StorageError::ColumnNotFound(col.clone()))?;
                    
                    // Convert to the appropriate type
                    let value = match column_def.data_type {
                        DataType::Int => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let i = cell_value.parse::<i64>()
                                    .map_err(|_| StorageError::ValueError(format!("Invalid integer: {}", cell_value)))?;
                                Value::Integer(i)
                            }
                        },
                        DataType::Float => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let f = cell_value.parse::<f64>()
                                    .map_err(|_| StorageError::ValueError(format!("Invalid float: {}", cell_value)))?;
                                Value::Float(f)
                            }
                        },
                        DataType::Boolean => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                let b = cell_value.to_lowercase() == "true";
                                Value::Boolean(b)
                            }
                        },
                        DataType::Text | DataType::String => {
                            if cell_value == "NULL" {
                                Value::Null
                            } else {
                                Value::String(cell_value.to_string())
                            }
                        },
                    };
                    
                    row_values.push(value);
                }
            }
            
            rows.push(row_values);
        }
        
        Ok(QueryResult {
            columns: selected_columns,
            rows,
        })
    }
}