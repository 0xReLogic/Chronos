use super::{
    error::StorageError,
    offline_queue::PersistentOfflineQueue,
    wal::{WalEntry, Operation},
    Filter,
    FilterOp,
    Row,
    StorageEngine,
    TableSchema,
};
use anyhow::Result;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use tokio::sync::Mutex as TokioMutex;
use lru::LruCache;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use crate::common::timestamp::HybridTimestamp;

const SCHEMA_TREE: &str = "__schemas__";
const INDEX_META_TREE: &str = "__indexes__";
#[allow(dead_code)]
const TTL_TREE: &str = "__ttl__";
const LWW_TS_TREE: &str = "__lww_ts__";

pub struct SledEngine {
    db: sled::Db,
    wal_sequence: u64,
    offline_queue: TokioMutex<PersistentOfflineQueue>,
    cache: TokioMutex<LruCache<(String, Vec<u8>), Row>>,
}

impl SledEngine {
    pub fn new(data_dir: &str) -> Result<Self> {
        let path = PathBuf::from(data_dir);
        std::fs::create_dir_all(&path)?;
        
        let db = sled::open(&path)
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        
        log::info!("Sled storage engine initialized at {:?}", path);
        
        let offline_queue = PersistentOfflineQueue::new(&db)?;
        let cache_capacity = NonZeroUsize::new(10_000).expect("non-zero cache size");
        let cache = TokioMutex::new(LruCache::new(cache_capacity));

        Ok(Self { db, wal_sequence: 0, offline_queue: TokioMutex::new(offline_queue), cache })
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

    fn wal_tree(&self) -> Result<sled::Tree> {
        self.db
            .open_tree("__wal__")
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }
    
    fn index_tree(&self, table_name: &str, column: &str) -> Result<sled::Tree> {
        self.db
            .open_tree(format!("index:{}:{}", table_name, column))
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }

    fn index_meta_tree(&self) -> Result<sled::Tree> {
        self.db
            .open_tree(INDEX_META_TREE)
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }

    #[allow(dead_code)]
    fn ttl_tree(&self) -> Result<sled::Tree> {
        self.db
            .open_tree(TTL_TREE)
            .map_err(|e| StorageError::SledError(e.to_string()).into())
    }

    fn lww_ts_tree(&self) -> Result<sled::Tree> {
        self.db
            .open_tree(LWW_TS_TREE)
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

fn compress_row(bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
    // Prefix with 0x01 to indicate compressed payload; legacy uncompressed
    // rows will not have this prefix and will be decoded as-is.
    let mut out = Vec::with_capacity(1 + bytes.len());
    out.push(1);
    let mut compressed = compress_prepend_size(bytes);
    out.append(&mut compressed);
    Ok(out)
}

fn decompress_row(bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
    match bytes.first() {
        Some(1) => {
            // New compressed format
            decompress_size_prepended(&bytes[1..])
                .map_err(|e| StorageError::SledError(e.to_string()))
        }
        _ => Ok(bytes.to_vec()), // Legacy uncompressed row
    }
}

#[async_trait::async_trait]
impl StorageEngine for SledEngine {
    async fn init(&mut self) -> Result<()> {
        self.recover_from_wal().await?;
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
        let wal_tree = self.wal_tree()?;
        
        let schema = self.get_schema(table_name).await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let ttl_seconds = schema.ttl_seconds;
        let ttl_tree = if ttl_seconds.is_some() {
            Some(self.ttl_tree()?)
        } else {
            None
        };
        
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
            let raw_value = bincode::encode_to_vec(&row, bincode::config::standard())?;
            let value = compress_row(&raw_value)?;

            self.wal_sequence = self.wal_sequence.wrapping_add(1);
            let op = Operation::Insert {
                table: table_name.to_string(),
                key: key.as_bytes().to_vec(),
                value: value.clone(),
            };
            let ts = crate::common::timestamp::HybridTimestamp {
                ts: uhlc::HLC::default().new_timestamp(),
                node_id: 0,
            };

            // Enqueue into persistent offline queue for Phase 2 sync.
            {
                let mut queue = self.offline_queue.lock().await;
                let _ = queue.enqueue(ts, op.clone())?;
            }

            let entry = WalEntry::new(self.wal_sequence, ts, op, 0);
            let wal_bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard())?;
            wal_tree.insert(self.wal_sequence.to_be_bytes(), wal_bytes)
                .map_err(|e| StorageError::SledError(e.to_string()))?;

            tree.insert(key.as_bytes(), value)?;

            if let (Some(ttl), Some(ttl_tree)) = (ttl_seconds, ttl_tree.as_ref()) {
                let expiration_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .saturating_add(ttl);
                let ttl_key = format!("{:020}:{}:{}", expiration_ts, table_name, key);
                ttl_tree
                    .insert(ttl_key.as_bytes(), &[])
                    .map_err(|e| StorageError::SledError(e.to_string()))?;
            }

            // Maintain secondary indexes for this table, if configured
            let index_meta = self.index_meta_tree()?;
            for col in &schema.columns {
                let meta_key = format!("{}:{}", table_name, col.name);
                let has_index = index_meta
                    .contains_key(meta_key.as_bytes())
                    .map_err(|e| StorageError::SledError(e.to_string()))?;
                if !has_index {
                    continue;
                }

                if let Some(col_value) = row.get(&col.name) {
                    let index_tree = self.index_tree(table_name, &col.name)?;
                    let index_key = format!(
                        "{}:{}",
                        serde_json::to_string(col_value)?,
                        &key
                    );
                    index_tree
                        .insert(index_key.as_bytes(), key.as_bytes())
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                }
            }
        }
        
        wal_tree.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;
        tree.flush_async().await
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        if let Some(ttl_tree) = ttl_tree {
            ttl_tree
                .flush_async()
                .await
                .map_err(|e| StorageError::SledError(e.to_string()))?;
        }
        
        log::debug!("Inserted {} rows into table '{}'", row_count, table_name);
        
        Ok(())
    }
    
    async fn query(&self, table_name: &str, filter: Option<Filter>) -> Result<Vec<Row>> {
        let tree = self.table_tree(table_name)?;
        let mut results = Vec::new();

        // Prefer secondary index for simple equality filters when available.
        // If no index or no hits, fall back to full table scan.
        if let Some(ref f) = filter {
            if matches!(f.op, FilterOp::Eq) {
                let index_meta = self.index_meta_tree()?;
                let meta_key = format!("{}:{}", table_name, f.column);
                let has_index = index_meta
                    .contains_key(meta_key.as_bytes())
                    .map_err(|e| StorageError::SledError(e.to_string()))?;

                if has_index {
                    if let Ok(index_tree) = self.index_tree(table_name, &f.column) {
                        let prefix = serde_json::to_string(&f.value)?;

                        for item in index_tree.scan_prefix(prefix.as_bytes()) {
                            let (_index_key, row_key) = item
                                .map_err(|e| StorageError::SledError(e.to_string()))?;

                            let cache_key = (table_name.to_string(), row_key.to_vec());

                            // Check cache first
                            let cached_row = {
                                let mut cache = self.cache.lock().await;
                                cache.get(&cache_key).cloned()
                            };

                            let row = if let Some(row) = cached_row {
                                row
                            } else {
                                // Fallback to storage
                                let stored = match tree
                                    .get(&row_key)
                                    .map_err(|e| StorageError::SledError(e.to_string()))?
                                {
                                    Some(v) => v,
                                    None => continue,
                                };
                                let value = decompress_row(&stored)?;
                                let (row, _): (Row, usize) =
                                    bincode::decode_from_slice(&value, bincode::config::standard())?;

                                // Insert into cache
                                {
                                    let mut cache = self.cache.lock().await;
                                    cache.put(cache_key, row.clone());
                                }

                                row
                            };

                            // Extra safety: still apply the filter in case of type mismatches
                            if let Some(ref f) = filter {
                                if !f.matches(&row) {
                                    continue;
                                }
                            }

                            results.push(row);
                        }

                        log::debug!(
                            "Query (indexed) returned {} rows from table '{}'",
                            results.len(),
                            table_name
                        );

                        // If the indexed lookup found rows, return them.
                        if !results.is_empty() {
                            return Ok(results);
                        }
                        // Otherwise, fall through to the full table scan below.
                    }
                }
            }
        }

        // Fallback: full table scan
        for item in tree.iter() {
            let (_key, stored) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let value = decompress_row(&stored)?;
            let (row, _): (Row, usize) =
                bincode::decode_from_slice(&value, bincode::config::standard())?;

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
        
        // Discover which columns have indexes for this table (if any)
        let schema = self.get_schema(table_name).await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let index_meta = self.index_meta_tree()?;
        let mut indexed_columns: Vec<String> = Vec::new();
        for col in &schema.columns {
            let meta_key = format!("{}:{}", table_name, col.name);
            let has_index = index_meta
                .contains_key(meta_key.as_bytes())
                .map_err(|e| StorageError::SledError(e.to_string()))?;
            if has_index {
                indexed_columns.push(col.name.clone());
            }
        }

        let mut to_delete = Vec::new();
        let mut rows_for_index: Vec<(sled::IVec, Row)> = Vec::new();
        for item in tree.iter() {
            let (key, stored) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let value = decompress_row(&stored)?;
            let (row, _): (Row, usize) =
                bincode::decode_from_slice(&value, bincode::config::standard())?;
            
            if filter.matches(&row) {
                to_delete.push(key.clone());
                if !indexed_columns.is_empty() {
                    rows_for_index.push((key, row));
                }
            }
        }

        // Remove index entries for the rows being deleted
        for (key, row) in &rows_for_index {
            let key_str = String::from_utf8_lossy(key).to_string();
            for col_name in &indexed_columns {
                if let Some(col_value) = row.get(col_name) {
                    let index_tree = self.index_tree(table_name, col_name)?;
                    let index_key = format!(
                        "{}:{}",
                        serde_json::to_string(col_value)?,
                        key_str
                    );
                    index_tree
                        .remove(index_key.as_bytes())
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                }
            }
        }

        // Invalidate cache entries for deleted rows
        if !rows_for_index.is_empty() {
            let table = table_name.to_string();
            let mut cache = self.cache.lock().await;
            for (key, _) in &rows_for_index {
                let cache_key = (table.clone(), key.to_vec());
                cache.pop(&cache_key);
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

        // Build the initial index from existing rows
        for item in table_tree.iter() {
            let (row_key, stored) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let value = decompress_row(&stored)?;
            let (row, _): (Row, usize) =
                bincode::decode_from_slice(&value, bincode::config::standard())?;

            if let Some(col_value) = row.get(column) {
                let index_key = format!(
                    "{}:{}",
                    serde_json::to_string(col_value)?,
                    String::from_utf8_lossy(&row_key)
                );
                index_tree.insert(index_key.as_bytes(), row_key)?;
            }
        }

        index_tree
            .flush_async()
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        // Register the index in metadata so future INSERT/DELETE keep it up to date
        let index_meta = self.index_meta_tree()?;
        let meta_key = format!("{}:{}", table_name, column);
        index_meta
            .insert(meta_key.as_bytes(), &[])
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

    async fn cleanup_expired(&mut self, now_secs: u64, limit: usize) -> Result<u64> {
        let ttl_tree = self.ttl_tree()?;
        let mut to_cleanup: Vec<(sled::IVec, String, String)> = Vec::new();

        for item in ttl_tree.iter() {
            let (key, _) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            let mut parts = key_str.splitn(3, ':');

            let ts_str = match parts.next() {
                Some(s) => s,
                None => continue,
            };
            let table_name = match parts.next() {
                Some(t) => t.to_string(),
                None => continue,
            };
            let row_key_str = match parts.next() {
                Some(r) => r.to_string(),
                None => continue,
            };

            let expiration_ts: u64 = match ts_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if expiration_ts > now_secs {
                break;
            }

            to_cleanup.push((key.clone(), table_name, row_key_str));

            if to_cleanup.len() >= limit {
                break;
            }
        }

        if to_cleanup.is_empty() {
            return Ok(0);
        }

        let mut per_table_indexed: HashMap<String, Vec<String>> = HashMap::new();
        let index_meta = self.index_meta_tree()?;
        let mut deleted_rows: u64 = 0;

        for (ttl_key, table_name, row_key_str) in to_cleanup {
            let table_tree = self.table_tree(&table_name)?;
            let row_key_bytes = row_key_str.as_bytes();

            let stored = match table_tree
                .get(row_key_bytes)
                .map_err(|e| StorageError::SledError(e.to_string()))?
            {
                Some(v) => v,
                None => {
                    ttl_tree
                        .remove(&ttl_key)
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                    continue;
                }
            };

            let value = decompress_row(&stored)?;
            let (row, _): (Row, usize) =
                bincode::decode_from_slice(&value, bincode::config::standard())?;

            let indexed_cols = if let Some(cols) = per_table_indexed.get(&table_name) {
                cols.clone()
            } else {
                let schema = match self.get_schema(&table_name).await? {
                    Some(s) => s,
                    None => {
                        ttl_tree
                            .remove(&ttl_key)
                            .map_err(|e| StorageError::SledError(e.to_string()))?;
                        continue;
                    }
                };

                let mut cols: Vec<String> = Vec::new();
                for col in &schema.columns {
                    let meta_key = format!("{}:{}", table_name, col.name);
                    let has_index = index_meta
                        .contains_key(meta_key.as_bytes())
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                    if has_index {
                        cols.push(col.name.clone());
                    }
                }
                per_table_indexed.insert(table_name.clone(), cols.clone());
                cols
            };

            if !indexed_cols.is_empty() {
                for col_name in &indexed_cols {
                    if let Some(col_value) = row.get(col_name) {
                        let index_tree = self.index_tree(&table_name, col_name)?;
                        let index_key = format!(
                            "{}:{}",
                            serde_json::to_string(col_value)?,
                            row_key_str
                        );
                        index_tree
                            .remove(index_key.as_bytes())
                            .map_err(|e| StorageError::SledError(e.to_string()))?;
                    }
                }
            }

            {
                let mut cache = self.cache.lock().await;
                let cache_key = (table_name.clone(), row_key_bytes.to_vec());
                cache.pop(&cache_key);
            }

            table_tree
                .remove(row_key_bytes)
                .map_err(|e| StorageError::SledError(e.to_string()))?;
            ttl_tree
                .remove(&ttl_key)
                .map_err(|e| StorageError::SledError(e.to_string()))?;

            deleted_rows += 1;
        }

        self.db
            .flush_async()
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        Ok(deleted_rows)
    }

    async fn drain_offline_queue(&self, limit: usize) -> Result<Vec<crate::storage::offline_queue::PersistentQueuedOperation>> {
        let mut queue = self.offline_queue.lock().await;
        queue.drain(limit).map_err(|e| StorageError::SledError(e.to_string()).into())
    }

    async fn requeue_offline_ops(&self, ops: Vec<crate::storage::offline_queue::PersistentQueuedOperation>) -> Result<()> {
        let mut queue = self.offline_queue.lock().await;
        for op in ops {
            queue.enqueue(op.timestamp, op.operation)?;
        }
        Ok(())
    }

    async fn apply_operation(&mut self, op: Operation) -> Result<()> {
        match op {
            Operation::Insert { table, key, value } => {
                let table_tree = self.table_tree(&table)?;
                table_tree
                    .insert(key.clone(), value)
                    .map_err(|e| StorageError::SledError(e.to_string()))?;

                if let Some(schema) = self.get_schema(&table).await? {
                    if let Some(ttl) = schema.ttl_seconds {
                        let expiration_ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            .saturating_add(ttl);
                        let ttl_tree = self.ttl_tree()?;
                        let row_key_str = String::from_utf8_lossy(&key).to_string();
                        let ttl_key = format!("{:020}:{}:{}", expiration_ts, table, row_key_str);
                        ttl_tree
                            .insert(ttl_key.as_bytes(), &[])
                            .map_err(|e| StorageError::SledError(e.to_string()))?;
                    }
                }
            }
            Operation::Update { table, key, value } => {
                let table_tree = self.table_tree(&table)?;
                table_tree
                    .insert(key, value)
                    .map_err(|e| StorageError::SledError(e.to_string()))?;
            }
            Operation::Delete { table, key } => {
                let table_tree = self.table_tree(&table)?;
                table_tree
                    .remove(key)
                    .map_err(|e| StorageError::SledError(e.to_string()))?;
            }
        }

        self.db
            .flush_async()
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        Ok(())
    }

    async fn get_lww_ts(&self, table: &str, key: &[u8]) -> Result<Option<HybridTimestamp>> {
        let tree = self.lww_ts_tree()?;
        let mut composite = Vec::with_capacity(table.len() + 1 + key.len());
        composite.extend_from_slice(table.as_bytes());
        composite.push(0);
        composite.extend_from_slice(key);

        match tree.get(&composite) {
            Ok(Some(value)) => {
                let (ts, _): (HybridTimestamp, usize) =
                    bincode::serde::decode_from_slice(&value, bincode::config::standard())
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                Ok(Some(ts))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::SledError(e.to_string()).into()),
        }
    }

    async fn set_lww_ts(&mut self, table: &str, key: &[u8], ts: HybridTimestamp) -> Result<()> {
        let tree = self.lww_ts_tree()?;
        let mut composite = Vec::with_capacity(table.len() + 1 + key.len());
        composite.extend_from_slice(table.as_bytes());
        composite.push(0);
        composite.extend_from_slice(key);

        let bytes = bincode::serde::encode_to_vec(ts, bincode::config::standard())
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        tree.insert(composite, bytes)
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        Ok(())
    }
}

impl SledEngine {
    async fn recover_from_wal(&mut self) -> Result<()> {
        let wal_tree = self.wal_tree()?;

        for item in wal_tree.iter() {
            let (_key, value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            let (entry, _): (WalEntry, usize) =
                bincode::serde::decode_from_slice(&value, bincode::config::standard())?;

            match entry.operation {
                Operation::Insert { table, key, value } => {
                    let table_tree = self.table_tree(&table)?;
                    table_tree
                        .insert(key, value)
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                }
                Operation::Update { table, key, value } => {
                    let table_tree = self.table_tree(&table)?;
                    table_tree
                        .insert(key, value)
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                }
                Operation::Delete { table, key } => {
                    let table_tree = self.table_tree(&table)?;
                    table_tree
                        .remove(key)
                        .map_err(|e| StorageError::SledError(e.to_string()))?;
                }
            }
        }

        self.db
            .flush_async()
            .await
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        log::debug!("WAL recovery completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Value;
    use crate::storage::{Column, DataType, TableSchema, Filter, FilterOp};
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_sled_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap()).unwrap();
        
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
            ttl_seconds: None,
        };
        
        engine.create_table("users", schema).await.unwrap();
        
        let retrieved = engine.get_schema("users").await.unwrap().unwrap();
        assert_eq!(retrieved.columns.len(), 2);
    }
    
    #[tokio::test]
    async fn test_sled_insert_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap()).unwrap();
        
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
            ttl_seconds: None,
        };
        
        engine.create_table("sensors", schema).await.unwrap();
        
        let mut row = Row::new();
        row.insert("sensor_id".to_string(), Value::Integer(1));
        row.insert("temperature".to_string(), Value::Float(25.5));
        
        engine.insert("sensors", vec![row]).await.unwrap();
        
        let results = engine.query("sensors", None).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_sled_indexed_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap()).unwrap();

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
            ttl_seconds: None,
        };

        engine.create_table("sensors", schema).await.unwrap();

        // Insert two rows with different sensor_ids
        let mut row1 = Row::new();
        row1.insert("sensor_id".to_string(), Value::Integer(1));
        row1.insert("temperature".to_string(), Value::Float(25.5));

        let mut row2 = Row::new();
        row2.insert("sensor_id".to_string(), Value::Integer(2));
        row2.insert("temperature".to_string(), Value::Float(30.0));

        engine.insert("sensors", vec![row1, row2]).await.unwrap();

        // Create an index on sensor_id
        engine.create_index("sensors", "sensor_id").await.unwrap();

        // Build a filter that should be able to use the index
        let filter = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
        };

        let results = engine.query("sensors", Some(filter)).await.unwrap();
        assert_eq!(results.len(), 1);
        let row = &results[0];
        assert_eq!(row.get("sensor_id"), Some(&Value::Integer(1)));
    }

    #[tokio::test]
    async fn test_sled_indexed_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap()).unwrap();

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
            ttl_seconds: None,
        };

        engine.create_table("sensors", schema).await.unwrap();

        // Insert three rows, two of which share the same sensor_id
        let mut row1 = Row::new();
        row1.insert("sensor_id".to_string(), Value::Integer(1));
        row1.insert("temperature".to_string(), Value::Float(25.5));

        let mut row2 = Row::new();
        row2.insert("sensor_id".to_string(), Value::Integer(2));
        row2.insert("temperature".to_string(), Value::Float(30.0));

        let mut row3 = Row::new();
        row3.insert("sensor_id".to_string(), Value::Integer(1));
        row3.insert("temperature".to_string(), Value::Float(26.0));

        engine.insert("sensors", vec![row1, row2, row3]).await.unwrap();

        // Create an index on sensor_id
        engine.create_index("sensors", "sensor_id").await.unwrap();

        // Delete all rows with sensor_id = 1
        let delete_filter = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
        };

        let deleted = engine.delete("sensors", delete_filter).await.unwrap();
        assert_eq!(deleted, 2);

        // Query again by sensor_id = 1 using the index; should return no rows
        let query_filter = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
        };

        let results = engine.query("sensors", Some(query_filter)).await.unwrap();
        assert_eq!(results.len(), 0);

        // And sensor_id = 2 should still be present
        let filter2 = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(2),
        };
        let results2 = engine.query("sensors", Some(filter2)).await.unwrap();
        assert_eq!(results2.len(), 1);
        let row = &results2[0];
        assert_eq!(row.get("sensor_id"), Some(&Value::Integer(2)));
    }

    #[tokio::test]
    async fn test_sled_ttl_cleanup_with_index() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledEngine::new(temp_dir.path().to_str().unwrap()).unwrap();

        let schema = TableSchema {
            name: "sensors_ttl".to_string(),
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
            ttl_seconds: Some(1),
        };

        engine.create_table("sensors_ttl", schema).await.unwrap();

        // Insert three rows, two of which share the same sensor_id
        let mut row1 = Row::new();
        row1.insert("sensor_id".to_string(), Value::Integer(1));
        row1.insert("temperature".to_string(), Value::Float(25.5));

        let mut row2 = Row::new();
        row2.insert("sensor_id".to_string(), Value::Integer(2));
        row2.insert("temperature".to_string(), Value::Float(30.0));

        let mut row3 = Row::new();
        row3.insert("sensor_id".to_string(), Value::Integer(1));
        row3.insert("temperature".to_string(), Value::Float(26.0));

        engine
            .insert("sensors_ttl", vec![row1, row2, row3])
            .await
            .unwrap();

        // Create an index on sensor_id so that TTL cleanup must also
        // maintain secondary index entries and the LRU cache.
        engine
            .create_index("sensors_ttl", "sensor_id")
            .await
            .unwrap();

        // Sanity check: rows are visible before TTL cleanup.
        let all_before = engine.query("sensors_ttl", None).await.unwrap();
        assert_eq!(all_before.len(), 3);

        let filter_before = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
        };
        let indexed_before = engine
            .query("sensors_ttl", Some(filter_before))
            .await
            .unwrap();
        assert_eq!(indexed_before.len(), 2);

        // Advance now_secs far beyond the TTL so that all rows are expired.
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cleaned = engine
            .cleanup_expired(now_secs + 3600, 1000)
            .await
            .unwrap();
        assert_eq!(cleaned, 3);

        // After cleanup, no rows should be visible via full scan...
        let all_after = engine.query("sensors_ttl", None).await.unwrap();
        assert_eq!(all_after.len(), 0);

        // ...or via the indexed query path.
        let filter_after = Filter {
            column: "sensor_id".to_string(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
        };
        let indexed_after = engine
            .query("sensors_ttl", Some(filter_after))
            .await
            .unwrap();
        assert_eq!(indexed_after.len(), 0);
    }
}
