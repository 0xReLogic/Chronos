use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use crate::common::timestamp::HybridTimestamp;
use crate::storage::error::StorageError;
use crate::storage::wal::Operation;

/// Persistent representation of an operation queued for later sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentQueuedOperation {
    pub id: u64,
    pub timestamp: HybridTimestamp,
    pub operation: Operation,
}

/// Persistent offline queue backed by a dedicated Sled tree.
///
/// This is intended to store operations that still need to be synced to a
/// remote cluster or cloud endpoint. It uses a monotonically increasing
/// sequence ID encoded in big-endian as the key so that iteration order
/// corresponds to insertion order (FIFO).
pub struct PersistentOfflineQueue {
    tree: sled::Tree,
    next_id: u64,
}

impl PersistentOfflineQueue {
    const TREE_NAME: &'static str = "__offline_queue__";
    const META_NEXT_ID_KEY: &'static [u8] = b"__next_id__";

    /// Open or create the offline queue for the given database.
    pub fn new(db: &sled::Db) -> Result<Self, StorageError> {
        let tree = db
            .open_tree(Self::TREE_NAME)
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        // Prefer a persisted next_id metadata entry if present, otherwise
        // fall back to scanning existing numeric keys for backward
        // compatibility.
        let mut next_id: u64 = 0;

        if let Ok(Some(bytes)) = tree.get(Self::META_NEXT_ID_KEY) {
            if bytes.len() == 8 {
                if let Ok(arr) = bytes.as_ref().try_into() as Result<[u8; 8], _> {
                    next_id = u64::from_be_bytes(arr);
                }
            }
        } else {
            // Legacy initialization path: determine next_id by looking at the
            // last numeric key in the tree, if any.
            let mut last_id: u64 = 0;
            for item in tree.iter() {
                let (key, _value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
                if key.len() == 8 {
                    if let Ok(id_bytes) = key.as_ref().try_into() as Result<[u8; 8], _> {
                        let id = u64::from_be_bytes(id_bytes);
                        if id > last_id {
                            last_id = id;
                        }
                    }
                }
            }
            next_id = last_id;
        }

        Ok(Self { tree, next_id })
    }

    /// Current length of the queue (number of enqueued items).
    pub fn len(&self) -> Result<usize, StorageError> {
        let mut count: usize = 0;
        for item in self.tree.iter() {
            let (key, _value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            if key.len() == 8 {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> Result<bool, StorageError> {
        for item in self.tree.iter() {
            let (key, _value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;
            if key.len() == 8 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Enqueue a new operation with its timestamp. Returns the assigned ID.
    pub fn enqueue(
        &mut self,
        timestamp: HybridTimestamp,
        operation: Operation,
    ) -> Result<u64, StorageError> {
        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;

        let entry = PersistentQueuedOperation {
            id,
            timestamp,
            operation,
        };

        let bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        self.tree
            .insert(id.to_be_bytes(), bytes)
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        // Persist the updated next_id metadata so IDs never reset even if the
        // queue is temporarily empty.
        self.tree
            .insert(Self::META_NEXT_ID_KEY, &id.to_be_bytes())
            .map_err(|e| StorageError::SledError(e.to_string()))?;

        Ok(id)
    }

    /// Drain up to `limit` operations from the head of the queue in FIFO order.
    /// Removed items are deleted from the underlying Sled tree.
    pub fn drain(&mut self, limit: usize) -> Result<Vec<PersistentQueuedOperation>, StorageError> {
        let mut drained = Vec::new();

        for item in self.tree.iter() {
            if drained.len() >= limit {
                break;
            }

            let (key, value) = item.map_err(|e| StorageError::SledError(e.to_string()))?;

            // Skip metadata entries such as the next_id marker; only keys
            // that are exactly 8 bytes long are real queued operations.
            if key.len() != 8 {
                continue;
            }

            let (entry, _): (PersistentQueuedOperation, usize) =
                bincode::serde::decode_from_slice(&value, bincode::config::standard())
                    .map_err(|e| StorageError::SledError(e.to_string()))?;

            self.tree
                .remove(key)
                .map_err(|e| StorageError::SledError(e.to_string()))?;

            drained.push(entry);
        }

        Ok(drained)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn enqueue_and_drain_persistent_queue() {
        let temp_dir = TempDir::new().unwrap();
        let db = sled::open(temp_dir.path()).unwrap();

        let mut queue = PersistentOfflineQueue::new(&db).unwrap();

        let ts = HybridTimestamp {
            ts: uhlc::HLC::default().new_timestamp(),
            node_id: 1,
        };

        queue
            .enqueue(
                ts,
                Operation::Delete {
                    table: "t".into(),
                    key: b"k1".to_vec(),
                },
            )
            .unwrap();

        queue
            .enqueue(
                ts,
                Operation::Delete {
                    table: "t".into(),
                    key: b"k2".to_vec(),
                },
            )
            .unwrap();

        assert_eq!(queue.len().unwrap(), 2);

        let drained = queue.drain(1).unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(queue.len().unwrap(), 1);

        let drained_all = queue.drain(10).unwrap();
        assert_eq!(drained_all.len(), 1);
        assert!(queue.is_empty().unwrap());
    }
}
