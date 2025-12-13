use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use crate::executor::Executor;
use crate::network::SharedSyncStatus;
use crate::network::SyncClient;
use crate::storage::offline_queue::PersistentQueuedOperation;

fn compact_lww_ops(ops: Vec<PersistentQueuedOperation>) -> Vec<PersistentQueuedOperation> {
    let mut latest: HashMap<(String, Vec<u8>), PersistentQueuedOperation> = HashMap::new();

    for op in ops {
        let (table, key) = match &op.operation {
            crate::storage::wal::Operation::Insert { table, key, .. } => {
                (table.clone(), key.clone())
            }
            crate::storage::wal::Operation::Update { table, key, .. } => {
                (table.clone(), key.clone())
            }
            crate::storage::wal::Operation::Delete { table, key } => (table.clone(), key.clone()),
        };

        let map_key = (table, key);
        let replace = match latest.get(&map_key) {
            Some(existing) => existing.timestamp < op.timestamp,
            None => true,
        };

        if replace {
            latest.insert(map_key, op);
        }
    }

    let mut compacted: Vec<PersistentQueuedOperation> = latest.into_values().collect();
    compacted.sort_by_key(|op| op.id);
    compacted
}

pub struct SyncWorker {
    executor: Arc<TokioMutex<Executor>>,
    target: String,
    interval: Duration,
    batch_size: usize,
    status: SharedSyncStatus,
    edge_id: String,
}

impl SyncWorker {
    pub fn new(
        executor: Arc<TokioMutex<Executor>>,
        target: String,
        interval: Duration,
        batch_size: usize,
        status: SharedSyncStatus,
        edge_id: String,
    ) -> Self {
        Self {
            executor,
            target,
            interval,
            batch_size,
            status,
            edge_id,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Err(e) = self.sync_once().await {
                log::error!("SyncWorker error: {e}");
            }
            sleep(self.interval).await;
        }
    }

    async fn sync_once(&mut self) -> Result<()> {
        // Drain a batch of operations from the persistent offline queue.
        let drained: Vec<PersistentQueuedOperation> = {
            let exec = self.executor.lock().await;
            exec.drain_offline_queue(self.batch_size)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?
        };
        if drained.is_empty() {
            return Ok(());
        }

        let drained_len = drained.len();
        let to_send = compact_lww_ops(drained);
        if to_send.is_empty() {
            return Ok(());
        }

        let send_len = to_send.len();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut client = SyncClient::new(&self.target);
        let token = std::env::var("CHRONOS_AUTH_TOKEN")
            .or_else(|_| std::env::var("CHRONOS_AUTH_TOKEN_ADMIN"))
            .ok();
        if let Some(token) = token {
            client = client.with_auth_token(token);
        }

        match client.sync_operations(&self.edge_id, to_send.clone()).await {
            Ok(applied) => {
                {
                    let mut status = self.status.lock().await;
                    status.last_sync_ts_ms = now_ms;
                    status.last_sync_applied = applied;
                    status.pending_ops = drained_len as u64;
                    status.last_error = None;
                }

                log::info!(
                    "SyncWorker synced {} operations to {} (applied={}, drained={})",
                    send_len,
                    self.target,
                    applied,
                    drained_len
                );
                Ok(())
            }
            Err(e) => {
                // On failure, re-enqueue operations so they are not lost.
                log::warn!("SyncWorker failed to sync batch: {e}. Re-enqueuing operations");
                let exec = self.executor.lock().await;
                exec.requeue_offline_ops(to_send.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

                {
                    let mut status = self.status.lock().await;
                    status.last_sync_ts_ms = now_ms;
                    status.last_sync_applied = 0;
                    status.pending_ops = drained_len as u64;
                    status.last_error = Some(e.to_string());
                }

                Err(anyhow::anyhow!(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::timestamp::HybridTimestamp;
    use crate::storage::wal::Operation as WalOperation;
    use uhlc::HLC;

    #[test]
    fn compact_lww_ops_keeps_latest_per_key() {
        let hlc = HLC::default();
        let ts1 = HybridTimestamp {
            ts: hlc.new_timestamp(),
            node_id: 1,
        };
        let ts2 = HybridTimestamp {
            ts: hlc.new_timestamp(),
            node_id: 1,
        };

        let ops = vec![
            PersistentQueuedOperation {
                id: 1,
                timestamp: ts1,
                operation: WalOperation::Insert {
                    table: "t".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
            },
            PersistentQueuedOperation {
                id: 2,
                timestamp: ts2,
                operation: WalOperation::Update {
                    table: "t".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v2".to_vec(),
                },
            },
            PersistentQueuedOperation {
                id: 3,
                timestamp: ts1,
                operation: WalOperation::Insert {
                    table: "t".to_string(),
                    key: b"k2".to_vec(),
                    value: b"v3".to_vec(),
                },
            },
        ];

        let compacted = compact_lww_ops(ops);
        assert_eq!(compacted.len(), 2);
        assert_eq!(compacted[0].id, 2);
        assert_eq!(compacted[1].id, 3);

        match &compacted[0].operation {
            WalOperation::Update { value, .. } => assert_eq!(value, b"v2"),
            other => panic!("unexpected operation: {:?}", other),
        }
    }

    #[test]
    fn compact_lww_ops_uses_timestamp_not_id() {
        let hlc = HLC::default();
        let ts_old = HybridTimestamp {
            ts: hlc.new_timestamp(),
            node_id: 1,
        };
        let ts_new = HybridTimestamp {
            ts: hlc.new_timestamp(),
            node_id: 1,
        };

        let ops = vec![
            PersistentQueuedOperation {
                id: 2,
                timestamp: ts_old,
                operation: WalOperation::Update {
                    table: "t".to_string(),
                    key: b"k".to_vec(),
                    value: b"old".to_vec(),
                },
            },
            PersistentQueuedOperation {
                id: 1,
                timestamp: ts_new,
                operation: WalOperation::Update {
                    table: "t".to_string(),
                    key: b"k".to_vec(),
                    value: b"new".to_vec(),
                },
            },
        ];

        let compacted = compact_lww_ops(ops);
        assert_eq!(compacted.len(), 1);
        assert_eq!(compacted[0].id, 1);
        match &compacted[0].operation {
            WalOperation::Update { value, .. } => assert_eq!(value, b"new"),
            other => panic!("unexpected operation: {:?}", other),
        }
    }
}
