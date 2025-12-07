use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

use crate::executor::Executor;
use crate::network::SyncClient;
use crate::storage::offline_queue::PersistentQueuedOperation;

pub struct SyncWorker {
    executor: Arc<TokioMutex<Executor>>,
    target: String,
    interval: Duration,
    batch_size: usize,
}

impl SyncWorker {
    pub fn new(executor: Arc<TokioMutex<Executor>>, target: String, interval: Duration, batch_size: usize) -> Self {
        Self {
            executor,
            target,
            interval,
            batch_size,
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

        let mut client = SyncClient::new(&self.target);

        match client.sync_operations(drained.clone()).await {
            Ok(applied) => {
                log::info!(
                    "SyncWorker synced {} operations to {} (applied={})",
                    drained.len(),
                    self.target,
                    applied
                );
                Ok(())
            }
            Err(e) => {
                // On failure, re-enqueue operations so they are not lost.
                log::warn!("SyncWorker failed to sync batch: {e}. Re-enqueuing operations");
                let exec = self.executor.lock().await;
                exec.requeue_offline_ops(drained)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                Err(anyhow::anyhow!(e.to_string()))
            }
        }
    }
}
