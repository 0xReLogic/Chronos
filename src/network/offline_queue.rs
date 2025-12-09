use std::collections::VecDeque;

use crate::common::timestamp::HybridTimestamp;
use crate::storage::wal::Operation;

#[derive(Debug, Clone)]
pub struct QueuedOperation {
    pub timestamp: HybridTimestamp,
    pub operation: Operation,
}

#[derive(Debug, Clone, Copy)]
pub enum OverflowStrategy {
    RejectNew,
    DropOldest,
}

#[derive(Debug)]
pub enum OfflineQueueError {
    QueueFull,
}

pub type Result<T> = std::result::Result<T, OfflineQueueError>;

pub struct OfflineQueue {
    memory_queue: VecDeque<QueuedOperation>,
    max_size: usize,
    strategy: OverflowStrategy,
}

impl OfflineQueue {
    pub fn new(max_size: usize, strategy: OverflowStrategy) -> Self {
        Self {
            memory_queue: VecDeque::new(),
            max_size,
            strategy,
        }
    }

    pub fn len(&self) -> usize {
        self.memory_queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.memory_queue.is_empty()
    }

    pub fn enqueue(&mut self, operation: Operation, timestamp: HybridTimestamp) -> Result<()> {
        if self.memory_queue.len() >= self.max_size {
            match self.strategy {
                OverflowStrategy::RejectNew => {
                    return Err(OfflineQueueError::QueueFull);
                }
                OverflowStrategy::DropOldest => {
                    self.memory_queue.pop_front();
                }
            }
        }

        self.memory_queue.push_back(QueuedOperation {
            timestamp,
            operation,
        });
        Ok(())
    }

    pub fn drain(&mut self, limit: usize) -> Vec<QueuedOperation> {
        let mut drained = Vec::new();
        for _ in 0..limit {
            if let Some(op) = self.memory_queue.pop_front() {
                drained.push(op);
            } else {
                break;
            }
        }
        drained
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uhlc::HLC;

    #[test]
    fn enqueue_and_drain_respects_max_size_drop_oldest() {
        let mut queue = OfflineQueue::new(2, OverflowStrategy::DropOldest);
        let ts = HybridTimestamp {
            ts: HLC::default().new_timestamp(),
            node_id: 1,
        };

        queue
            .enqueue(
                Operation::Delete {
                    table: "t".into(),
                    key: b"k1".to_vec(),
                },
                ts,
            )
            .unwrap();
        queue
            .enqueue(
                Operation::Delete {
                    table: "t".into(),
                    key: b"k2".to_vec(),
                },
                ts,
            )
            .unwrap();
        queue
            .enqueue(
                Operation::Delete {
                    table: "t".into(),
                    key: b"k3".to_vec(),
                },
                ts,
            )
            .unwrap();

        assert_eq!(queue.len(), 2);
        let drained = queue.drain(10);
        assert_eq!(drained.len(), 2);
    }

    #[test]
    fn enqueue_rejects_when_full_and_strategy_reject_new() {
        let mut queue = OfflineQueue::new(1, OverflowStrategy::RejectNew);
        let ts = HybridTimestamp {
            ts: HLC::default().new_timestamp(),
            node_id: 1,
        };

        queue
            .enqueue(
                Operation::Delete {
                    table: "t".into(),
                    key: b"k1".to_vec(),
                },
                ts,
            )
            .unwrap();
        let res = queue.enqueue(
            Operation::Delete {
                table: "t".into(),
                key: b"k2".to_vec(),
            },
            ts,
        );
        assert!(matches!(res, Err(OfflineQueueError::QueueFull)));
    }
}
