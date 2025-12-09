use crate::common::timestamp::HybridTimestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Insert {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Update {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: HybridTimestamp,
    pub operation: Operation,
    pub checksum: u32,
}

impl WalEntry {
    pub fn new(
        sequence: u64,
        timestamp: HybridTimestamp,
        operation: Operation,
        checksum: u32,
    ) -> Self {
        Self {
            sequence,
            timestamp,
            operation,
            checksum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_entry_roundtrip_bincode() {
        let ts = HybridTimestamp {
            ts: uhlc::HLC::default().new_timestamp(),
            node_id: 1,
        };

        let op = Operation::Insert {
            table: "t".to_string(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        let entry = WalEntry::new(1, ts, op, 0);

        let encoded = bincode::serde::encode_to_vec(&entry, bincode::config::standard()).unwrap();
        let (decoded, _): (WalEntry, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.timestamp.node_id, 1);
        match decoded.operation {
            Operation::Insert { table, .. } => assert_eq!(table, "t"),
            _ => panic!("unexpected operation"),
        }
    }
}
