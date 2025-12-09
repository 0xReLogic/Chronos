use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use super::RaftError;

#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct LogEntry {
    pub term: u64,
    pub command: Vec<u8>,
}

pub struct Log {
    entries: Vec<LogEntry>,
    log_file: PathBuf,
}

impl Log {
    pub fn new(data_dir: &str) -> Self {
        let log_dir = PathBuf::from(data_dir).join("raft");
        if !log_dir.exists() {
            std::fs::create_dir_all(&log_dir).expect("Failed to create raft log directory");
        }

        let log_file = log_dir.join("log.bin");

        let mut log = Self {
            entries: Vec::new(),
            log_file,
        };

        // Load existing log entries
        if log.log_file.exists() {
            match log.load_from_disk() {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error loading log from disk: {e}");
                    // In a production system, we might want to handle this more gracefully
                }
            }
        }

        // Add a dummy entry at index 0
        if log.entries.is_empty() {
            log.entries.push(LogEntry {
                term: 0,
                command: Vec::new(),
            });
        }

        log
    }

    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64 - 1
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }

        self.entries.get(index as usize).map(|e| e.term)
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<u64, RaftError> {
        self.entries.push(entry.clone());
        let index = self.entries.len() as u64 - 1;

        // Append to disk
        self.append_to_disk(&entry)?;

        Ok(index)
    }

    pub fn get_entry(&self, index: u64) -> Result<Option<LogEntry>, RaftError> {
        Ok(self.entries.get(index as usize).cloned())
    }

    pub fn get_entries(&self, start: u64, end: Option<u64>) -> Result<Vec<LogEntry>, RaftError> {
        let end = end.unwrap_or(self.entries.len() as u64);

        if start >= end || start >= self.entries.len() as u64 {
            return Ok(Vec::new());
        }

        let end = end.min(self.entries.len() as u64);

        Ok(self.entries[start as usize..end as usize].to_vec())
    }

    pub fn truncate(&mut self, index: u64) -> Result<(), RaftError> {
        if index < 1 {
            return Err(RaftError::InvalidLogIndex(index));
        }

        if index >= self.entries.len() as u64 {
            return Ok(());
        }

        self.entries.truncate(index as usize + 1);

        // Rewrite the log file
        self.save_to_disk()?;

        Ok(())
    }

    fn load_from_disk(&mut self) -> Result<(), RaftError> {
        let mut file = File::open(&self.log_file)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(());
        }

        let (entries, _): (Vec<LogEntry>, usize) =
            bincode::decode_from_slice(&buffer, bincode::config::standard())
                .map_err(|e| RaftError::SerializationError(e.to_string()))?;

        self.entries = entries;

        Ok(())
    }

    fn save_to_disk(&self) -> Result<(), RaftError> {
        let encoded = bincode::encode_to_vec(&self.entries, bincode::config::standard())
            .map_err(|e| RaftError::SerializationError(e.to_string()))?;

        let mut file = File::create(&self.log_file)?;
        file.write_all(&encoded)?;

        Ok(())
    }

    fn append_to_disk(&self, _entry: &LogEntry) -> Result<(), RaftError> {
        // In a real implementation, we would use a more efficient append-only log format
        // For simplicity, we'll just rewrite the entire log file
        self.save_to_disk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn new_initializes_dummy_entry() {
        let tmp = tempdir().expect("tempdir");
        let data_dir = tmp.path().to_string_lossy().to_string();

        let log = Log::new(&data_dir);

        // Index 0 should always exist as a dummy entry with term 0.
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.term_at(0), Some(0));
    }

    #[test]
    fn append_and_persist_entries_across_restarts() {
        let tmp = tempdir().expect("tempdir");
        let data_dir = tmp.path().to_string_lossy().to_string();

        {
            // First instance: append a couple of entries and ensure indices are correct.
            let mut log = Log::new(&data_dir);
            let idx1 = log
                .append(LogEntry {
                    term: 1,
                    command: b"cmd1".to_vec(),
                })
                .expect("append 1");
            let idx2 = log
                .append(LogEntry {
                    term: 2,
                    command: b"cmd2".to_vec(),
                })
                .expect("append 2");

            assert_eq!(idx1, 1);
            assert_eq!(idx2, 2);
            assert_eq!(log.last_index(), 2);
        }

        // New instance reading from the same data_dir should see the persisted entries.
        let log = Log::new(&data_dir);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));

        // get_entries should return the correct slice.
        let entries = log.get_entries(1, None).expect("get_entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].term, 1);
        assert_eq!(entries[1].term, 2);
    }

    #[test]
    fn truncate_discards_entries_and_rewrites_log() {
        let tmp = tempdir().expect("tempdir");
        let data_dir = tmp.path().to_string_lossy().to_string();

        let mut log = Log::new(&data_dir);
        for term in 1..=3u64 {
            log.append(LogEntry {
                term,
                command: vec![],
            })
            .expect("append");
        }

        assert_eq!(log.last_index(), 3);

        // Truncate down to index 1 should keep dummy + first real entry.
        log.truncate(1).expect("truncate");
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.term_at(1), Some(1));

        // Truncating with index 0 is invalid.
        let err = log.truncate(0).unwrap_err();
        match err {
            RaftError::InvalidLogIndex(i) => assert_eq!(i, 0),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
