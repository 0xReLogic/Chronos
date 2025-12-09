use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: u64,
    pub http_port: u16,
    pub raft_port: u16,
    pub peers: Vec<PeerConfig>,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "sled")]
    Sled { data_dir: String },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Sled {
            data_dir: "./data".to_string(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            http_port: 8080,
            raft_port: 9090,
            peers: vec![],
            storage: StorageConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_config_default_uses_sled_data_dir() {
        let cfg = StorageConfig::default();
        match cfg {
            StorageConfig::Sled { data_dir } => {
                assert_eq!(data_dir, "./data");
            }
        }
    }

    #[test]
    fn config_default_values_are_sensible() {
        let cfg = Config::default();
        assert_eq!(cfg.node_id, 1);
        assert_eq!(cfg.http_port, 8080);
        assert_eq!(cfg.raft_port, 9090);
        assert!(cfg.peers.is_empty());

        match cfg.storage {
            StorageConfig::Sled { data_dir } => {
                assert_eq!(data_dir, "./data");
            }
        }
    }
}
