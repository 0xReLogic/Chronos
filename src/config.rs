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
    #[serde(rename = "csv")]
    Csv { data_dir: String },
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
