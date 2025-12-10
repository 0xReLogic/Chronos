use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: String,
    pub data_dir: String,
    pub peers: HashMap<String, String>, // node_id -> address
    pub election_timeout_min: u64,      // in milliseconds
    pub election_timeout_max: u64,      // in milliseconds
    pub heartbeat_interval: u64,        // in milliseconds
}

impl RaftConfig {
    pub fn new(node_id: &str, data_dir: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            data_dir: data_dir.to_string(),
            peers: HashMap::new(),
            // Defaults tuned for small edge nodes (low CPU/RAM) to reduce
            // spurious elections under scheduler and network jitter.
            election_timeout_min: 800,
            election_timeout_max: 1600,
            heartbeat_interval: 100,
        }
    }

    pub fn add_peer(&mut self, peer_id: &str, address: &str) {
        self.peers.insert(peer_id.to_string(), address.to_string());
    }
}
