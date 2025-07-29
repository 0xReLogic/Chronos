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
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
        }
    }
    
    pub fn add_peer(&mut self, peer_id: &str, address: &str) {
        self.peers.insert(peer_id.to_string(), address.to_string());
    }
}