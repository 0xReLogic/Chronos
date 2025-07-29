mod error;
mod node;
mod log;
mod state;
mod config;

pub use self::error::RaftError;
pub use self::node::RaftNode;
pub use self::log::{LogEntry, Log};
pub use self::state::{NodeState, NodeRole};
pub use self::config::RaftConfig;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::mpsc;
// Use external log crate, not our own log module
use ::log::{info, error, debug};

// Message types for Raft communication
#[derive(Debug, Clone)]
pub enum RaftMessage {
    // Leader election messages
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    
    // Log replication messages
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

// Main Raft service
pub struct Raft {
    pub node: Arc<Mutex<RaftNode>>,
}

impl Raft {
    pub fn new(config: RaftConfig) -> Self {
        let node = RaftNode::new(config);
        Self {
            node: Arc::new(Mutex::new(node)),
        }
    }
    
    pub async fn start(&self) -> Result<(), RaftError> {
        let node = Arc::clone(&self.node);
        
        // Create channels for message passing
        let (tx, mut rx) = mpsc::channel(100);
        
        // Store the sender in the node
        {
            let mut node_lock = node.lock().unwrap();
            node_lock.set_message_sender(tx.clone());
        }
        
        // Spawn a task to handle incoming messages
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let mut node = node_clone.lock().unwrap();
                match node.handle_message(message) {
                    Ok(_) => {},
                    Err(e) => error!("Error handling message: {}", e),
                }
            }
        });
        
        // Spawn a task for election timeout
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            loop {
                let timeout = {
                    let node = node_clone.lock().unwrap();
                    if node.is_leader() {
                        // Leaders don't have election timeouts
                        Duration::from_millis(node.config().heartbeat_interval)
                    } else {
                        // Random election timeout
                        node.get_election_timeout()
                    }
                };
                
                sleep(timeout).await;
                
                let mut node = node_clone.lock().unwrap();
                if node.is_leader() {
                    // Send heartbeats
                    match node.send_heartbeats() {
                        Ok(_) => debug!("Sent heartbeats"),
                        Err(e) => error!("Error sending heartbeats: {}", e),
                    }
                } else {
                    // Check if election timeout has elapsed
                    if node.election_timeout_elapsed() {
                        info!("Election timeout elapsed, starting election");
                        match node.start_election() {
                            Ok(_) => debug!("Started election"),
                            Err(e) => error!("Error starting election: {}", e),
                        }
                    }
                }
            }
        });
        
        Ok(())
    }
    
    pub fn submit_command(&self, command: Vec<u8>) -> Result<(), RaftError> {
        let mut node = self.node.lock().unwrap();
        node.submit_command(command)
    }
}