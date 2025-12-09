use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: NodeRole,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
}
