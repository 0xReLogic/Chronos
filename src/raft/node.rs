use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
// Use external log crate, not our own log module
use ::log::{debug, error, info};

use super::{Log, LogEntry, NodeRole, NodeState, RaftConfig, RaftError, RaftMessage};

pub struct RaftNode {
    // Node identity
    id: String,

    // Raft state
    state: NodeState,
    log: Log,

    // Configuration
    config: RaftConfig,

    // Communication
    peers: HashMap<String, String>, // node_id -> address
    message_sender: Option<mpsc::Sender<RaftMessage>>,

    // Election state
    last_election_time: Instant,
    votes_received: HashMap<String, bool>,

    // Leader state
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    // Leader lease for low-latency reads
    lease_expiry: Option<Instant>,
    lease_duration: Duration,
    // Optional channel to forward committed commands to an external state
    // machine (e.g., the SQL Executor) for application.
    apply_sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
    // For the current leader: map of log index -> oneshot sender used to
    // notify when a given entry has been committed and applied locally.
    pending_commands: HashMap<u64, oneshot::Sender<()>>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        let peers = config.peers.clone();
        let heartbeat_interval = config.heartbeat_interval;

        Self {
            id: config.node_id.clone(),
            state: NodeState {
                current_term: 0,
                voted_for: None,
                role: NodeRole::Follower,
                leader_id: None,
                commit_index: 0,
                last_applied: 0,
            },
            log: Log::new(&config.data_dir),
            config,
            peers,
            message_sender: None,
            last_election_time: Instant::now(),
            votes_received: HashMap::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            lease_expiry: None,
            lease_duration: Duration::from_millis(heartbeat_interval * 3),
            apply_sender: None,
            pending_commands: HashMap::new(),
        }
    }

    pub fn set_apply_sender(&mut self, sender: mpsc::UnboundedSender<Vec<u8>>) {
        self.apply_sender = Some(sender);
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn state(&self) -> &NodeState {
        &self.state
    }

    pub fn set_message_sender(&mut self, sender: mpsc::Sender<RaftMessage>) {
        self.message_sender = Some(sender);
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state.role, NodeRole::Leader)
    }

    pub fn get_election_timeout(&self) -> Duration {
        let mut rng = rand::rng();
        let timeout_ms =
            rng.random_range(self.config.election_timeout_min..=self.config.election_timeout_max);
        Duration::from_millis(timeout_ms)
    }

    pub fn election_timeout_elapsed(&self) -> bool {
        let elapsed = self.last_election_time.elapsed().as_millis() as u64;
        elapsed > self.config.election_timeout_max
    }

    fn abort_pending_commands(&mut self) {
        // Dropping the senders will cause any awaiters on the corresponding
        // oneshot::Receiver to observe an error instead of hanging forever.
        self.pending_commands.clear();
    }

    pub fn start_election(&mut self) -> Result<(), RaftError> {
        // Increment current term
        self.state.current_term += 1;

        // Vote for self
        self.state.voted_for = Some(self.id.clone());
        self.state.role = NodeRole::Candidate;

        // Reset election timer
        self.last_election_time = Instant::now();

        // Reset votes received
        self.votes_received.clear();
        self.votes_received.insert(self.id.clone(), true);

        // Send RequestVote RPCs to all peers
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.term_at(last_log_index).unwrap_or(0);

        let request = RaftMessage::RequestVote {
            term: self.state.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
        };

        self.broadcast_message(request)?;

        // Check if we have already won the election (e.g. single node cluster)
        let majority_count = self.peers.len().div_ceil(2) + 1;
        if self.votes_received.len() >= majority_count {
            info!(
                "Election won (single node), becoming leader for term {}",
                self.state.current_term
            );
            self.become_leader()?;
        }

        Ok(())
    }

    pub fn start_pre_vote(&mut self) -> Result<(), RaftError> {
        // Pre-vote does not change current_term or voted_for.
        self.state.role = NodeRole::Candidate;
        self.last_election_time = Instant::now();
        self.votes_received.clear();
        // Count self as a pre-vote yes.
        self.votes_received.insert(self.id.clone(), true);

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.term_at(last_log_index).unwrap_or(0);

        let request = RaftMessage::PreVote {
            term: self.state.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
        };

        self.broadcast_message(request)?;

        // Single-node optimization
        let majority_count = self.peers.len().div_ceil(2) + 1;
        if self.votes_received.len() >= majority_count {
            // Proceed to real election
            self.start_election()?;
        }

        Ok(())
    }

    pub fn send_heartbeats(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader);
        }

        for peer_id in self.peers.keys() {
            let next_idx = self.next_index.get(peer_id).cloned().unwrap_or(1);
            let prev_log_index = next_idx - 1;
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

            // Get entries to send (empty for heartbeat)
            let entries = Vec::new();

            let message = RaftMessage::AppendEntries {
                term: self.state.current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.commit_index,
            };

            self.send_message_to_peer(peer_id, message.clone())?;
        }
        // Extend leader lease after sending heartbeats
        self.lease_expiry = Some(Instant::now() + self.lease_duration);

        Ok(())
    }

    pub fn submit_command(&mut self, command: Vec<u8>) -> Result<oneshot::Receiver<()>, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader);
        }

        // Append to local log
        let entry = LogEntry {
            term: self.state.current_term,
            command,
        };

        let log_index = self.log.append(entry)?;
        let (tx, rx) = oneshot::channel();
        self.pending_commands.insert(log_index, tx);
        // Update commit index immediately after appending so that single-node
        // clusters (with no peers) can commit entries based on the leader's
        // own log, while multi-node clusters still wait for follower
        // match_index updates.
        self.update_commit_index()?;

        // Replicate to followers
        self.replicate_log()?;

        Ok(rx)
    }

    pub fn replicate_log(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader);
        }

        for peer_id in self.peers.keys() {
            let next_idx = self.next_index.get(peer_id).cloned().unwrap_or(1);
            let prev_log_index = next_idx - 1;
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

            // Get entries to send
            let entries = self.log.get_entries(next_idx, None)?;

            let message = RaftMessage::AppendEntries {
                term: self.state.current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.commit_index,
            };

            self.send_message_to_peer(peer_id, message.clone())?;
        }

        Ok(())
    }

    pub fn handle_message(
        &mut self,
        message: RaftMessage,
    ) -> Result<Option<RaftMessage>, RaftError> {
        match message {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, candidate_id, last_log_index, last_log_term),
            RaftMessage::PreVote {
                term,
                candidate_id: _,
                last_log_index,
                last_log_term,
            } => {
                // Pre-vote check: do NOT change term or voted_for.
                let mut vote_granted = false;
                let our_last_log_index = self.log.last_index();
                let our_last_log_term = self.log.term_at(our_last_log_index).unwrap_or(0);
                if term >= self.state.current_term
                    && (last_log_term > our_last_log_term
                        || (last_log_term == our_last_log_term
                            && last_log_index >= our_last_log_index))
                {
                    vote_granted = true;
                }
                let response = RaftMessage::PreVoteResponse {
                    term: self.state.current_term,
                    vote_granted,
                };
                Ok(Some(response))
            }
            RaftMessage::RequestVoteResponseFromPeer {
                peer_id,
                term,
                vote_granted,
            } => self
                .handle_request_vote_response(peer_id, term, vote_granted)
                .map(|_| None),
            RaftMessage::PreVoteResponseFromPeer {
                peer_id,
                term,
                vote_granted,
            } => {
                // Ignore responses with higher term (become follower)
                if term > self.state.current_term {
                    self.state.current_term = term;
                    self.state.voted_for = None;
                    self.state.role = NodeRole::Follower;
                    self.abort_pending_commands();
                    return Ok(None);
                }
                if self.state.role == NodeRole::Candidate
                    && term == self.state.current_term
                    && vote_granted
                {
                    self.votes_received.insert(peer_id, true);
                    let votes_needed = self.peers.len().div_ceil(2) + 1; // +1 self
                    if self.votes_received.values().filter(|&&v| v).count() >= votes_needed {
                        // Start the real election now
                        self.start_election()?;
                    }
                }
                Ok(None)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            RaftMessage::AppendEntriesResponseFromPeer {
                peer_id,
                term,
                success,
                match_index,
            } => self
                .handle_append_entries_response(peer_id, term, success, match_index)
                .map(|_| None),
            // Plain response variants should not normally be sent into the RaftNode
            // event loop; they are wrapped into *_FromPeer by send_message_to_peer.
            // Handle them as no-ops for robustness.
            RaftMessage::RequestVoteResponse { .. }
            | RaftMessage::PreVoteResponse { .. }
            | RaftMessage::AppendEntriesResponse { .. } => Ok(None),
        }
    }

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Result<Option<RaftMessage>, RaftError> {
        let mut vote_granted = false;

        // If the term is greater than our current term, update it
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.role = NodeRole::Follower;
            self.abort_pending_commands();
        }

        // Decide whether to grant vote
        if term >= self.state.current_term
            && (self.state.voted_for.is_none()
                || self.state.voted_for.as_ref() == Some(&candidate_id))
        {
            // Check if candidate's log is at least as up-to-date as ours
            let our_last_log_index = self.log.last_index();
            let our_last_log_term = self.log.term_at(our_last_log_index).unwrap_or(0);

            if last_log_term > our_last_log_term
                || (last_log_term == our_last_log_term && last_log_index >= our_last_log_index)
            {
                vote_granted = true;
                self.state.voted_for = Some(candidate_id.clone());
                self.last_election_time = Instant::now(); // Reset election timeout
            }
        }

        // Create response
        let response = RaftMessage::RequestVoteResponse {
            term: self.state.current_term,
            vote_granted,
        };

        Ok(Some(response))
    }

    fn handle_request_vote_response(
        &mut self,
        peer_id: String,
        term: u64,
        vote_granted: bool,
    ) -> Result<(), RaftError> {
        // If the term is greater than our current term, update it and become follower
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.role = NodeRole::Follower;
            self.abort_pending_commands();
            return Ok(());
        }

        // Only process if we're still a candidate and in the same term
        if self.state.role == NodeRole::Candidate && term == self.state.current_term && vote_granted
        {
            // Count the vote for this specific peer
            self.votes_received.insert(peer_id, true);

            // Check if we have majority (peers + self)
            let votes_needed = self.peers.len().div_ceil(2) + 1; // +1 for self
            if self.votes_received.values().filter(|&&v| v).count() >= votes_needed {
                // We won the election!
                self.become_leader()?;
            }
        }

        Ok(())
    }

    fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<Option<RaftMessage>, RaftError> {
        let mut success = false;

        // If the term is greater than our current term, update it
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.role = NodeRole::Follower;
        }

        // Reply false if term < currentTerm
        if term < self.state.current_term {
            let response = RaftMessage::AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
                match_index: 0,
            };

            return Ok(Some(response));
        }

        // Reset election timeout since we heard from the leader
        self.last_election_time = Instant::now();

        // Update leader ID
        self.state.leader_id = Some(leader_id.clone());

        // Ensure we're a follower
        if self.state.role != NodeRole::Follower {
            self.state.role = NodeRole::Follower;
            self.abort_pending_commands();
        }

        // Check if log contains an entry at prevLogIndex with prevLogTerm
        let log_ok = if prev_log_index == 0 {
            // Special case for empty log
            true
        } else if prev_log_index > self.log.last_index() {
            // Our log is too short
            false
        } else {
            // Check if terms match
            self.log.term_at(prev_log_index) == Some(prev_log_term)
        };

        if log_ok {
            // If we have existing entries that conflict with new ones, delete them
            if !entries.is_empty() {
                let new_entries = entries.clone();
                let start_idx = prev_log_index + 1;

                // Find the first conflicting entry
                let mut conflict_idx = None;
                for (i, entry) in new_entries.iter().enumerate() {
                    let log_idx = start_idx + i as u64;
                    if let Some(existing_term) = self.log.term_at(log_idx) {
                        if existing_term != entry.term {
                            conflict_idx = Some(i);
                            break;
                        }
                    } else {
                        // We've reached the end of our log
                        break;
                    }
                }

                // Delete conflicting entries and append new ones
                if let Some(idx) = conflict_idx {
                    self.log.truncate(start_idx + idx as u64 - 1)?;
                    for entry in new_entries[idx..].iter() {
                        self.log.append(entry.clone())?;
                    }
                } else {
                    // No conflicts, just append any entries we don't have
                    let last_new_idx = start_idx + new_entries.len() as u64 - 1;
                    if last_new_idx > self.log.last_index() {
                        let append_start_idx = self.log.last_index() + 1 - start_idx;
                        for entry in new_entries[append_start_idx as usize..].iter() {
                            self.log.append(entry.clone())?;
                        }
                    }
                }
            }

            // Update commit index
            if leader_commit > self.state.commit_index {
                self.state.commit_index = leader_commit.min(self.log.last_index());
                // Apply committed entries (in a real implementation)
                self.apply_committed_entries()?;
            }

            success = true;
        }

        // Create response
        let response = RaftMessage::AppendEntriesResponse {
            term: self.state.current_term,
            success,
            match_index: if success {
                prev_log_index + entries.len() as u64
            } else {
                0
            },
        };

        Ok(Some(response))
    }

    fn handle_append_entries_response(
        &mut self,
        peer_id: String,
        term: u64,
        success: bool,
        match_index: u64,
    ) -> Result<(), RaftError> {
        // If the term is greater than our current term, update it and become follower
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.role = NodeRole::Follower;
            self.abort_pending_commands();
            return Ok(());
        }

        // Only process if we're still the leader and in the same term
        if self.state.role == NodeRole::Leader && term == self.state.current_term {
            if success {
                // Update nextIndex and matchIndex for the follower
                self.match_index.insert(peer_id.clone(), match_index);
                self.next_index.insert(peer_id, match_index + 1);

                // Check if we can commit any entries
                self.update_commit_index()?;
            } else {
                // If AppendEntries failed because of log inconsistency, decrement nextIndex and retry
                let next_idx = self.next_index.get(&peer_id).cloned().unwrap_or(1);
                if next_idx > 1 {
                    self.next_index.insert(peer_id, next_idx - 1);
                    // Retry log replication (in a real implementation)
                    // self.replicate_log_to_peer(&peer_id)?;
                }
            }
        }

        Ok(())
    }

    fn become_leader(&mut self) -> Result<(), RaftError> {
        if self.state.role != NodeRole::Candidate {
            return Ok(());
        }

        info!(
            "Node {} becoming leader for term {}",
            self.id, self.state.current_term
        );

        self.state.role = NodeRole::Leader;
        self.state.leader_id = Some(self.id.clone());
        // Initialize leader lease
        self.lease_expiry = Some(Instant::now() + self.lease_duration);

        // Initialize leader state
        let last_log_idx = self.log.last_index();
        for peer_id in self.peers.keys() {
            self.next_index.insert(peer_id.clone(), last_log_idx + 1);
            self.match_index.insert(peer_id.clone(), 0);
        }

        // Send initial empty AppendEntries RPCs (heartbeats)
        self.send_heartbeats()?;

        Ok(())
    }

    pub fn can_serve_read_locally(&self) -> bool {
        if !self.is_leader() {
            return false;
        }
        match self.lease_expiry {
            Some(exp) => Instant::now() < exp,
            None => false,
        }
    }

    fn update_commit_index(&mut self) -> Result<(), RaftError> {
        if self.state.role != NodeRole::Leader {
            return Ok(());
        }

        // Find the highest log index that is replicated on a majority of servers
        let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
        match_indices.push(self.log.last_index()); // Include the leader's log

        match_indices.sort_unstable();
        let majority_idx = match_indices[match_indices.len() / 2];

        // Only commit if the entry is from the current term
        if majority_idx > self.state.commit_index {
            if let Some(term) = self.log.term_at(majority_idx) {
                if term == self.state.current_term {
                    self.state.commit_index = majority_idx;
                    // Apply committed entries (in a real implementation)
                    self.apply_committed_entries()?;
                }
            }
        }

        Ok(())
    }

    fn apply_committed_entries(&mut self) -> Result<(), RaftError> {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;

            // Get the entry
            if let Some(entry) = self.log.get_entry(self.state.last_applied)? {
                // Apply the command to the state machine
                debug!(
                    "Applying log entry {} (term {})",
                    self.state.last_applied, entry.term
                );
                if let Some(tx) = &self.apply_sender {
                    if let Err(e) = tx.send(entry.command.clone()) {
                        error!(
                            "Failed to forward committed command at index {}: {}",
                            self.state.last_applied, e
                        );
                    }
                }

                if self.is_leader() {
                    if let Some(tx) = self.pending_commands.remove(&self.state.last_applied) {
                        let _ = tx.send(());
                    }
                }
            }
        }

        Ok(())
    }

    fn broadcast_message(&self, message: RaftMessage) -> Result<(), RaftError> {
        for peer_id in self.peers.keys() {
            self.send_message_to_peer(peer_id, message.clone())?;
        }

        Ok(())
    }

    fn send_message_to_peer(&self, peer_id: &str, message: RaftMessage) -> Result<(), RaftError> {
        debug!("Sending message to peer {peer_id}: {message:?}");

        // Get the peer address
        let peer_addr = self
            .peers
            .get(peer_id)
            .ok_or_else(|| RaftError::NetworkError(format!("Unknown peer: {peer_id}")))?;

        // In a real implementation, we would use the network client to send the message
        // For now, we'll just create a task to send it
        let peer_addr = peer_addr.clone();
        let message_clone = message.clone();
        let peer_id_clone = peer_id.to_string(); // Clone the peer_id for the async block
        let tx_opt = self.message_sender.clone();

        tokio::spawn(async move {
            use crate::network::RaftClient;

            let mut client = RaftClient::new(&peer_addr);

            match message_clone {
                RaftMessage::RequestVote {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                } => {
                    let resp = client
                        .request_vote(term, &candidate_id, last_log_index, last_log_term)
                        .await;
                    match (tx_opt, resp) {
                        (Some(tx), Ok(RaftMessage::RequestVoteResponse { term, vote_granted })) => {
                            let msg = RaftMessage::RequestVoteResponseFromPeer {
                                peer_id: peer_id_clone.clone(),
                                term,
                                vote_granted,
                            };
                            if let Err(e) = tx.send(msg).await {
                                error!("Failed to forward RequestVoteResponse from {peer_id_clone}: {e}");
                            }
                        }
                        (_, Err(e)) => {
                            debug!("Failed to send RequestVote to {peer_id_clone}: {e}");
                        }
                        _ => {}
                    }
                }
                RaftMessage::PreVote {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                } => {
                    let resp = client
                        .pre_vote(term, &candidate_id, last_log_index, last_log_term)
                        .await;
                    match (tx_opt, resp) {
                        (Some(tx), Ok(RaftMessage::PreVoteResponse { term, vote_granted })) => {
                            let msg = RaftMessage::PreVoteResponseFromPeer {
                                peer_id: peer_id_clone.clone(),
                                term,
                                vote_granted,
                            };
                            if let Err(e) = tx.send(msg).await {
                                error!(
                                    "Failed to forward PreVoteResponse from {peer_id_clone}: {e}"
                                );
                            }
                        }
                        (_, Err(e)) => {
                            debug!("Failed to send PreVote to {peer_id_clone}: {e}");
                        }
                        _ => {}
                    }
                }
                RaftMessage::AppendEntries {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => {
                    let resp = client
                        .append_entries(
                            term,
                            &leader_id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        )
                        .await;
                    match (tx_opt, resp) {
                        (
                            Some(tx),
                            Ok(RaftMessage::AppendEntriesResponse {
                                term,
                                success,
                                match_index,
                            }),
                        ) => {
                            let msg = RaftMessage::AppendEntriesResponseFromPeer {
                                peer_id: peer_id_clone.clone(),
                                term,
                                success,
                                match_index,
                            };
                            if let Err(e) = tx.send(msg).await {
                                error!("Failed to forward AppendEntriesResponse from {peer_id_clone}: {e}");
                            }
                        }
                        (_, Err(e)) => {
                            debug!("Failed to send AppendEntries to {peer_id_clone}: {e}");
                        }
                        _ => {}
                    }
                }
                _ => {
                    error!("Cannot send response message to peer: {message_clone:?}");
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    #[test]
    fn start_election_single_node_becomes_leader_and_sets_lease() {
        let tmp = tempdir().expect("tempdir");
        let cfg = RaftConfig::new("n1", tmp.path().to_str().unwrap());
        let mut node = RaftNode::new(cfg);

        assert_eq!(node.state.current_term, 0);
        assert!(matches!(node.state.role, NodeRole::Follower));

        node.start_election().expect("start_election");

        assert!(node.is_leader());
        assert_eq!(node.state.current_term, 1);
        assert_eq!(node.state.voted_for.as_deref(), Some("n1"));
        assert_eq!(node.state.leader_id.as_deref(), Some("n1"));
        assert!(node.lease_expiry.is_some());
        assert!(node.can_serve_read_locally());
    }

    #[test]
    fn can_serve_read_locally_false_when_not_leader_or_no_lease() {
        let tmp = tempdir().expect("tempdir");
        let cfg = RaftConfig::new("n1", tmp.path().to_str().unwrap());
        let node = RaftNode::new(cfg);

        assert!(!node.is_leader());
        assert!(!node.can_serve_read_locally());
    }

    #[test]
    fn can_serve_read_locally_false_when_lease_expired() {
        let tmp = tempdir().expect("tempdir");
        let cfg = RaftConfig::new("n1", tmp.path().to_str().unwrap());
        let mut node = RaftNode::new(cfg);

        node.state.role = NodeRole::Leader;
        node.lease_expiry = Some(Instant::now() - Duration::from_millis(1));
        assert!(!node.can_serve_read_locally());
    }

    #[tokio::test]
    async fn abort_pending_commands_cancels_waiters() {
        let tmp = tempdir().expect("tempdir");
        let cfg = RaftConfig::new("n1", tmp.path().to_str().unwrap());
        let mut node = RaftNode::new(cfg);

        let (tx, rx) = oneshot::channel::<()>();
        node.pending_commands.insert(1, tx);

        node.abort_pending_commands();

        assert!(rx.await.is_err());
    }
}
