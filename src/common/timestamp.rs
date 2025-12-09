use serde::{Deserialize, Serialize};
use uhlc::{Timestamp as HlcTimestamp, HLC};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HybridTimestamp {
    pub ts: HlcTimestamp,
    pub node_id: u64,
}

pub struct Clock {
    hlc: HLC,
    node_id: u64,
}

impl Clock {
    /// Create a new clock bound to a specific node id.
    pub fn new(node_id: u64) -> Self {
        Self {
            hlc: HLC::default(),
            node_id,
        }
    }

    /// Generate a new local timestamp for an outgoing operation.
    pub fn now(&self) -> HybridTimestamp {
        HybridTimestamp {
            ts: self.hlc.new_timestamp(),
            node_id: self.node_id,
        }
    }

    /// Merge this clock with a remote timestamp and return an updated local timestamp.
    ///
    /// This should be called when receiving operations from other nodes.
    pub fn merge(&self, remote: &HybridTimestamp) -> Result<HybridTimestamp, String> {
        self.hlc.update_with_timestamp(&remote.ts)?;
        Ok(self.now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_timestamps_increase() {
        let clock = Clock::new(1);
        let t1 = clock.now();
        let t2 = clock.now();
        assert!(t2.ts > t1.ts);
        assert_eq!(t1.node_id, 1);
        assert_eq!(t2.node_id, 1);
    }

    #[test]
    fn merge_with_remote_timestamp_produces_newer_ts() {
        let local = Clock::new(1);
        let remote_clock = Clock::new(2);

        let remote_ts = remote_clock.now();
        let merged_ts = local.merge(&remote_ts).expect("merge failed");

        assert!(merged_ts.ts > remote_ts.ts);
        assert_eq!(merged_ts.node_id, 1);
    }
}
