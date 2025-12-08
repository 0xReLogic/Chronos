use chronos::raft::Log;
use proptest::prelude::*;
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum LogOp {
    Append { term: u64 },
    Truncate { index: u64 },
}

fn log_op_strategy() -> impl Strategy<Value = LogOp> {
    prop_oneof![
        (0u64..10_000).prop_map(|term| LogOp::Append { term }),
        (0u64..50).prop_map(|index| LogOp::Truncate { index }),
    ]
}

proptest! {
    #[test]
    fn raft_log_preserves_prefix_and_indices(ops in proptest::collection::vec(log_op_strategy(), 1..50)) {
        let tmp = TempDir::new().expect("tempdir");
        let data_dir = tmp.path().to_str().expect("utf8");

        let mut log = Log::new(data_dir);
        let mut shadow: Vec<(u64, Vec<u8>)> = Vec::new();
        shadow.push((0, Vec::new()));

        for op in ops {
            match op {
                LogOp::Append { term } => {
                    let entry = chronos::raft::LogEntry { term, command: Vec::new() };
                    let _ = log.append(entry);
                    shadow.push((term, Vec::new()));
                }
                LogOp::Truncate { index } => {
                    let _ = log.truncate(index);
                    if index >= 1 && (index as usize) < shadow.len() {
                        shadow.truncate(index as usize + 1);
                    }
                }
            }
        }

        let last = log.last_index();
        prop_assert_eq!(last as usize + 1, shadow.len());

        for (idx, (term, _cmd)) in shadow.iter().enumerate() {
            let idx_u64 = idx as u64;
            let t = log.term_at(idx_u64).unwrap();
            prop_assert_eq!(*term, t, "term mismatch at index {}", idx_u64);
            let entry = log.get_entry(idx_u64).unwrap().expect("entry must exist");
            prop_assert_eq!(*term, entry.term, "entry.term mismatch at index {}", idx_u64);
        }

        for idx in (last+1)..=(last+5) {
            let entry = log.get_entry(idx).unwrap();
            prop_assert!(entry.is_none());
        }
    }
}
