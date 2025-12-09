use std::sync::atomic::{AtomicU64, Ordering};

pub static SQL_READ_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static SQL_WRITE_TOTAL: AtomicU64 = AtomicU64::new(0);

pub fn record_sql(is_read: bool) {
    if is_read {
        SQL_READ_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else {
        SQL_WRITE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn snapshot_sql() -> (u64, u64) {
    (
        SQL_READ_TOTAL.load(Ordering::Relaxed),
        SQL_WRITE_TOTAL.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_snapshot_move_counters_forward() {
        let (read_before, write_before) = snapshot_sql();

        // Record a read and a write. Other tests may also be updating
        // these counters concurrently, so we only assert monotonicity
        // and that the total count increases by at least 2.
        record_sql(true);
        record_sql(false);

        let (read_after, write_after) = snapshot_sql();

        assert!(read_after >= read_before);
        assert!(write_after >= write_before);

        let total_before = read_before + write_before;
        let total_after = read_after + write_after;
        assert!(
            total_after >= total_before + 2,
            "expected at least two additional operations recorded",
        );
    }
}
