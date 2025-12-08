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
