//! `PostgreSQL` bind-parameter budget helpers.
//!
//! The `PostgreSQL` extended-query protocol encodes the number of bind
//! parameters of a prepared statement as a `u16`, so a single statement can
//! carry at most 65535 arguments. Multi-row `INSERT ... VALUES` batches and
//! large `IN (...)` lists must be split into chunks that stay under that
//! budget, otherwise sqlx rejects the statement at encode time with
//! "too many arguments for query".

/// Hard wire-protocol limit on bind parameters per statement (`u16::MAX`).
pub const PG_BIND_LIMIT: usize = u16::MAX as usize;

/// Chunk size for large `IN (...)` id lists.
///
/// Leaves headroom for the handful of extra binds (timestamps, status
/// filters, pagination) the surrounding query may add.
pub const IN_LIST_CHUNK: usize = PG_BIND_LIMIT - 64;

/// Maximum rows per multi-row `INSERT` for an entity binding `columns`
/// parameters per row.
///
/// Clamped to at least one row so degenerate inputs still make progress.
pub const fn max_rows_per_insert(columns: usize) -> usize {
    let columns = if columns == 0 { 1 } else { columns };
    let rows = PG_BIND_LIMIT / columns;
    if rows == 0 { 1 } else { rows }
}

#[cfg(test)]
mod tests {
    use super::{PG_BIND_LIMIT, max_rows_per_insert};

    #[test]
    fn insert_chunks_stay_under_bind_limit() {
        for columns in 1..=64 {
            assert!(max_rows_per_insert(columns) * columns <= PG_BIND_LIMIT);
        }
    }

    #[test]
    fn degenerate_inputs_still_progress() {
        assert_eq!(max_rows_per_insert(0), PG_BIND_LIMIT);
        assert_eq!(max_rows_per_insert(PG_BIND_LIMIT * 2), 1);
    }
}
