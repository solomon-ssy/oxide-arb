//! `PostgreSQL` batch operation utilities.
//!
//! `PostgreSQL` has a hard limit of 65,535 bind parameters per statement.
//! Batch INSERT/UPDATE/IN-clause operations must be chunked to respect this.
//! This module provides compile-time-safe helpers for computing chunk sizes
//! and splitting slices into safe batches.

/// Maximum bind parameters per `PostgreSQL` prepared statement.
pub const MAX_BIND_VARIABLES: usize = 65_535;

/// Maximum elements in an `IN (...)` clause (each element = 1 bind variable).
pub const MAX_IN_LIST_LEN: usize = MAX_BIND_VARIABLES;

/// Compute the maximum number of rows insertable in one statement
/// given the number of columns each row occupies.
///
/// Panics (debug) or returns 1 (release) if `columns_per_row == 0`.
pub const fn max_rows_per_insert(columns_per_row: usize) -> usize {
    if columns_per_row == 0 {
        return 1;
    }
    let rows = MAX_BIND_VARIABLES / columns_per_row;
    if rows == 0 { 1 } else { rows }
}

/// Chunk a slice into batches safe for `PostgreSQL` bind limit during INSERT.
///
/// Each chunk contains at most `max_rows_per_insert(cols_per_row)` elements.
///
/// # Example
///
/// ```
/// use quant_pivot_repository::batch::chunk_for_insert;
///
/// let items: Vec<i32> = (0..200).collect();
/// // 10 columns per row -> 6553 rows max per batch
/// for batch in chunk_for_insert(&items, 10) {
///     assert!(batch.len() <= 6553);
/// }
/// ```
pub fn chunk_for_insert<T>(items: &[T], cols_per_row: usize) -> impl Iterator<Item = &[T]> {
    items.chunks(max_rows_per_insert(cols_per_row))
}

/// Chunk IDs for `WHERE id IN (...)` clauses.
///
/// Each chunk contains at most 65,535 elements.
pub fn chunk_for_in_clause<T>(ids: &[T]) -> impl Iterator<Item = &[T]> {
    ids.chunks(MAX_IN_LIST_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_rows_basic() {
        assert_eq!(max_rows_per_insert(1), 65_535);
        assert_eq!(max_rows_per_insert(10), 6_553);
        assert_eq!(max_rows_per_insert(20), 3_276);
    }

    #[test]
    fn max_rows_large_column_count() {
        assert_eq!(max_rows_per_insert(65_535), 1);
        assert_eq!(max_rows_per_insert(100_000), 1);
    }

    #[test]
    fn max_rows_zero_columns_returns_one() {
        assert_eq!(max_rows_per_insert(0), 1);
    }

    #[test]
    fn chunk_for_insert_splits_correctly() {
        let items: Vec<u32> = (0..10_000).collect();
        let chunks: Vec<_> = chunk_for_insert(&items, 10).collect();
        // 10 cols -> 6553 rows per batch -> need 2 batches for 10k items
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 6_553);
        assert_eq!(chunks[1].len(), 3_447);
    }

    #[test]
    fn chunk_for_in_clause_single_batch_when_small() {
        let ids: Vec<u64> = (0..100).collect();
        let chunks: Vec<_> = chunk_for_in_clause(&ids).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 100);
    }

    #[test]
    fn chunk_for_in_clause_splits_large() {
        let ids: Vec<u64> = (0..130_000).collect();
        let chunks: Vec<_> = chunk_for_in_clause(&ids).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 65_535);
        assert_eq!(chunks[1].len(), 64_465);
    }
}
