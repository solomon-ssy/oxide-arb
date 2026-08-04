//! Bounded input batching for native `ClickHouse` array predicates.

use quant_pivot_error::storage::StorageError;
use quant_pivot_storage::clickhouse::ClickHouseQueryLimits;

// `clickhouse::Query::bind` renders array values into the SQL request body.
// Keep the rendered list far below ClickHouse's default 256 KiB query limit,
// leaving room for the selected field projection and predicate text. Four
// rendered bytes per source byte is a conservative bound for escaped strings.
const INLINE_LIST_BYTES: usize = 96 * 1_024;
const INLINE_LIST_ITEMS: usize = 500;
const INLINE_VALUE_OVERHEAD: usize = 3;
pub const UUID_INLINE_BYTES: usize = 36;

pub fn canonical_values<T>(mut values: Vec<T>) -> Vec<T>
where
    T: Ord,
{
    values.sort_unstable();
    values.dedup();
    values
}

pub fn canonical_by<T, K>(mut values: Vec<T>, mut key: impl FnMut(&T) -> K) -> Vec<T>
where
    K: Ord,
{
    values.sort_unstable_by_key(&mut key);
    values.dedup_by(|left, right| key(left) == key(right));
    values
}

pub fn query_chunks<'a, T>(
    values: &'a [T],
    encoded_len: impl Fn(&T) -> usize,
    entity: &'static str,
) -> Result<Vec<&'a [T]>, StorageError> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut rendered_bytes = 0usize;

    for (index, value) in values.iter().enumerate() {
        let value_bytes = encoded_len(value)
            .saturating_mul(4)
            .saturating_add(INLINE_VALUE_OVERHEAD);
        if value_bytes > INLINE_LIST_BYTES {
            return Err(StorageError::invariant_violation(
                Some(entity),
                format!(
                    "one ClickHouse predicate value requires {value_bytes} rendered bytes; maximum is {INLINE_LIST_BYTES}"
                ),
            ));
        }
        let chunk_items = index.saturating_sub(start);
        if chunk_items > 0
            && (chunk_items >= INLINE_LIST_ITEMS
                || rendered_bytes.saturating_add(value_bytes) > INLINE_LIST_BYTES)
        {
            chunks.push(&values[start..index]);
            start = index;
            rendered_bytes = 0;
        }
        rendered_bytes = rendered_bytes.saturating_add(value_bytes);
    }
    if start < values.len() {
        chunks.push(&values[start..]);
    }
    Ok(chunks)
}

pub fn extend_rows<T>(
    rows: &mut Vec<T>,
    page: Vec<T>,
    limits: ClickHouseQueryLimits,
    entity: &'static str,
) -> Result<(), StorageError> {
    let total = rows.len().checked_add(page.len()).ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity),
            "ClickHouse batched result row count overflowed usize",
        )
    })?;
    let total = u64::try_from(total).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity),
            format!("ClickHouse batched result row count is not representable: {error}"),
        )
    })?;
    if total > limits.max_result_rows() {
        return Err(StorageError::invariant_violation(
            Some(entity),
            format!(
                "ClickHouse batched result contains {total} rows; governed maximum is {}",
                limits.max_result_rows()
            ),
        ));
    }
    rows.extend(page);
    Ok(())
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_storage::clickhouse::ClickHouseQueryLimits;

    use super::{INLINE_LIST_BYTES, canonical_by, canonical_values, extend_rows, query_chunks};

    #[test]
    fn chunks_bound_payload() -> Result<(), StorageError> {
        let values = (0..2_500)
            .map(|index| format!("{index:036}"))
            .collect::<Vec<_>>();
        let chunks = query_chunks(&values, String::len, "test")?;

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| {
            chunk.iter().map(|value| value.len() * 4 + 3).sum::<usize>() <= INLINE_LIST_BYTES
        }));
        assert_eq!(chunks.iter().map(|chunk| chunk.len()).sum::<usize>(), 2_500);
        Ok(())
    }

    #[test]
    fn canonicalization_deduplicates() {
        assert_eq!(canonical_values(vec![3, 1, 2, 1]), vec![1, 2, 3]);
        assert_eq!(
            canonical_by(vec![(3, 'c'), (1, 'a'), (1, 'z')], |value| value.0),
            vec![(1, 'a'), (3, 'c')]
        );
    }

    #[test]
    fn merge_enforces_budget() {
        const LIMITS: ClickHouseQueryLimits = ClickHouseQueryLimits::new("test", 2, 1_024);
        let mut rows = vec![1];
        assert!(extend_rows(&mut rows, vec![2, 3], LIMITS, "test").is_err());
        assert_eq!(rows, vec![1]);
    }
}
