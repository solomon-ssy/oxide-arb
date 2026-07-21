//! Exact namespace-only Redis cleanup for guarded preproduction reset.

use std::collections::BTreeSet;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::RedisConfig;

use super::connect_pool;

const PREPRODUCTION_DATABASE: u8 = 0;
const PREPRODUCTION_KEY_PREFIX: &str = "qp:";
const SCAN_COUNT: u64 = 1_000;
const MAX_UNLINK_PASSES: usize = 16;

pub async fn count_preproduction_namespace(config: &RedisConfig) -> Result<u64, StorageError> {
    validate_preproduction_target(config)?;
    let pool = connect_pool(config).await?;
    let mut connection = pool.get().await?;
    let mut cursor = 0_u64;
    let mut unique_keys = BTreeSet::new();
    loop {
        let (next, keys) = ::redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("qp:*")
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async::<(u64, Vec<Vec<u8>>)>(&mut connection)
            .await
            .map_err(StorageError::Redis)?;
        unique_keys.extend(keys);
        cursor = next;
        if cursor == 0 {
            return key_count(unique_keys.len());
        }
    }
}

pub async fn unlink_preproduction_namespace(config: &RedisConfig) -> Result<u64, StorageError> {
    validate_preproduction_target(config)?;
    let pool = connect_pool(config).await?;
    let mut deleted = 0_u64;
    let mut consecutive_empty_passes = 0_u8;
    for _ in 0..MAX_UNLINK_PASSES {
        let mut connection = pool.get().await?;
        let mut cursor = 0_u64;
        let mut observed = BTreeSet::new();
        loop {
            let (next, keys) = ::redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("qp:*")
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async::<(u64, Vec<Vec<u8>>)>(&mut connection)
                .await
                .map_err(StorageError::Redis)?;
            let unique_batch = keys
                .into_iter()
                .filter(|key| observed.insert(key.clone()))
                .collect::<Vec<_>>();
            if !unique_batch.is_empty() {
                let unlinked = ::redis::cmd("UNLINK")
                    .arg(unique_batch)
                    .query_async::<u64>(&mut connection)
                    .await
                    .map_err(StorageError::Redis)?;
                deleted = deleted.saturating_add(unlinked);
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        if observed.is_empty() {
            consecutive_empty_passes = consecutive_empty_passes.saturating_add(1);
            if consecutive_empty_passes == 2 {
                return Ok(deleted);
            }
            tokio::task::yield_now().await;
        } else {
            consecutive_empty_passes = 0;
        }
    }
    Err(StorageError::state_conflict(
        "redis_preproduction_namespace",
        Some(PREPRODUCTION_KEY_PREFIX),
        "qp:* keys are still being created; stop project-owned processes before reset",
    ))
}

fn validate_preproduction_target(config: &RedisConfig) -> Result<(), StorageError> {
    if config.database != PREPRODUCTION_DATABASE || config.key_prefix != PREPRODUCTION_KEY_PREFIX {
        return Err(StorageError::state_conflict(
            "redis_preproduction_namespace",
            Some(&config.key_prefix),
            "reset only permits Redis DB0 with the exact non-empty `qp:` namespace",
        ));
    }
    Ok(())
}

fn key_count(value: usize) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|error| {
        StorageError::invariant_violation(
            Some("redis_preproduction_namespace"),
            format!("Redis SCAN batch size overflow: {error}"),
        )
    })
}
