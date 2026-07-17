use crate::postgres::primitives;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::ENTRY_CONDITION_INPUT_CHANNEL;
use sea_orm::ConnectionTrait;

/// Transactional wake hint for condition workers. `PostgreSQL` delivers NOTIFY
/// only after commit, so a rolled-back projection is never observable.
pub async fn notify_input_change(
    db: &impl ConnectionTrait,
    input_kind: &'static str,
) -> Result<(), StorageError> {
    primitives::notify(db, ENTRY_CONDITION_INPUT_CHANNEL, input_kind).await
}
