use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::ENTRY_CONDITION_INPUT_CHANNEL;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

/// Transactional wake hint for condition workers. `PostgreSQL` delivers NOTIFY
/// only after commit, so a rolled-back projection is never observable.
pub async fn notify_input_change(
    db: &impl ConnectionTrait,
    input_kind: &'static str,
) -> Result<(), StorageError> {
    db.execute(Statement::from_sql_and_values(
        DbBackend::Postgres,
        format!("SELECT pg_notify('{ENTRY_CONDITION_INPUT_CHANNEL}', $1)"),
        [input_kind.into()],
    ))
    .await
    .map_err(StorageError::from)?;
    Ok(())
}
