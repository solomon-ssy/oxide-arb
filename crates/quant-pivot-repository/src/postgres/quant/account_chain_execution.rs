//! PostgreSQL account-chain execution ledger.

use quant_pivot_error::storage::{StorageError, entity::QUANT_ACCOUNT_CHAIN_EXECUTION};
use quant_pivot_models::{
    domain::quant::{
        AccountChainEventCursor, AccountChainExecutionInsertOutcome, NewAccountChainExecution,
    },
    entities::quant_account_chain_execution::{Column, Entity, Model},
    types::ExecutionAccountId,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect, TryInsertResult, sea_query::OnConflict,
};

use crate::traits::AccountChainExecutionRepository;

pub struct PgAccountChainExecutionRepository {
    db: DatabaseConnection,
}

impl PgAccountChainExecutionRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl AccountChainExecutionRepository for PgAccountChainExecutionRepository {
    async fn latest_cursor(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<AccountChainEventCursor>, StorageError> {
        let row = Entity::find()
            .filter(Column::ExecutionAccountId.eq(*execution_account_id))
            .order_by_desc(Column::BlockNumber)
            .order_by_desc(Column::TransactionIndex)
            .order_by_desc(Column::LogIndex)
            .limit(1)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        row.map(|row| {
            Ok(AccountChainEventCursor {
                block_number: u64::try_from(row.block_number).map_err(|error| {
                    StorageError::invariant_violation(
                        Some(QUANT_ACCOUNT_CHAIN_EXECUTION),
                        format!("negative persisted block number: {error}"),
                    )
                })?,
                transaction_index: u64::try_from(row.transaction_index).map_err(|error| {
                    StorageError::invariant_violation(
                        Some(QUANT_ACCOUNT_CHAIN_EXECUTION),
                        format!("negative persisted transaction index: {error}"),
                    )
                })?,
                log_index: u64::try_from(row.log_index).map_err(|error| {
                    StorageError::invariant_violation(
                        Some(QUANT_ACCOUNT_CHAIN_EXECUTION),
                        format!("negative persisted log index: {error}"),
                    )
                })?,
            })
        })
        .transpose()
    }

    async fn append(
        &self,
        executions: Vec<NewAccountChainExecution>,
    ) -> Result<AccountChainExecutionInsertOutcome, StorageError> {
        let mut inserted = 0_u64;
        let mut replayed = 0_u64;
        for execution in executions {
            let id = execution.account_chain_execution_id;
            let insert = Entity::insert(execution.clone().into_active_model())
                .on_conflict(
                    OnConflict::column(Column::AccountChainExecutionId)
                        .do_nothing()
                        .to_owned(),
                )
                .try_insert()
                .exec_without_returning(&self.db)
                .await
                .map_err(StorageError::from)?;
            match insert {
                TryInsertResult::Inserted(1) => inserted += 1,
                TryInsertResult::Conflicted | TryInsertResult::Inserted(0) => {
                    let stored = Entity::find_by_id(id)
                        .one(&self.db)
                        .await
                        .map_err(StorageError::from)?
                        .ok_or_else(|| {
                            StorageError::not_found(QUANT_ACCOUNT_CHAIN_EXECUTION, id)
                        })?;
                    if !matches_new(&stored, &execution) {
                        return Err(StorageError::state_conflict(
                            QUANT_ACCOUNT_CHAIN_EXECUTION,
                            Some(&id),
                            "source event replay changed account execution economics or provenance",
                        ));
                    }
                    replayed += 1;
                }
                TryInsertResult::Inserted(rows) => {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_ACCOUNT_CHAIN_EXECUTION),
                        format!("single account execution insert affected {rows} rows"),
                    ));
                }
                TryInsertResult::Empty => {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_ACCOUNT_CHAIN_EXECUTION),
                        "non-empty account execution insert produced no statement",
                    ));
                }
            }
        }
        Ok(AccountChainExecutionInsertOutcome { inserted, replayed })
    }
}

fn matches_new(stored: &Model, incoming: &NewAccountChainExecution) -> bool {
    stored.account_chain_execution_id == incoming.account_chain_execution_id
        && stored.execution_account_id == incoming.execution_account_id
        && stored.role == incoming.role
        && stored.chain_id == incoming.chain_id
        && stored.protocol_version == incoming.protocol_version
        && stored.exchange_address == incoming.exchange_address
        && stored.block_number == incoming.block_number
        && stored.block_hash == incoming.block_hash
        && stored.transaction_hash == incoming.transaction_hash
        && stored.transaction_index == incoming.transaction_index
        && stored.log_index == incoming.log_index
        && stored.order_id == incoming.order_id
        && stored.maker_address == incoming.maker_address
        && stored.taker_address == incoming.taker_address
        && stored.order_side == incoming.order_side
        && stored.order_token_id == incoming.order_token_id
        && stored.maker_amount_raw == incoming.maker_amount_raw
        && stored.taker_amount_raw == incoming.taker_amount_raw
        && stored.account_side == incoming.account_side
        && stored.account_token_id == incoming.account_token_id
        && stored.shares == incoming.shares
        && stored.principal_usd == incoming.principal_usd
        && stored.exact_fee_usd == incoming.exact_fee_usd
        && stored.builder_code == incoming.builder_code
        && stored.metadata == incoming.metadata
        && stored.source_event_hash == incoming.source_event_hash
        && stored.availability_policy_hash == incoming.availability_policy_hash
        && stored.observed_at == incoming.observed_at
        && stored.available_at == incoming.available_at
}
