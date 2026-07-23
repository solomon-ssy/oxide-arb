//! `PostgreSQL` execution-account identity repository.

use quant_pivot_error::storage::{StorageError, entity::QUANT_EXECUTION_ACCOUNT};
use quant_pivot_models::{
    domain::quant::{ExecutionAccountInfo, NewExecutionAccount},
    entities::quant_execution_account::{Column, Entity},
    types::ExecutionAccountId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel, sea_query::OnConflict};

use crate::{postgres::error, traits::ExecutionAccountRepository};

pub struct PgExecutionAccountRepository {
    db: DatabaseConnection,
}

impl PgExecutionAccountRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExecutionAccountRepository for PgExecutionAccountRepository {
    async fn ensure(
        &self,
        account: NewExecutionAccount,
    ) -> Result<ExecutionAccountInfo, StorageError> {
        let expected = account.clone();
        Entity::insert(account.into_active_model())
            .on_conflict(
                OnConflict::column(Column::ExecutionAccountId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let stored = Entity::find_by_id(expected.execution_account_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(QUANT_EXECUTION_ACCOUNT, expected.execution_account_id)
            })?;
        if stored.chain_id != expected.chain_id
            || stored.funder_address != expected.funder_address
            || stored.wallet_kind != expected.wallet_kind
            || stored.owner_address != expected.owner_address
            || stored.controller_address != expected.controller_address
            || stored.wallet_factory_address != expected.wallet_factory_address
            || stored.wallet_implementation_code_hash != expected.wallet_implementation_code_hash
            || stored.identity_digest != expected.identity_digest
        {
            return Err(error::state_conflict(
                QUANT_EXECUTION_ACCOUNT,
                Some(expected.execution_account_id),
                "content-addressed execution account identity mismatch",
            ));
        }
        Ok(stored.into())
    }

    async fn find_by_id(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<ExecutionAccountInfo>, StorageError> {
        Entity::find_by_id(*execution_account_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
