//! Postgres-backed reserved-capital reader.

use sea_orm::DatabaseConnection;

use crate::{postgres::quant::capital_allocation, traits::ReservedCapitalRepository};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::Usd;

/// Postgres-backed reserved-capital reader.
pub struct PgReservedCapitalRepository {
    db: DatabaseConnection,
}

impl PgReservedCapitalRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ReservedCapitalRepository for PgReservedCapitalRepository {
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        capital_allocation::sum_reserved_usd(&self.db).await
    }
}
