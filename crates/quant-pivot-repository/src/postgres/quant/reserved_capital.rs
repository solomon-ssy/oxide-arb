//! Postgres-backed reserved-capital reader.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::Usd;
use sea_orm::DatabaseConnection;

use crate::{
    postgres::quant::capital_allocation::PgCapitalAllocationRepository,
    traits::ReservedCapitalRepository,
};

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
        PgCapitalAllocationRepository::sum_reserved_usd(&self.db).await
    }
}
