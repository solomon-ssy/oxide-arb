//! Postgres-backed read-only portfolio-plan repository.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::PortfolioPlanInfo, entities::quant_portfolio_plan::Entity,
    types::PortfolioPlanId,
};
use sea_orm::{DatabaseConnection, EntityTrait};

use crate::traits::PortfolioPlanRepository;

/// Postgres-backed read-only portfolio-plan repository.
pub struct PgPortfolioPlanRepository {
    db: DatabaseConnection,
}

impl PgPortfolioPlanRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl PortfolioPlanRepository for PgPortfolioPlanRepository {
    async fn find_by_id(
        &self,
        portfolio_plan_id: &PortfolioPlanId,
    ) -> Result<Option<PortfolioPlanInfo>, StorageError> {
        Entity::find_by_id(*portfolio_plan_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
