use super::orm::{DatabaseConnection, EntityTrait, IntoActiveModel};
use crate::traits::ReconciliationRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{domain::NewReconciliationReport, entities::reconciliation_report::Entity};

pub struct PgReconciliationRepository {
    db: DatabaseConnection,
}

impl PgReconciliationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl ReconciliationRepository for PgReconciliationRepository {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError> {
        Entity::insert(report.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
