use super::orm::{DatabaseConnection, EntityTrait, IntoActiveModel, TransactionTrait};
use crate::traits::RiskAuditRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{domain::NewRiskAuditEvent, entities::risk_audit_event::Entity};

pub struct PgRiskAuditRepository {
    db: DatabaseConnection,
}

impl PgRiskAuditRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RiskAuditRepository for PgRiskAuditRepository {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError> {
        Entity::insert(event.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn create_batch(&self, events: Vec<NewRiskAuditEvent>) -> Result<(), StorageError> {
        if events.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let models: Vec<_> = events
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect();
        Entity::insert_many(models)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}
