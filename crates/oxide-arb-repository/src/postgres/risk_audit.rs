use crate::traits::RiskAuditRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewRiskAuditEvent;
use oxide_arb_models::entities::risk_audit_event::Entity;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

pub struct PgRiskAuditRepository {
    db: DatabaseConnection,
}

impl PgRiskAuditRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl RiskAuditRepository for PgRiskAuditRepository {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError> {
        Entity::insert(event.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
