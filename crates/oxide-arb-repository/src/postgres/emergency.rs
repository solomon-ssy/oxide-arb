use crate::traits::EmergencyRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewEmergencySnapshot;
use oxide_arb_models::entities::emergency_snapshot::Entity;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

pub struct PgEmergencyRepository {
    db: DatabaseConnection,
}

impl PgEmergencyRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl EmergencyRepository for PgEmergencyRepository {
    async fn create(&self, snapshot: NewEmergencySnapshot) -> Result<(), StorageError> {
        Entity::insert(snapshot.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
