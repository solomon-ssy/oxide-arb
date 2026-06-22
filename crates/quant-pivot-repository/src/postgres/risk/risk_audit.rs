use crate::traits::RiskAuditRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewRiskAuditEvent, PageRequest, Paginated, RiskAuditEventInfo, TimeWindow},
    entities::risk_audit_event::{Column, Entity},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

pub struct PgRiskAuditRepository {
    db: DatabaseConnection,
}

impl PgRiskAuditRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

/// Append one audit event on any connection or transaction handle.
pub(crate) async fn do_create(
    db: &impl ConnectionTrait,
    event: NewRiskAuditEvent,
) -> Result<(), StorageError> {
    Entity::insert(event.into_active_model())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_find_between(
    db: &impl ConnectionTrait,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<RiskAuditEventInfo>, StorageError> {
    Entity::find()
        .filter(Column::CreatedAt.gte(from))
        .filter(Column::CreatedAt.lt(to))
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::Id)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn do_find_between_page(
    db: &impl ConnectionTrait,
    window: TimeWindow,
    page: PageRequest,
) -> Result<Paginated<RiskAuditEventInfo>, StorageError> {
    let window_page = page.normalized();
    let condition = || {
        Entity::find()
            .filter(Column::CreatedAt.gte(window.from))
            .filter(Column::CreatedAt.lt(window.to))
    };
    let total = condition().count(db).await.map_err(StorageError::from)?;
    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), 0, &window_page));
    }
    let rows = condition()
        .order_by_desc(Column::CreatedAt)
        .order_by_desc(Column::Id)
        .offset(window_page.offset())
        .limit(window_page.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = rows.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window_page))
}

#[async_trait::async_trait]
impl RiskAuditRepository for PgRiskAuditRepository {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError> {
        do_create(&self.db, event).await
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

    async fn find_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RiskAuditEventInfo>, StorageError> {
        do_find_between(&self.db, from, to).await
    }

    async fn find_between_page(
        &self,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<RiskAuditEventInfo>, StorageError> {
        do_find_between_page(&self.db, window, page).await
    }
}
