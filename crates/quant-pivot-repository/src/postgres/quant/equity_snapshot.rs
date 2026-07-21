//! Postgres-backed strategy-capital equity snapshot repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        pagination::{PageWindow, Paginated},
        quant::{
            EquitySnapshotInfo, EquitySnapshotQuery, NewEquitySnapshot, capital_drawdown, hwm_merge,
        },
    },
    entities::quant_equity_snapshot::{Column, Entity},
    types::EquitySnapshotId,
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{postgres::query::paginate_mapped, traits::EquitySnapshotRepository};

/// Postgres-backed strategy-capital equity history repository.
pub struct PgEquitySnapshotRepository {
    db: DatabaseConnection,
}

impl PgEquitySnapshotRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl EquitySnapshotRepository for PgEquitySnapshotRepository {
    async fn create(
        &self,
        snapshot: NewEquitySnapshot,
    ) -> Result<EquitySnapshotInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let inserted = insert_equity_snapshot_monotonic(&txn, snapshot).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn find_by_id(
        &self,
        id: &EquitySnapshotId,
    ) -> Result<Option<EquitySnapshotInfo>, StorageError> {
        Entity::find_by_id(id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest(&self) -> Result<Option<EquitySnapshotInfo>, StorageError> {
        Entity::find()
            .order_by_desc(Column::AsOf)
            .order_by_desc(Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_at_or_before(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<Option<EquitySnapshotInfo>, StorageError> {
        Entity::find()
            .filter(Column::AsOf.lte(as_of))
            .order_by_desc(Column::AsOf)
            .order_by_desc(Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: EquitySnapshotQuery,
    ) -> Result<Paginated<EquitySnapshotInfo>, StorageError> {
        paginate_mapped(
            Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::AsOf)
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}

pub(super) async fn insert_equity_snapshot_monotonic<C>(
    db: &C,
    mut snapshot: NewEquitySnapshot,
) -> Result<EquitySnapshotInfo, StorageError>
where
    C: ConnectionTrait,
{
    let previous = Entity::find()
        .filter(Column::AsOf.lte(snapshot.as_of))
        .order_by_desc(Column::AsOf)
        .order_by_desc(Column::CreatedAt)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?;

    snapshot.high_water_mark_usd = hwm_merge(
        previous.map(|row| row.high_water_mark_usd),
        snapshot.high_water_mark_usd,
        snapshot.capital_base_usd,
    );
    snapshot.drawdown_pct =
        capital_drawdown(snapshot.capital_base_usd, snapshot.high_water_mark_usd);

    Entity::insert(snapshot.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

fn page_condition(query: &EquitySnapshotQuery) -> Condition {
    Condition::all()
        .add_option(query.from.map(|from| Column::AsOf.gte(from)))
        .add_option(query.to.map(|to| Column::AsOf.lt(to)))
}
