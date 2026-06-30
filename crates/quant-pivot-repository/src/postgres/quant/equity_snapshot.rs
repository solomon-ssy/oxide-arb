//! Postgres-backed strategy-capital equity snapshot repository.

use crate::{postgres::query::paginate_mapped, traits::EquitySnapshotRepository};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        EquitySnapshotInfo, EquitySnapshotQuery, NewEquitySnapshot, Paginated, capital_drawdown,
        hwm_merge,
    },
    entities::quant_equity_snapshot,
    types::EquitySnapshotId,
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

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
        quant_equity_snapshot::Entity::find_by_id(id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest(&self) -> Result<Option<EquitySnapshotInfo>, StorageError> {
        quant_equity_snapshot::Entity::find()
            .order_by_desc(quant_equity_snapshot::Column::AsOf)
            .order_by_desc(quant_equity_snapshot::Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_at_or_before(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<Option<EquitySnapshotInfo>, StorageError> {
        quant_equity_snapshot::Entity::find()
            .filter(quant_equity_snapshot::Column::AsOf.lte(as_of))
            .order_by_desc(quant_equity_snapshot::Column::AsOf)
            .order_by_desc(quant_equity_snapshot::Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: EquitySnapshotQuery,
    ) -> Result<Paginated<EquitySnapshotInfo>, StorageError> {
        let query = query.normalized();
        paginate_mapped(
            quant_equity_snapshot::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_equity_snapshot::Column::AsOf)
                .order_by_desc(quant_equity_snapshot::Column::CreatedAt),
            &self.db,
            &query.page,
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
    let previous = quant_equity_snapshot::Entity::find()
        .filter(quant_equity_snapshot::Column::AsOf.lte(snapshot.as_of))
        .order_by_desc(quant_equity_snapshot::Column::AsOf)
        .order_by_desc(quant_equity_snapshot::Column::CreatedAt)
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

    quant_equity_snapshot::Entity::insert(snapshot.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

fn page_condition(query: &EquitySnapshotQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .from
                .map(|from| quant_equity_snapshot::Column::AsOf.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_equity_snapshot::Column::AsOf.lt(to)),
        )
}
