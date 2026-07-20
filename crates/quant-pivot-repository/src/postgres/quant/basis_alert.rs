//! Postgres-backed basis-cross-check exceedance ledger (append-only, plus the
//! single governed `acknowledge` mutation).

use std::collections::HashSet;

use chrono::Utc;

use crate::{
    batch::chunk_for_in_clause,
    postgres::{query::paginate_mapped, write::insert_many_chunked},
    traits::BasisAlertRepository,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BasisAlertInfo, BasisAlertListQuery, NewBasisAlert, PageWindow, Paginated},
    entities::quant_basis_alert,
    types::{BasisAlertId, MarketId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
};

/// Postgres-backed append-only basis-alert ledger.
pub struct PgBasisAlertRepository {
    db: DatabaseConnection,
}

impl PgBasisAlertRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl BasisAlertRepository for PgBasisAlertRepository {
    async fn record(&self, alert: NewBasisAlert) -> Result<BasisAlertInfo, StorageError> {
        quant_basis_alert::Entity::insert(alert.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<BasisAlertInfo>, StorageError> {
        quant_basis_alert::Entity::find()
            .filter(quant_basis_alert::Column::MarketId.eq(market_id.clone()))
            .order_by_desc(quant_basis_alert::Column::AsOf)
            .order_by_desc(quant_basis_alert::Column::AlertId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_for_markets(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<BasisAlertInfo>, StorageError> {
        let mut seen = HashSet::with_capacity(market_ids.len());
        let unique_market_ids = market_ids
            .iter()
            .filter(|market_id| seen.insert((*market_id).clone()))
            .cloned()
            .collect::<Vec<_>>();
        let mut alerts = Vec::with_capacity(unique_market_ids.len());
        for chunk in chunk_for_in_clause(&unique_market_ids) {
            let rows = quant_basis_alert::Entity::find()
                .filter(quant_basis_alert::Column::MarketId.is_in(chunk.iter().cloned()))
                .distinct_on([(
                    quant_basis_alert::Entity,
                    quant_basis_alert::Column::MarketId,
                )])
                .order_by_asc(quant_basis_alert::Column::MarketId)
                .order_by_desc(quant_basis_alert::Column::AsOf)
                .order_by_desc(quant_basis_alert::Column::AlertId)
                .all(&self.db)
                .await
                .map_err(StorageError::from)?;
            alerts.extend(rows.into_iter().map(Into::into));
        }
        Ok(alerts)
    }

    async fn record_many(&self, alerts: Vec<NewBasisAlert>) -> Result<(), StorageError> {
        insert_many_chunked::<quant_basis_alert::Entity, _>(&self.db, alerts)
            .await
            .map(drop)
    }

    async fn page(
        &self,
        query: BasisAlertListQuery,
    ) -> Result<Paginated<BasisAlertInfo>, StorageError> {
        let mut condition = Condition::all()
            .add_option(
                query
                    .market_id
                    .clone()
                    .map(|market_id| quant_basis_alert::Column::MarketId.eq(market_id)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_basis_alert::Column::AsOf.gte(from)),
            )
            .add_option(query.to.map(|to| quant_basis_alert::Column::AsOf.lt(to)));
        if query.open_only {
            condition = condition.add(quant_basis_alert::Column::Acknowledged.eq(false));
        }
        let select = quant_basis_alert::Entity::find()
            .filter(condition)
            .order_by_desc(quant_basis_alert::Column::AsOf)
            .order_by_desc(quant_basis_alert::Column::AlertId);
        paginate_mapped(select, &self.db, PageWindow::from_query(&query), Into::into).await
    }

    async fn acknowledge(
        &self,
        alert_id: &BasisAlertId,
        actor: String,
    ) -> Result<BasisAlertInfo, StorageError> {
        let Some(row) = quant_basis_alert::Entity::find_by_id(alert_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::NotFound {
                entity: "quant_basis_alert",
                id: alert_id.to_string(),
            });
        };
        if row.acknowledged {
            // Idempotent: the first acknowledgement wins, a replay is a no-op.
            return Ok(row.into());
        }
        let mut active = row.into_active_model();
        active.acknowledged = ActiveValue::Set(true);
        active.acknowledged_at = ActiveValue::Set(Some(Utc::now()));
        active.acknowledged_by = ActiveValue::Set(Some(actor));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
