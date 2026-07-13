//! Postgres-backed market-linkage ledger repository (append-only, bitemporal).

use crate::{postgres::query::paginate_mapped, traits::MarketLinkageRepository};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionSource, MarketLinkageInfo, MarketLinkageListQuery,
        NewMarketLinkage, PageWindow, Paginated,
    },
    entities::quant_market_linkage,
    types::{MarketId, MarketLinkageId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, sea_query::OnConflict,
};

/// Reduce rows already ordered `(market_id ASC, derived_at DESC, created_at
/// DESC, linkage_id DESC)` to the first (highest-ranked) row per market.
fn reduce_to_first_per_market(rows: Vec<quant_market_linkage::Model>) -> Vec<MarketLinkageInfo> {
    let mut kept: Vec<MarketLinkageInfo> = Vec::new();
    for row in rows {
        if kept
            .last()
            .is_none_or(|previous| previous.market_id != row.market_id)
        {
            kept.push(row.into());
        }
    }
    kept
}

/// Postgres-backed append-only linkage ledger.
pub struct PgMarketLinkageRepository {
    db: DatabaseConnection,
}

impl PgMarketLinkageRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl MarketLinkageRepository for PgMarketLinkageRepository {
    async fn append(&self, linkage: NewMarketLinkage) -> Result<MarketLinkageInfo, StorageError> {
        let content_hash = linkage.content_hash.clone();
        // Idempotent append: a duplicate content hash is a resolver no-op, not
        // an error — the ledger row for that outcome already exists.
        quant_market_linkage::Entity::insert(linkage.into_active_model())
            .on_conflict(
                OnConflict::column(quant_market_linkage::Column::ContentHash)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        let row = quant_market_linkage::Entity::find()
            .filter(quant_market_linkage::Column::ContentHash.eq(content_hash.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        row.map(Into::into).ok_or(StorageError::NotFound {
            entity: "quant_market_linkage",
            id: content_hash.to_string(),
        })
    }

    async fn valid_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        quant_market_linkage::Entity::find()
            .filter(quant_market_linkage::Column::MarketId.eq(market_id.clone()))
            .filter(
                quant_market_linkage::Column::DerivedAt
                    .lte(boundary.cutoff_for(DecisionSource::Linkage)),
            )
            .filter(quant_market_linkage::Column::CreatedAt.lte(boundary.decision_at()))
            .order_by_desc(quant_market_linkage::Column::DerivedAt)
            .order_by_desc(quant_market_linkage::Column::CreatedAt)
            .order_by_desc(quant_market_linkage::Column::LinkageId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn valid_at_for_markets(
        &self,
        market_ids: &[MarketId],
        boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Apply the same effective <= source_cutoff and available <= decision_at
        // constraints as `valid_at`; this is its batch form, not a filtered
        // `latest_for_markets` read.
        let rows = quant_market_linkage::Entity::find()
            .filter(quant_market_linkage::Column::MarketId.is_in(market_ids.to_vec()))
            .filter(
                quant_market_linkage::Column::DerivedAt
                    .lte(boundary.cutoff_for(DecisionSource::Linkage)),
            )
            .filter(quant_market_linkage::Column::CreatedAt.lte(boundary.decision_at()))
            .order_by_asc(quant_market_linkage::Column::MarketId)
            .order_by_desc(quant_market_linkage::Column::DerivedAt)
            .order_by_desc(quant_market_linkage::Column::CreatedAt)
            .order_by_desc(quant_market_linkage::Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(reduce_to_first_per_market(rows))
    }

    async fn latest_for_markets(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        // DISTINCT ON is not portable through sea_query; the window is small
        // (bounded by the caller's market set), so reduce in memory over the
        // same stable ordering `valid_at` uses.
        let rows = quant_market_linkage::Entity::find()
            .filter(quant_market_linkage::Column::MarketId.is_in(market_ids.to_vec()))
            .order_by_asc(quant_market_linkage::Column::MarketId)
            .order_by_desc(quant_market_linkage::Column::DerivedAt)
            .order_by_desc(quant_market_linkage::Column::CreatedAt)
            .order_by_desc(quant_market_linkage::Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(reduce_to_first_per_market(rows))
    }

    async fn ledger_for_markets(
        &self,
        market_ids: &[MarketId],
        end_boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        quant_market_linkage::Entity::find()
            .filter(quant_market_linkage::Column::MarketId.is_in(market_ids.to_vec()))
            .filter(
                quant_market_linkage::Column::DerivedAt
                    .lte(end_boundary.cutoff_for(DecisionSource::Linkage)),
            )
            .filter(quant_market_linkage::Column::CreatedAt.lte(end_boundary.decision_at()))
            .order_by_asc(quant_market_linkage::Column::MarketId)
            .order_by_asc(quant_market_linkage::Column::DerivedAt)
            .order_by_asc(quant_market_linkage::Column::CreatedAt)
            .order_by_asc(quant_market_linkage::Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        linkage_id: &MarketLinkageId,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        quant_market_linkage::Entity::find_by_id(linkage_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: MarketLinkageListQuery,
    ) -> Result<Paginated<MarketLinkageInfo>, StorageError> {
        let mut condition = Condition::all()
            .add_option(
                query
                    .status
                    .map(|status| quant_market_linkage::Column::Status.eq(status)),
            )
            .add_option(
                query
                    .family
                    .map(|family| quant_market_linkage::Column::DomainFamily.eq(family)),
            )
            .add_option(
                query
                    .market_id
                    .clone()
                    .map(|market_id| quant_market_linkage::Column::MarketId.eq(market_id)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_market_linkage::Column::DerivedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_market_linkage::Column::DerivedAt.lt(to)),
            );
        if query.latest_only {
            // Correlated "no newer row for the same market" guard keeps the
            // catalog to one row per market while staying index-friendly.
            condition = condition.add(sea_orm::sea_query::Expr::cust(
                "NOT EXISTS (SELECT 1 FROM quant_market_linkage newer \
                 WHERE newer.market_id = quant_market_linkage.market_id \
                 AND (newer.derived_at, newer.created_at, newer.linkage_id) > \
                     (quant_market_linkage.derived_at, quant_market_linkage.created_at, \
                      quant_market_linkage.linkage_id))",
            ));
        }
        let select = quant_market_linkage::Entity::find()
            .filter(condition)
            .order_by_desc(quant_market_linkage::Column::DerivedAt)
            .order_by_desc(quant_market_linkage::Column::CreatedAt)
            .order_by_desc(quant_market_linkage::Column::LinkageId);
        paginate_mapped(select, &self.db, PageWindow::from_query(&query), Into::into).await
    }
}
