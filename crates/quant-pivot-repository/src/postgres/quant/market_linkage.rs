//! Postgres-backed market-linkage ledger repository (append-only, bitemporal).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::MarketLinkageListQuery,
        data_plane::{DecisionBoundary, DecisionSource},
        pagination::{PageWindow, Paginated},
        quant::{LinkageOutcome, MarketLinkageInfo, NewMarketLinkage},
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_market_linkage::{Column, Entity, Model},
        quant_market_linkage_source::{
            ActiveModel, Column as QuantMarketLinkageSourceColumn,
            Entity as QuantMarketLinkageSourceEntity,
        },
    },
    enums::market::MarketStatus,
    types::{MarketId, MarketLinkageId},
};
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, EntityTrait,
    ExprTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Alias, Expr, Query},
};

use crate::{postgres::query::paginate_mapped, traits::MarketLinkageRepository};

/// Reduce rows already ordered `(market_id ASC, derived_at DESC, created_at
/// DESC, linkage_id DESC)` to the first (highest-ranked) row per market.
fn first_per_market(rows: Vec<Model>) -> Vec<MarketLinkageInfo> {
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

fn latest_linkage_condition() -> Expr {
    let newer = Alias::new("newer");
    let newer_col = |column| Expr::col((newer.clone(), column));
    let current_col = |column| Expr::col((Entity, column));
    let newer_rank = Condition::any()
        .add(newer_col(Column::DerivedAt).gt(current_col(Column::DerivedAt)))
        .add(
            Condition::all()
                .add(newer_col(Column::DerivedAt).eq(current_col(Column::DerivedAt)))
                .add(newer_col(Column::CreatedAt).gt(current_col(Column::CreatedAt))),
        )
        .add(
            Condition::all()
                .add(newer_col(Column::DerivedAt).eq(current_col(Column::DerivedAt)))
                .add(newer_col(Column::CreatedAt).eq(current_col(Column::CreatedAt)))
                .add(newer_col(Column::LinkageId).gt(current_col(Column::LinkageId))),
        );
    let mut query = Query::select();
    query
        .expr(Expr::value(1_i32))
        .from_as(Entity, newer.clone())
        .and_where(newer_col(Column::MarketId).eq(current_col(Column::MarketId)))
        .cond_where(newer_rank);
    Expr::exists(query.take()).not()
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

    async fn append_to(
        txn: &DatabaseTransaction,
        linkage: NewMarketLinkage,
    ) -> Result<MarketLinkageInfo, StorageError> {
        let content_hash = linkage.content_hash;
        let source_bindings = match linkage.outcome.clone() {
            LinkageOutcome::Resolved(binding) => binding.source_bindings,
            LinkageOutcome::Unresolved { .. } => Vec::new(),
        };
        // Idempotent append: a duplicate content hash is a resolver no-op, not
        // an error — the ledger row for that outcome already exists.
        Entity::insert(linkage.into_active_model())
            .on_conflict_do_nothing_on([Column::ContentHash])
            .exec(txn)
            .await
            .map_err(StorageError::from)?;
        let row = Entity::find()
            .filter(Column::ContentHash.eq(content_hash))
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or(StorageError::NotFound {
                entity: "quant_market_linkage",
                id: content_hash.to_string(),
            })?;
        for binding in source_bindings {
            QuantMarketLinkageSourceEntity::insert(ActiveModel {
                linkage_id: ActiveValue::Set(row.linkage_id),
                role: ActiveValue::Set(binding.role),
                source_id: ActiveValue::Set(binding.source_id),
                instrument_key: ActiveValue::Set(binding.instrument_key),
                binding_hash: ActiveValue::Set(binding.binding_hash),
                available_at: ActiveValue::Set(binding.available_at),
                created_at: ActiveValue::NotSet,
            })
            .on_conflict_do_nothing_on([
                QuantMarketLinkageSourceColumn::LinkageId,
                QuantMarketLinkageSourceColumn::Role,
                QuantMarketLinkageSourceColumn::SourceId,
                QuantMarketLinkageSourceColumn::InstrumentKey,
            ])
            .exec(txn)
            .await
            .map_err(StorageError::from)?;
        }
        Ok(row.into())
    }
}

#[async_trait::async_trait]
impl MarketLinkageRepository for PgMarketLinkageRepository {
    async fn append(&self, linkage: NewMarketLinkage) -> Result<MarketLinkageInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = Self::append_to(&txn, linkage).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(row)
    }

    async fn append_batch(
        &self,
        linkages: Vec<NewMarketLinkage>,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let mut rows = Vec::with_capacity(linkages.len());
        for linkage in linkages {
            rows.push(Self::append_to(&txn, linkage).await?);
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(rows)
    }

    async fn valid_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Entity::find()
            .filter(Column::MarketId.eq(market_id.clone()))
            .filter(Column::DerivedAt.lte(boundary.cutoff_for(DecisionSource::Linkage)))
            .filter(Column::CreatedAt.lte(boundary.decision_at()))
            .order_by_desc(Column::DerivedAt)
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::LinkageId)
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
        let rows = Entity::find()
            .filter(Column::MarketId.is_in(market_ids.to_vec()))
            .filter(Column::DerivedAt.lte(boundary.cutoff_for(DecisionSource::Linkage)))
            .filter(Column::CreatedAt.lte(boundary.decision_at()))
            .order_by_asc(Column::MarketId)
            .order_by_desc(Column::DerivedAt)
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(first_per_market(rows))
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
        let rows = Entity::find()
            .filter(Column::MarketId.is_in(market_ids.to_vec()))
            .order_by_asc(Column::MarketId)
            .order_by_desc(Column::DerivedAt)
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(first_per_market(rows))
    }

    async fn latest_for_active_markets(&self) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        let market_ids = MarketEntity::find()
            .select_only()
            .column(MarketColumn::MarketId)
            .filter(MarketColumn::Status.eq(MarketStatus::Active))
            .into_tuple::<MarketId>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        self.latest_for_markets(&market_ids).await
    }

    async fn ledger_for_markets(
        &self,
        market_ids: &[MarketId],
        end_boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        Entity::find()
            .filter(Column::MarketId.is_in(market_ids.to_vec()))
            .filter(Column::DerivedAt.lte(end_boundary.cutoff_for(DecisionSource::Linkage)))
            .filter(Column::CreatedAt.lte(end_boundary.decision_at()))
            .order_by_asc(Column::MarketId)
            .order_by_asc(Column::DerivedAt)
            .order_by_asc(Column::CreatedAt)
            .order_by_asc(Column::LinkageId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        linkage_id: &MarketLinkageId,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Entity::find_by_id(*linkage_id)
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
            .add_option(query.status.map(|status| Column::Status.eq(status)))
            .add_option(query.family.map(|family| Column::DomainFamily.eq(family)))
            .add_option(
                query
                    .market_id
                    .clone()
                    .map(|market_id| Column::MarketId.eq(market_id)),
            )
            .add_option(query.from.map(|from| Column::DerivedAt.gte(from)))
            .add_option(query.to.map(|to| Column::DerivedAt.lt(to)));
        if query.latest_only {
            condition = condition.add(latest_linkage_condition());
        }
        let select = Entity::find()
            .filter(condition)
            .order_by_desc(Column::DerivedAt)
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::LinkageId);
        paginate_mapped(select, &self.db, PageWindow::from_query(&query), Into::into).await
    }
}
