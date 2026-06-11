use crate::{
    postgres::bind_limit::{IN_LIST_CHUNK, max_rows_per_insert},
    traits::MarketRepository,
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        MarketInfo, MarketPageQuery, MarketPitSnapshotInfo, NewMarketPitSnapshot, Paginated,
        UpsertMarket,
    },
    entities::{
        market::{
            ActiveModel as MarketActiveModel, Column as MarketColumn, Entity as MarketEntity,
        },
        market_pit_snapshot::{
            ActiveModel as MarketPitSnapshotActiveModel, Column as MarketPitSnapshotColumn,
            Entity as MarketPitSnapshotEntity,
        },
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, MarketPitSnapshotId, TokenId},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, Iterable, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
    sea_query::{
        Condition, Expr, OnConflict,
        extension::postgres::{PgBinOper, PgExpr},
    },
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct PgMarketRepository {
    db: DatabaseConnection,
}

impl PgMarketRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgMarketRepositoryTxn<'_> {
        PgMarketRepositoryTxn { txn }
    }
}

pub struct PgMarketRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_find_by_id(
    db: &impl ConnectionTrait,
    id: &MarketId,
) -> Result<Option<Arc<MarketInfo>>, StorageError> {
    MarketEntity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(|model| Arc::new(model.into())))
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Arc<[MarketInfo]>, StorageError> {
    MarketEntity::find()
        .filter(MarketColumn::Status.eq(MarketStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect::<Vec<_>>().into())
}

async fn do_find_by_ids(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
    let mut markets = Vec::with_capacity(ids.len());
    // Chunk the IN list to stay under the Postgres bind-parameter limit.
    for chunk in ids.chunks(IN_LIST_CHUNK) {
        let rows = MarketEntity::find()
            .filter(MarketColumn::MarketId.is_in(chunk.iter().map(MarketId::as_str)))
            .all(db)
            .await
            .map_err(StorageError::from)?;
        markets.extend(rows.into_iter().map(|model| Arc::new(model.into())));
    }
    Ok(markets)
}

async fn do_find_by_event(
    db: &impl ConnectionTrait,
    event_id: &str,
) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
    MarketEntity::find()
        .filter(MarketColumn::EventId.eq(event_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(|model| Arc::new(model.into())).collect())
}

async fn do_find_endgame_candidates(
    db: &impl ConnectionTrait,
    before_deadline: DateTime<Utc>,
) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
    MarketEntity::find()
        .filter(MarketColumn::Status.eq(MarketStatus::Active))
        .filter(MarketColumn::EndDate.is_not_null())
        .filter(MarketColumn::EndDate.lte(before_deadline))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(|model| Arc::new(model.into())).collect())
}

async fn do_find_existing_ids(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<HashSet<String>, StorageError> {
    let mut existing = HashSet::with_capacity(ids.len());
    // Chunk the IN list to stay under the Postgres bind-parameter limit.
    for chunk in ids.chunks(IN_LIST_CHUNK) {
        let rows = MarketEntity::find()
            .filter(MarketColumn::MarketId.is_in(chunk.iter().map(MarketId::as_str)))
            .select_only()
            .column(MarketColumn::MarketId)
            .into_tuple::<String>()
            .all(db)
            .await?;
        existing.extend(rows);
    }
    Ok(existing)
}

/// `ON CONFLICT (market_id) DO UPDATE` clause shared by single and batch upserts.
fn market_upsert_on_conflict() -> OnConflict {
    OnConflict::column(MarketColumn::MarketId)
        .update_columns([
            MarketColumn::EventId,
            MarketColumn::Question,
            MarketColumn::Slug,
            MarketColumn::Categories,
            MarketColumn::Status,
            MarketColumn::YesTokenId,
            MarketColumn::NoTokenId,
            MarketColumn::TickSize,
            MarketColumn::NegRisk,
            MarketColumn::Outcome,
            MarketColumn::EndDate,
            MarketColumn::ResolvedAt,
            MarketColumn::FeesEnabled,
            MarketColumn::FeeRate,
            MarketColumn::FeeExponent,
            MarketColumn::FeeTakerOnly,
            MarketColumn::FeeRebateRate,
            MarketColumn::FeeSource,
            MarketColumn::FeeObservedAt,
        ])
        .to_owned()
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    dto: UpsertMarket,
) -> Result<Arc<MarketInfo>, StorageError> {
    let am: MarketActiveModel = dto.into_active_model();
    let model = MarketEntity::insert(am)
        .on_conflict(market_upsert_on_conflict())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    let market = Arc::new(model.into());
    write_market_pit_snapshot_if_changed(db, &market).await?;
    Ok(market)
}

async fn do_upsert_batch(
    db: &impl ConnectionTrait,
    dtos: Vec<UpsertMarket>,
) -> Result<u64, StorageError> {
    if dtos.is_empty() {
        return Ok(0);
    }
    let count = ToPrimitive::to_u64(&dtos.len()).unwrap_or(u64::MAX);
    let ids = dtos
        .iter()
        .map(|dto| dto.market_id.clone())
        .collect::<Vec<_>>();
    let models: Vec<MarketActiveModel> = dtos
        .into_iter()
        .map(IntoActiveModel::into_active_model)
        .collect();
    // A full Gamma sync upserts tens of thousands of markets; one multi-row
    // INSERT would exceed the Postgres bind-parameter limit, so split into
    // bounded statements (all within the caller's transaction).
    let rows_per_insert = max_rows_per_insert(MarketColumn::iter().count());
    for chunk in models.chunks(rows_per_insert) {
        MarketEntity::insert_many(chunk.to_vec())
            .on_conflict(market_upsert_on_conflict())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
    }
    let markets = do_find_by_ids(db, &ids).await?;
    let markets = markets.iter().map(Arc::as_ref).collect::<Vec<_>>();
    write_market_pit_snapshots_if_changed(db, &markets).await?;
    Ok(count)
}

async fn do_update_status(
    db: &impl ConnectionTrait,
    id: &MarketId,
    status: &str,
    outcome: Option<&str>,
) -> Result<(), StorageError> {
    let mut stmt = MarketEntity::update_many().col_expr(MarketColumn::Status, Expr::value(status));

    if let Some(o) = outcome {
        stmt = stmt.col_expr(MarketColumn::Outcome, Expr::value(Some(o.to_string())));
    }

    let result = stmt
        .filter(MarketColumn::MarketId.eq(id.as_str()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    if result.rows_affected == 0 {
        return Err(StorageError::NotFound {
            entity: "market",
            id: id.to_string(),
        });
    }
    if let Some(market) = do_find_by_id(db, id).await? {
        write_market_pit_snapshot_if_changed(db, &market).await?;
    }
    Ok(())
}

async fn do_latest_pit_snapshots_before(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
    as_of: DateTime<Utc>,
) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
    let mut latest = Vec::new();
    // Chunk the IN list to stay under the Postgres bind-parameter limit;
    // ids are disjoint across chunks, so per-chunk dedup stays correct.
    for chunk in ids.chunks(IN_LIST_CHUNK) {
        let rows = MarketPitSnapshotEntity::find()
            .filter(MarketPitSnapshotColumn::MarketId.is_in(chunk.iter().map(MarketId::as_str)))
            .filter(MarketPitSnapshotColumn::ObservedAt.lte(as_of))
            .order_by_asc(MarketPitSnapshotColumn::MarketId)
            .order_by_desc(MarketPitSnapshotColumn::ObservedAt)
            .all(db)
            .await
            .map_err(StorageError::from)?;
        let mut seen = HashSet::new();
        for row in rows {
            if seen.insert(row.market_id.clone()) {
                latest.push(row.into());
            }
        }
    }
    Ok(latest)
}

async fn write_market_pit_snapshots_if_changed(
    db: &impl ConnectionTrait,
    markets: &[&MarketInfo],
) -> Result<(), StorageError> {
    if markets.is_empty() {
        return Ok(());
    }
    let ids = markets
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<Vec<_>>();
    let latest = latest_pit_snapshots(db, &ids).await?;
    let latest_by_market = latest
        .iter()
        .map(|snapshot| (snapshot.market_id.clone(), snapshot.payload_hash.as_str()))
        .collect::<HashMap<_, _>>();
    let mut snapshots = Vec::new();
    for market in markets {
        let payload_hash = market_replay_payload_hash(market)?;
        let unchanged = latest_by_market
            .get(&market.market_id)
            .is_some_and(|latest_hash| *latest_hash == payload_hash);
        if !unchanged {
            snapshots.push(new_market_pit_snapshot(market, payload_hash));
        }
    }
    if snapshots.is_empty() {
        return Ok(());
    }
    let models = snapshots
        .into_iter()
        .map(IntoActiveModel::into_active_model)
        .collect::<Vec<MarketPitSnapshotActiveModel>>();
    // Chunk the multi-row INSERT to stay under the bind-parameter limit.
    let rows_per_insert = max_rows_per_insert(MarketPitSnapshotColumn::iter().count());
    for chunk in models.chunks(rows_per_insert) {
        MarketPitSnapshotEntity::insert_many(chunk.to_vec())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(())
}

async fn write_market_pit_snapshot_if_changed(
    db: &impl ConnectionTrait,
    market: &MarketInfo,
) -> Result<(), StorageError> {
    write_market_pit_snapshots_if_changed(db, &[market]).await
}

async fn latest_pit_snapshots(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
    let mut latest = Vec::new();
    // Chunk the IN list to stay under the Postgres bind-parameter limit;
    // ids are disjoint across chunks, so per-chunk dedup stays correct.
    for chunk in ids.chunks(IN_LIST_CHUNK) {
        let rows = MarketPitSnapshotEntity::find()
            .filter(MarketPitSnapshotColumn::MarketId.is_in(chunk.iter().map(MarketId::as_str)))
            .order_by_asc(MarketPitSnapshotColumn::MarketId)
            .order_by_desc(MarketPitSnapshotColumn::ObservedAt)
            .all(db)
            .await
            .map_err(StorageError::from)?;
        let mut seen = HashSet::new();
        for row in rows {
            if seen.insert(row.market_id.clone()) {
                latest.push(row.into());
            }
        }
    }
    Ok(latest)
}

#[derive(Serialize)]
struct MarketReplayPayload<'a> {
    market_id: &'a MarketId,
    event_id: &'a EventId,
    question: &'a str,
    slug: &'a str,
    categories: &'a [MarketCategory],
    status: MarketStatus,
    outcome: &'a Option<String>,
    yes_token_id: &'a TokenId,
    no_token_id: &'a TokenId,
    tick_size: TickSize,
    neg_risk: bool,
    end_date: Option<DateTime<Utc>>,
    resolved_at: Option<DateTime<Utc>>,
    fees_enabled: bool,
    fee_rate: &'a Option<rust_decimal::Decimal>,
    fee_exponent: &'a Option<rust_decimal::Decimal>,
    fee_taker_only: Option<bool>,
    fee_rebate_rate: &'a Option<rust_decimal::Decimal>,
    fee_source: &'a Option<String>,
    fee_observed_at: Option<DateTime<Utc>>,
}

fn market_replay_payload_hash(market: &MarketInfo) -> Result<String, StorageError> {
    let payload = MarketReplayPayload {
        market_id: &market.market_id,
        event_id: &market.event_id,
        question: &market.question,
        slug: &market.slug,
        categories: &market.categories,
        status: market.status,
        outcome: &market.outcome,
        yes_token_id: &market.yes_token_id,
        no_token_id: &market.no_token_id,
        tick_size: market.tick_size,
        neg_risk: market.neg_risk,
        end_date: market.end_date,
        resolved_at: market.resolved_at,
        fees_enabled: market.fees_enabled,
        fee_rate: &market.fee_rate,
        fee_exponent: &market.fee_exponent,
        fee_taker_only: market.fee_taker_only,
        fee_rebate_rate: &market.fee_rebate_rate,
        fee_source: &market.fee_source,
        fee_observed_at: market.fee_observed_at,
    };
    let bytes =
        serde_json::to_vec(&payload).map_err(|error| StorageError::Codec(error.to_string()))?;
    Ok(format!(
        "blake3:{}",
        hex::encode(blake3::hash(&bytes).as_bytes())
    ))
}

fn new_market_pit_snapshot(market: &MarketInfo, payload_hash: String) -> NewMarketPitSnapshot {
    NewMarketPitSnapshot {
        market_pit_snapshot_id: MarketPitSnapshotId::from_v7(),
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        question: market.question.clone(),
        slug: market.slug.clone(),
        categories: market.category_set(),
        status: market.status,
        outcome: market.outcome.clone(),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        tick_size: market.tick_size,
        neg_risk: market.neg_risk,
        end_date: market.end_date,
        resolved_at: market.resolved_at,
        fees_enabled: market.fees_enabled,
        fee_rate: market.fee_rate,
        fee_exponent: market.fee_exponent,
        fee_taker_only: market.fee_taker_only,
        fee_rebate_rate: market.fee_rebate_rate,
        fee_source: market.fee_source.clone(),
        fee_observed_at: market.fee_observed_at,
        payload_hash,
        observed_at: market.updated_at,
    }
}

fn page_condition(query: &MarketPageQuery) -> Condition {
    let mut condition = Condition::all();
    if let Some(status) = query.status {
        condition = condition.add(MarketColumn::Status.eq(status));
    }
    if let Some(category) = query.category {
        // Any-match against the text[] membership column (GIN-indexed).
        condition = condition.add(
            Expr::col(MarketColumn::Categories)
                .binary(PgBinOper::Contains, Expr::val(vec![category])),
        );
    }
    if let Some(event_id) = &query.event_id {
        condition = condition.add(MarketColumn::EventId.eq(event_id.as_str()));
    }
    if let Some(keyword) = query.keyword.as_deref().filter(|kw| !kw.is_empty()) {
        let pattern = format!("%{keyword}%");
        condition = condition.add(
            Condition::any()
                .add(Expr::col(MarketColumn::Question).ilike(pattern.clone()))
                .add(Expr::col(MarketColumn::Slug).ilike(pattern)),
        );
    }
    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: MarketPageQuery,
) -> Result<Paginated<MarketInfo>, StorageError> {
    let window = query.page.normalized();
    let condition = page_condition(&query);
    let total = MarketEntity::find()
        .filter(condition.clone())
        .count(db)
        .await
        .map_err(StorageError::from)?;
    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), total, &window));
    }
    let models = MarketEntity::find()
        .filter(condition)
        .order_by_desc(MarketColumn::CreatedAt)
        .order_by_desc(MarketColumn::MarketId)
        .offset(window.offset())
        .limit(window.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = models.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window))
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepository {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        do_page(&self.db, query).await
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_ids(&self.db, ids).await
    }

    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
        do_latest_pit_snapshots_before(&self.db, ids, as_of).await
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_event(&self.db, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_endgame_candidates(&self.db, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn upsert(&self, dto: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let market = do_upsert(&txn, dto).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(market)
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let count = do_upsert_batch(&txn, dtos).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(count)
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        do_update_status(&txn, id, status, outcome).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        do_page(self.txn, query).await
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_ids(self.txn, ids).await
    }

    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
        do_latest_pit_snapshots_before(self.txn, ids, as_of).await
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_event(self.txn, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_endgame_candidates(self.txn, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn upsert(&self, dto: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        do_upsert(self.txn, dto).await
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        do_upsert_batch(self.txn, dtos).await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        do_update_status(self.txn, id, status, outcome).await
    }
}
