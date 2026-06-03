use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, MarketPitSnapshotInfo, NewMarketPitSnapshot, UpsertMarket},
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
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, OnConflict},
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    MarketEntity::find()
        .filter(MarketColumn::MarketId.is_in(ids.iter().map(MarketId::as_str)))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(|model| Arc::new(model.into())).collect())
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
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let id_strs: Vec<&str> = ids.iter().map(MarketId::as_str).collect();
    let rows = MarketEntity::find()
        .filter(MarketColumn::MarketId.is_in(id_strs))
        .select_only()
        .column(MarketColumn::MarketId)
        .into_tuple::<String>()
        .all(db)
        .await?;
    Ok(rows.into_iter().collect())
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    dto: UpsertMarket,
) -> Result<Arc<MarketInfo>, StorageError> {
    let am: MarketActiveModel = dto.into_active_model();
    let model = MarketEntity::insert(am)
        .on_conflict(
            OnConflict::column(MarketColumn::MarketId)
                .update_columns([
                    MarketColumn::EventId,
                    MarketColumn::Question,
                    MarketColumn::Slug,
                    MarketColumn::Category,
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
                .to_owned(),
        )
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
    MarketEntity::insert_many(models)
        .on_conflict(
            OnConflict::column(MarketColumn::MarketId)
                .update_columns([
                    MarketColumn::EventId,
                    MarketColumn::Question,
                    MarketColumn::Slug,
                    MarketColumn::Category,
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
                .to_owned(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = MarketPitSnapshotEntity::find()
        .filter(MarketPitSnapshotColumn::MarketId.is_in(ids.iter().map(MarketId::as_str)))
        .filter(MarketPitSnapshotColumn::ObservedAt.lte(as_of))
        .order_by_asc(MarketPitSnapshotColumn::MarketId)
        .order_by_desc(MarketPitSnapshotColumn::ObservedAt)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let mut seen = HashSet::new();
    let mut latest = Vec::new();
    for row in rows {
        if seen.insert(row.market_id.clone()) {
            latest.push(row.into());
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
    MarketPitSnapshotEntity::insert_many(models)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = MarketPitSnapshotEntity::find()
        .filter(MarketPitSnapshotColumn::MarketId.is_in(ids.iter().map(MarketId::as_str)))
        .order_by_asc(MarketPitSnapshotColumn::MarketId)
        .order_by_desc(MarketPitSnapshotColumn::ObservedAt)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let mut seen = HashSet::new();
    let mut latest = Vec::new();
    for row in rows {
        if seen.insert(row.market_id.clone()) {
            latest.push(row.into());
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
    category: MarketCategory,
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
        category: market.category,
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
        market_pit_snapshot_id: MarketPitSnapshotId::new_v7(),
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        question: market.question.clone(),
        slug: market.slug.clone(),
        category: market.category,
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

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepository {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        do_find_by_id(&self.db, id).await
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
