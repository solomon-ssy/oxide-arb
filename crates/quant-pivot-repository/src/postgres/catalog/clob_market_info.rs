//! Append-only CLOB market-info persistence and PIT reads.

use std::{collections::BTreeMap, fmt::Display};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    entities::clob_market_info_version::{ActiveModel, Column, Entity, Model},
    types::{ClobMarketInfoVersion, ClobTokenSet, ContentHash, HistoryCoverage, MarketId},
};
use sea_orm::{
    ActiveValue, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult,
    QueryFilter, QueryOrder, QuerySelect, sea_query::OnConflict,
};

use crate::traits::ClobMarketInfoRepository;

pub struct PgClobMarketInfoRepository {
    db: DatabaseConnection,
}

impl PgClobMarketInfoRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ClobMarketInfoRepository for PgClobMarketInfoRepository {
    async fn research_history_coverage(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<HistoryCoverage>, StorageError> {
        let row = Entity::find()
            .select_only()
            .column_as(Column::EffectiveAt.min(), "earliest_event_time")
            .column_as(Column::EffectiveAt.max(), "latest_event_time")
            .column_as(Column::VersionId.count(), "row_count")
            .filter(Column::EffectiveAt.lte(as_of))
            .into_model::<HistoryRangeRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .unwrap_or_default();
        Ok(vec![history_coverage(
            "clob_market_info_version",
            "effective_at",
            &row,
        )?])
    }

    async fn insert_observation(
        &self,
        observation: ClobMarketInfoVersion,
    ) -> Result<ClobMarketInfoVersion, StorageError> {
        observation
            .validate()
            .map_err(|detail| invariant("invalid observation", detail))?;
        let market_id = observation.market_id.clone();
        let payload_hash = observation.payload_hash;
        let maker_bps = exact_i32(
            "builder_maker_fee_rate_bps",
            observation.builder_maker_fee_rate_bps,
        )?;
        let taker_bps = exact_i32(
            "builder_taker_fee_rate_bps",
            observation.builder_taker_fee_rate_bps,
        )?;
        let minimum_order_age_secs = observation
            .minimum_order_age_secs
            .map(|value| {
                i64::try_from(value)
                    .map_err(|error| invariant("minimum_order_age_secs", error.to_string()))
            })
            .transpose()?;
        let active = ActiveModel {
            version_id: Set(observation.version_id),
            market_id: Set(observation.market_id),
            tokens_json: Set(ClobTokenSet(observation.tokens)),
            tick_size: Set(observation.tick_size),
            minimum_order_size: Set(observation.minimum_order_size),
            neg_risk: Set(observation.neg_risk),
            taker_order_delay_enabled: Set(observation.taker_order_delay_enabled),
            minimum_order_age_secs: Set(minimum_order_age_secs),
            blockaid_check_enabled: Set(observation.blockaid_check_enabled),
            fee_details_json: Set(observation.fee_details),
            builder_maker_fee_rate_bps: Set(maker_bps),
            builder_taker_fee_rate_bps: Set(taker_bps),
            effective_at: Set(observation.effective_at),
            available_at: Set(observation.available_at),
            payload_hash: Set(observation.payload_hash),
            raw_payload: Set(observation.raw_payload.into()),
            created_at: ActiveValue::NotSet,
        };
        Entity::insert(active)
            .on_conflict(
                OnConflict::columns([Column::MarketId, Column::PayloadHash])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        find_by_hash(&self.db, &market_id, &payload_hash)
            .await?
            .ok_or_else(|| {
                invariant(
                    "insert_observation",
                    "content-addressed row was not observable after insert".to_owned(),
                )
            })
            .and_then(model_to_domain)
    }

    async fn at(
        &self,
        market_id: &MarketId,
        effective_at: DateTime<Utc>,
        available_at_cutoff: DateTime<Utc>,
    ) -> Result<Option<ClobMarketInfoVersion>, StorageError> {
        Entity::find()
            .filter(Column::MarketId.eq(market_id.clone()))
            .filter(Column::EffectiveAt.lte(effective_at))
            .filter(Column::AvailableAt.lte(available_at_cutoff))
            .order_by_desc(Column::EffectiveAt)
            .order_by_desc(Column::AvailableAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(model_to_domain)
            .transpose()
    }

    async fn latest(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ClobMarketInfoVersion>, StorageError> {
        Entity::find()
            .filter(Column::MarketId.eq(market_id.clone()))
            .order_by_desc(Column::AvailableAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(model_to_domain)
            .transpose()
    }

    async fn at_many(
        &self,
        market_ids: &[MarketId],
        effective_at: DateTime<Utc>,
        available_at_cutoff: DateTime<Utc>,
    ) -> Result<Vec<ClobMarketInfoVersion>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        Entity::find()
            .filter(Column::MarketId.is_in(market_ids.iter().cloned()))
            .filter(Column::EffectiveAt.lte(effective_at))
            .filter(Column::AvailableAt.lte(available_at_cutoff))
            .distinct_on([Column::MarketId])
            .order_by_asc(Column::MarketId)
            .order_by_desc(Column::EffectiveAt)
            .order_by_desc(Column::AvailableAt)
            .order_by_desc(Column::PayloadHash)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(model_to_domain)
            .collect()
    }

    async fn window(
        &self,
        market_ids: &[MarketId],
        effective_from: DateTime<Utc>,
        effective_to: DateTime<Utc>,
        available_by: DateTime<Utc>,
    ) -> Result<Vec<ClobMarketInfoVersion>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = Entity::find()
            .filter(Column::MarketId.is_in(market_ids.iter().cloned()))
            .filter(Column::EffectiveAt.lt(effective_to))
            .filter(Column::AvailableAt.lte(available_by))
            .order_by_asc(Column::MarketId)
            .order_by_asc(Column::EffectiveAt)
            .order_by_asc(Column::AvailableAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut baselines = BTreeMap::new();
        let mut selected = Vec::new();
        for row in rows {
            if row.effective_at < effective_from {
                baselines.insert(row.market_id.clone(), row);
            } else {
                selected.push(row);
            }
        }
        selected.extend(baselines.into_values());
        selected.sort_by(|left, right| {
            (
                &left.market_id,
                left.effective_at,
                left.available_at,
                &left.payload_hash,
            )
                .cmp(&(
                    &right.market_id,
                    right.effective_at,
                    right.available_at,
                    &right.payload_hash,
                ))
        });
        selected.into_iter().map(model_to_domain).collect()
    }
}

#[derive(Debug, Default, FromQueryResult)]
struct HistoryRangeRow {
    earliest_event_time: Option<DateTime<Utc>>,
    latest_event_time: Option<DateTime<Utc>>,
    row_count: i64,
}

fn history_coverage(
    object: &str,
    time_column: &str,
    row: &HistoryRangeRow,
) -> Result<HistoryCoverage, StorageError> {
    Ok(HistoryCoverage {
        object: object.to_owned(),
        time_column: time_column.to_owned(),
        earliest_event_time: row.earliest_event_time,
        latest_event_time: row.latest_event_time,
        row_count: u64::try_from(row.row_count)
            .map_err(|error| invariant("row_count", error.to_string()))?,
    })
}

async fn find_by_hash(
    db: &DatabaseConnection,
    market_id: &MarketId,
    payload_hash: &ContentHash,
) -> Result<Option<Model>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.clone()))
        .filter(Column::PayloadHash.eq(*payload_hash))
        .one(db)
        .await
        .map_err(StorageError::from)
}

fn model_to_domain(model: Model) -> Result<ClobMarketInfoVersion, StorageError> {
    Ok(ClobMarketInfoVersion {
        version_id: model.version_id,
        market_id: model.market_id,
        tokens: model.tokens_json.0,
        tick_size: model.tick_size,
        minimum_order_size: model.minimum_order_size,
        neg_risk: model.neg_risk,
        taker_order_delay_enabled: model.taker_order_delay_enabled,
        minimum_order_age_secs: model
            .minimum_order_age_secs
            .map(|value| {
                u64::try_from(value)
                    .map_err(|error| invariant("minimum_order_age_secs", error.to_string()))
            })
            .transpose()?,
        blockaid_check_enabled: model.blockaid_check_enabled,
        fee_details: model.fee_details_json,
        builder_maker_fee_rate_bps: exact_u32(
            "builder_maker_fee_rate_bps",
            model.builder_maker_fee_rate_bps,
        )?,
        builder_taker_fee_rate_bps: exact_u32(
            "builder_taker_fee_rate_bps",
            model.builder_taker_fee_rate_bps,
        )?,
        effective_at: model.effective_at,
        available_at: model.available_at,
        payload_hash: model.payload_hash,
        raw_payload: model.raw_payload.into_inner(),
    })
}

fn exact_i32(field: &'static str, value: u32) -> Result<i32, StorageError> {
    i32::try_from(value).map_err(|error| invariant(field, error.to_string()))
}

fn exact_u32(field: &'static str, value: i32) -> Result<u32, StorageError> {
    u32::try_from(value).map_err(|error| invariant(field, error.to_string()))
}

fn invariant(field: &'static str, detail: impl Display) -> StorageError {
    StorageError::InvariantViolation {
        entity: Some("clob_market_info_version"),
        detail: format!("{field}: {detail}"),
    }
}
