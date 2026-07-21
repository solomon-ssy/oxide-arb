//! Atomic typed domain projections and durable `ClickHouse` event outbox.

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        data_plane::{
            CryptoPriceReport, CryptoPriceTransition, DomainCursorStatus, DomainEventEnvelope,
            DomainEventPayload, DomainEventType, DomainSourceCheckpoint, UpsertDomainSourceCursor,
            WeatherDailyTemperatureExtremeChange, WeatherObservationDayClosed,
            WeatherObservationReport, WeatherObservationReportKind,
        },
        quant::{CryptoPriceProjectionInfo, WeatherDailyTemperatureProjectionInfo},
    },
    entities::{
        quant_crypto_price_projection::{
            ActiveModel, Entity, Model as QuantCryptoPriceProjectionModel,
        },
        quant_domain_event_outbox::{
            ActiveModel as QuantDomainEventOutboxActiveModel,
            Column as QuantDomainEventOutboxColumn, Entity as QuantDomainEventOutboxEntity,
        },
        quant_domain_source_cursor::{
            Column as QuantDomainSourceCursorColumn, Entity as QuantDomainSourceCursorEntity,
        },
        quant_weather_daily_temperature_projection::{
            ActiveModel as QuantWeatherDailyTemperatureProjectionActiveModel, Column,
            Entity as QuantWeatherDailyTemperatureProjectionEntity, Model,
        },
        quant_weather_observation_current::{
            ActiveModel as QuantWeatherObservationCurrentActiveModel,
            Column as QuantWeatherObservationCurrentColumn,
            Entity as QuantWeatherObservationCurrentEntity,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainEventId, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId,
        IcaoStation, TemperatureCelsius, Usd, WeatherTemperatureStatistic, WeatherVariable,
        WorkerId,
    },
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, ExprTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::{quant::condition_wake::notify_input_change, write::upsert_many_chunked},
    traits::DomainProjectionRepository,
};

const ENTITY: &str = "quant_domain_projection";

pub struct PgDomainProjectionRepository {
    db: DatabaseConnection,
}

impl PgDomainProjectionRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl DomainProjectionRepository for PgDomainProjectionRepository {
    async fn apply_crypto_report(
        &self,
        report: CryptoPriceReport,
        checkpoint: DomainSourceCheckpoint,
        gap_generation: u64,
        source_healthy: bool,
    ) -> Result<CryptoPriceProjectionInfo, StorageError> {
        validate_binding(&report.source_id, &report.instrument_key)?;
        let gap_generation = to_i64(gap_generation, "crypto gap generation")?;
        let source_sequence = to_i64(report.source_sequence, "crypto source sequence")?;
        let checkpoint_hash = hash_checkpoint(&checkpoint)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let existing =
            Entity::find_by_id((report.source_id.clone(), report.instrument_key.clone()))
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?;

        let model = match existing {
            Some(existing) if existing.report_hash == report.report_hash => existing,
            Some(existing) => {
                if source_sequence < existing.source_sequence
                    || (source_sequence == existing.source_sequence
                        && report.available_at <= existing.available_at)
                {
                    return Err(conflict("crypto report is older than current projection"));
                }
                let previous_price = existing.current_price;
                let mut active = existing.into_active_model();
                active.previous_price = ActiveValue::Set(Some(previous_price));
                active.current_price = ActiveValue::Set(report.price);
                active.source_sequence = ActiveValue::Set(source_sequence);
                active.event_time = ActiveValue::Set(report.event_time);
                active.available_at = ActiveValue::Set(report.available_at);
                active.report_hash = ActiveValue::Set(report.report_hash.clone());
                active.gap_generation = ActiveValue::Set(gap_generation);
                active.source_healthy = ActiveValue::Set(source_healthy);
                let updated = active.update(&txn).await.map_err(StorageError::from)?;
                if previous_price != report.price {
                    let event = crypto_event(
                        &report,
                        previous_price,
                        gap_generation,
                        checkpoint_hash.clone(),
                    )?;
                    insert_outbox(&txn, event).await?;
                }
                updated
            }
            None => ActiveModel {
                source_id: ActiveValue::Set(report.source_id.clone()),
                instrument_key: ActiveValue::Set(report.instrument_key.clone()),
                previous_price: ActiveValue::Set(None),
                current_price: ActiveValue::Set(report.price),
                source_sequence: ActiveValue::Set(source_sequence),
                event_time: ActiveValue::Set(report.event_time),
                available_at: ActiveValue::Set(report.available_at),
                report_hash: ActiveValue::Set(report.report_hash.clone()),
                gap_generation: ActiveValue::Set(gap_generation),
                source_healthy: ActiveValue::Set(source_healthy),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(StorageError::from)?,
        };
        upsert_cursor(
            &txn,
            &report.source_id,
            &report.instrument_key,
            checkpoint,
            checkpoint_hash,
        )
        .await?;
        notify_input_change(&txn, "crypto").await?;
        txn.commit().await.map_err(StorageError::from)?;
        crypto_info(model)
    }

    async fn apply_weather_report(
        &self,
        report: WeatherObservationReport,
        timezone: String,
        local_date: NaiveDate,
        checkpoint: DomainSourceCheckpoint,
        gap_generation: u64,
        source_healthy: bool,
    ) -> Result<Vec<WeatherDailyTemperatureProjectionInfo>, StorageError> {
        let (station, temperature) = validate_weather_temperature_report(&report)?;
        let instrument_key = report.instrument_key.clone();
        validate_binding(&report.source_id, &instrument_key)?;
        let gap_generation = to_i64(gap_generation, "weather gap generation")?;
        let checkpoint_hash = hash_checkpoint(&checkpoint)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let observation_changed =
            upsert_weather_observation(&txn, &report, &station, temperature, local_date).await?;
        let projections = upsert_weather_daily_temperature_projections(
            &txn,
            WeatherProjectionInput {
                report: &report,
                station: &station,
                timezone: &timezone,
                local_date,
                instrument_key: &instrument_key,
                gap_generation,
                source_healthy,
                checkpoint_hash: &checkpoint_hash,
                observation_changed,
            },
        )
        .await?;
        insert_outboxes(
            &txn,
            projections
                .iter()
                .filter_map(|(_, event)| event.clone())
                .collect(),
        )
        .await?;
        upsert_cursor(
            &txn,
            &report.source_id,
            &instrument_key,
            checkpoint,
            checkpoint_hash,
        )
        .await?;
        notify_input_change(&txn, "weather").await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(projections
            .into_iter()
            .map(|(model, _)| weather_info(model))
            .collect())
    }

    async fn close_weather_day(
        &self,
        station: &IcaoStation,
        local_date: NaiveDate,
        closed_at: DateTime<Utc>,
    ) -> Result<Vec<WeatherDailyTemperatureProjectionInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let instrument_key = DomainInstrumentKey::aviation_weather(station);
        let rows = QuantWeatherDailyTemperatureProjectionEntity::find()
            .filter(Column::SourceId.eq(DomainSourceId::aviation_weather()))
            .filter(Column::InstrumentKey.eq(instrument_key.clone()))
            .filter(Column::LocalDate.eq(local_date))
            .order_by_asc(Column::TemperatureStatistic)
            .lock_exclusive()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        if rows.is_empty() {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(Vec::new());
        }
        let checkpoint_hash = if rows.iter().any(|row| !row.day_closed) {
            Some(
                QuantDomainSourceCursorEntity::find_by_id((
                    DomainSourceId::aviation_weather(),
                    DomainInstrumentKey::aviation_weather(station),
                ))
                .lock_shared()
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| invariant("weather projection has no authoritative source cursor"))?
                .checkpoint_hash,
            )
        } else {
            None
        };
        let mut updated = Vec::with_capacity(rows.len());
        for row in rows {
            if row.day_closed {
                updated.push(weather_info(row));
                continue;
            }
            let event = weather_close_event(
                &row,
                closed_at,
                checkpoint_hash
                    .clone()
                    .ok_or_else(|| invariant("open weather projection has no checkpoint hash"))?,
            )?;
            let revision = checked_add(row.revision, "weather daily close revision")?;
            let mut active = row.into_active_model();
            active.day_closed = ActiveValue::Set(true);
            active.revision = ActiveValue::Set(revision);
            active.last_event_id = ActiveValue::Set(Some(event.id.clone()));
            active.available_at = ActiveValue::Set(closed_at);
            let model = active.update(&txn).await.map_err(StorageError::from)?;
            insert_outbox(&txn, event).await?;
            updated.push(weather_info(model));
        }
        if !updated.iter().all(|row| row.day_closed) {
            return Err(invariant(
                "weather day close left an open temperature statistic",
            ));
        }
        notify_input_change(&txn, "weather_day_closed").await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated)
    }

    async fn mark_crypto_source_gap(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = Entity::find_by_id((source_id.clone(), instrument_key.clone()))
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(0);
        };
        let generation = checked_add(row.gap_generation, "crypto gap generation")?;
        let mut active = row.into_active_model();
        active.gap_generation = ActiveValue::Set(generation);
        active.source_healthy = ActiveValue::Set(false);
        active.available_at = ActiveValue::Set(observed_at);
        active.update(&txn).await.map_err(StorageError::from)?;
        notify_input_change(&txn, "crypto_gap").await?;
        txn.commit().await.map_err(StorageError::from)?;
        from_i64(generation, "crypto gap generation")
    }

    async fn mark_weather_source_gap(
        &self,
        station: &IcaoStation,
        local_date: NaiveDate,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let instrument_key = DomainInstrumentKey::aviation_weather(station);
        let rows = QuantWeatherDailyTemperatureProjectionEntity::find()
            .filter(Column::SourceId.eq(DomainSourceId::aviation_weather()))
            .filter(Column::InstrumentKey.eq(instrument_key.clone()))
            .filter(Column::LocalDate.eq(local_date))
            .lock_exclusive()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(first) = rows.first() else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(0);
        };
        if rows
            .iter()
            .any(|row| row.gap_generation != first.gap_generation)
        {
            return Err(invariant(
                "weather maximum/minimum projections have divergent gap generations",
            ));
        }
        let generation = checked_add(first.gap_generation, "weather gap generation")?;
        let row_count = u64::try_from(rows.len())
            .map_err(|error| invariant(format!("weather projection count overflow: {error}")))?;
        let updated = QuantWeatherDailyTemperatureProjectionEntity::update_many()
            .col_expr(Column::GapGeneration, Expr::value(generation))
            .col_expr(Column::SourceHealthy, Expr::value(false))
            .col_expr(Column::AvailableAt, Expr::value(observed_at))
            .filter(Column::SourceId.eq(DomainSourceId::aviation_weather()))
            .filter(Column::InstrumentKey.eq(instrument_key))
            .filter(Column::LocalDate.eq(local_date))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != row_count {
            return Err(conflict(
                "weather projections changed while applying the locked gap transition",
            ));
        }
        notify_input_change(&txn, "weather_gap").await?;
        txn.commit().await.map_err(StorageError::from)?;
        from_i64(generation, "weather gap generation")
    }

    async fn claim_pending_events(
        &self,
        worker_id: WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<DomainEventEnvelope>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rows = QuantDomainEventOutboxEntity::find()
            .filter(QuantDomainEventOutboxColumn::PublishedAt.is_null())
            .filter(
                Condition::any()
                    .add(QuantDomainEventOutboxColumn::LeaseExpiresAt.is_null())
                    .add(QuantDomainEventOutboxColumn::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(QuantDomainEventOutboxColumn::CreatedAt)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        for row in &rows {
            row.publish_attempts
                .checked_add(1)
                .ok_or_else(|| invariant("domain event publish attempt overflow"))?;
        }
        let row_count = u64::try_from(rows.len())
            .map_err(|error| invariant(format!("domain event claim count overflow: {error}")))?;
        let event_ids = rows
            .iter()
            .map(|row| row.event_id.clone())
            .collect::<Vec<_>>();
        let updated = QuantDomainEventOutboxEntity::update_many()
            .col_expr(
                QuantDomainEventOutboxColumn::ClaimOwner,
                Expr::value(Some(worker_id)),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::LeaseExpiresAt,
                Expr::value(Some(lease_expires_at)),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::PublishAttempts,
                Expr::col(QuantDomainEventOutboxColumn::PublishAttempts).add(1),
            )
            .filter(QuantDomainEventOutboxColumn::EventId.is_in(event_ids))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != row_count {
            return Err(conflict(
                "domain event claim set changed while rows were locked",
            ));
        }
        let events = rows.into_iter().map(|row| row.envelope_json).collect();
        txn.commit().await.map_err(StorageError::from)?;
        Ok(events)
    }

    async fn mark_event_published(
        &self,
        event_id: &DomainEventId,
        worker_id: WorkerId,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        QuantDomainEventOutboxEntity::update_many()
            .col_expr(
                QuantDomainEventOutboxColumn::PublishedAt,
                Expr::value(published_at),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::LastError,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::ClaimOwner,
                Expr::value(Option::<WorkerId>::None),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(QuantDomainEventOutboxColumn::EventId.eq(event_id.clone()))
            .filter(QuantDomainEventOutboxColumn::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn mark_event_failed(
        &self,
        event_id: &DomainEventId,
        worker_id: WorkerId,
        detail: String,
    ) -> Result<(), StorageError> {
        QuantDomainEventOutboxEntity::update_many()
            .col_expr(
                QuantDomainEventOutboxColumn::LastError,
                Expr::value(Some(detail)),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::ClaimOwner,
                Expr::value(Option::<WorkerId>::None),
            )
            .col_expr(
                QuantDomainEventOutboxColumn::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(QuantDomainEventOutboxColumn::EventId.eq(event_id.clone()))
            .filter(QuantDomainEventOutboxColumn::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

async fn upsert_weather_observation(
    txn: &DatabaseTransaction,
    report: &WeatherObservationReport,
    station: &IcaoStation,
    temperature: TemperatureCelsius,
    local_date: NaiveDate,
) -> Result<bool, StorageError> {
    let observation_key = (station.as_str().to_owned(), local_date, report.observed_at);
    let observation = QuantWeatherObservationCurrentEntity::find_by_id(observation_key)
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?;
    let changed = observation
        .as_ref()
        .is_none_or(|current| current.report_hash != report.report_hash);
    match observation {
        Some(current) if current.report_hash == report.report_hash => {}
        Some(current) => {
            if (
                report.available_at,
                report.published_at,
                &report.report_hash,
            ) <= (
                current.available_at,
                current.published_at,
                &current.report_hash,
            ) {
                return Err(conflict(
                    "weather correction is older than current observation",
                ));
            }
            let revision = checked_add(current.revision, "weather observation revision")?;
            let mut active = current.into_active_model();
            active.temperature_celsius = ActiveValue::Set(temperature.value());
            active.report_hash = ActiveValue::Set(report.report_hash.clone());
            active.revision = ActiveValue::Set(revision);
            active.published_at = ActiveValue::Set(report.published_at);
            active.available_at = ActiveValue::Set(report.available_at);
            active.update(txn).await.map_err(StorageError::from)?;
        }
        None => {
            QuantWeatherObservationCurrentActiveModel {
                station: ActiveValue::Set(station.as_str().to_owned()),
                local_date: ActiveValue::Set(local_date),
                observation_time: ActiveValue::Set(report.observed_at),
                temperature_celsius: ActiveValue::Set(temperature.value()),
                report_hash: ActiveValue::Set(report.report_hash.clone()),
                revision: ActiveValue::Set(0),
                published_at: ActiveValue::Set(report.published_at),
                available_at: ActiveValue::Set(report.available_at),
                ..Default::default()
            }
            .insert(txn)
            .await
            .map_err(StorageError::from)?;
        }
    }
    Ok(changed)
}

struct WeatherProjectionInput<'a> {
    report: &'a WeatherObservationReport,
    station: &'a IcaoStation,
    timezone: &'a str,
    local_date: NaiveDate,
    instrument_key: &'a DomainInstrumentKey,
    gap_generation: i64,
    source_healthy: bool,
    checkpoint_hash: &'a ContentHash,
    observation_changed: bool,
}

async fn upsert_weather_daily_temperature_projections(
    txn: &DatabaseTransaction,
    input: WeatherProjectionInput<'_>,
) -> Result<Vec<(Model, Option<DomainEventEnvelope>)>, StorageError> {
    let mut projections = Vec::with_capacity(2);
    for statistic in [
        WeatherTemperatureStatistic::Maximum,
        WeatherTemperatureStatistic::Minimum,
    ] {
        let observations = QuantWeatherObservationCurrentEntity::find()
            .filter(QuantWeatherObservationCurrentColumn::Station.eq(input.station.as_str()))
            .filter(QuantWeatherObservationCurrentColumn::LocalDate.eq(input.local_date));
        let extreme =
            match statistic {
                WeatherTemperatureStatistic::Maximum => observations
                    .order_by_desc(QuantWeatherObservationCurrentColumn::TemperatureCelsius),
                WeatherTemperatureStatistic::Minimum => observations
                    .order_by_asc(QuantWeatherObservationCurrentColumn::TemperatureCelsius),
            }
            .order_by_desc(QuantWeatherObservationCurrentColumn::AvailableAt)
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| invariant("weather daily projection has no current observations"))?;
        let existing = QuantWeatherDailyTemperatureProjectionEntity::find_by_id((
            input.report.source_id.clone(),
            input.instrument_key.clone(),
            input.local_date,
            statistic,
        ))
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?;
        let previous_extreme = existing.as_ref().map(|row| row.current_extreme_celsius);
        let current_extreme = extreme.temperature_celsius;
        let event = if input.observation_changed && previous_extreme != Some(current_extreme) {
            Some(weather_change_event(WeatherChangeEventInput {
                report: input.report,
                local_date: input.local_date,
                temperature_statistic: statistic,
                previous_extreme,
                current_extreme,
                gap_generation: input.gap_generation,
                checkpoint_hash: input.checkpoint_hash.clone(),
                supersedes_event_id: existing.as_ref().and_then(|row| row.last_event_id.clone()),
            })?)
        } else {
            None
        };
        let model = match existing {
            Some(existing) if !input.observation_changed => existing,
            Some(existing) => {
                update_weather_daily_temperature_projection(
                    txn,
                    existing,
                    &input,
                    previous_extreme,
                    current_extreme,
                    event.as_ref(),
                )
                .await?
            }
            None => {
                insert_weather_daily_temperature_projection(
                    txn,
                    &input,
                    statistic,
                    current_extreme,
                    event.as_ref(),
                )
                .await?
            }
        };
        projections.push((model, event));
    }
    Ok(projections)
}

async fn update_weather_daily_temperature_projection(
    txn: &DatabaseTransaction,
    existing: Model,
    input: &WeatherProjectionInput<'_>,
    previous_extreme: Option<Decimal>,
    current_extreme: Decimal,
    event: Option<&DomainEventEnvelope>,
) -> Result<Model, StorageError> {
    if existing.timezone != input.timezone {
        return Err(conflict("weather station timezone binding drift"));
    }
    let revision = checked_add(existing.revision, "weather daily revision")?;
    let last_event_id = event.map_or_else(
        || existing.last_event_id.clone(),
        |event| Some(event.id.clone()),
    );
    let mut active = existing.into_active_model();
    active.previous_extreme_celsius = ActiveValue::Set(previous_extreme);
    active.current_extreme_celsius = ActiveValue::Set(current_extreme);
    active.last_observation_time = ActiveValue::Set(input.report.observed_at);
    active.last_report_hash = ActiveValue::Set(input.report.report_hash.clone());
    active.last_event_id = ActiveValue::Set(last_event_id);
    active.revision = ActiveValue::Set(revision);
    active.gap_generation = ActiveValue::Set(input.gap_generation);
    active.source_healthy = ActiveValue::Set(input.source_healthy);
    active.available_at = ActiveValue::Set(input.report.available_at);
    active.update(txn).await.map_err(StorageError::from)
}

async fn insert_weather_daily_temperature_projection(
    txn: &DatabaseTransaction,
    input: &WeatherProjectionInput<'_>,
    statistic: WeatherTemperatureStatistic,
    current_extreme: Decimal,
    event: Option<&DomainEventEnvelope>,
) -> Result<Model, StorageError> {
    QuantWeatherDailyTemperatureProjectionActiveModel {
        source_id: ActiveValue::Set(input.report.source_id.clone()),
        instrument_key: ActiveValue::Set(input.instrument_key.clone()),
        station: ActiveValue::Set(input.station.as_str().to_owned()),
        local_date: ActiveValue::Set(input.local_date),
        temperature_statistic: ActiveValue::Set(statistic),
        timezone: ActiveValue::Set(input.timezone.to_owned()),
        current_extreme_celsius: ActiveValue::Set(current_extreme),
        previous_extreme_celsius: ActiveValue::Set(None),
        last_observation_time: ActiveValue::Set(input.report.observed_at),
        last_report_hash: ActiveValue::Set(input.report.report_hash.clone()),
        last_event_id: ActiveValue::Set(event.map(|event| event.id.clone())),
        revision: ActiveValue::Set(0),
        day_closed: ActiveValue::Set(false),
        gap_generation: ActiveValue::Set(input.gap_generation),
        source_healthy: ActiveValue::Set(input.source_healthy),
        available_at: ActiveValue::Set(input.report.available_at),
        ..Default::default()
    }
    .insert(txn)
    .await
    .map_err(StorageError::from)
}

struct EventBuildInput {
    source: DomainSourceId,
    event_type: DomainEventType,
    subject: String,
    time: DateTime<Utc>,
    published_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    supersedes_event_id: Option<DomainEventId>,
    source_checkpoint_hash: ContentHash,
    payload: DomainEventPayload,
}

struct WeatherChangeEventInput<'a> {
    report: &'a WeatherObservationReport,
    local_date: NaiveDate,
    temperature_statistic: WeatherTemperatureStatistic,
    previous_extreme: Option<Decimal>,
    current_extreme: Decimal,
    gap_generation: i64,
    checkpoint_hash: ContentHash,
    supersedes_event_id: Option<DomainEventId>,
}

fn crypto_event(
    report: &CryptoPriceReport,
    previous_price: Usd,
    gap_generation: i64,
    checkpoint_hash: ContentHash,
) -> Result<DomainEventEnvelope, StorageError> {
    let payload = DomainEventPayload::CryptoPriceTransition(CryptoPriceTransition {
        instrument_key: report.instrument_key.clone(),
        previous_price,
        current_price: report.price,
        source_sequence: report.source_sequence,
        gap_generation: from_i64(gap_generation, "crypto gap generation")?,
        report_hash: report.report_hash.clone(),
    });
    build_event(EventBuildInput {
        source: report.source_id.clone(),
        event_type: DomainEventType::CryptoPriceTransition,
        subject: report.instrument_key.to_string(),
        time: report.event_time,
        published_at: report.published_at,
        available_at: report.available_at,
        supersedes_event_id: None,
        source_checkpoint_hash: checkpoint_hash,
        payload,
    })
}

fn weather_change_event(
    input: WeatherChangeEventInput<'_>,
) -> Result<DomainEventEnvelope, StorageError> {
    let corrected = input.report.report_kind == WeatherObservationReportKind::Correction
        || input
            .previous_extreme
            .is_some_and(|previous| match input.temperature_statistic {
                WeatherTemperatureStatistic::Maximum => input.current_extreme < previous,
                WeatherTemperatureStatistic::Minimum => input.current_extreme > previous,
            });
    let event_type = if corrected {
        DomainEventType::WeatherDailyTemperatureExtremeCorrected
    } else {
        DomainEventType::WeatherDailyTemperatureExtremeAdvanced
    };
    let change = WeatherDailyTemperatureExtremeChange {
        station: input.report.subject_key.clone(),
        local_date: input.local_date,
        temperature_statistic: input.temperature_statistic,
        previous_extreme: input.previous_extreme.map(TemperatureCelsius::new),
        current_extreme: TemperatureCelsius::new(input.current_extreme),
        report_hash: input.report.report_hash.clone(),
        gap_generation: from_i64(input.gap_generation, "weather gap generation")?,
    };
    let payload = if corrected {
        DomainEventPayload::WeatherDailyTemperatureExtremeCorrected(change)
    } else {
        DomainEventPayload::WeatherDailyTemperatureExtremeAdvanced(change)
    };
    build_event(EventBuildInput {
        source: input.report.source_id.clone(),
        event_type,
        subject: format!(
            "{}:{}:{}",
            input.report.subject_key,
            input.local_date,
            input.temperature_statistic.as_str()
        ),
        time: input.report.observed_at,
        published_at: input.report.published_at,
        available_at: input.report.available_at,
        supersedes_event_id: input.supersedes_event_id,
        source_checkpoint_hash: input.checkpoint_hash,
        payload,
    })
}

fn weather_close_event(
    row: &Model,
    closed_at: DateTime<Utc>,
    checkpoint_hash: ContentHash,
) -> Result<DomainEventEnvelope, StorageError> {
    build_event(EventBuildInput {
        source: DomainSourceId::aviation_weather(),
        event_type: DomainEventType::WeatherObservationDayClosed,
        subject: format!(
            "{}:{}:{}",
            row.station, row.local_date, row.temperature_statistic
        ),
        time: closed_at,
        published_at: closed_at,
        available_at: closed_at,
        supersedes_event_id: row.last_event_id.clone(),
        source_checkpoint_hash: checkpoint_hash,
        payload: DomainEventPayload::WeatherObservationDayClosed(WeatherObservationDayClosed {
            station: row.station.clone(),
            local_date: row.local_date,
            temperature_statistic: row.temperature_statistic,
            final_noaa_extreme: TemperatureCelsius::new(row.current_extreme_celsius),
            last_report_hash: row.last_report_hash.clone(),
            gap_generation: from_i64(row.gap_generation, "weather gap generation")?,
        }),
    })
}

fn build_event(input: EventBuildInput) -> Result<DomainEventEnvelope, StorageError> {
    let payload_hash = CanonicalDigest::content_hash_json(&input.payload)
        .map_err(|error| invariant(error.to_string()))?;
    let content_hash = CanonicalDigest::content_hash_json(&(
        &input.source,
        input.event_type,
        &input.subject,
        input.time,
        &input.supersedes_event_id,
        &payload_hash,
        &input.source_checkpoint_hash,
    ))
    .map_err(|error| invariant(error.to_string()))?;
    Ok(DomainEventEnvelope {
        id: DomainEventId::from_content_hash(&content_hash),
        source: input.source,
        event_type: input.event_type,
        subject: input.subject,
        time: input.time,
        schema_version: 1,
        published_at: input.published_at,
        available_at: input.available_at,
        revision: 0,
        supersedes_event_id: input.supersedes_event_id,
        payload_hash,
        source_checkpoint_hash: input.source_checkpoint_hash,
        payload: input.payload,
    })
}

async fn insert_outbox<C: ConnectionTrait>(
    db: &C,
    event: DomainEventEnvelope,
) -> Result<(), StorageError> {
    insert_outboxes(db, vec![event]).await
}

async fn insert_outboxes<C: ConnectionTrait>(
    db: &C,
    events: Vec<DomainEventEnvelope>,
) -> Result<(), StorageError> {
    let rows = events
        .into_iter()
        .map(|event| QuantDomainEventOutboxActiveModel {
            event_id: ActiveValue::Set(event.id.clone()),
            envelope_json: ActiveValue::Set(event),
            published_at: ActiveValue::Set(None),
            publish_attempts: ActiveValue::Set(0),
            claim_owner: ActiveValue::Set(None),
            lease_expires_at: ActiveValue::Set(None),
            last_error: ActiveValue::Set(None),
            ..Default::default()
        })
        .collect();
    upsert_many_chunked::<QuantDomainEventOutboxEntity, _>(
        db,
        rows,
        OnConflict::column(QuantDomainEventOutboxColumn::EventId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn upsert_cursor<C: ConnectionTrait>(
    db: &C,
    source_id: &DomainSourceId,
    instrument_key: &DomainInstrumentKey,
    checkpoint: DomainSourceCheckpoint,
    checkpoint_hash: ContentHash,
) -> Result<(), StorageError> {
    let cursor = UpsertDomainSourceCursor {
        source_id: source_id.clone(),
        instrument_key: instrument_key.clone(),
        checkpoint_json: checkpoint,
        checkpoint_hash,
        status: DomainCursorStatus::Live,
        last_error: None,
        updated_at: Utc::now(),
    };
    QuantDomainSourceCursorEntity::insert(cursor.into_active_model())
        .on_conflict(
            OnConflict::columns([
                QuantDomainSourceCursorColumn::SourceId,
                QuantDomainSourceCursorColumn::InstrumentKey,
            ])
            .update_columns([
                QuantDomainSourceCursorColumn::CheckpointJson,
                QuantDomainSourceCursorColumn::CheckpointHash,
                QuantDomainSourceCursorColumn::Status,
                QuantDomainSourceCursorColumn::LastError,
                QuantDomainSourceCursorColumn::UpdatedAt,
            ])
            .to_owned(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

fn validate_binding(
    source_id: &DomainSourceId,
    instrument_key: &DomainInstrumentKey,
) -> Result<(), StorageError> {
    if instrument_key.source_id().as_ref() != Some(source_id) {
        return Err(invariant("source and instrument binding do not match"));
    }
    Ok(())
}

fn validate_weather_temperature_report(
    report: &WeatherObservationReport,
) -> Result<(IcaoStation, TemperatureCelsius), StorageError> {
    if report.source_id != DomainSourceId::aviation_weather()
        || report.variable != WeatherVariable::Temperature
        || report.unit != DomainMeasurementUnit::Celsius
        || report.precision <= Decimal::ZERO
        || report.available_at < report.published_at
    {
        return Err(invariant(
            "weather daily-temperature projection received an incompatible fact",
        ));
    }
    let station = report
        .instrument_key
        .as_aviation_weather_station()
        .filter(|station| station.as_str() == report.subject_key)
        .ok_or_else(|| invariant("weather observation subject/instrument binding mismatch"))?;
    Ok((station, TemperatureCelsius::new(report.value)))
}

fn hash_checkpoint(checkpoint: &DomainSourceCheckpoint) -> Result<ContentHash, StorageError> {
    CanonicalDigest::content_hash_json(checkpoint).map_err(|error| invariant(error.to_string()))
}

fn crypto_info(
    row: QuantCryptoPriceProjectionModel,
) -> Result<CryptoPriceProjectionInfo, StorageError> {
    Ok(CryptoPriceProjectionInfo {
        source_id: row.source_id,
        instrument_key: row.instrument_key,
        previous_price: row.previous_price,
        current_price: row.current_price,
        source_sequence: from_i64(row.source_sequence, "crypto source sequence")?,
        event_time: row.event_time,
        available_at: row.available_at,
        report_hash: row.report_hash,
        gap_generation: row.gap_generation,
        source_healthy: row.source_healthy,
    })
}

fn weather_info(row: Model) -> WeatherDailyTemperatureProjectionInfo {
    WeatherDailyTemperatureProjectionInfo {
        source_id: row.source_id,
        instrument_key: row.instrument_key,
        station: row.station,
        local_date: row.local_date,
        timezone: row.timezone,
        temperature_statistic: row.temperature_statistic,
        current_extreme: TemperatureCelsius::new(row.current_extreme_celsius),
        last_observation_time: row.last_observation_time,
        last_report_hash: row.last_report_hash,
        revision: row.revision,
        day_closed: row.day_closed,
        gap_generation: row.gap_generation,
        source_healthy: row.source_healthy,
        available_at: row.available_at,
    }
}

fn checked_add(value: i64, field: &'static str) -> Result<i64, StorageError> {
    value
        .checked_add(1)
        .ok_or_else(|| invariant(format!("{field} overflow")))
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|error| invariant(format!("{field}: {error}")))
}

fn from_i64(value: i64, field: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|error| invariant(format!("{field}: {error}")))
}

fn invariant(detail: impl Into<String>) -> StorageError {
    StorageError::InvariantViolation {
        entity: Some(ENTITY),
        detail: detail.into(),
    }
}

fn conflict(detail: impl Into<String>) -> StorageError {
    StorageError::StateConflict {
        entity: ENTITY,
        id: None,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
        types::{
            ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
            WeatherVariable,
        },
    };
    use rust_decimal::Decimal;

    use super::validate_weather_temperature_report;

    #[test]
    fn weather_projection_preserves_receipt_before_nominal_observation_time() {
        let station = IcaoStation::parse("KBKF").expect("station");
        let published_at = Utc.timestamp_millis_opt(1_752_802_715_000).unwrap();
        let observed_at = Utc.timestamp_millis_opt(1_752_802_800_000).unwrap();
        let mut report = WeatherObservationReport {
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::aviation_weather(&station),
            subject_key: station.to_string(),
            report_kind: WeatherObservationReportKind::Metar,
            variable: WeatherVariable::Temperature,
            value: Decimal::new(22, 0),
            unit: DomainMeasurementUnit::Celsius,
            precision: Decimal::new(1, 1),
            observed_at,
            valid_from: None,
            valid_to: None,
            published_at,
            available_at: published_at,
            report_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
                .expect("report hash"),
            raw_report: "official fixture".to_owned(),
        };

        assert!(validate_weather_temperature_report(&report).is_ok());

        report.available_at = published_at - Duration::milliseconds(1);
        assert!(validate_weather_temperature_report(&report).is_err());
    }
}
