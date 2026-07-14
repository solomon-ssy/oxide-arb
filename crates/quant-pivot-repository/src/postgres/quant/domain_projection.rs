//! Atomic typed domain projections and durable `ClickHouse` event outbox.

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CryptoPriceProjectionInfo, CryptoPriceReport, CryptoPriceTransition, DomainEventEnvelope,
        DomainEventPayload, DomainEventType, DomainSourceCheckpoint, UpsertDomainSourceCursor,
        WeatherDailyHighChange, WeatherDailyHighProjectionInfo, WeatherObservationDayClosed,
        WeatherObservationReport, WeatherObservationReportKind,
    },
    entities::{
        quant_crypto_price_projection, quant_domain_event_outbox, quant_domain_source_cursor,
        quant_weather_daily_high_projection, quant_weather_observation_current,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainEventId, DomainInstrumentKey, DomainSourceId, IcaoStation,
        TemperatureCelsius, Usd,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};
use uuid::Uuid;

use crate::{
    postgres::quant::condition_wake::notify_input_change, traits::DomainProjectionRepository,
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
        let checkpoint_hash = hash_checkpoint(&checkpoint)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let existing = quant_crypto_price_projection::Entity::find_by_id((
            report.source_id.clone(),
            report.instrument_key.clone(),
        ))
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?;

        let model = match existing {
            Some(existing) if existing.report_hash == report.report_hash => existing,
            Some(existing) => {
                if report.source_sequence < existing.source_sequence
                    || (report.source_sequence == existing.source_sequence
                        && report.available_at <= existing.available_at)
                {
                    return Err(conflict("crypto report is older than current projection"));
                }
                let previous_price = existing.current_price;
                let mut active = existing.into_active_model();
                active.previous_price = ActiveValue::Set(Some(previous_price));
                active.current_price = ActiveValue::Set(report.price);
                active.source_sequence = ActiveValue::Set(report.source_sequence);
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
            None => quant_crypto_price_projection::ActiveModel {
                source_id: ActiveValue::Set(report.source_id.clone()),
                instrument_key: ActiveValue::Set(report.instrument_key.clone()),
                previous_price: ActiveValue::Set(None),
                current_price: ActiveValue::Set(report.price),
                source_sequence: ActiveValue::Set(report.source_sequence),
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
        Ok(crypto_info(model))
    }

    async fn apply_weather_report(
        &self,
        report: WeatherObservationReport,
        timezone: String,
        local_date: NaiveDate,
        checkpoint: DomainSourceCheckpoint,
        gap_generation: u64,
        source_healthy: bool,
    ) -> Result<WeatherDailyHighProjectionInfo, StorageError> {
        let instrument_key = DomainInstrumentKey::aviation_weather(&report.station);
        validate_binding(&report.source_id, &instrument_key)?;
        let gap_generation = to_i64(gap_generation, "weather gap generation")?;
        let checkpoint_hash = hash_checkpoint(&checkpoint)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let observation_changed = upsert_weather_observation(&txn, &report, local_date).await?;
        let (model, event) = upsert_weather_daily_projection(
            &txn,
            WeatherProjectionInput {
                report: &report,
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
        if let Some(event) = event {
            insert_outbox(&txn, event).await?;
        }
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
        Ok(weather_info(model))
    }

    async fn close_weather_day(
        &self,
        station: &IcaoStation,
        local_date: NaiveDate,
        closed_at: DateTime<Utc>,
    ) -> Result<Option<WeatherDailyHighProjectionInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let instrument_key = DomainInstrumentKey::aviation_weather(station);
        let row = quant_weather_daily_high_projection::Entity::find_by_id((
            DomainSourceId::aviation_weather(),
            instrument_key,
            local_date,
        ))
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        if row.day_closed {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(Some(weather_info(row)));
        }
        let cursor = quant_domain_source_cursor::Entity::find_by_id((
            DomainSourceId::aviation_weather(),
            DomainInstrumentKey::aviation_weather(station),
        ))
        .lock_shared()
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| invariant("weather projection has no authoritative source cursor"))?;
        let event = weather_close_event(&row, closed_at, cursor.checkpoint_hash)?;
        let revision = checked_add(row.revision, "weather daily close revision")?;
        let mut active = row.into_active_model();
        active.day_closed = ActiveValue::Set(true);
        active.revision = ActiveValue::Set(revision);
        active.last_event_id = ActiveValue::Set(Some(event.id.clone()));
        active.available_at = ActiveValue::Set(closed_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        insert_outbox(&txn, event).await?;
        notify_input_change(&txn, "weather_day_closed").await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(weather_info(updated)))
    }

    async fn mark_crypto_source_gap(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_crypto_price_projection::Entity::find_by_id((
            source_id.clone(),
            instrument_key.clone(),
        ))
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
        let row = quant_weather_daily_high_projection::Entity::find_by_id((
            DomainSourceId::aviation_weather(),
            instrument_key,
            local_date,
        ))
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(0);
        };
        let generation = checked_add(row.gap_generation, "weather gap generation")?;
        let mut active = row.into_active_model();
        active.gap_generation = ActiveValue::Set(generation);
        active.source_healthy = ActiveValue::Set(false);
        active.available_at = ActiveValue::Set(observed_at);
        active.update(&txn).await.map_err(StorageError::from)?;
        notify_input_change(&txn, "weather_gap").await?;
        txn.commit().await.map_err(StorageError::from)?;
        from_i64(generation, "weather gap generation")
    }

    async fn claim_pending_events(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<DomainEventEnvelope>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rows = quant_domain_event_outbox::Entity::find()
            .filter(quant_domain_event_outbox::Column::PublishedAt.is_null())
            .filter(
                Condition::any()
                    .add(quant_domain_event_outbox::Column::LeaseExpiresAt.is_null())
                    .add(quant_domain_event_outbox::Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(quant_domain_event_outbox::Column::CreatedAt)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let attempts = row
                .publish_attempts
                .checked_add(1)
                .ok_or_else(|| invariant("domain event publish attempt overflow"))?;
            let mut active = row.into_active_model();
            active.claim_owner = ActiveValue::Set(Some(worker_id));
            active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
            active.publish_attempts = ActiveValue::Set(attempts);
            let claimed = active.update(&txn).await.map_err(StorageError::from)?;
            events.push(claimed.envelope_json);
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(events)
    }

    async fn mark_event_published(
        &self,
        event_id: &DomainEventId,
        worker_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        quant_domain_event_outbox::Entity::update_many()
            .col_expr(
                quant_domain_event_outbox::Column::PublishedAt,
                Expr::value(published_at),
            )
            .col_expr(
                quant_domain_event_outbox::Column::LastError,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                quant_domain_event_outbox::Column::ClaimOwner,
                Expr::value(Option::<Uuid>::None),
            )
            .col_expr(
                quant_domain_event_outbox::Column::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(quant_domain_event_outbox::Column::EventId.eq(event_id.clone()))
            .filter(quant_domain_event_outbox::Column::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn mark_event_failed(
        &self,
        event_id: &DomainEventId,
        worker_id: Uuid,
        detail: String,
    ) -> Result<(), StorageError> {
        quant_domain_event_outbox::Entity::update_many()
            .col_expr(
                quant_domain_event_outbox::Column::LastError,
                Expr::value(Some(detail)),
            )
            .col_expr(
                quant_domain_event_outbox::Column::ClaimOwner,
                Expr::value(Option::<Uuid>::None),
            )
            .col_expr(
                quant_domain_event_outbox::Column::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(quant_domain_event_outbox::Column::EventId.eq(event_id.clone()))
            .filter(quant_domain_event_outbox::Column::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

async fn upsert_weather_observation(
    txn: &DatabaseTransaction,
    report: &WeatherObservationReport,
    local_date: NaiveDate,
) -> Result<bool, StorageError> {
    let observation_key = (
        report.station.as_str().to_owned(),
        local_date,
        report.observation_time,
    );
    let observation = quant_weather_observation_current::Entity::find_by_id(observation_key)
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
            active.temperature_celsius = ActiveValue::Set(report.temperature.value());
            active.report_hash = ActiveValue::Set(report.report_hash.clone());
            active.revision = ActiveValue::Set(revision);
            active.published_at = ActiveValue::Set(report.published_at);
            active.available_at = ActiveValue::Set(report.available_at);
            active.update(txn).await.map_err(StorageError::from)?;
        }
        None => {
            quant_weather_observation_current::ActiveModel {
                station: ActiveValue::Set(report.station.as_str().to_owned()),
                local_date: ActiveValue::Set(local_date),
                observation_time: ActiveValue::Set(report.observation_time),
                temperature_celsius: ActiveValue::Set(report.temperature.value()),
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
    timezone: &'a str,
    local_date: NaiveDate,
    instrument_key: &'a DomainInstrumentKey,
    gap_generation: i64,
    source_healthy: bool,
    checkpoint_hash: &'a ContentHash,
    observation_changed: bool,
}

async fn upsert_weather_daily_projection(
    txn: &DatabaseTransaction,
    input: WeatherProjectionInput<'_>,
) -> Result<
    (
        quant_weather_daily_high_projection::Model,
        Option<DomainEventEnvelope>,
    ),
    StorageError,
> {
    let daily_max = quant_weather_observation_current::Entity::find()
        .filter(
            quant_weather_observation_current::Column::Station.eq(input.report.station.as_str()),
        )
        .filter(quant_weather_observation_current::Column::LocalDate.eq(input.local_date))
        .order_by_desc(quant_weather_observation_current::Column::TemperatureCelsius)
        .order_by_desc(quant_weather_observation_current::Column::AvailableAt)
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| invariant("weather daily projection has no current observations"))?;
    let existing = quant_weather_daily_high_projection::Entity::find_by_id((
        input.report.source_id.clone(),
        input.instrument_key.clone(),
        input.local_date,
    ))
    .lock_exclusive()
    .one(txn)
    .await
    .map_err(StorageError::from)?;
    let previous_high = existing.as_ref().map(|row| row.current_high_celsius);
    let current_high = daily_max.temperature_celsius;
    let event = if input.observation_changed && previous_high != Some(current_high) {
        Some(weather_change_event(WeatherChangeEventInput {
            report: input.report,
            local_date: input.local_date,
            previous_high,
            current_high,
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
            update_weather_daily_projection(
                txn,
                existing,
                &input,
                previous_high,
                current_high,
                event.as_ref(),
            )
            .await?
        }
        None => insert_weather_daily_projection(txn, &input, current_high, event.as_ref()).await?,
    };
    Ok((model, event))
}

async fn update_weather_daily_projection(
    txn: &DatabaseTransaction,
    existing: quant_weather_daily_high_projection::Model,
    input: &WeatherProjectionInput<'_>,
    previous_high: Option<rust_decimal::Decimal>,
    current_high: rust_decimal::Decimal,
    event: Option<&DomainEventEnvelope>,
) -> Result<quant_weather_daily_high_projection::Model, StorageError> {
    if existing.timezone != input.timezone {
        return Err(conflict("weather station timezone binding drift"));
    }
    let revision = checked_add(existing.revision, "weather daily revision")?;
    let last_event_id = event.map_or_else(
        || existing.last_event_id.clone(),
        |event| Some(event.id.clone()),
    );
    let mut active = existing.into_active_model();
    active.previous_high_celsius = ActiveValue::Set(previous_high);
    active.current_high_celsius = ActiveValue::Set(current_high);
    active.last_observation_time = ActiveValue::Set(input.report.observation_time);
    active.last_report_hash = ActiveValue::Set(input.report.report_hash.clone());
    active.last_event_id = ActiveValue::Set(last_event_id);
    active.revision = ActiveValue::Set(revision);
    active.gap_generation = ActiveValue::Set(input.gap_generation);
    active.source_healthy = ActiveValue::Set(input.source_healthy);
    active.available_at = ActiveValue::Set(input.report.available_at);
    active.update(txn).await.map_err(StorageError::from)
}

async fn insert_weather_daily_projection(
    txn: &DatabaseTransaction,
    input: &WeatherProjectionInput<'_>,
    current_high: rust_decimal::Decimal,
    event: Option<&DomainEventEnvelope>,
) -> Result<quant_weather_daily_high_projection::Model, StorageError> {
    quant_weather_daily_high_projection::ActiveModel {
        source_id: ActiveValue::Set(input.report.source_id.clone()),
        instrument_key: ActiveValue::Set(input.instrument_key.clone()),
        station: ActiveValue::Set(input.report.station.as_str().to_owned()),
        local_date: ActiveValue::Set(input.local_date),
        timezone: ActiveValue::Set(input.timezone.to_owned()),
        current_high_celsius: ActiveValue::Set(current_high),
        previous_high_celsius: ActiveValue::Set(None),
        last_observation_time: ActiveValue::Set(input.report.observation_time),
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
    previous_high: Option<rust_decimal::Decimal>,
    current_high: rust_decimal::Decimal,
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
            .previous_high
            .is_some_and(|previous| input.current_high < previous);
    let event_type = if corrected {
        DomainEventType::WeatherDailyHighCorrected
    } else {
        DomainEventType::WeatherDailyHighAdvanced
    };
    let change = WeatherDailyHighChange {
        station: input.report.station.to_string(),
        local_date: input.local_date,
        previous_high: input.previous_high.map(TemperatureCelsius::new),
        current_high: TemperatureCelsius::new(input.current_high),
        report_hash: input.report.report_hash.clone(),
        gap_generation: from_i64(input.gap_generation, "weather gap generation")?,
    };
    let payload = if corrected {
        DomainEventPayload::WeatherDailyHighCorrected(change)
    } else {
        DomainEventPayload::WeatherDailyHighAdvanced(change)
    };
    build_event(EventBuildInput {
        source: input.report.source_id.clone(),
        event_type,
        subject: format!("{}:{}", input.report.station, input.local_date),
        time: input.report.observation_time,
        published_at: input.report.published_at,
        available_at: input.report.available_at,
        supersedes_event_id: input.supersedes_event_id,
        source_checkpoint_hash: input.checkpoint_hash,
        payload,
    })
}

fn weather_close_event(
    row: &quant_weather_daily_high_projection::Model,
    closed_at: DateTime<Utc>,
    checkpoint_hash: ContentHash,
) -> Result<DomainEventEnvelope, StorageError> {
    build_event(EventBuildInput {
        source: DomainSourceId::aviation_weather(),
        event_type: DomainEventType::WeatherObservationDayClosed,
        subject: format!("{}:{}", row.station, row.local_date),
        time: closed_at,
        published_at: closed_at,
        available_at: closed_at,
        supersedes_event_id: row.last_event_id.clone(),
        source_checkpoint_hash: checkpoint_hash,
        payload: DomainEventPayload::WeatherObservationDayClosed(WeatherObservationDayClosed {
            station: row.station.clone(),
            local_date: row.local_date,
            final_noaa_high: TemperatureCelsius::new(row.current_high_celsius),
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

async fn insert_outbox<C: sea_orm::ConnectionTrait>(
    db: &C,
    event: DomainEventEnvelope,
) -> Result<(), StorageError> {
    quant_domain_event_outbox::Entity::insert(quant_domain_event_outbox::ActiveModel {
        event_id: ActiveValue::Set(event.id.clone()),
        envelope_json: ActiveValue::Set(event),
        published_at: ActiveValue::Set(None),
        publish_attempts: ActiveValue::Set(0),
        claim_owner: ActiveValue::Set(None),
        lease_expires_at: ActiveValue::Set(None),
        last_error: ActiveValue::Set(None),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::column(quant_domain_event_outbox::Column::EventId)
            .do_nothing()
            .to_owned(),
    )
    .do_nothing()
    .exec(db)
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

async fn upsert_cursor<C: sea_orm::ConnectionTrait>(
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
        status: "live".to_owned(),
        last_error: None,
        updated_at: Utc::now(),
    };
    quant_domain_source_cursor::Entity::insert(cursor.into_active_model())
        .on_conflict(
            OnConflict::columns([
                quant_domain_source_cursor::Column::SourceId,
                quant_domain_source_cursor::Column::InstrumentKey,
            ])
            .update_columns([
                quant_domain_source_cursor::Column::CheckpointJson,
                quant_domain_source_cursor::Column::CheckpointHash,
                quant_domain_source_cursor::Column::Status,
                quant_domain_source_cursor::Column::LastError,
                quant_domain_source_cursor::Column::UpdatedAt,
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

fn hash_checkpoint(checkpoint: &DomainSourceCheckpoint) -> Result<ContentHash, StorageError> {
    CanonicalDigest::content_hash_json(checkpoint).map_err(|error| invariant(error.to_string()))
}

fn crypto_info(row: quant_crypto_price_projection::Model) -> CryptoPriceProjectionInfo {
    CryptoPriceProjectionInfo {
        source_id: row.source_id,
        instrument_key: row.instrument_key,
        previous_price: row.previous_price,
        current_price: row.current_price,
        source_sequence: row.source_sequence,
        event_time: row.event_time,
        available_at: row.available_at,
        report_hash: row.report_hash,
        gap_generation: row.gap_generation,
        source_healthy: row.source_healthy,
    }
}

fn weather_info(row: quant_weather_daily_high_projection::Model) -> WeatherDailyHighProjectionInfo {
    WeatherDailyHighProjectionInfo {
        source_id: row.source_id,
        instrument_key: row.instrument_key,
        station: row.station,
        local_date: row.local_date,
        timezone: row.timezone,
        current_high: TemperatureCelsius::new(row.current_high_celsius),
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
