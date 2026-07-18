use sea_orm_migration::prelude::*;

use crate::{MigrationSpec, audit, migration_spec};

use super::support::{phase_11_9, v1};

const NAME: &str = "m20260718_000006_weather_daily_temperature_projection";
const SOURCE: &[u8] = include_bytes!("m20260718_000006_weather_daily_temperature_projection.rs");

const UP_SQL: &str = r#"
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM quant_weather_daily_high_projection projection
        WHERE NOT EXISTS (
            SELECT 1
            FROM quant_weather_observation_current observation
            WHERE observation.station = projection.station
              AND observation.local_date = projection.local_date
        )
    ) THEN
        RAISE EXCEPTION 'weather projection migration found a row without current observations';
    END IF;
END
$$;

ALTER TABLE quant_weather_daily_high_projection
    RENAME TO quant_weather_daily_temperature_projection;
ALTER TABLE quant_weather_daily_temperature_projection
    RENAME COLUMN current_high_celsius TO current_extreme_celsius;
ALTER TABLE quant_weather_daily_temperature_projection
    RENAME COLUMN previous_high_celsius TO previous_extreme_celsius;
ALTER TABLE quant_weather_daily_temperature_projection
    ADD COLUMN temperature_statistic TEXT NOT NULL DEFAULT 'maximum';
ALTER TABLE quant_weather_daily_temperature_projection
    ALTER COLUMN temperature_statistic DROP DEFAULT;
ALTER TABLE quant_weather_daily_temperature_projection
    ADD CONSTRAINT ck_quant_weather_daily_temperature_statistic
        CHECK (temperature_statistic IN ('maximum', 'minimum'));
ALTER TABLE quant_weather_daily_temperature_projection
    DROP CONSTRAINT "pk-quant_weather_daily_high_projection";
ALTER TABLE quant_weather_daily_temperature_projection
    ADD CONSTRAINT "pk-quant_weather_daily_temperature_projection"
        PRIMARY KEY (source_id, instrument_key, local_date, temperature_statistic);

ALTER INDEX idx_quant_weather_daily_high_open
    RENAME TO idx_quant_weather_daily_temperature_open;
ALTER TRIGGER trg_quant_weather_daily_high_projection_updated_at
    ON quant_weather_daily_temperature_projection
    RENAME TO trg_quant_weather_daily_temperature_projection_updated_at;

INSERT INTO quant_weather_daily_temperature_projection (
    source_id,
    instrument_key,
    station,
    local_date,
    temperature_statistic,
    timezone,
    current_extreme_celsius,
    previous_extreme_celsius,
    last_observation_time,
    last_report_hash,
    last_event_id,
    revision,
    day_closed,
    gap_generation,
    source_healthy,
    available_at,
    updated_at
)
SELECT
    projection.source_id,
    projection.instrument_key,
    projection.station,
    projection.local_date,
    'minimum',
    projection.timezone,
    MIN(observation.temperature_celsius),
    NULL,
    projection.last_observation_time,
    projection.last_report_hash,
    NULL,
    projection.revision,
    projection.day_closed,
    projection.gap_generation,
    projection.source_healthy,
    projection.available_at,
    projection.updated_at
FROM quant_weather_daily_temperature_projection projection
JOIN quant_weather_observation_current observation
  ON observation.station = projection.station
 AND observation.local_date = projection.local_date
WHERE projection.temperature_statistic = 'maximum'
GROUP BY
    projection.source_id,
    projection.instrument_key,
    projection.station,
    projection.local_date,
    projection.timezone,
    projection.last_observation_time,
    projection.last_report_hash,
    projection.revision,
    projection.day_closed,
    projection.gap_generation,
    projection.source_healthy,
    projection.available_at,
    projection.updated_at;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM quant_weather_daily_temperature_projection maximum
        WHERE maximum.temperature_statistic = 'maximum'
          AND NOT EXISTS (
              SELECT 1
              FROM quant_weather_daily_temperature_projection minimum
              WHERE minimum.source_id = maximum.source_id
                AND minimum.instrument_key = maximum.instrument_key
                AND minimum.local_date = maximum.local_date
                AND minimum.temperature_statistic = 'minimum'
          )
    ) THEN
        RAISE EXCEPTION 'weather projection migration failed to rebuild a minimum row';
    END IF;
END
$$;
"#;

const DOWN_SQL: &str = r#"
DELETE FROM quant_weather_daily_temperature_projection
WHERE temperature_statistic = 'minimum';

ALTER TRIGGER trg_quant_weather_daily_temperature_projection_updated_at
    ON quant_weather_daily_temperature_projection
    RENAME TO trg_quant_weather_daily_high_projection_updated_at;
ALTER INDEX idx_quant_weather_daily_temperature_open
    RENAME TO idx_quant_weather_daily_high_open;
ALTER TABLE quant_weather_daily_temperature_projection
    DROP CONSTRAINT "pk-quant_weather_daily_temperature_projection";
ALTER TABLE quant_weather_daily_temperature_projection
    ADD CONSTRAINT "pk-quant_weather_daily_high_projection"
        PRIMARY KEY (source_id, instrument_key, local_date);
ALTER TABLE quant_weather_daily_temperature_projection
    DROP CONSTRAINT ck_quant_weather_daily_temperature_statistic;
ALTER TABLE quant_weather_daily_temperature_projection
    DROP COLUMN temperature_statistic;
ALTER TABLE quant_weather_daily_temperature_projection
    RENAME COLUMN current_extreme_celsius TO current_high_celsius;
ALTER TABLE quant_weather_daily_temperature_projection
    RENAME COLUMN previous_extreme_celsius TO previous_high_celsius;
ALTER TABLE quant_weather_daily_temperature_projection
    RENAME TO quant_weather_daily_high_projection;
"#;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        phase_11_9::execute_batch(manager, UP_SQL).await?;
        audit::record(manager, spec()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        audit::remove(manager, NAME).await?;
        phase_11_9::execute_batch(manager, DOWN_SQL).await?;
        Ok(())
    }
}

pub fn spec() -> MigrationSpec {
    migration_spec(NAME, &[SOURCE, phase_11_9::SOURCE, v1::SOURCE])
}
