use sea_orm_migration::prelude::*;

use crate::{MigrationSpec, audit, migration_spec};

use super::support::{phase_11_9, v1};

const NAME: &str = "m20260718_000007_airnow_pm25_operational_state";
const SOURCE: &[u8] = include_bytes!("m20260718_000007_airnow_pm25_operational_state.rs");

const UP_SQL: &str = r"
DELETE FROM quant_domain_source_expectation
WHERE source_id = 'airnow'
  AND instrument_key ~ '^AIRNOW:[^:]+:[^:]+:(OBS|FORECAST)$';

DELETE FROM quant_domain_source_cursor
WHERE source_id = 'airnow'
  AND (
      instrument_key ~ '^AIRNOW:[^:]+:[^:]+:(OBS|FORECAST)$'
      OR checkpoint_json ->> 'kind' IN ('air_now', 'air_now_forecast')
  );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM quant_domain_source_cursor
        WHERE source_id = 'airnow'
          AND checkpoint_json ->> 'kind' IN ('air_now', 'air_now_forecast')
    ) THEN
        RAISE EXCEPTION 'legacy generic AirNow cursor survived PM2.5 operational-state cleanup';
    END IF;
END
$$;
";

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
        audit::remove(manager, NAME).await
    }
}

pub fn spec() -> MigrationSpec {
    migration_spec(NAME, &[SOURCE, phase_11_9::SOURCE, v1::SOURCE])
}
