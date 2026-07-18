//! Current corrected airport-local-day maximum/minimum-temperature projection.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, DomainEventId, DomainInstrumentKey, DomainSourceId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_weather_daily_temperature_projection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: DomainSourceId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub instrument_key: DomainInstrumentKey,
    #[sea_orm(primary_key, auto_increment = false)]
    pub local_date: NaiveDate,
    #[sea_orm(primary_key, auto_increment = false)]
    pub temperature_statistic: String,
    pub station: String,
    pub timezone: String,
    pub current_extreme_celsius: Decimal,
    pub previous_extreme_celsius: Option<Decimal>,
    pub last_observation_time: DateTime<Utc>,
    pub last_report_hash: ContentHash,
    pub last_event_id: Option<DomainEventId>,
    pub revision: i64,
    pub day_closed: bool,
    pub gap_generation: i64,
    pub source_healthy: bool,
    pub available_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
