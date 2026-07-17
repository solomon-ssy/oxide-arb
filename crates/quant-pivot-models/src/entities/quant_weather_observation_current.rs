//! Latest corrected observation for one station/local-day/observation instant.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use crate::types::ContentHash;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_weather_observation_current")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub station: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub local_date: NaiveDate,
    #[sea_orm(primary_key, auto_increment = false)]
    pub observation_time: DateTime<Utc>,
    pub temperature_celsius: Decimal,
    pub report_hash: ContentHash,
    pub revision: i64,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
