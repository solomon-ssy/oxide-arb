//! Typed raw domain facts, derived events, and condition evaluation traces.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::{ChDecimal64, ChSchemaVersion},
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionInstanceId},
};

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct CryptoPriceReportRow {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub source_sequence: u64,
    pub price: ChDecimal64,
    pub quantity: Option<ChDecimal64>,
    pub event_time: i64,
    pub published_at: i64,
    pub available_at: i64,
    pub valid_from: Option<i64>,
    pub observations_timestamp: Option<i64>,
    pub expires_at: Option<i64>,
    pub report_hash: ContentHash,
    pub raw_report: String,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct WeatherObservationReportRow {
    pub source_id: DomainSourceId,
    pub station: String,
    pub local_date: String,
    pub report_kind: String,
    pub temperature_celsius: ChDecimal64,
    pub precision_celsius: ChDecimal64,
    pub observation_time: i64,
    pub published_at: i64,
    pub available_at: i64,
    pub revision: u32,
    pub report_hash: ContentHash,
    pub supersedes_report_hash: Option<ContentHash>,
    pub raw_report: String,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct WeatherForecastPointRow {
    pub source_id: DomainSourceId,
    pub station: String,
    pub reference_time: i64,
    pub valid_time: i64,
    pub available_at: i64,
    pub lead_hours: u16,
    pub member: u8,
    pub tmax_celsius: ChDecimal64,
    pub grid_binding_hash: ContentHash,
    pub run_manifest_hash: ContentHash,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct DomainEventRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    pub source: String,
    pub event_type: String,
    pub subject: String,
    pub event_time: i64,
    pub published_at: i64,
    pub available_at: i64,
    pub schema_version: ChSchemaVersion,
    pub revision: u32,
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub supersedes_event_id: Option<Uuid>,
    pub payload_hash: ContentHash,
    pub source_checkpoint_hash: ContentHash,
    pub payload_json: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize, FromJsonQueryResult,
)]
pub struct EntryConditionEvaluationEventRow {
    pub evaluation_id: ContentHash,
    pub condition_instance_id: EntryConditionInstanceId,
    pub base_revision: i64,
    pub applied_revision: Option<i64>,
    pub trace_kind: String,
    pub evaluator_version: u32,
    pub evaluated_at: i64,
    pub state: String,
    pub truth: String,
    pub evaluation_hash: ContentHash,
    pub input_fingerprint: ContentHash,
    pub continuity_hash: ContentHash,
    pub tree_json: String,
    pub schema_version: ChSchemaVersion,
}
