//! Durable entry-condition artifact, instance, lease, and audit DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    entities::{
        quant_entry_condition_artifact, quant_entry_condition_audit, quant_entry_condition_instance,
    },
    enums::quant::{EntryConditionAuditAction, EntryConditionState},
    types::{
        ConditionTruth, ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionArtifactId,
        EntryConditionArtifactV1, EntryConditionAuditId, EntryConditionFoldState,
        EntryConditionInstanceId, OrderIntentId, RecommendationId, TemperatureCelsius, Usd,
        WeatherTemperatureStatistic,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_entry_condition_artifact::Entity")]
pub struct EntryConditionArtifactInfo {
    pub artifact_id: EntryConditionArtifactId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub evaluator_version: i32,
    pub payload_json: EntryConditionArtifactV1,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    EntryConditionArtifactInfo,
    quant_entry_condition_artifact::Model,
    {
        artifact_id,
        content_hash,
        schema_version,
        evaluator_version,
        payload_json,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_entry_condition_artifact::ActiveModel")]
pub struct NewEntryConditionArtifact {
    pub artifact_id: EntryConditionArtifactId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub evaluator_version: i32,
    pub payload_json: EntryConditionArtifactV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_entry_condition_instance::Entity")]
pub struct EntryConditionInstanceInfo {
    pub condition_instance_id: EntryConditionInstanceId,
    pub recommendation_id: RecommendationId,
    pub artifact_id: Option<EntryConditionArtifactId>,
    pub artifact_hash: Option<ContentHash>,
    pub state: EntryConditionState,
    pub truth_json: Option<ConditionTruth>,
    pub revision: i64,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub fold_state_json: EntryConditionFoldState,
    pub confirmation_started_at: Option<DateTime<Utc>>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub next_evaluation_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub lease_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub lease_epoch: i64,
    pub claimed_by_intent_id: Option<OrderIntentId>,
    pub claim_admission_state_version: Option<ContentHash>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    EntryConditionInstanceInfo,
    quant_entry_condition_instance::Model,
    {
        condition_instance_id,
        recommendation_id,
        artifact_id,
        artifact_hash,
        state,
        truth_json,
        revision,
        evaluation_hash,
        input_fingerprint,
        continuity_hash,
        fold_state_json,
        confirmation_started_at,
        last_evaluated_at,
        next_evaluation_at,
        expires_at,
        lease_owner,
        lease_expires_at,
        lease_epoch,
        claimed_by_intent_id,
        claim_admission_state_version,
        consumed_at,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_entry_condition_instance::ActiveModel")]
pub struct NewEntryConditionInstance {
    pub condition_instance_id: EntryConditionInstanceId,
    pub recommendation_id: RecommendationId,
    pub artifact_id: Option<EntryConditionArtifactId>,
    pub artifact_hash: Option<ContentHash>,
    pub state: EntryConditionState,
    pub truth_json: Option<ConditionTruth>,
    pub revision: i64,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub fold_state_json: EntryConditionFoldState,
    pub confirmation_started_at: Option<DateTime<Utc>>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub next_evaluation_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub lease_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub lease_epoch: i64,
    pub claimed_by_intent_id: Option<OrderIntentId>,
    pub claim_admission_state_version: Option<ContentHash>,
    pub consumed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_entry_condition_audit::Entity")]
pub struct EntryConditionAuditInfo {
    pub audit_id: EntryConditionAuditId,
    pub condition_instance_id: EntryConditionInstanceId,
    pub revision: i64,
    pub action: EntryConditionAuditAction,
    pub from_state: Option<EntryConditionState>,
    pub to_state: EntryConditionState,
    pub truth_json: Option<ConditionTruth>,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub lease_epoch: i64,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(EntryConditionAuditInfo, quant_entry_condition_audit::Model, {
    audit_id,
    condition_instance_id,
    revision,
    action,
    from_state,
    to_state,
    truth_json,
    evaluation_hash,
    input_fingerprint,
    continuity_hash,
    lease_epoch,
    detail,
    occurred_at,
    created_at,
});

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_entry_condition_audit::ActiveModel")]
pub struct NewEntryConditionAudit {
    pub audit_id: EntryConditionAuditId,
    pub condition_instance_id: EntryConditionInstanceId,
    pub revision: i64,
    pub action: EntryConditionAuditAction,
    pub from_state: Option<EntryConditionState>,
    pub to_state: EntryConditionState,
    pub truth_json: Option<ConditionTruth>,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub lease_epoch: i64,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

/// CAS input for one live/replay evaluation transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyEntryConditionEvaluation {
    pub expected_revision: i64,
    pub expected_lease_epoch: i64,
    pub state: EntryConditionState,
    pub truth: ConditionTruth,
    pub evaluation_hash: ContentHash,
    pub input_fingerprint: ContentHash,
    pub continuity_hash: ContentHash,
    pub fold_state: EntryConditionFoldState,
    pub confirmation_started_at: Option<DateTime<Utc>>,
    pub evaluated_at: DateTime<Utc>,
    pub next_evaluation_at: Option<DateTime<Utc>>,
    pub evaluator_version: u32,
    pub tree_json: String,
}

/// Result of one evaluation CAS. `transitioned` is true only when durable
/// semantic state changed and a new WORM audit revision was appended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyEntryConditionEvaluationOutcome {
    pub instance: EntryConditionInstanceInfo,
    pub transitioned: bool,
}

/// Exact evidence revalidated by the submission claim transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryConditionClaim {
    pub condition_instance_id: EntryConditionInstanceId,
    pub order_intent_id: OrderIntentId,
    pub artifact_id: Option<EntryConditionArtifactId>,
    pub artifact_hash: Option<ContentHash>,
    pub expected_revision: i64,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub admission_state_version: ContentHash,
    pub claimed_at: DateTime<Utc>,
}

/// Current same-source crypto transition used by the live evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoPriceProjectionInfo {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub previous_price: Option<Usd>,
    pub current_price: Usd,
    pub source_sequence: u64,
    pub event_time: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub gap_generation: i64,
    pub source_healthy: bool,
}

/// Current corrected NOAA airport-local-day temperature extreme used by the
/// live evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherDailyTemperatureProjectionInfo {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub station: String,
    pub local_date: chrono::NaiveDate,
    pub timezone: String,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub current_extreme: TemperatureCelsius,
    pub last_observation_time: DateTime<Utc>,
    pub last_report_hash: ContentHash,
    pub revision: i64,
    pub day_closed: bool,
    pub gap_generation: i64,
    pub source_healthy: bool,
    pub available_at: DateTime<Utc>,
}
