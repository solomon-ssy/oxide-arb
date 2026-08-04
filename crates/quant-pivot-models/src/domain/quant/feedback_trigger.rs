//! Append-only provenance for every feedback-cycle trigger intent.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_feedback_trigger_event,
    enums::quant::{FeedbackEvaluationMode, FeedbackTriggerFamily},
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCycleId, FeedbackTriggerEventId, PolicyIdempotencyKey, RoleCode,
        UserId,
    },
};

const FORMAT_VERSION: u32 = 1;
const HASH_DOMAIN: &str = "quant-pivot/feedback-trigger-event";
const MAX_ACTOR_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 128;

#[derive(Serialize)]
struct TriggerEventPreimage<'a> {
    format_version: u32,
    feedback_cycle_id: FeedbackCycleId,
    trigger_family: FeedbackTriggerFamily,
    evaluation_mode: FeedbackEvaluationMode,
    idempotency_key: &'a PolicyIdempotencyKey,
    actor_user_id: Option<UserId>,
    actor_label: &'a str,
    actor_role: Option<&'a RoleCode>,
    reason_code: &'a str,
}

/// Complete trigger provenance supplied to the immutable event sealer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackTriggerEventInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub trigger_family: FeedbackTriggerFamily,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub actor_role: Option<RoleCode>,
    pub reason_code: String,
}

/// Validated immutable insert for one manual or scheduled trigger intent.
#[derive(Debug, Clone, PartialEq, Eq, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_feedback_trigger_event::ActiveModel")]
pub struct NewFeedbackTriggerEvent {
    pub feedback_trigger_event_id: FeedbackTriggerEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub trigger_family: FeedbackTriggerFamily,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub actor_role: Option<RoleCode>,
    pub reason_code: String,
    pub event_hash: ContentHash,
}

impl NewFeedbackTriggerEvent {
    pub fn try_seal(input: FeedbackTriggerEventInput) -> Result<Self, FeedbackError> {
        validate_actor(
            input.actor_user_id,
            &input.actor_label,
            input.actor_role.as_ref(),
            &input.reason_code,
        )?;
        let event_hash = CanonicalDigest::content_hash_typed(
            HASH_DOMAIN,
            FORMAT_VERSION,
            &TriggerEventPreimage {
                format_version: FORMAT_VERSION,
                feedback_cycle_id: input.feedback_cycle_id,
                trigger_family: input.trigger_family,
                evaluation_mode: input.evaluation_mode,
                idempotency_key: &input.idempotency_key,
                actor_user_id: input.actor_user_id,
                actor_label: &input.actor_label,
                actor_role: input.actor_role.as_ref(),
                reason_code: &input.reason_code,
            },
        )?;
        Ok(Self {
            feedback_trigger_event_id: FeedbackTriggerEventId::from_event_hash(&event_hash),
            feedback_cycle_id: input.feedback_cycle_id,
            trigger_family: input.trigger_family,
            evaluation_mode: input.evaluation_mode,
            idempotency_key: input.idempotency_key,
            actor_user_id: input.actor_user_id,
            actor_label: input.actor_label,
            actor_role: input.actor_role,
            reason_code: input.reason_code,
            event_hash,
        })
    }
}

/// Durable trigger provenance returned to API and evidence consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_feedback_trigger_event::Entity")]
pub struct FeedbackTriggerEventInfo {
    pub feedback_trigger_event_id: FeedbackTriggerEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub trigger_family: FeedbackTriggerFamily,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub actor_role: Option<RoleCode>,
    pub reason_code: String,
    pub event_hash: ContentHash,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    FeedbackTriggerEventInfo,
    quant_feedback_trigger_event::Model,
    {
        feedback_trigger_event_id,
        feedback_cycle_id,
        trigger_family,
        evaluation_mode,
        idempotency_key,
        actor_user_id,
        actor_label,
        actor_role,
        reason_code,
        event_hash,
        occurred_at,
        created_at,
    }
);

impl FeedbackTriggerEventInfo {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        validate_actor(
            self.actor_user_id,
            &self.actor_label,
            self.actor_role.as_ref(),
            &self.reason_code,
        )?;
        let event_hash = CanonicalDigest::content_hash_typed(
            HASH_DOMAIN,
            FORMAT_VERSION,
            &TriggerEventPreimage {
                format_version: FORMAT_VERSION,
                feedback_cycle_id: self.feedback_cycle_id,
                trigger_family: self.trigger_family,
                evaluation_mode: self.evaluation_mode,
                idempotency_key: &self.idempotency_key,
                actor_user_id: self.actor_user_id,
                actor_label: &self.actor_label,
                actor_role: self.actor_role.as_ref(),
                reason_code: &self.reason_code,
            },
        )?;
        if self.feedback_trigger_event_id != FeedbackTriggerEventId::from_event_hash(&event_hash)
            || self.event_hash != event_hash
            || self.created_at != self.occurred_at
        {
            return Err(invalid(
                "feedback trigger event identity, hash, or database timestamp is invalid",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn matches_new(&self, event: &NewFeedbackTriggerEvent) -> bool {
        self.feedback_trigger_event_id == event.feedback_trigger_event_id
            && self.feedback_cycle_id == event.feedback_cycle_id
            && self.trigger_family == event.trigger_family
            && self.evaluation_mode == event.evaluation_mode
            && self.idempotency_key == event.idempotency_key
            && self.actor_user_id == event.actor_user_id
            && self.actor_label == event.actor_label
            && self.actor_role == event.actor_role
            && self.reason_code == event.reason_code
            && self.event_hash == event.event_hash
    }
}

fn validate_actor(
    actor_user_id: Option<UserId>,
    actor_label: &str,
    actor_role: Option<&RoleCode>,
    reason_code: &str,
) -> Result<(), FeedbackError> {
    let governed_shape = actor_user_id.is_some() == actor_role.is_some();
    let valid_actor = !actor_label.is_empty()
        && actor_label.len() <= MAX_ACTOR_BYTES
        && actor_label == actor_label.trim()
        && !actor_label.chars().any(char::is_control);
    let valid_role = actor_role.is_none_or(|role| {
        let role = role.as_str();
        !role.is_empty()
            && role.len() <= 64
            && role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    });
    let valid_reason = !reason_code.is_empty()
        && reason_code.len() <= MAX_REASON_BYTES
        && reason_code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if governed_shape && valid_actor && valid_role && valid_reason {
        Ok(())
    } else {
        Err(invalid(
            "feedback trigger actor, role, or reason code violates its contract",
        ))
    }
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidCycleIdentity {
        detail: detail.into(),
    }
}
