//! Durable PostgreSQL-authoritative feedback scheduler contracts.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_feedback_scheduler_state,
    types::{
        ContentHash, FeedbackCycleId, ResearchProfileArtifact, ResearchProfileArtifactId,
        ResearchProfileId, WorkerId,
    },
};

const MAX_CONTROL_NOTE_BYTES: usize = 1_024;

/// Initial or profile-version reconciliation payload.
#[derive(Debug, Clone, PartialEq, Eq, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_scheduler_state::ActiveModel")]
pub struct NewFeedbackSchedulerState {
    pub research_profile_id: ResearchProfileId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub cadence_secs: i64,
    pub cooldown_secs: i64,
    pub next_due_at: DateTime<Utc>,
}

impl NewFeedbackSchedulerState {
    pub fn try_new(
        profile: &ResearchProfileArtifact,
        database_now: DateTime<Utc>,
    ) -> Result<Self, FeedbackError> {
        let cadence_secs = i64::try_from(profile.spec.feedback_policy.feedback_cadence_secs)
            .map_err(|error| {
                invalid(format!(
                    "feedback cadence exceeds PostgreSQL bigint: {error}"
                ))
            })?;
        let cooldown_secs = i64::try_from(profile.spec.feedback_policy.retraining_cooldown_secs)
            .map_err(|error| {
                invalid(format!(
                    "feedback cooldown exceeds PostgreSQL bigint: {error}"
                ))
            })?;
        let state = Self {
            research_profile_id: profile.profile_ref.id.clone(),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                &profile.profile_ref,
            ),
            profile_hash: profile.profile_ref.content_hash,
            feedback_policy_hash: profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| invalid(error.to_string()))?,
            cadence_secs,
            cooldown_secs,
            next_due_at: cadence_cutoff(database_now, cadence_secs)?,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.research_profile_id.as_str().trim().is_empty()
            || self.cadence_secs <= 0
            || self.cooldown_secs < self.cadence_secs
            || self.next_due_at.timestamp() <= 0
        {
            return Err(invalid(
                "feedback scheduler profile, cadence, cooldown, or first due time is invalid",
            ));
        }
        Ok(())
    }
}

/// Full scheduler row exposed to operations and API views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_scheduler_state::Entity")]
pub struct FeedbackSchedulerStateInfo {
    pub research_profile_id: ResearchProfileId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub cadence_secs: i64,
    pub cooldown_secs: i64,
    pub next_due_at: DateTime<Utc>,
    pub last_cycle_id: Option<FeedbackCycleId>,
    pub last_cutoff: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub paused: bool,
    pub pause_revision: i64,
    pub pause_reason_code: Option<String>,
    pub pause_note: Option<String>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    FeedbackSchedulerStateInfo,
    quant_feedback_scheduler_state::Model,
    {
        research_profile_id,
        research_profile_artifact_id,
        profile_hash,
        feedback_policy_hash,
        cadence_secs,
        cooldown_secs,
        next_due_at,
        last_cycle_id,
        last_cutoff,
        cooldown_until,
        lease_owner,
        lease_expires_at,
        attempt,
        retry_at,
        last_error,
        paused,
        pause_revision,
        pause_reason_code,
        pause_note,
        revision,
        created_at,
        updated_at,
    }
);

impl FeedbackSchedulerStateInfo {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        let lease_shape = self.lease_owner.is_some() == self.lease_expires_at.is_some();
        let pause_shape = if self.paused {
            self.pause_reason_code.as_deref().is_some_and(valid_reason)
                && self
                    .pause_note
                    .as_deref()
                    .is_some_and(|note| valid_note(note, MAX_CONTROL_NOTE_BYTES))
        } else {
            self.pause_reason_code.is_none() && self.pause_note.is_none()
        };
        let completion_shape = self.last_cycle_id.is_some() == self.last_cutoff.is_some();
        let retry_shape = self.retry_at.is_some() == self.last_error.is_some()
            && self
                .last_error
                .as_deref()
                .is_none_or(|error| valid_note(error, 4_096));
        if self.research_profile_id.as_str().trim().is_empty()
            || self.cadence_secs <= 0
            || self.cooldown_secs < self.cadence_secs
            || self.next_due_at.timestamp() <= 0
            || self.attempt < 0
            || self.pause_revision < 0
            || self.revision < 0
            || self.updated_at < self.created_at
            || !lease_shape
            || !pause_shape
            || !completion_shape
            || !retry_shape
        {
            return Err(invalid(
                "persisted feedback scheduler state violates its lifecycle contract",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn matches_profile(&self, candidate: &NewFeedbackSchedulerState) -> bool {
        self.research_profile_id == candidate.research_profile_id
            && self.research_profile_artifact_id == candidate.research_profile_artifact_id
            && self.profile_hash == candidate.profile_hash
            && self.feedback_policy_hash == candidate.feedback_policy_hash
            && self.cadence_secs == candidate.cadence_secs
            && self.cooldown_secs == candidate.cooldown_secs
    }
}

/// Exact scheduler lease precondition for one claimed profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackSchedulerLease {
    pub research_profile_id: ResearchProfileId,
    pub expected_revision: i64,
    pub worker_id: WorkerId,
}

/// One due profile claimed with `FOR UPDATE SKIP LOCKED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackSchedulerClaim {
    pub state: FeedbackSchedulerStateInfo,
    pub lease: FeedbackSchedulerLease,
    pub claimed_at: DateTime<Utc>,
}

/// Successful materialization cursor update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackSchedulerSuccess {
    pub feedback_cycle_id: FeedbackCycleId,
    pub label_cutoff: DateTime<Utc>,
}

impl FeedbackSchedulerSuccess {
    pub fn validate(
        &self,
        state: &FeedbackSchedulerStateInfo,
        database_now: DateTime<Utc>,
    ) -> Result<(), FeedbackError> {
        let aligned = cadence_cutoff(self.label_cutoff, state.cadence_secs)?;
        if self.label_cutoff > database_now
            || self.label_cutoff != state.next_due_at
            || aligned != self.label_cutoff
        {
            return Err(invalid(
                "feedback scheduler success cutoff is future, unaligned, or differs from the lease",
            ));
        }
        Ok(())
    }
}

/// Pause/resume CAS intent with mandatory audit context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackSchedulerControl {
    pub research_profile_id: ResearchProfileId,
    pub expected_pause_revision: i64,
    pub pause: bool,
    pub reason_code: String,
    pub note: String,
}

impl FeedbackSchedulerControl {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.research_profile_id.as_str().trim().is_empty()
            || self.expected_pause_revision < 0
            || !valid_reason(&self.reason_code)
            || !valid_note(&self.note, MAX_CONTROL_NOTE_BYTES)
        {
            return Err(invalid(
                "feedback scheduler control profile, CAS, reason, or note is invalid",
            ));
        }
        Ok(())
    }
}

pub fn cadence_cutoff(
    database_now: DateTime<Utc>,
    cadence_secs: i64,
) -> Result<DateTime<Utc>, FeedbackError> {
    if cadence_secs <= 0 {
        return Err(invalid("feedback scheduler cadence must be positive"));
    }
    let bucket = database_now.timestamp().div_euclid(cadence_secs) * cadence_secs;
    DateTime::from_timestamp(bucket, 0)
        .ok_or_else(|| invalid("feedback scheduler cadence cutoff exceeds chrono bounds"))
}

fn valid_reason(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_note(value: &str, maximum: usize) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty() && value.len() <= maximum
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidCoordinatorState {
        detail: detail.into(),
    }
}
