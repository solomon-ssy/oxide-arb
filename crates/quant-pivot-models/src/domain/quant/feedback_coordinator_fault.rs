//! Immutable coordinator-corruption evidence persisted with cycle quarantine.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_feedback_coordinator_fault,
    enums::quant::FeedbackStage,
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCoordinatorFaultId, FeedbackCycleId, FeedbackStageEventId, WorkerId,
    },
};

const FAULT_CODE: &str = "invalid_coordinator_state";
const FAULT_DETAIL_DOMAIN: &str = "quant-pivot/feedback-coordinator-fault-detail";
const FAULT_DOMAIN: &str = "quant-pivot/feedback-coordinator-fault";
const FAULT_VERSION: u32 = 1;
const MAX_FAULT_DETAIL_BYTES: usize = 2_048;

/// Stable, validated reason for a deterministic coordinator quarantine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCoordinatorFaultReason {
    detail: String,
}

impl FeedbackCoordinatorFaultReason {
    #[must_use]
    pub fn invalid_state(detail: &str) -> Self {
        let detail = detail.trim();
        let detail = if detail.is_empty() {
            "unspecified invalid coordinator state".to_owned()
        } else if detail.len() <= MAX_FAULT_DETAIL_BYTES {
            detail.to_owned()
        } else {
            let mut end = MAX_FAULT_DETAIL_BYTES;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail[..end].to_owned()
        };
        Self { detail }
    }

    pub fn try_invalid_state(detail: String) -> Result<Self, FeedbackError> {
        if detail.trim().is_empty() || detail.len() > MAX_FAULT_DETAIL_BYTES {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!(
                    "coordinator fault detail must contain 1..={MAX_FAULT_DETAIL_BYTES} bytes"
                ),
            });
        }
        Ok(Self { detail })
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Exact persisted timeline head captured while the cycle row is locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeedbackCoordinatorTimelineHead {
    pub active_stage: Option<FeedbackStage>,
    pub last_event_sequence: Option<i64>,
    pub last_stage_event_id: Option<FeedbackStageEventId>,
    pub last_stage_event_hash: Option<ContentHash>,
}

impl FeedbackCoordinatorTimelineHead {
    pub fn validate(self) -> Result<(), FeedbackError> {
        let all_present = self.last_event_sequence.is_some()
            && self.last_stage_event_id.is_some()
            && self.last_stage_event_hash.is_some();
        let all_absent = self.last_event_sequence.is_none()
            && self.last_stage_event_id.is_none()
            && self.last_stage_event_hash.is_none();
        if !all_present && !all_absent {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator fault timeline head is only partially populated".to_owned(),
            });
        }
        Ok(())
    }
}

/// Database-timestamped input for one immutable coordinator fault.
#[derive(Debug, Clone)]
pub struct FeedbackCoordinatorFaultInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub lease_generation: i64,
    pub worker_id: WorkerId,
    pub timeline_head: FeedbackCoordinatorTimelineHead,
    pub reason: FeedbackCoordinatorFaultReason,
    pub observed_at: DateTime<Utc>,
}

/// Content-addressed WORM coordinator fault row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_coordinator_fault::ActiveModel")]
pub struct NewFeedbackCoordinatorFault {
    feedback_coordinator_fault_id: FeedbackCoordinatorFaultId,
    feedback_cycle_id: FeedbackCycleId,
    lease_generation: i64,
    worker_id: WorkerId,
    active_stage: Option<FeedbackStage>,
    last_event_sequence: Option<i64>,
    last_stage_event_id: Option<FeedbackStageEventId>,
    last_stage_event_hash: Option<ContentHash>,
    fault_code: String,
    detail: String,
    detail_hash: ContentHash,
    fault_hash: ContentHash,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct FaultDetailDocument<'a> {
    fault_code: &'static str,
    detail: &'a str,
}

#[derive(Debug, Serialize)]
struct FaultDocument<'a> {
    feedback_cycle_id: FeedbackCycleId,
    lease_generation: i64,
    worker_id: WorkerId,
    timeline_head: FeedbackCoordinatorTimelineHead,
    fault_code: &'static str,
    detail_hash: ContentHash,
    observed_at: DateTime<Utc>,
    detail: &'a str,
}

impl NewFeedbackCoordinatorFault {
    pub fn try_seal(input: FeedbackCoordinatorFaultInput) -> Result<Self, FeedbackError> {
        if input.lease_generation < 0 {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator fault lease generation cannot be negative".to_owned(),
            });
        }
        input.timeline_head.validate()?;
        let detail_hash = CanonicalDigest::content_hash_typed(
            FAULT_DETAIL_DOMAIN,
            FAULT_VERSION,
            &FaultDetailDocument {
                fault_code: FAULT_CODE,
                detail: input.reason.detail(),
            },
        )?;
        let fault_hash = CanonicalDigest::content_hash_typed(
            FAULT_DOMAIN,
            FAULT_VERSION,
            &FaultDocument {
                feedback_cycle_id: input.feedback_cycle_id,
                lease_generation: input.lease_generation,
                worker_id: input.worker_id,
                timeline_head: input.timeline_head,
                fault_code: FAULT_CODE,
                detail_hash,
                observed_at: input.observed_at,
                detail: input.reason.detail(),
            },
        )?;
        Ok(Self {
            feedback_coordinator_fault_id: FeedbackCoordinatorFaultId::from_fault_hash(&fault_hash),
            feedback_cycle_id: input.feedback_cycle_id,
            lease_generation: input.lease_generation,
            worker_id: input.worker_id,
            active_stage: input.timeline_head.active_stage,
            last_event_sequence: input.timeline_head.last_event_sequence,
            last_stage_event_id: input.timeline_head.last_stage_event_id,
            last_stage_event_hash: input.timeline_head.last_stage_event_hash,
            fault_code: FAULT_CODE.to_owned(),
            detail: input.reason.detail,
            detail_hash,
            fault_hash,
            observed_at: input.observed_at,
        })
    }

    #[must_use]
    pub const fn feedback_coordinator_fault_id(&self) -> FeedbackCoordinatorFaultId {
        self.feedback_coordinator_fault_id
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }
}

/// Read projection for immutable coordinator fault evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_coordinator_fault::Entity")]
pub struct FeedbackCoordinatorFaultInfo {
    pub feedback_coordinator_fault_id: FeedbackCoordinatorFaultId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub lease_generation: i64,
    pub worker_id: WorkerId,
    pub active_stage: Option<FeedbackStage>,
    pub last_event_sequence: Option<i64>,
    pub last_stage_event_id: Option<FeedbackStageEventId>,
    pub last_stage_event_hash: Option<ContentHash>,
    pub fault_code: String,
    pub detail: String,
    pub detail_hash: ContentHash,
    pub fault_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl FeedbackCoordinatorFaultInfo {
    /// Whether a retry carries the exact immutable fault payload.
    #[must_use]
    pub fn matches_new(&self, fault: &NewFeedbackCoordinatorFault) -> bool {
        self.feedback_coordinator_fault_id == fault.feedback_coordinator_fault_id
            && self.feedback_cycle_id == fault.feedback_cycle_id
            && self.lease_generation == fault.lease_generation
            && self.worker_id == fault.worker_id
            && self.active_stage == fault.active_stage
            && self.last_event_sequence == fault.last_event_sequence
            && self.last_stage_event_id == fault.last_stage_event_id
            && self.last_stage_event_hash == fault.last_stage_event_hash
            && self.fault_code == fault.fault_code
            && self.detail == fault.detail
            && self.detail_hash == fault.detail_hash
            && self.fault_hash == fault.fault_hash
            && self.observed_at == fault.observed_at
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.created_at < self.observed_at || self.fault_code != FAULT_CODE {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator fault timestamp or code is invalid".to_owned(),
            });
        }
        let expected = NewFeedbackCoordinatorFault::try_seal(FeedbackCoordinatorFaultInput {
            feedback_cycle_id: self.feedback_cycle_id,
            lease_generation: self.lease_generation,
            worker_id: self.worker_id,
            timeline_head: FeedbackCoordinatorTimelineHead {
                active_stage: self.active_stage,
                last_event_sequence: self.last_event_sequence,
                last_stage_event_id: self.last_stage_event_id,
                last_stage_event_hash: self.last_stage_event_hash,
            },
            reason: FeedbackCoordinatorFaultReason::try_invalid_state(self.detail.clone())?,
            observed_at: self.observed_at,
        })?;
        if self.matches_new(&expected) {
            Ok(())
        } else {
            Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator fault hash or content-addressed id is invalid".to_owned(),
            })
        }
    }
}

info_from_model!(
    FeedbackCoordinatorFaultInfo,
    quant_feedback_coordinator_fault::Model,
    {
        feedback_coordinator_fault_id,
        feedback_cycle_id,
        lease_generation,
        worker_id,
        active_stage,
        last_event_sequence,
        last_stage_event_id,
        last_stage_event_hash,
        fault_code,
        detail,
        detail_hash,
        fault_hash,
        observed_at,
        created_at,
    }
);
