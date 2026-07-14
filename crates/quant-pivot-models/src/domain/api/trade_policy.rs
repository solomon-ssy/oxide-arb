//! Trade-policy research and governance HTTP contracts.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use crate::{
    domain::{TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo, pagination::PageRequest},
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus},
    types::{
        ContentHash, TradePolicyArtifactId, TradePolicyArtifactPayload,
        TradePolicyConditionCandidate, TradePolicyFitContract, TradePolicyGovernanceAuditId,
        TradePolicyPublicationBlocker, TrainingDatasetId, VerticalActivationTarget,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct FitTradePolicyRequest {
    pub contract: TradePolicyFitContract,
    pub activation_target: VerticalActivationTarget,
    #[validate(length(min = 1, max = 16))]
    pub condition_candidates: Vec<TradePolicyConditionCandidate>,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TradePolicyFitPreflightRequest {
    pub contract: TradePolicyFitContract,
    pub activation_target: VerticalActivationTarget,
    #[validate(length(min = 1, max = 16))]
    pub condition_candidates: Vec<TradePolicyConditionCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TradePolicyGovernanceRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyListQuery {
    pub status: Option<TradePolicyStatus>,
    pub source_dataset_id: Option<TrainingDatasetId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyAuditListQuery {
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyGovernanceAuditView {
    pub audit_id: TradePolicyGovernanceAuditId,
    pub artifact_id: TradePolicyArtifactId,
    pub action: TradePolicyGovernanceAction,
    pub from_status: TradePolicyStatus,
    pub to_status: TradePolicyStatus,
    pub content_hash: ContentHash,
    pub actor_id: Uuid,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl From<TradePolicyGovernanceAuditInfo> for TradePolicyGovernanceAuditView {
    fn from(info: TradePolicyGovernanceAuditInfo) -> Self {
        Self {
            audit_id: info.audit_id,
            artifact_id: info.artifact_id,
            action: info.action,
            from_status: info.from_status,
            to_status: info.to_status,
            content_hash: info.content_hash,
            actor_id: info.actor_id,
            reason: info.reason,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicySummaryView {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub cohort_count: usize,
    pub executable_coverage: Option<rust_decimal::Decimal>,
    pub publishable: bool,
    pub publication_blocker_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradePolicyArtifactInfo> for TradePolicySummaryView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        let publication_blockers = info.payload_json.publication_blockers();
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            cohort_count: info.payload_json.cohorts.len(),
            executable_coverage: info
                .payload_json
                .cohorts
                .iter()
                .map(|cohort| cohort.executable_coverage)
                .min(),
            publishable: publication_blockers.is_empty(),
            publication_blocker_count: publication_blockers.len(),
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyDetailView {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub payload: TradePolicyArtifactPayload,
    pub publication_blockers: Vec<TradePolicyPublicationBlocker>,
    pub allowed_governance_actions: Vec<TradePolicyGovernanceAction>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradePolicyArtifactInfo> for TradePolicyDetailView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        let blockers = info.payload_json.publication_blockers();
        let allowed_governance_actions = match info.status {
            TradePolicyStatus::Draft if blockers.is_empty() => {
                vec![TradePolicyGovernanceAction::Validate]
            }
            TradePolicyStatus::Validated if blockers.is_empty() => {
                vec![TradePolicyGovernanceAction::Publish]
            }
            TradePolicyStatus::Published => vec![TradePolicyGovernanceAction::Retire],
            TradePolicyStatus::Draft
            | TradePolicyStatus::Validated
            | TradePolicyStatus::Retired => Vec::new(),
        };
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            payload: info.payload_json,
            publication_blockers: blockers,
            allowed_governance_actions,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyFitPreflightView {
    pub contract_valid: TradePolicyPreflightCheckStatus,
    pub source_dataset_ready: TradePolicyPreflightCheckStatus,
    pub raw_trajectory_labels_present: TradePolicyPreflightCheckStatus,
    pub fit_window_contained: TradePolicyPreflightCheckStatus,
    pub runtime_config_matches: TradePolicyPreflightCheckStatus,
    pub pit_cutoff_valid: TradePolicyPreflightCheckStatus,
    pub labels_matured_by_cutoff: u64,
    pub labels_excluded_after_cutoff: u64,
    pub full_l2_trajectory_present: TradePolicyPreflightCheckStatus,
    pub fee_model_present: TradePolicyPreflightCheckStatus,
    pub publishable_input: TradePolicyPreflightCheckStatus,
    pub canonical_condition_candidates: Option<Vec<TradePolicyConditionCandidate>>,
    pub condition_candidate_set_hash: Option<ContentHash>,
    pub messages: Vec<String>,
}

/// Binary outcome of one deterministic trade-policy fit preflight check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyPreflightCheckStatus {
    Pass,
    Fail,
}

impl From<bool> for TradePolicyPreflightCheckStatus {
    fn from(value: bool) -> Self {
        if value { Self::Pass } else { Self::Fail }
    }
}
