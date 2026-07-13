//! Trade-policy research and governance HTTP contracts.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{TradePolicyArtifactInfo, pagination::PageRequest},
    enums::quant::TradePolicyStatus,
    types::{
        ContentHash, TradePolicyArtifactId, TradePolicyArtifactPayload, TradePolicyFitContract,
        TrainingDatasetId,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct FitTradePolicyRequest {
    pub contract: TradePolicyFitContract,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TradePolicyFitPreflightRequest {
    pub contract: TradePolicyFitContract,
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

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicySummaryView {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub cohort_count: usize,
    pub executable_coverage: rust_decimal::Decimal,
    pub validation_passed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradePolicyArtifactInfo> for TradePolicySummaryView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            cohort_count: info.payload_json.cohorts.len(),
            executable_coverage: info.payload_json.validation.executable_coverage,
            validation_passed: info.payload_json.validation.passed,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradePolicyArtifactInfo> for TradePolicyDetailView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            payload: info.payload_json,
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
    pub full_l2_trajectory_present: TradePolicyPreflightCheckStatus,
    pub fee_model_present: TradePolicyPreflightCheckStatus,
    pub publishable_input: TradePolicyPreflightCheckStatus,
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
