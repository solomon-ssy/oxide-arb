//! Persistence DTOs for the append-only policy-fit trial ledger.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_trade_policy_trial_attempt,
    enums::quant::{TradePolicyTrialScope, TradePolicyTrialStatus},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, ResearchJobId, TradePolicyTrialAttemptId, TradePolicyTrialMetrics,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_policy_trial_attempt::ActiveModel")]
pub struct NewTradePolicyTrialAttempt {
    pub trial_attempt_id: TradePolicyTrialAttemptId,
    pub fit_job_id: ResearchJobId,
    pub attempt_ordinal: i64,
    pub experiment_family_hash: ContentHash,
    pub research_program_hash: ContentHash,
    pub candidate_id: String,
    pub candidate_hash: ContentHash,
    pub scope: TradePolicyTrialScope,
    pub fold_index: Option<i32>,
    pub path_index: Option<i32>,
    pub status: TradePolicyTrialStatus,
    pub metrics_json: Option<TradePolicyTrialMetrics>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub evidence_row_count: Option<i64>,
    pub failure_detail: Option<String>,
    pub row_hash: ContentHash,
}

impl NewTradePolicyTrialAttempt {
    /// Hash every immutable field except `row_hash` and the DB timestamp. This
    /// is the single producer/consumer contract used by Fit and persistence.
    pub fn expected_row_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(&TradePolicyTrialAttemptHashInput {
            contract: "trade_policy_trial_attempt_v1",
            trial_attempt_id: &self.trial_attempt_id,
            fit_job_id: &self.fit_job_id,
            attempt_ordinal: self.attempt_ordinal,
            experiment_family_hash: &self.experiment_family_hash,
            research_program_hash: &self.research_program_hash,
            candidate_id: &self.candidate_id,
            candidate_hash: &self.candidate_hash,
            scope: self.scope,
            fold_index: self.fold_index,
            path_index: self.path_index,
            status: self.status,
            metrics_json: &self.metrics_json,
            evidence_uri: &self.evidence_uri,
            evidence_hash: &self.evidence_hash,
            evidence_row_count: self.evidence_row_count,
            failure_detail: &self.failure_detail,
        })
    }
}

#[derive(Serialize)]
struct TradePolicyTrialAttemptHashInput<'a> {
    contract: &'static str,
    trial_attempt_id: &'a TradePolicyTrialAttemptId,
    fit_job_id: &'a ResearchJobId,
    attempt_ordinal: i64,
    experiment_family_hash: &'a ContentHash,
    research_program_hash: &'a ContentHash,
    candidate_id: &'a str,
    candidate_hash: &'a ContentHash,
    scope: TradePolicyTrialScope,
    fold_index: Option<i32>,
    path_index: Option<i32>,
    status: TradePolicyTrialStatus,
    metrics_json: &'a Option<TradePolicyTrialMetrics>,
    evidence_uri: &'a Option<ArtifactUri>,
    evidence_hash: &'a Option<ContentHash>,
    evidence_row_count: Option<i64>,
    failure_detail: &'a Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_trade_policy_trial_attempt::Entity")]
pub struct TradePolicyTrialAttemptInfo {
    pub trial_attempt_id: TradePolicyTrialAttemptId,
    pub fit_job_id: ResearchJobId,
    pub attempt_ordinal: i64,
    pub experiment_family_hash: ContentHash,
    pub research_program_hash: ContentHash,
    pub candidate_id: String,
    pub candidate_hash: ContentHash,
    pub scope: TradePolicyTrialScope,
    pub fold_index: Option<i32>,
    pub path_index: Option<i32>,
    pub status: TradePolicyTrialStatus,
    pub metrics_json: Option<TradePolicyTrialMetrics>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub evidence_row_count: Option<i64>,
    pub failure_detail: Option<String>,
    pub row_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl TradePolicyTrialAttemptInfo {
    pub fn expected_row_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(&TradePolicyTrialAttemptHashInput {
            contract: "trade_policy_trial_attempt_v1",
            trial_attempt_id: &self.trial_attempt_id,
            fit_job_id: &self.fit_job_id,
            attempt_ordinal: self.attempt_ordinal,
            experiment_family_hash: &self.experiment_family_hash,
            research_program_hash: &self.research_program_hash,
            candidate_id: &self.candidate_id,
            candidate_hash: &self.candidate_hash,
            scope: self.scope,
            fold_index: self.fold_index,
            path_index: self.path_index,
            status: self.status,
            metrics_json: &self.metrics_json,
            evidence_uri: &self.evidence_uri,
            evidence_hash: &self.evidence_hash,
            evidence_row_count: self.evidence_row_count,
            failure_detail: &self.failure_detail,
        })
    }
}

info_from_model!(
    TradePolicyTrialAttemptInfo,
    quant_trade_policy_trial_attempt::Model,
    {
        trial_attempt_id,
        fit_job_id,
        attempt_ordinal,
        experiment_family_hash,
        research_program_hash,
        candidate_id,
        candidate_hash,
        scope,
        fold_index,
        path_index,
        status,
        metrics_json,
        evidence_uri,
        evidence_hash,
        evidence_row_count,
        failure_detail,
        row_hash,
        created_at,
    }
);
