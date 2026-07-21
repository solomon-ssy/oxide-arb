//! Submit-time verification of the frozen recommendation trade policy.

use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::quant::{ModelVersionInfo, RecommendationInfo},
    enums::quant::{PublicationStatus, TradePolicyStatus},
    types::{RecommendationTradePlan, ResearchProfileRef, TradePolicyArtifactId},
};
use quant_pivot_repository::traits::TradePolicyRepository;
use quant_pivot_research::hashing::ResearchHasher;

/// Re-validate policy governance, content identity, model binding, cohort, and
/// exact notional tier at every risk-increasing boundary.
pub async fn require_frozen_trade_policy(
    policies: &dyn TradePolicyRepository,
    model_version: &ModelVersionInfo,
    recommendation: &RecommendationInfo,
) -> QuantResult<ResearchProfileRef> {
    let RecommendationTradePlan::Frozen { policy, sizing, .. } = &recommendation.trade_plan else {
        return Err(denied("recommendation trade plan is unavailable").into());
    };
    if model_version.publication_status != PublicationStatus::Published {
        return Err(denied("model version is no longer Published").into());
    }
    if model_version.profile_ref != recommendation.profile_ref {
        return Err(denied(
            "recommendation research profile does not match the frozen model version",
        )
        .into());
    }
    if model_version.trade_policy_artifact_id.as_ref() != Some(&policy.artifact_id)
        || model_version.trade_policy_hash.as_ref() != Some(&policy.artifact_hash)
    {
        return Err(
            denied("model version trade-policy binding does not match recommendation").into(),
        );
    }
    let artifact = policies
        .find(&policy.artifact_id)
        .await?
        .ok_or_else(|| denied("frozen trade-policy artifact no longer exists"))?;
    if artifact.status != TradePolicyStatus::Published {
        return Err(denied("frozen trade-policy artifact is not Published").into());
    }
    let computed_hash = ResearchHasher::canonical(&artifact.payload_json)?;
    if artifact.content_hash != policy.artifact_hash
        || computed_hash != policy.artifact_hash
        || TradePolicyArtifactId::from_content_hash(&computed_hash) != policy.artifact_id
    {
        return Err(denied("frozen trade-policy artifact identity does not verify").into());
    }
    if !artifact.payload_json.is_publishable() {
        return Err(
            denied("frozen trade-policy artifact no longer passes publication gates").into(),
        );
    }
    if artifact.payload_json.fit_contract.profile_ref != model_version.profile_ref {
        return Err(denied(
            "trade-policy research profile does not match the frozen model version",
        )
        .into());
    }
    let cohort_index = usize::try_from(policy.cohort_index)
        .map_err(|_| denied("frozen trade-policy cohort index is not representable"))?;
    let cohort = artifact
        .payload_json
        .cohorts
        .get(cohort_index)
        .ok_or_else(|| denied("frozen trade-policy cohort does not exist"))?;
    if cohort.key != policy.cohort_key {
        return Err(denied("frozen trade-policy cohort provenance does not match artifact").into());
    }
    if cohort.key.profile_ref != recommendation.profile_ref {
        return Err(denied(
            "trade-policy cohort research profile does not match the recommendation",
        )
        .into());
    }
    if cohort.key.cash_budget_tier != sizing.suggested_usd {
        return Err(denied(
            "recommendation sizing does not exactly match the validated cash-budget tier",
        )
        .into());
    }
    Ok(model_version.profile_ref.clone())
}

fn denied(reason: impl Into<String>) -> ExecutionError {
    ExecutionError::IntentDenied {
        reason: reason.into(),
    }
}
