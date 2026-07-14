//! Trade-policy artifact fitting, catalog reads, and governed transitions.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        FitTradePolicyRequest, NewTradePolicyArtifact, NewTradePolicyGovernanceAudit, Paginated,
        TradePolicyArtifactInfo, TradePolicyAuditListQuery, TradePolicyFitPreflightRequest,
        TradePolicyFitPreflightView, TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicyPort, TrainingDatasetInfo,
    },
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus, TrainingDatasetStatus},
    hashing::CanonicalDigest,
    types::{
        ContentHash, TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TradePolicyEvidenceGap, TradePolicyExecutionEvidence,
        TradePolicyFitContract, TradePolicyGovernanceAuditId, TradePolicyPitCutoffEvidence,
        TradePolicyValidationEvidence, canonicalize_condition_candidates,
    },
};
use quant_pivot_repository::traits::{TradePolicyRepository, TrainingDatasetRepository};
use quant_pivot_research::{
    artifact::ArtifactStore,
    hashing::ResearchHasher,
    training::{TrainingExample, TrainingLabel},
};
use uuid::Uuid;

use crate::service::training_dataset::{
    require_dataset_materialization, verify_frozen_dataset_artifact,
};

pub struct TradePolicyService {
    datasets: Arc<dyn TrainingDatasetRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    policies: Arc<dyn TradePolicyRepository>,
}

impl TradePolicyService {
    #[must_use]
    pub const fn new(
        datasets: Arc<dyn TrainingDatasetRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        policies: Arc<dyn TradePolicyRepository>,
    ) -> Self {
        Self {
            datasets,
            artifacts,
            policies,
        }
    }

    async fn source(
        &self,
        request: &FitTradePolicyRequest,
    ) -> QuantResult<(TrainingDatasetInfo, Vec<TrainingExample>)> {
        request
            .contract
            .validate()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        canonicalize_condition_candidates(request.condition_candidates.clone())
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        let dataset = self
            .datasets
            .find_by_id(&request.contract.source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: request.contract.source_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy fitting requires a Ready dataset, got {}",
                    dataset.status.as_str()
                ),
            }
            .into());
        }
        if request.contract.fit_window_start < dataset.window_start
            || request.contract.fit_window_end > dataset.window_end
        {
            return Err(ResearchError::DatasetBuild {
                detail: "trade-policy fit window must be contained in the source dataset"
                    .to_owned(),
            }
            .into());
        }
        if request.contract.runtime_config_version_id != dataset.runtime_config_version_id {
            return Err(ResearchError::DatasetBuild {
                detail:
                    "trade-policy fit must use the source dataset's frozen runtime-config version"
                        .to_owned(),
            }
            .into());
        }
        let materialization = require_dataset_materialization(&dataset)?;
        let bytes = self.artifacts.get(materialization.parquet_uri).await?;
        let examples = verify_frozen_dataset_artifact(&dataset, &bytes)?;
        Ok((dataset, examples))
    }
}

#[async_trait]
impl TradePolicyPort for TradePolicyService {
    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        let dataset = self
            .datasets
            .find_by_id(&request.contract.source_dataset_id)
            .await?;
        let mut messages = Vec::new();
        let canonical_condition_candidates =
            match request.contract.validate().and_then(|()| {
                canonicalize_condition_candidates(request.condition_candidates.clone())
            }) {
                Ok(candidates) => Some(candidates),
                Err(detail) => {
                    messages.push(detail);
                    None
                }
            };
        let contract_valid = canonical_condition_candidates.is_some();
        let condition_candidate_set_hash = canonical_condition_candidates
            .as_ref()
            .map(CanonicalDigest::content_hash_json)
            .transpose()?;
        let source_dataset_ready = dataset
            .as_ref()
            .is_some_and(|row| row.status == TrainingDatasetStatus::Ready);
        if !source_dataset_ready {
            messages.push("source dataset is missing or not Ready".to_owned());
        }
        let fit_window_contained = dataset.as_ref().is_some_and(|row| {
            request.contract.fit_window_start >= row.window_start
                && request.contract.fit_window_end <= row.window_end
        });
        if !fit_window_contained {
            messages.push("fit window is outside the source dataset".to_owned());
        }
        let runtime_config_matches = dataset.as_ref().is_some_and(|row| {
            request.contract.runtime_config_version_id == row.runtime_config_version_id
        });
        if !runtime_config_matches {
            messages.push(
                "runtime-config version differs from the source dataset's frozen version"
                    .to_owned(),
            );
        }
        let raw_trajectory_labels_present = dataset
            .as_ref()
            .and_then(|row| row.manifest_json.as_ref())
            .is_some_and(|manifest| manifest.sample_count > 0);
        if !raw_trajectory_labels_present {
            messages.push("source dataset has no materialized trajectory samples".to_owned());
        }
        let pit_cutoff_valid = request.contract.fit_window_end <= request.contract.pit_cutoff;
        if !pit_cutoff_valid {
            messages.push("fit window ends after the PIT cutoff".to_owned());
        }
        let (labels_matured_by_cutoff, labels_excluded_after_cutoff) =
            if source_dataset_ready && fit_window_contained {
                if let Some(dataset) = dataset.as_ref() {
                    let materialization = require_dataset_materialization(dataset)?;
                    let bytes = self.artifacts.get(materialization.parquet_uri).await?;
                    let examples = verify_frozen_dataset_artifact(dataset, &bytes)?;
                    label_cutoff_counts(&request.contract, &examples)
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            };
        let full_l2_trajectory_present = false;
        let fee_model_present = false;
        messages.push(
            "source artifact contains top-of-book trajectories only; full L2 ladder replay is required for publication"
                .to_owned(),
        );
        messages.push(
            "source artifact does not carry an applied venue-fee simulation; publication is blocked"
                .to_owned(),
        );
        let publishable_input = contract_valid
            && source_dataset_ready
            && raw_trajectory_labels_present
            && fit_window_contained
            && runtime_config_matches
            && pit_cutoff_valid
            && full_l2_trajectory_present
            && fee_model_present;
        Ok(TradePolicyFitPreflightView {
            contract_valid: contract_valid.into(),
            source_dataset_ready: source_dataset_ready.into(),
            raw_trajectory_labels_present: raw_trajectory_labels_present.into(),
            fit_window_contained: fit_window_contained.into(),
            runtime_config_matches: runtime_config_matches.into(),
            pit_cutoff_valid: pit_cutoff_valid.into(),
            labels_matured_by_cutoff,
            labels_excluded_after_cutoff,
            full_l2_trajectory_present: full_l2_trajectory_present.into(),
            fee_model_present: fee_model_present.into(),
            publishable_input: publishable_input.into(),
            canonical_condition_candidates,
            condition_candidate_set_hash,
            messages,
        })
    }

    async fn fit(&self, request: FitTradePolicyRequest) -> QuantResult<TradePolicyArtifactInfo> {
        let (dataset, examples) = self.source(&request).await?;
        let materialization = require_dataset_materialization(&dataset)?;
        let payload = fit_payload(
            &request,
            &examples,
            materialization.dataset_hash,
            materialization.feature_schema_hash,
            materialization.label_schema_hash,
        )?;
        let content_hash = ResearchHasher::canonical(&payload)?;
        let artifact_id = TradePolicyArtifactId::from_content_hash(&content_hash);
        if let Some(existing) = self.policies.find(&artifact_id).await? {
            if existing.content_hash != content_hash {
                return Err(StorageError::InvariantViolation {
                    entity: Some("trade_policy_artifact"),
                    detail: format!(
                        "content-addressed artifact {artifact_id} resolved to a different hash"
                    ),
                }
                .into());
            }
            return Ok(existing);
        }
        self.policies
            .insert(NewTradePolicyArtifact {
                artifact_id,
                content_hash,
                status: TradePolicyStatus::Draft,
                source_dataset_id: request.contract.source_dataset_id,
                payload_json: payload,
            })
            .await
            .map_err(Into::into)
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>> {
        self.policies.find(artifact_id).await.map_err(Into::into)
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>> {
        self.policies.page(query).await.map_err(Into::into)
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>> {
        self.policies
            .page_audits(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: Uuid,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let current =
            self.policies
                .find(artifact_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "trade_policy_artifact",
                    id: artifact_id.to_string(),
                })?;
        let publication_blockers = current.payload_json.publication_blockers();
        if matches!(
            target,
            TradePolicyStatus::Validated | TradePolicyStatus::Published
        ) && !publication_blockers.is_empty()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("trade policy is not publishable: {publication_blockers:?}"),
            }
            .into());
        }
        let action = match target {
            TradePolicyStatus::Validated => TradePolicyGovernanceAction::Validate,
            TradePolicyStatus::Published => TradePolicyGovernanceAction::Publish,
            TradePolicyStatus::Retired => TradePolicyGovernanceAction::Retire,
            TradePolicyStatus::Draft => {
                return Err(ResearchError::ValidationMethodology {
                    detail: "trade-policy governance cannot transition back to Draft".to_owned(),
                }
                .into());
            }
        };
        self.policies
            .transition(
                artifact_id,
                current.status,
                target,
                NewTradePolicyGovernanceAudit {
                    audit_id: TradePolicyGovernanceAuditId::from_v7(),
                    artifact_id: artifact_id.clone(),
                    action,
                    from_status: current.status,
                    to_status: target,
                    content_hash: current.content_hash,
                    actor_id,
                    reason,
                },
            )
            .await
            .map_err(Into::into)
    }
}

fn fit_payload(
    request: &FitTradePolicyRequest,
    examples: &[TrainingExample],
    source_dataset_hash: &ContentHash,
    feature_schema_hash: &ContentHash,
    label_schema_hash: &ContentHash,
) -> QuantResult<TradePolicyArtifactPayload> {
    let projection = examples
        .iter()
        .filter(|example| {
            let at = example.decision_at();
            at >= request.contract.fit_window_start && at < request.contract.fit_window_end
        })
        .filter_map(|example| {
            let labels = example
                .labels
                .iter()
                .filter(|label| {
                    label_visible_at_cutoff(
                        &request.contract,
                        example.decision_at(),
                        label.matured_at,
                    )
                })
                .collect::<Vec<_>>();
            (!labels.is_empty()).then_some(PitFitSampleProjection {
                market_id: example.market_id.as_str(),
                token_id: example.token_id.as_str(),
                decision_at: example.decision_at(),
                labels,
            })
        })
        .collect::<Vec<_>>();
    let filtered_sample_count =
        u64::try_from(projection.len()).map_err(|_| ResearchError::DatasetBuild {
            detail: "PIT-filtered sample count exceeds u64".to_owned(),
        })?;
    let (labels_matured_by_cutoff, labels_excluded_after_cutoff) =
        label_cutoff_counts(&request.contract, examples);
    let filtered_sample_hash = ResearchHasher::canonical(&projection)?;
    let condition_candidates =
        canonicalize_condition_candidates(request.condition_candidates.clone())
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let condition_candidate_set_hash = CanonicalDigest::content_hash_json(&condition_candidates)?;
    Ok(TradePolicyArtifactPayload {
        format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        activation_target: request.activation_target,
        fit_contract: request.contract.clone(),
        source_dataset_hash: source_dataset_hash.clone(),
        feature_schema_hash: feature_schema_hash.clone(),
        label_schema_hash: label_schema_hash.clone(),
        fill_simulator_version: "unavailable-until-11.7.2".to_owned(),
        pit_cutoff_evidence: Some(TradePolicyPitCutoffEvidence {
            filtered_sample_count,
            labels_matured_by_cutoff,
            labels_excluded_after_cutoff,
            filtered_sample_hash,
        }),
        execution_evidence: degraded_execution_evidence(filtered_sample_count),
        condition_candidate_set_hash,
        condition_candidates,
        vertical_gate_evidence: Vec::new(),
        cohorts: Vec::new(),
        validation: TradePolicyValidationEvidence {
            trial_ledger_hash: None,
            cpcv_path_count: None,
            deflated_sharpe_ratio: None,
            probability_of_backtest_overfitting: None,
            effective_sample_size: None,
            ambiguous_touch_rate: None,
            depth_failure_rate: None,
        },
    })
}

#[derive(serde::Serialize)]
struct PitFitSampleProjection<'a> {
    market_id: &'a str,
    token_id: &'a str,
    decision_at: chrono::DateTime<chrono::Utc>,
    labels: Vec<&'a TrainingLabel>,
}

fn label_cutoff_counts(
    contract: &TradePolicyFitContract,
    examples: &[TrainingExample],
) -> (u64, u64) {
    let mut matured = 0_u64;
    let mut excluded = 0_u64;
    for example in examples.iter().filter(|example| {
        let at = example.decision_at();
        at >= contract.fit_window_start && at < contract.fit_window_end
    }) {
        for label in &example.labels {
            if label_visible_at_cutoff(contract, example.decision_at(), label.matured_at) {
                matured += 1;
            } else {
                excluded += 1;
            }
        }
    }
    (matured, excluded)
}

fn label_visible_at_cutoff(
    contract: &TradePolicyFitContract,
    decision_at: chrono::DateTime<chrono::Utc>,
    matured_at: chrono::DateTime<chrono::Utc>,
) -> bool {
    decision_at >= contract.fit_window_start
        && decision_at < contract.fit_window_end
        && matured_at <= contract.pit_cutoff
}

fn degraded_execution_evidence(eligible: u64) -> TradePolicyExecutionEvidence {
    TradePolicyExecutionEvidence {
        entry_basis: None,
        exit_basis: None,
        full_l2_sample_count: 0,
        degraded_top_of_book_sample_count: eligible,
        full_l2_coverage: None,
        fee_model_hash: None,
        gaps: vec![
            TradePolicyEvidenceGap::FullL2EntryUnavailable,
            TradePolicyEvidenceGap::FullL2ExitUnavailable,
            TradePolicyEvidenceGap::PitFeeModelUnavailable,
            TradePolicyEvidenceGap::TrialLedgerUnavailable,
            TradePolicyEvidenceGap::CpcvUnavailable,
            TradePolicyEvidenceGap::AmbiguousTouchEvidenceUnavailable,
            TradePolicyEvidenceGap::DepthFailureEvidenceUnavailable,
        ],
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::types::{
        Bps, RuntimeConfigVersionId, TradePolicyFitContract, TradePolicyQualityGate,
        TrainingDatasetId, Usd,
    };
    use rust_decimal_macros::dec;

    use super::label_visible_at_cutoff;

    fn contract() -> TradePolicyFitContract {
        let start = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        let end = start + Duration::days(10);
        TradePolicyFitContract {
            source_dataset_id: TrainingDatasetId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            fit_window_start: start,
            fit_window_end: end,
            pit_cutoff: end + Duration::days(2),
            embargo_secs: 86_400,
            notional_tiers: vec![Usd::new(dec!(25))],
            maximum_scale_out_targets: 3,
            quality_gate: TradePolicyQualityGate {
                min_cohort_samples: 100,
                min_executable_coverage: dec!(0.8),
                min_full_l2_coverage: dec!(0.8),
                min_cpcv_paths: 16,
                min_deflated_sharpe_ratio: dec!(0.1),
                max_probability_of_backtest_overfitting: dec!(0.2),
                max_ambiguous_touch_rate: dec!(0.01),
                max_depth_failure_rate: dec!(0.05),
                min_lower_confidence_utility_bps: Bps::new(dec!(1)),
            },
        }
    }

    #[test]
    fn decision_inside_fit_window_with_label_maturing_after_cutoff_is_excluded() {
        let contract = contract();
        let decision_at = contract.fit_window_end - Duration::hours(1);

        assert!(!label_visible_at_cutoff(
            &contract,
            decision_at,
            contract.pit_cutoff + Duration::seconds(1),
        ));
        assert!(label_visible_at_cutoff(
            &contract,
            decision_at,
            contract.pit_cutoff,
        ));
    }
}
