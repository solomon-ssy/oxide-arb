//! Trade-policy artifact fitting, catalog reads, and governed transitions.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        FitTradePolicyRequest, NewTradePolicyGovernanceAudit, Paginated, TradePolicyArtifactInfo,
        TradePolicyAuditListQuery, TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
        TradePolicyFitSelection, TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicyPort, TrainingDatasetInfo,
    },
    enums::quant::{
        DatasetPurpose, TradePolicyGovernanceAction, TradePolicyStatus, TrainingDatasetStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{DecimalString, PolicyValidationConfig, RuntimeConfig},
    types::{
        ArtifactUri, Bps, ContentHash, RuntimeConfigVersionId, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TradePolicyCandidateSpec, TradePolicyEvidenceBundleManifest,
        TradePolicyGovernanceAuditId, TradePolicyQualityGate, VerticalActivationTarget,
        canonicalize_policy_candidates,
    },
};
use quant_pivot_repository::traits::{
    RuntimeConfigVersionRepository, TradePolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{artifact::ArtifactStore, training::TrainingExample};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::service::training_dataset::{
    require_dataset_materialization, verify_frozen_dataset_artifact,
};

pub struct TradePolicyService {
    datasets: Arc<dyn TrainingDatasetRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    policies: Arc<dyn TradePolicyRepository>,
    runtime_configs: Arc<dyn RuntimeConfigVersionRepository>,
}

struct RuntimePolicyFloors {
    quality_gate: TradePolicyQualityGate,
    max_candidates: u32,
    methodology_hash: ContentHash,
}

struct ContractPreflight {
    valid: bool,
    requested_gate_tight_enough: bool,
    runtime_config_version_id: Option<RuntimeConfigVersionId>,
    runtime_floors: Option<RuntimePolicyFloors>,
    canonical_candidates: Option<Vec<TradePolicyCandidateSpec>>,
    candidate_set_hash: Option<ContentHash>,
    messages: Vec<String>,
}

#[derive(Clone, Copy)]
enum PreflightCheck {
    Pass,
    Fail,
}

impl PreflightCheck {
    const fn is_pass(self) -> bool {
        matches!(self, Self::Pass)
    }
}

impl From<bool> for PreflightCheck {
    fn from(value: bool) -> Self {
        if value { Self::Pass } else { Self::Fail }
    }
}

struct DatasetPreflight {
    ready: PreflightCheck,
    policy_fit: PreflightCheck,
    raw_trajectory_labels_present: PreflightCheck,
    fit_window_contained: PreflightCheck,
    pit_cutoff_valid: PreflightCheck,
    labels_matured_by_cutoff: u64,
    labels_excluded_after_cutoff: u64,
    messages: Vec<String>,
}

impl TradePolicyService {
    #[must_use]
    pub const fn new(
        datasets: Arc<dyn TrainingDatasetRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        policies: Arc<dyn TradePolicyRepository>,
        runtime_configs: Arc<dyn RuntimeConfigVersionRepository>,
    ) -> Self {
        Self {
            datasets,
            artifacts,
            policies,
            runtime_configs,
        }
    }

    async fn runtime_policy_floors(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<RuntimePolicyFloors> {
        let version = self
            .runtime_configs
            .load_version(version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "runtime_config_version",
                id: version_id.to_string(),
            })?;
        let config = RuntimeConfig::from_json(&version.config_json)?;
        let methodology_hash =
            CanonicalDigest::content_hash_json(&config.research.policy_validation)?;
        Ok(RuntimePolicyFloors {
            quality_gate: quality_gate_from_runtime(&config)?,
            max_candidates: config
                .research
                .policy_validation
                .max_candidates_per_experiment,
            methodology_hash,
        })
    }

    async fn evaluate_contract(
        &self,
        request: &TradePolicyFitPreflightRequest,
        dataset: Option<&TrainingDatasetInfo>,
    ) -> QuantResult<ContractPreflight> {
        let mut messages = Vec::new();
        let runtime_config_version_id =
            dataset.map(|dataset| dataset.runtime_config_version_id.clone());
        let runtime_floors = match runtime_config_version_id.as_ref() {
            Some(version_id) => match self.runtime_policy_floors(version_id).await {
                Ok(floors) => Some(floors),
                Err(error) => {
                    messages.push(format!(
                        "frozen runtime v13 methodology is unavailable: {error}"
                    ));
                    None
                }
            },
            None => None,
        };
        let selection_valid = match request.selection.validate() {
            Ok(()) => true,
            Err(detail) => {
                messages.push(detail);
                false
            }
        };
        let activation_target_allowed =
            request.activation_target == VerticalActivationTarget::SemiAuto;
        if !activation_target_allowed {
            messages.push(
                "Runtime v13 policy fitting supports SemiAuto only; AutoExecution remains blocked"
                    .to_owned(),
            );
        }
        let canonical_candidates = match canonicalize_policy_candidates(request.candidates.clone())
        {
            Ok(candidates) => Some(candidates),
            Err(detail) => {
                messages.push(detail);
                None
            }
        };
        let requested_gate_tight_enough = runtime_floors.as_ref().is_some_and(|floors| {
            request
                .selection
                .quality_gate
                .as_ref()
                .is_none_or(|requested| quality_gate_is_at_least(requested, &floors.quality_gate))
        });
        if !requested_gate_tight_enough {
            messages.push(
                "requested quality gates are weaker than the frozen runtime v13 floors".to_owned(),
            );
        }
        let candidate_count_allowed = runtime_floors.as_ref().is_some_and(|floors| {
            canonical_candidates.as_ref().is_some_and(|candidates| {
                u32::try_from(candidates.len()).is_ok_and(|count| count <= floors.max_candidates)
            })
        });
        if !candidate_count_allowed {
            messages.push(
                "candidate count exceeds the frozen runtime experiment limit; preflight never truncates"
                    .to_owned(),
            );
        }
        let valid = selection_valid
            && activation_target_allowed
            && canonical_candidates.is_some()
            && requested_gate_tight_enough
            && candidate_count_allowed;
        let candidate_set_hash = canonical_candidates
            .as_ref()
            .map(CanonicalDigest::content_hash_json)
            .transpose()?;
        Ok(ContractPreflight {
            valid,
            requested_gate_tight_enough,
            runtime_config_version_id,
            runtime_floors,
            canonical_candidates,
            candidate_set_hash,
            messages,
        })
    }

    async fn evaluate_dataset(
        &self,
        selection: &TradePolicyFitSelection,
        dataset: Option<&TrainingDatasetInfo>,
    ) -> QuantResult<DatasetPreflight> {
        let mut messages = Vec::new();
        let ready = dataset.is_some_and(|row| row.status == TrainingDatasetStatus::Ready);
        if !ready {
            messages.push("source dataset is missing or not Ready".to_owned());
        }
        let policy_fit = dataset.is_some_and(|row| row.purpose == DatasetPurpose::PolicyFit);
        if !policy_fit {
            messages.push("source dataset purpose must be PolicyFit".to_owned());
        }
        let fit_window_contained = dataset.is_some_and(|row| {
            selection.fit_window_start >= row.window_start
                && selection.fit_window_end <= row.window_end
        });
        if !fit_window_contained {
            messages.push("fit window is outside the source dataset".to_owned());
        }
        let raw_trajectory_labels_present = dataset
            .and_then(|row| row.manifest_json.as_ref())
            .is_some_and(|manifest| manifest.sample_count > 0);
        if !raw_trajectory_labels_present {
            messages.push("source dataset has no materialized trajectory samples".to_owned());
        }
        let pit_cutoff_valid = selection.fit_window_end <= selection.pit_cutoff;
        if !pit_cutoff_valid {
            messages.push("fit window ends after the PIT cutoff".to_owned());
        }
        let (labels_matured_by_cutoff, labels_excluded_after_cutoff) =
            if ready && fit_window_contained {
                let dataset = dataset.ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "ready dataset disappeared during policy preflight".to_owned(),
                })?;
                let materialization = require_dataset_materialization(dataset)?;
                let bytes = self.artifacts.get(materialization.parquet_uri).await?;
                let examples = verify_frozen_dataset_artifact(dataset, &bytes)?;
                label_cutoff_counts(selection, &examples)
            } else {
                (0, 0)
            };
        Ok(DatasetPreflight {
            ready: ready.into(),
            policy_fit: policy_fit.into(),
            raw_trajectory_labels_present: raw_trajectory_labels_present.into(),
            fit_window_contained: fit_window_contained.into(),
            pit_cutoff_valid: pit_cutoff_valid.into(),
            labels_matured_by_cutoff,
            labels_excluded_after_cutoff,
            messages,
        })
    }

    async fn verify_evidence_bundle(
        &self,
        payload: &TradePolicyArtifactPayload,
        require_production_durability: bool,
    ) -> QuantResult<()> {
        let bundle = payload.evidence_bundle.as_ref().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "trade policy has no evidence bundle".to_owned(),
            }
        })?;
        let manifest_bytes = self
            .read_and_hash_manifest(&bundle.manifest_uri, &bundle.manifest_hash)
            .await?;
        let manifest = serde_json::from_slice::<TradePolicyEvidenceBundleManifest>(&manifest_bytes)
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid trade-policy evidence manifest: {error}"),
            })?;
        manifest
            .validate()
            .map_err(|detail| QuantError::from(ResearchError::ValidationMethodology { detail }))?;
        verify_evidence_identity(payload, &manifest)?;
        if require_production_durability {
            require_durable_artifact(&self.artifacts, &bundle.manifest_uri).await?;
        }
        for object in &manifest.objects {
            let actual_hash = hash_streamed_artifact(&self.artifacts, &object.uri).await?;
            if actual_hash != object.byte_hash {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "policy evidence object {:?} byte hash mismatch",
                        object.kind
                    ),
                }
                .into());
            }
            if require_production_durability {
                require_durable_artifact(&self.artifacts, &object.uri).await?;
            }
        }
        Ok(())
    }

    async fn read_and_hash_manifest(
        &self,
        uri: &ArtifactUri,
        expected_hash: &ContentHash,
    ) -> QuantResult<Vec<u8>> {
        const MAX_MANIFEST_BYTES: usize = 1_048_576;

        let mut stream = self.artifacts.get_stream(uri).await?;
        let mut hasher = blake3::Hasher::new();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "policy evidence manifest size overflow".to_owned(),
                }
            })?;
            if next_len > MAX_MANIFEST_BYTES {
                return Err(ResearchError::ValidationMethodology {
                    detail: "policy evidence manifest exceeds 1 MiB".to_owned(),
                }
                .into());
            }
            hasher.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        let actual_hash = content_hash_from_hasher(&hasher)?;
        if &actual_hash != expected_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "policy evidence manifest byte hash mismatch".to_owned(),
            }
            .into());
        }
        Ok(bytes)
    }
}

async fn hash_streamed_artifact(
    artifacts: &Arc<dyn ArtifactStore>,
    uri: &ArtifactUri,
) -> QuantResult<ContentHash> {
    let mut stream = artifacts.get_stream(uri).await?;
    let mut hasher = blake3::Hasher::new();
    while let Some(chunk) = stream.next().await {
        hasher.update(&chunk?);
    }
    content_hash_from_hasher(&hasher)
}

fn content_hash_from_hasher(hasher: &blake3::Hasher) -> QuantResult<ContentHash> {
    ContentHash::parse(format!("blake3:{}", hasher.finalize().to_hex())).map_err(Into::into)
}

async fn require_durable_artifact(
    artifacts: &Arc<dyn ArtifactStore>,
    uri: &ArtifactUri,
) -> QuantResult<()> {
    if !artifacts
        .durability(uri)
        .await?
        .permits_production_publish()
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "artifact {} is not backed by versioned Object-Lock storage",
                uri.as_str()
            ),
        }
        .into());
    }
    Ok(())
}

fn verify_evidence_identity(
    payload: &TradePolicyArtifactPayload,
    manifest: &TradePolicyEvidenceBundleManifest,
) -> QuantResult<()> {
    let bundle =
        payload
            .evidence_bundle
            .as_ref()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "trade policy has no evidence bundle".to_owned(),
            })?;
    let identity_matches = manifest.source_dataset_hash == payload.source_dataset_hash
        && manifest.candidate_set_hash == payload.candidate_set_hash
        && manifest.simulator_hash == bundle.simulator_hash
        && manifest.code_hash == bundle.code_hash
        && manifest.methodology_hash == bundle.methodology_hash
        && manifest.latency_profile_hash == bundle.latency_profile_hash
        && manifest.catalog_ledger_hash == bundle.catalog_ledger_hash
        && manifest.archive_manifest_set_hash == bundle.archive_manifest_set_hash
        && manifest.trial_ledger_hash == bundle.trial_ledger_hash
        && payload.validation.trial_ledger_hash.as_ref() == Some(&manifest.trial_ledger_hash)
        && payload.validation.trial_ledger_cutoff == Some(manifest.trial_ledger_cutoff);
    if !identity_matches {
        return Err(ResearchError::ValidationMethodology {
            detail: "policy evidence manifest identity does not match the frozen artifact"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

fn quality_gate_from_runtime(config: &RuntimeConfig) -> QuantResult<TradePolicyQualityGate> {
    let policy = &config.research.policy_validation;
    let parse = |field: &'static str, value: &DecimalString| -> QuantResult<Decimal> {
        value.value.parse::<Decimal>().map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("{field} is not a valid decimal: {error}"),
            }
            .into()
        })
    };
    Ok(TradePolicyQualityGate {
        min_effective_sample_size: policy.min_effective_sample_size,
        min_full_l2_coverage: parse(
            "research.policy_validation.min_full_l2_coverage",
            &policy.min_full_l2_coverage,
        )?,
        min_common_candidate_support: parse(
            "research.policy_validation.min_common_candidate_support",
            &policy.min_common_candidate_support,
        )?,
        min_passive_reconciled_trade_coverage: parse(
            "research.policy_validation.min_passive_reconciled_trade_coverage",
            &policy.min_passive_reconciled_trade_coverage,
        )?,
        min_fee_catalog_coverage: parse(
            "research.policy_validation.min_fee_catalog_coverage",
            &policy.min_fee_catalog_coverage,
        )?,
        min_universe_coverage: parse(
            "research.policy_validation.min_universe_coverage",
            &policy.min_universe_coverage,
        )?,
        min_cpcv_paths: PolicyValidationConfig::COMPLETE_PATH_COUNT,
        min_deflated_sharpe_ratio: Decimal::ONE
            - parse(
                "research.policy_validation.dsr_significance",
                &policy.dsr_significance,
            )?,
        max_probability_of_backtest_overfitting: parse(
            "research.policy_validation.max_pbo",
            &policy.max_pbo,
        )?,
        max_ambiguous_touch_rate: parse(
            "research.policy_validation.max_ambiguity_rate",
            &policy.max_ambiguity_rate,
        )?,
        max_depth_failure_rate: parse(
            "research.policy_validation.max_depth_failure_rate",
            &policy.max_depth_failure_rate,
        )?,
        min_lower_confidence_utility_bps: Bps::new(parse(
            "research.policy_validation.min_utility_lower_bound_bps",
            &policy.min_utility_lower_bound_bps,
        )?),
    })
}

fn quality_gate_is_at_least(
    requested: &TradePolicyQualityGate,
    floor: &TradePolicyQualityGate,
) -> bool {
    requested.min_effective_sample_size >= floor.min_effective_sample_size
        && requested.min_full_l2_coverage >= floor.min_full_l2_coverage
        && requested.min_common_candidate_support >= floor.min_common_candidate_support
        && requested.min_passive_reconciled_trade_coverage
            >= floor.min_passive_reconciled_trade_coverage
        && requested.min_fee_catalog_coverage >= floor.min_fee_catalog_coverage
        && requested.min_universe_coverage >= floor.min_universe_coverage
        && requested.min_cpcv_paths == floor.min_cpcv_paths
        && requested.min_deflated_sharpe_ratio >= floor.min_deflated_sharpe_ratio
        && requested.max_probability_of_backtest_overfitting
            <= floor.max_probability_of_backtest_overfitting
        && requested.max_ambiguous_touch_rate <= floor.max_ambiguous_touch_rate
        && requested.max_depth_failure_rate <= floor.max_depth_failure_rate
        && requested.min_lower_confidence_utility_bps >= floor.min_lower_confidence_utility_bps
}

#[async_trait]
impl TradePolicyPort for TradePolicyService {
    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        let dataset = self
            .datasets
            .find_by_id(&request.selection.source_dataset_id)
            .await?;
        let mut contract = self.evaluate_contract(request, dataset.as_ref()).await?;
        let mut dataset = self
            .evaluate_dataset(&request.selection, dataset.as_ref())
            .await?;
        let full_l2_trajectory_present = false;
        let fee_model_present = false;
        let latency_profile_present = false;
        contract.messages.append(&mut dataset.messages);
        contract.messages.push(
            "source artifact contains top-of-book trajectories only; full L2 ladder replay is required for publication"
                .to_owned(),
        );
        contract.messages.push(
            "source artifact does not carry an applied venue-fee simulation; publication is blocked"
                .to_owned(),
        );
        contract.messages.push(
            "no signed 24-hour production latency profile is bound; fit enqueue is blocked"
                .to_owned(),
        );
        let publishable_input = contract.valid
            && dataset.ready.is_pass()
            && dataset.policy_fit.is_pass()
            && dataset.raw_trajectory_labels_present.is_pass()
            && dataset.fit_window_contained.is_pass()
            && contract.requested_gate_tight_enough
            && dataset.pit_cutoff_valid.is_pass()
            && full_l2_trajectory_present
            && fee_model_present
            && latency_profile_present;
        let methodology_hash = contract
            .runtime_floors
            .as_ref()
            .map(|floors| floors.methodology_hash.clone());
        Ok(TradePolicyFitPreflightView {
            contract_valid: contract.valid.into(),
            source_dataset_ready: dataset.ready.is_pass().into(),
            source_dataset_policy_fit: dataset.policy_fit.is_pass().into(),
            raw_trajectory_labels_present: dataset.raw_trajectory_labels_present.is_pass().into(),
            fit_window_contained: dataset.fit_window_contained.is_pass().into(),
            requested_gate_tight_enough: contract.requested_gate_tight_enough.into(),
            runtime_quality_gate: contract.runtime_floors.map(|floors| floors.quality_gate),
            runtime_config_version_id: contract.runtime_config_version_id,
            methodology_hash,
            latency_profile_present: latency_profile_present.into(),
            pit_cutoff_valid: dataset.pit_cutoff_valid.is_pass().into(),
            labels_matured_by_cutoff: dataset.labels_matured_by_cutoff,
            labels_excluded_after_cutoff: dataset.labels_excluded_after_cutoff,
            full_l2_trajectory_present: full_l2_trajectory_present.into(),
            fee_model_present: fee_model_present.into(),
            publishable_input: publishable_input.into(),
            canonical_candidates: contract.canonical_candidates,
            candidate_set_hash: contract.candidate_set_hash,
            messages: contract.messages,
        })
    }

    async fn fit(&self, request: FitTradePolicyRequest) -> QuantResult<TradePolicyArtifactInfo> {
        let preflight = self
            .preflight(&TradePolicyFitPreflightRequest {
                selection: request.selection,
                activation_target: request.activation_target,
                candidates: request.candidates,
            })
            .await?;
        Err(ResearchError::ValidationMethodology {
            detail: format!(
                "trade-policy fit preflight is blocked; no Draft was created: {}",
                preflight.messages.join("; ")
            ),
        }
        .into())
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
        if matches!(
            target,
            TradePolicyStatus::Validated | TradePolicyStatus::Published
        ) {
            self.verify_evidence_bundle(
                &current.payload_json,
                target == TradePolicyStatus::Published,
            )
            .await?;
        }
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

fn label_cutoff_counts(
    selection: &TradePolicyFitSelection,
    examples: &[TrainingExample],
) -> (u64, u64) {
    let mut matured = 0_u64;
    let mut excluded = 0_u64;
    for example in examples.iter().filter(|example| {
        let at = example.decision_at();
        at >= selection.fit_window_start && at < selection.fit_window_end
    }) {
        for label in &example.labels {
            if label_visible_at_cutoff(selection, example.decision_at(), label.matured_at) {
                matured += 1;
            } else {
                excluded += 1;
            }
        }
    }
    (matured, excluded)
}

fn label_visible_at_cutoff(
    selection: &TradePolicyFitSelection,
    decision_at: chrono::DateTime<chrono::Utc>,
    matured_at: chrono::DateTime<chrono::Utc>,
) -> bool {
    decision_at >= selection.fit_window_start
        && decision_at < selection.fit_window_end
        && matured_at <= selection.pit_cutoff
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::TradePolicyFitSelection,
        types::{Bps, TradePolicyQualityGate, TrainingDatasetId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::label_visible_at_cutoff;

    fn selection() -> TradePolicyFitSelection {
        let start = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        let end = start + Duration::days(10);
        TradePolicyFitSelection {
            source_dataset_id: TrainingDatasetId::from_v7(),
            fit_window_start: start,
            fit_window_end: end,
            pit_cutoff: end + Duration::days(2),
            notional_tiers: vec![Usd::new(dec!(25)), Usd::new(dec!(100)), Usd::new(dec!(500))],
            quality_gate: Some(TradePolicyQualityGate {
                min_effective_sample_size: 100,
                min_full_l2_coverage: dec!(0.95),
                min_common_candidate_support: dec!(0.95),
                min_passive_reconciled_trade_coverage: dec!(0.95),
                min_fee_catalog_coverage: dec!(1),
                min_universe_coverage: dec!(0.95),
                min_cpcv_paths: 21,
                min_deflated_sharpe_ratio: dec!(0.95),
                max_probability_of_backtest_overfitting: dec!(0.5),
                max_ambiguous_touch_rate: dec!(0.05),
                max_depth_failure_rate: dec!(0.05),
                min_lower_confidence_utility_bps: Bps::ZERO,
            }),
        }
    }

    #[test]
    fn decision_inside_fit_window_with_label_maturing_after_cutoff_is_excluded() {
        let selection = selection();
        let decision_at = selection.fit_window_end - Duration::hours(1);

        assert!(!label_visible_at_cutoff(
            &selection,
            decision_at,
            selection.pit_cutoff + Duration::seconds(1),
        ));
        assert!(label_visible_at_cutoff(
            &selection,
            decision_at,
            selection.pit_cutoff,
        ));
    }
}
