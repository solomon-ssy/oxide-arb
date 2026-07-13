//! Trade-policy artifact fitting, catalog reads, and governed transitions.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        FitTradePolicyRequest, NewTradePolicyArtifact, NewTradePolicyGovernanceAudit, Paginated,
        TradePolicyArtifactInfo, TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
        TradePolicyListQuery, TradePolicyPort, TrainingDatasetInfo,
    },
    enums::{
        common::MarketCategory,
        quant::{TradePolicyGovernanceAction, TradePolicyStatus, TrainingDatasetStatus},
    },
    types::{
        Bps, ContentHash, EntryOrderTemplate, EntryTriggerTemplate, Price, ScaleOutTemplate,
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactId, TradePolicyArtifactPayload,
        TradePolicyCohort, TradePolicyCohortKey, TradePolicyExecutionEvidence,
        TradePolicyGovernanceAuditId, TradePolicyValidation, Usd,
    },
};
use quant_pivot_repository::traits::{TradePolicyRepository, TrainingDatasetRepository};
use quant_pivot_research::{
    artifact::ArtifactStore,
    hashing::ResearchHasher,
    training::{MAX_ADVERSE_EXCURSION_BPS, MAX_FAVORABLE_EXCURSION_BPS, TrainingExample},
};
use rust_decimal::Decimal;
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
        let contract_valid = match request.contract.validate() {
            Ok(()) => true,
            Err(detail) => {
                messages.push(detail);
                false
            }
        };
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
            && full_l2_trajectory_present
            && fee_model_present;
        Ok(TradePolicyFitPreflightView {
            contract_valid: contract_valid.into(),
            source_dataset_ready: source_dataset_ready.into(),
            raw_trajectory_labels_present: raw_trajectory_labels_present.into(),
            fit_window_contained: fit_window_contained.into(),
            runtime_config_matches: runtime_config_matches.into(),
            full_l2_trajectory_present: full_l2_trajectory_present.into(),
            fee_model_present: fee_model_present.into(),
            publishable_input: publishable_input.into(),
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
        ) && !current.payload_json.publication_blockers().is_empty()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "trade policy is not publishable: {}",
                    current.payload_json.publication_blockers().join("; ")
                ),
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

#[derive(Default)]
struct CohortSamples {
    category: Option<MarketCategory>,
    horizon_secs: u64,
    price_bucket: u8,
    liquidity_tier: String,
    favorable: Vec<Decimal>,
    adverse: Vec<Decimal>,
    candidate_count: u64,
}

type CohortGroups = HashMap<(MarketCategory, u64, u8, String), CohortSamples>;

struct CohortBuild {
    cohorts: Vec<TradePolicyCohort>,
    candidate_count: u64,
    executable_count: u64,
}

fn fit_payload(
    request: &FitTradePolicyRequest,
    examples: &[TrainingExample],
    source_dataset_hash: &ContentHash,
    feature_schema_hash: &ContentHash,
    label_schema_hash: &ContentHash,
) -> QuantResult<TradePolicyArtifactPayload> {
    let (grouped, eligible) = group_cohort_samples(request, examples);
    let cohort_build = build_cohorts(request, grouped)?;
    let coverage = Decimal::from(cohort_build.executable_count)
        / Decimal::from(cohort_build.candidate_count.max(eligible).max(1));
    let validation = degraded_validation(request, coverage);
    Ok(TradePolicyArtifactPayload {
        format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        fit_contract: request.contract.clone(),
        source_dataset_hash: source_dataset_hash.clone(),
        feature_schema_hash: feature_schema_hash.clone(),
        label_schema_hash: label_schema_hash.clone(),
        fill_simulator_version: "top-of-book-degraded-v1".to_owned(),
        fee_model_hash: ResearchHasher::canonical(&"fees-not-applied")?,
        execution_evidence: degraded_execution_evidence(eligible),
        cohorts: cohort_build.cohorts,
        validation,
    })
}

fn group_cohort_samples(
    request: &FitTradePolicyRequest,
    examples: &[TrainingExample],
) -> (CohortGroups, u64) {
    let mut grouped = CohortGroups::new();
    let mut eligible = 0_u64;
    for example in examples.iter().filter(|example| {
        let at = example.decision_at();
        at >= request.contract.fit_window_start && at < request.contract.fit_window_end
    }) {
        let Some(capture) = &example.decision_capture else {
            continue;
        };
        let Some(entry) = capture.market_context.best_ask else {
            continue;
        };
        eligible += 1;
        let price_bucket = ((entry.inner() * Decimal::from(10)).floor())
            .try_into()
            .unwrap_or(9_u8)
            .min(9);
        let liquidity_tier = liquidity_tier(example.selected_market.liquidity_usd);
        for mfe in example
            .labels
            .iter()
            .filter(|label| label.label_name == MAX_FAVORABLE_EXCURSION_BPS)
        {
            let key = (
                example.selected_market.category,
                mfe.horizon_secs,
                price_bucket,
                liquidity_tier.to_owned(),
            );
            let cohort = grouped.entry(key).or_default();
            cohort.category = Some(example.selected_market.category);
            cohort.horizon_secs = mfe.horizon_secs;
            cohort.price_bucket = price_bucket;
            liquidity_tier.clone_into(&mut cohort.liquidity_tier);
            cohort.favorable.push(mfe.value.max(Decimal::ZERO));
            cohort.candidate_count += 1;
        }
        for mae in example
            .labels
            .iter()
            .filter(|label| label.label_name == MAX_ADVERSE_EXCURSION_BPS)
        {
            let key = (
                example.selected_market.category,
                mae.horizon_secs,
                price_bucket,
                liquidity_tier.to_owned(),
            );
            let cohort = grouped.entry(key).or_default();
            cohort.adverse.push(mae.value.abs());
        }
    }
    (grouped, eligible)
}

fn build_cohorts(
    request: &FitTradePolicyRequest,
    grouped: CohortGroups,
) -> QuantResult<CohortBuild> {
    let mut cohorts = Vec::new();
    let mut candidate_count = 0_u64;
    let mut executable_count = 0_u64;
    for mut samples in grouped.into_values() {
        let executable = samples.favorable.len().min(samples.adverse.len());
        if executable == 0 {
            continue;
        }
        let upper = quantile(&mut samples.favorable, 7, 10).max(Decimal::ONE);
        let lower = quantile(&mut samples.adverse, 8, 10).max(Decimal::ONE);
        candidate_count += samples.candidate_count;
        executable_count += executable as u64;
        for notional_tier in &request.contract.notional_tiers {
            cohorts.push(build_cohort(
                request,
                &samples,
                *notional_tier,
                executable,
                upper,
                lower,
            )?);
        }
    }
    Ok(CohortBuild {
        cohorts,
        candidate_count,
        executable_count,
    })
}

fn build_cohort(
    request: &FitTradePolicyRequest,
    samples: &CohortSamples,
    notional_tier: Usd,
    executable: usize,
    upper: Decimal,
    lower: Decimal,
) -> QuantResult<TradePolicyCohort> {
    let category = samples
        .category
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "trade-policy cohort lost category identity".to_owned(),
        })?;
    let bucket_min = Decimal::from(samples.price_bucket) / Decimal::from(10);
    let bucket_max = Decimal::from(samples.price_bucket + 1) / Decimal::from(10);
    let coverage = Decimal::from(executable as u64) / Decimal::from(samples.candidate_count.max(1));
    let scale_out_targets = if request.contract.maximum_scale_out_targets == 0 {
        Vec::new()
    } else {
        vec![ScaleOutTemplate {
            target_id: "empirical_1".to_owned(),
            trigger_return_bps: Bps::new(upper * Decimal::new(6, 1)),
            target_cumulative_exit_pct: Decimal::new(5, 1),
        }]
    };
    Ok(TradePolicyCohort {
        key: TradePolicyCohortKey {
            category,
            horizon_secs: samples.horizon_secs,
            entry_price_min: Price::new(bucket_min),
            entry_price_max: Price::new(bucket_max.min(Decimal::ONE)),
            notional_tier,
            liquidity_tier: samples.liquidity_tier.clone(),
            volatility_regime: "unclassified".to_owned(),
        },
        entry_trigger: EntryTriggerTemplate::Immediate,
        entry_order: EntryOrderTemplate::Passive { post_only: true },
        upper_barrier_bps: Bps::new(upper),
        lower_barrier_bps: Bps::new(lower),
        vertical_barrier_secs: samples.horizon_secs,
        scale_out_targets,
        trailing_stop: None,
        min_score_retention: Decimal::new(6, 1),
        min_expected_return_bps: Bps::ZERO,
        require_execution_eligibility: true,
        sample_count: samples.candidate_count,
        executable_sample_count: executable as u64,
        executable_coverage: coverage,
        lower_confidence_utility_bps: upper - lower,
        parent_cohort_index: None,
    })
}

fn degraded_validation(
    request: &FitTradePolicyRequest,
    coverage: Decimal,
) -> TradePolicyValidation {
    let mut failure_reasons = Vec::new();
    if coverage < request.contract.minimum_executable_coverage {
        failure_reasons.push(format!(
            "executable coverage {coverage} is below required {}",
            request.contract.minimum_executable_coverage
        ));
    }
    failure_reasons
        .push("CPCV/DSR/PBO evidence is not yet attached; artifact remains Draft".to_owned());
    TradePolicyValidation {
        cpcv_path_count: 0,
        deflated_sharpe_ratio: Decimal::ZERO,
        probability_of_backtest_overfitting: Decimal::ONE,
        executable_coverage: coverage,
        passed: false,
        failure_reasons,
    }
}

fn degraded_execution_evidence(eligible: u64) -> TradePolicyExecutionEvidence {
    TradePolicyExecutionEvidence {
        entry_basis: "decision_best_ask".to_owned(),
        exit_basis: "forward_best_bid".to_owned(),
        full_l2_sample_count: 0,
        degraded_top_of_book_sample_count: eligible,
        full_l2_coverage: Decimal::ZERO,
        fees_included: false,
        degradation_reasons: vec![
            "historical artifact contains top-of-book observations without complete ladders"
                .to_owned(),
            "venue fees were not applied to raw MFE/MAE trajectory labels".to_owned(),
        ],
    }
}

fn liquidity_tier(liquidity: Option<Usd>) -> &'static str {
    match liquidity.map(Usd::inner) {
        Some(value) if value >= Decimal::from(10_000) => "deep",
        Some(value) if value >= Decimal::from(1_000) => "medium",
        _ => "shallow",
    }
}

fn quantile(values: &mut [Decimal], numerator: usize, denominator: usize) -> Decimal {
    values.sort_unstable();
    let index = values
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1)
        .min(values.len().saturating_sub(1));
    values.get(index).copied().unwrap_or(Decimal::ZERO)
}
