//! Independent Weather policy replay over one verified Source Slice.
//!
//! This module is the only I/O-free orchestration adapter between frozen
//! Dataset rows/Source-Slice pages and the pure policy replay kernel. Fit and
//! Validate call the same functions; validation never consumes fitter output.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
    domain::{
        data_plane::{
            DecisionBoundary, DecisionSource, WeatherObservationFact, WeatherObservationReportKind,
        },
        market::{CatalogMarketChangeInfo, MarketRegistryInfo},
        quant::{LinkageOutcome, MarketLinkage, MarketSubject, ModelVersionInfo},
    },
    enums::{
        common::{MarketCategory, TickSize},
        domain::LinkageSourceRole,
        execution::ExitReason,
        quant::{OutcomeSide, PriceComparison},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ClobMarketInfoVersion, ClockAnchor, ClockCondition, ConditionTruth,
        ConditionUnavailableReason, ConfirmationPolicy, ContentHash, DecisionPolicySnapshotId,
        ENTRY_CONDITION_EVALUATOR_VERSION, ENTRY_CONDITION_SCHEMA_VERSION,
        EntryConditionArtifactV1, EntryConditionBinding, EntryConditionFactorBinding,
        EntryConditionFoldState, EntryConditionInputSet, EntryConditionSourceBinding,
        EntryConditionTemplate, EntryConditionTemplateV1, EntryConditionV1, EntryOrderTemplate,
        ExecutablePriceInput, FactorCondition, FactorSnapshotInput, MarketEventCondition,
        MarketEventTemplate, MarketId, MarketSelectionId, ModelRunId, ModelVersionId, Price,
        PriceCondition, RecommendationId, ResearchProfileArtifact, ShadowLatencyProfileV1,
        StructuralVolatilityOosEvidence, StructuralVolatilityOosFoldRow, TemperatureCelsius,
        TokenId, TradePolicyCandidateSpec, TradePolicyCandidateTrialRow, TradePolicyCohort,
        TradePolicyCohortDimension, TradePolicyCohortKey, TradePolicyCohortTrialRow,
        TradePolicyCoverageGapRow, TradePolicyCpcvPathRow, TradePolicyEvidenceFillOutcome,
        TradePolicyEvidenceLiquidityRole, TradePolicyEvidenceObjectKind,
        TradePolicyFillEvidenceRow, TradePolicyLatencyScenario, TradePolicyObservationCapability,
        TradePolicyObservationEligibilityRow, TradePolicyParameterSource, TradePolicyQualityGate,
        TradePolicyReplayGap, TradePolicyStatisticalSummaryRow, Usd, VerticalGateEvidence,
        WeatherDailyTemperatureCrossedTerminalBound, WeatherDailyTemperatureEnteredBand,
        WeatherDailyTemperatureInput, WeatherObservationDayClosedOutsideBand,
        WeatherTemperatureStatistic,
    },
};
use quant_pivot_research::{
    execution_semantics::{BookWalkOutcome, LiquidityRole, PitFeeSchedule},
    model::{QuantModelRuntime, SignalCandidate},
    pit::BookSnapshotAt,
    policy_evidence::PolicyEvidenceRecord,
    policy_replay::{
        PolicyReplayBook, PolicyReplayLatency, PolicyReplayObservation, PolicyReplayOutcome,
        PolicyReplayResolution, PolicyReplaySignal, PolicyReplayTrade, replay_policy_candidate,
    },
    policy_validation::{
        PolicyPerformanceObservation, PolicyPerformanceRequest, PolicyPerformanceSummary,
        evaluate_policy_performance,
    },
    training::TrainingExample,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    execution::entry_condition::evaluate_entry_condition, prefetch::replay_page::ReplayPage,
    projection::inference_batch::build_frozen_runtime_input,
};

pub(super) const WEATHER_REPLAY_ORCHESTRATOR_VERSION: &str = "weather_policy_orchestrator_v1";

#[derive(Debug, Clone)]
struct TimedSignal {
    decision_at: DateTime<Utc>,
    candidate: Option<SignalCandidate>,
}

/// Frozen model output timeline. Absence at one decision is explicit and
/// shadows any earlier recommendation; it never falls back to a stale signal.
#[derive(Debug, Clone)]
pub(super) struct FrozenPolicySignals {
    by_market: HashMap<MarketId, Vec<TimedSignal>>,
}

impl FrozenPolicySignals {
    fn at(
        &self,
        market_id: &MarketId,
        at: DateTime<Utc>,
        valid_for_secs: u64,
    ) -> Option<&SignalCandidate> {
        let timeline = self.by_market.get(market_id)?;
        let index = timeline.partition_point(|item| item.decision_at <= at);
        let item = index.checked_sub(1).and_then(|index| timeline.get(index))?;
        let valid_for = i64::try_from(valid_for_secs).ok().map(Duration::seconds)?;
        (at - item.decision_at <= valid_for)
            .then_some(item.candidate.as_ref())
            .flatten()
    }

    fn exact(&self, market_id: &MarketId, decision_at: DateTime<Utc>) -> Option<&SignalCandidate> {
        self.by_market
            .get(market_id)?
            .binary_search_by_key(&decision_at, |item| item.decision_at)
            .ok()
            .and_then(|index| self.by_market.get(market_id)?.get(index))?
            .candidate
            .as_ref()
    }
}

/// Re-infer every frozen cross-section once. The runtime is hash/schema
/// was built from the complete verified model preimage and consumes Dataset
/// bytes verbatim.
pub(super) async fn reinfer_frozen_policy_signals(
    runtime: &dyn QuantModelRuntime,
    model_version: &ModelVersionInfo,
    feature_schema_hash: &ContentHash,
    factor_schema_hash: &ContentHash,
    examples: &[TrainingExample],
) -> QuantResult<FrozenPolicySignals> {
    let contract = model_version
        .verified_serving_contract()
        .map_err(|error| methodology(format!("invalid persisted serving contract: {error}")))?;
    let bindings = contract.bindings();
    if bindings.schemas.feature_schema_hash != *feature_schema_hash
        || bindings.factors.plane.factor_schema_hash() != *factor_schema_hash
    {
        return Err(methodology(format!(
            "model {} serving schema/factor plane differs from frozen policy Dataset",
            model_version.model_version_id
        )));
    }
    let mut groups = BTreeMap::<DateTime<Utc>, Vec<&TrainingExample>>::new();
    for example in examples {
        groups
            .entry(example.decision_at())
            .or_default()
            .push(example);
    }
    let mut by_market = HashMap::<MarketId, Vec<TimedSignal>>::new();
    for (decision_at, mut group) in groups {
        group.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let input = build_frozen_runtime_input(runtime, &ModelRunId::from_v7(), &group)?;
        let output = runtime.infer_batch(input).await?;
        let mut emitted = HashMap::<MarketId, SignalCandidate>::new();
        for candidate in output.candidates {
            if candidate.decision_at != decision_at {
                return Err(methodology(format!(
                    "model re-inference emitted decision {} for frozen cross-section {decision_at}",
                    candidate.decision_at
                )));
            }
            match emitted.entry(candidate.market_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                Entry::Occupied(mut entry) => {
                    if candidate.route_rank < entry.get().route_rank {
                        entry.insert(candidate);
                    }
                }
            }
        }
        for example in group {
            by_market
                .entry(example.market_id.clone())
                .or_default()
                .push(TimedSignal {
                    decision_at,
                    candidate: emitted.get(&example.market_id).cloned(),
                });
        }
    }
    for timeline in by_market.values_mut() {
        timeline.sort_by_key(|item| item.decision_at);
        if timeline
            .windows(2)
            .any(|pair| pair[0].decision_at == pair[1].decision_at)
        {
            return Err(methodology(
                "policy Dataset contains duplicate market/decision rows".to_owned(),
            ));
        }
    }
    Ok(FrozenPolicySignals { by_market })
}

pub(super) struct WeatherReplayRequest<'a> {
    pub page: &'a ReplayPage,
    pub examples: &'a [TrainingExample],
    pub signals: &'a FrozenPolicySignals,
    pub candidates: &'a [TradePolicyCandidateSpec],
    pub profile: &'a ResearchProfileArtifact,
    pub model_version_id: &'a ModelVersionId,
    pub decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    pub latency_profile: &'a ShadowLatencyProfileV1,
}

/// One replayed Dataset row across every governed candidate at 1× and 2×
/// `ReportOnly` latency.
#[derive(Debug, Clone)]
pub(super) struct WeatherExampleReplay {
    pub example: TrainingExample,
    pub token_id: TokenId,
    pub weather_linkage_available: bool,
    pub model_reinference_available: bool,
    pub outcomes: Vec<PolicyReplayOutcome>,
}

#[derive(Debug, Clone)]
pub(super) struct PolicyStatisticalRun {
    pub cohort_hash: ContentHash,
    pub latency_multiplier: Decimal,
    pub summary: PolicyPerformanceSummary,
}

/// Fully typed, deterministic evidence projection. These rows are encoded and
/// sealed only after every statistical run and execution-quality gate passes.
#[derive(Debug, Clone)]
pub(super) struct WeatherPolicyEvidence {
    pub observation_eligibility: Vec<TradePolicyObservationEligibilityRow>,
    pub fills: Vec<TradePolicyFillEvidenceRow>,
    pub candidate_trials: Vec<TradePolicyCandidateTrialRow>,
    pub cohort_trials: Vec<TradePolicyCohortTrialRow>,
    pub cpcv_paths: Vec<TradePolicyCpcvPathRow>,
    pub coverage_gaps: Vec<TradePolicyCoverageGapRow>,
    pub statistical_summaries: Vec<TradePolicyStatisticalSummaryRow>,
    pub vertical_gate_evidence: Vec<VerticalGateEvidence>,
    pub structural_volatility_oos: StructuralVolatilityOosEvidence,
    pub structural_volatility_folds: Vec<StructuralVolatilityOosFoldRow>,
    pub statistical_runs: Vec<PolicyStatisticalRun>,
    pub cohorts: Vec<TradePolicyCohort>,
    pub all_gates_passed: bool,
}

impl WeatherPolicyEvidence {
    pub(crate) fn records_by_kind(
        &self,
    ) -> QuantResult<BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>> {
        let mut records = BTreeMap::new();
        records.insert(
            TradePolicyEvidenceObjectKind::ObservationEligibility,
            self.observation_eligibility
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!("{}:{}", row.cohort_hash, row.example_id),
                        Some(row.decision_at),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::Fills,
            self.fills
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!(
                            "{}:{}:{}:{}:{:010}",
                            row.cohort_hash,
                            row.example_id,
                            row.candidate_id,
                            row.latency_multiplier,
                            row.leg_ordinal
                        ),
                        Some(row.filled_at),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::CandidateTrials,
            self.candidate_trials
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!(
                            "{}:{}:{}:{}",
                            row.cohort_hash,
                            row.example_id,
                            row.candidate_id,
                            row.latency_multiplier
                        ),
                        row.terminal_at.or(row.entered_at),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::CohortTrials,
            self.cohort_trials
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!(
                            "{}:{}:{}",
                            row.cohort_hash, row.candidate_id, row.latency_multiplier
                        ),
                        None,
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::CpcvPaths,
            self.cpcv_paths
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!(
                            "{}:{}:{:010}",
                            row.cohort_hash, row.latency_multiplier, row.path_index
                        ),
                        None,
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::CoverageGaps,
            self.coverage_gaps
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!(
                            "{}:{}:{}:{}:{:?}",
                            row.cohort_hash
                                .as_ref()
                                .map_or_else(|| "global".to_owned(), ToString::to_string),
                            row.example_id,
                            row.candidate_id.as_deref().unwrap_or("all"),
                            row.latency_multiplier
                                .map_or_else(|| "all".to_owned(), |value| value.to_string()),
                            row.gap
                        ),
                        Some(row.decision_at),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::StatisticalSummaries,
            self.statistical_summaries
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!("{}:{}", row.cohort_hash, row.latency_multiplier),
                        None,
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::VerticalGates,
            self.vertical_gate_evidence
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!("{:?}:{:?}", row.gate, row.target),
                        Some(row.evidence_window_end),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        records.insert(
            TradePolicyEvidenceObjectKind::StructuralVolatilityOos,
            self.structural_volatility_folds
                .iter()
                .map(|row| {
                    PolicyEvidenceRecord::from_typed(
                        format!("{:010}", row.fold_index),
                        Some(row.test_window_end),
                        row,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        );
        Ok(records)
    }
}

pub(super) struct WeatherEvidenceRequest<'a> {
    pub profile: &'a ResearchProfileArtifact,
    pub candidates: &'a [TradePolicyCandidateSpec],
    pub experiment_family_hash: &'a ContentHash,
    pub min_embargo_secs: u64,
    pub replayed: &'a [WeatherExampleReplay],
    pub structural_volatility_oos: StructuralVolatilityOosEvidence,
    pub structural_volatility_folds: Vec<StructuralVolatilityOosFoldRow>,
}

struct WeatherLatencyRun {
    latency_multiplier: Decimal,
    summary: PolicyPerformanceSummary,
    selected_metrics: CandidateTrialMetrics,
    passed: bool,
}

pub(super) fn evaluate_weather_policy_evidence(
    request: &WeatherEvidenceRequest<'_>,
) -> QuantResult<WeatherPolicyEvidence> {
    let candidate_ids = request
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_id.clone())
        .collect::<Vec<_>>();
    let mut evidence = WeatherPolicyEvidence {
        observation_eligibility: Vec::new(),
        fills: Vec::new(),
        candidate_trials: Vec::new(),
        cohort_trials: Vec::new(),
        cpcv_paths: Vec::new(),
        coverage_gaps: Vec::new(),
        statistical_summaries: Vec::new(),
        vertical_gate_evidence: Vec::new(),
        structural_volatility_oos: request.structural_volatility_oos.clone(),
        structural_volatility_folds: request.structural_volatility_folds.clone(),
        statistical_runs: Vec::new(),
        cohorts: Vec::new(),
        all_gates_passed: request.structural_volatility_oos.valid,
    };
    for cash_budget in &request.profile.spec.allowed_cash_budget_tiers {
        let cohort = pooled_weather_cohort(request.profile, *cash_budget)?;
        let cohort_hash = CanonicalDigest::content_hash_json(&cohort)?;
        append_row_evidence(
            &mut evidence,
            request,
            &cohort_hash,
            *cash_budget,
            &candidate_ids,
        )?;
        let mut runs = Vec::new();
        for latency_multiplier in [Decimal::ONE, Decimal::TWO] {
            let run = evaluate_weather_latency_run(
                request,
                &candidate_ids,
                *cash_budget,
                latency_multiplier,
            )?;
            append_weather_latency_evidence(&mut evidence, request, &cohort, &cohort_hash, &run)?;
            evidence.all_gates_passed &= run.passed;
            runs.push(run);
        }
        let first = runs
            .first()
            .ok_or_else(|| methodology("Weather policy evidence produced no 1x run".to_owned()))?;
        let second = runs
            .get(1)
            .ok_or_else(|| methodology("Weather policy evidence produced no 2x run".to_owned()))?;
        if first.summary.selected_candidate_id != second.summary.selected_candidate_id {
            evidence.all_gates_passed = false;
        }
        let selected = request
            .candidates
            .iter()
            .find(|candidate| candidate.candidate_id == second.summary.selected_candidate_id)
            .ok_or_else(|| methodology("selected Weather candidate is unavailable".to_owned()))?;
        evidence.cohorts.push(fitted_cohort(FittedCohortRequest {
            key: cohort,
            cohort_hash,
            selected,
            one_x: &first.summary,
            two_x: &second.summary,
            one_x_metrics: &first.selected_metrics,
            two_x_metrics: &second.selected_metrics,
            trial_count: u32::try_from(request.candidates.len()).map_err(|error| {
                methodology(format!("Weather candidate count does not fit u32: {error}"))
            })?,
        })?);
        evidence
            .statistical_runs
            .extend(runs.into_iter().map(|run| PolicyStatisticalRun {
                cohort_hash,
                latency_multiplier: run.latency_multiplier,
                summary: run.summary,
            }));
    }
    Ok(evidence)
}

#[derive(Debug, Clone, Copy)]
struct CandidateTrialMetrics {
    sample_count: u64,
    executable_coverage: Decimal,
    full_l2_coverage: Decimal,
    fee_catalog_coverage: Decimal,
    ambiguous_touch_rate: Decimal,
    depth_failure_rate: Decimal,
    passive_reconciled_trade_coverage: Option<Decimal>,
}

fn evaluate_weather_latency_run(
    request: &WeatherEvidenceRequest<'_>,
    candidate_ids: &[String],
    cash_budget: Usd,
    latency_multiplier: Decimal,
) -> QuantResult<WeatherLatencyRun> {
    let observations = performance_observations(
        request.replayed,
        candidate_ids,
        cash_budget,
        latency_multiplier,
        request.profile.spec.target_horizon_secs,
    )?;
    let period_length = Duration::seconds(
        i64::try_from(request.profile.spec.decision_cadence_secs).map_err(|error| {
            methodology(format!(
                "Weather decision cadence does not fit chrono: {error}"
            ))
        })?,
    );
    let summary = evaluate_policy_performance(&PolicyPerformanceRequest {
        candidate_ids,
        observations: &observations,
        experiment_family_hash: request.experiment_family_hash,
        min_embargo_secs: request.min_embargo_secs,
        period_length,
    })?;
    let selected_metrics = trial_metrics_for_candidate(
        request.replayed,
        &summary.selected_candidate_id,
        cash_budget,
        latency_multiplier,
    )?;
    let selected_candidate = request
        .candidates
        .iter()
        .find(|candidate| candidate.candidate_id == summary.selected_candidate_id)
        .ok_or_else(|| methodology("selected Weather candidate is unavailable".to_owned()))?;
    let passed = statistical_gate_passes(
        &request.profile.spec.quality_gate,
        &summary,
        &selected_metrics,
        matches!(
            selected_candidate.entry_execution,
            EntryOrderTemplate::PassivePostOnly { .. }
        ),
    );
    Ok(WeatherLatencyRun {
        latency_multiplier,
        summary,
        selected_metrics,
        passed,
    })
}

fn append_weather_latency_evidence(
    evidence: &mut WeatherPolicyEvidence,
    request: &WeatherEvidenceRequest<'_>,
    cohort: &TradePolicyCohortKey,
    cohort_hash: &ContentHash,
    run: &WeatherLatencyRun,
) -> QuantResult<()> {
    for candidate_performance in &run.summary.candidate_performance {
        let metrics = trial_metrics_for_candidate(
            request.replayed,
            &candidate_performance.candidate_id,
            cohort.cash_budget_tier,
            run.latency_multiplier,
        )?;
        evidence.cohort_trials.push(TradePolicyCohortTrialRow {
            cohort: cohort.clone(),
            cohort_hash: *cohort_hash,
            candidate_id: candidate_performance.candidate_id.clone(),
            latency_multiplier: run.latency_multiplier,
            sample_count: metrics.sample_count,
            effective_sample_size: run.summary.effective_sample_size,
            weighted_mean_return_bps: candidate_performance.weighted_mean_return_bps,
            sharpe_ratio: candidate_performance.sharpe_ratio,
            executable_coverage: metrics.executable_coverage,
            full_l2_coverage: metrics.full_l2_coverage,
            fee_catalog_coverage: metrics.fee_catalog_coverage,
            ambiguous_touch_rate: metrics.ambiguous_touch_rate,
            depth_failure_rate: metrics.depth_failure_rate,
        });
    }
    evidence.cpcv_paths.extend(
        run.summary
            .cpcv_paths
            .iter()
            .map(|path| TradePolicyCpcvPathRow {
                cohort_hash: *cohort_hash,
                latency_multiplier: run.latency_multiplier,
                path_index: path.path_index,
                group_returns: path.group_returns.clone(),
                sharpe_ratio: path.sharpe_ratio,
                max_drawdown: path.max_drawdown,
                tail_loss: path.tail_loss,
            }),
    );
    evidence
        .statistical_summaries
        .push(TradePolicyStatisticalSummaryRow {
            cohort_hash: *cohort_hash,
            selected_candidate_id: run.summary.selected_candidate_id.clone(),
            latency_multiplier: run.latency_multiplier,
            sample_count: run.summary.sample_count,
            common_sample_count: run.summary.common_sample_count,
            common_candidate_support: run.summary.common_candidate_support,
            effective_sample_size: run.summary.effective_sample_size,
            cpcv_combination_count: run.summary.cpcv_combination_count,
            cpcv_path_count: u32::try_from(run.summary.cpcv_paths.len()).map_err(|error| {
                methodology(format!("CPCV path count does not fit u32: {error}"))
            })?,
            deflated_sharpe_ratio: run.summary.deflated_sharpe_ratio,
            dsr_benchmark_sharpe: run.summary.dsr_benchmark_sharpe,
            probability_of_backtest_overfitting: run.summary.probability_of_backtest_overfitting,
            lower_confidence_utility_bps: Bps::new(run.summary.lower_confidence_utility_bps),
            passed: run.passed,
        });
    Ok(())
}

fn append_row_evidence(
    evidence: &mut WeatherPolicyEvidence,
    request: &WeatherEvidenceRequest<'_>,
    cohort_hash: &ContentHash,
    cash_budget: Usd,
    candidate_ids: &[String],
) -> QuantResult<()> {
    for replay in request.replayed {
        let horizon_end = replay
            .example
            .decision_at()
            .checked_add_signed(Duration::seconds(
                i64::try_from(request.profile.spec.target_horizon_secs).map_err(|error| {
                    methodology(format!(
                        "Weather evidence horizon does not fit chrono: {error}"
                    ))
                })?,
            ))
            .ok_or_else(|| methodology("Weather evidence horizon overflows chrono".to_owned()))?;
        let common = |multiplier| {
            candidate_ids.iter().all(|candidate_id| {
                replay.outcomes.iter().any(|outcome| {
                    outcome.candidate_id == *candidate_id
                        && outcome.cash_budget == cash_budget
                        && outcome.latency.stress_multiplier == multiplier
                        && outcome.net_return_bps.is_some()
                })
            })
        };
        let available_capabilities = [
            (
                replay.outcomes.iter().all(|outcome| outcome.full_l2),
                TradePolicyObservationCapability::FullL2,
            ),
            (
                replay.outcomes.iter().all(|outcome| outcome.fee_covered),
                TradePolicyObservationCapability::PitFeeSchedule,
            ),
            (
                replay.model_reinference_available,
                TradePolicyObservationCapability::ModelReinference,
            ),
            (
                replay.weather_linkage_available,
                TradePolicyObservationCapability::WeatherLinkage,
            ),
        ]
        .into_iter()
        .filter_map(|(available, capability)| available.then_some(capability))
        .collect::<BTreeSet<_>>();
        let common_candidate_eligible_scenarios = [
            (common(Decimal::ONE), TradePolicyLatencyScenario::Base1x),
            (common(Decimal::TWO), TradePolicyLatencyScenario::Stress2x),
        ]
        .into_iter()
        .filter_map(|(eligible, scenario)| eligible.then_some(scenario))
        .collect::<BTreeSet<_>>();
        evidence
            .observation_eligibility
            .push(TradePolicyObservationEligibilityRow {
                example_id: replay.example.example_id,
                market_id: replay.example.market_id.clone(),
                token_id: replay.token_id.clone(),
                decision_at: replay.example.decision_at(),
                label_horizon_end: horizon_end,
                cohort_hash: *cohort_hash,
                candidate_count: u32::try_from(candidate_ids.len()).map_err(|error| {
                    methodology(format!("candidate count does not fit u32: {error}"))
                })?,
                available_capabilities,
                common_candidate_eligible_scenarios,
            });
        for outcome in replay
            .outcomes
            .iter()
            .filter(|outcome| outcome.cash_budget == cash_budget)
        {
            evidence
                .candidate_trials
                .push(TradePolicyCandidateTrialRow {
                    example_id: replay.example.example_id,
                    market_id: replay.example.market_id.clone(),
                    token_id: replay.token_id.clone(),
                    candidate_id: outcome.candidate_id.clone(),
                    cohort_hash: *cohort_hash,
                    outcome_side: outcome.outcome_side,
                    latency_multiplier: outcome.latency.stress_multiplier,
                    entry_triggered_at: outcome.entry_triggered_at,
                    entered_at: outcome.entered_at,
                    terminal_at: outcome.terminal_at,
                    terminal_reason: outcome.terminal_reason,
                    entry_fill_ratio: outcome.entry_fill_ratio,
                    exit_fill_ratio: outcome.exit_fill_ratio,
                    entry_filled_shares: outcome.entry_filled_shares,
                    exited_shares: outcome.exited_shares,
                    total_fees: outcome.total_fees,
                    net_return_bps: outcome.net_return_bps,
                    ambiguous_touch: outcome.ambiguous_touch,
                    full_l2: outcome.full_l2,
                    fee_covered: outcome.fee_covered,
                    passive_reconciled_trade_covered: outcome.passive_reconciled_trade_covered,
                    gap: outcome.gap,
                });
            if let Some(gap) = outcome.gap {
                evidence.coverage_gaps.push(TradePolicyCoverageGapRow {
                    example_id: replay.example.example_id,
                    market_id: replay.example.market_id.clone(),
                    token_id: replay.token_id.clone(),
                    candidate_id: Some(outcome.candidate_id.clone()),
                    cohort_hash: Some(*cohort_hash),
                    latency_multiplier: Some(outcome.latency.stress_multiplier),
                    decision_at: replay.example.decision_at(),
                    gap,
                    detail: format!("shared replay kernel terminal: {gap:?}"),
                });
            }
            for fill in &outcome.fills {
                evidence.fills.push(TradePolicyFillEvidenceRow {
                    example_id: replay.example.example_id,
                    cohort_hash: *cohort_hash,
                    candidate_id: outcome.candidate_id.clone(),
                    outcome_side: outcome.outcome_side,
                    latency_multiplier: outcome.latency.stress_multiplier,
                    leg_ordinal: fill.leg_ordinal,
                    side: fill.side,
                    exit_reason: fill.exit_reason,
                    triggered_at: fill.triggered_at,
                    filled_at: fill.filled_at,
                    liquidity_role: if fill.exit_reason == Some(ExitReason::ResolutionRedeem) {
                        TradePolicyEvidenceLiquidityRole::Resolution
                    } else {
                        match fill.liquidity_role {
                            LiquidityRole::Maker => TradePolicyEvidenceLiquidityRole::Maker,
                            LiquidityRole::Taker => TradePolicyEvidenceLiquidityRole::Taker,
                        }
                    },
                    outcome: match fill.outcome {
                        BookWalkOutcome::Filled => TradePolicyEvidenceFillOutcome::Filled,
                        BookWalkOutcome::Partial => TradePolicyEvidenceFillOutcome::Partial,
                        BookWalkOutcome::Unfilled => TradePolicyEvidenceFillOutcome::Unfilled,
                    },
                    requested_shares: fill.requested_shares,
                    filled_shares: fill.filled_shares,
                    vwap: fill.vwap,
                    gross_amount: fill.gross_amount,
                    fee: fill.fee,
                    cash_delta: fill.cash_delta,
                    fee_schedule_hash: fill.fee_schedule_hash,
                    stream_session_id: fill.stream_session_id,
                    token_sequence: fill.token_sequence,
                    source_event_hash: fill.source_event_hash,
                });
            }
        }
    }
    Ok(())
}

fn performance_observations(
    replayed: &[WeatherExampleReplay],
    candidate_ids: &[String],
    cash_budget: Usd,
    latency_multiplier: Decimal,
    horizon_secs: u64,
) -> QuantResult<Vec<PolicyPerformanceObservation>> {
    replayed
        .iter()
        .map(|replay| {
            let label_horizon_end = replay
                .example
                .decision_at()
                .checked_add_signed(Duration::seconds(i64::try_from(horizon_secs).map_err(
                    |error| methodology(format!("policy horizon does not fit chrono: {error}")),
                )?))
                .ok_or_else(|| methodology("policy horizon overflows chrono".to_owned()))?;
            Ok(PolicyPerformanceObservation {
                observation_id: format!(
                    "{}:{}:{}",
                    replay.example.example_id, cash_budget, latency_multiplier
                ),
                market_id: replay.example.market_id.clone(),
                decision_at: replay.example.decision_at(),
                label_horizon_end,
                candidate_return_bps: candidate_ids
                    .iter()
                    .map(|candidate_id| {
                        replay
                            .outcomes
                            .iter()
                            .find(|outcome| {
                                outcome.candidate_id == *candidate_id
                                    && outcome.cash_budget == cash_budget
                                    && outcome.latency.stress_multiplier == latency_multiplier
                            })
                            .and_then(|outcome| outcome.net_return_bps)
                    })
                    .collect(),
            })
        })
        .collect()
}

fn trial_metrics_for_candidate(
    replayed: &[WeatherExampleReplay],
    candidate_id: &str,
    cash_budget: Usd,
    latency_multiplier: Decimal,
) -> QuantResult<CandidateTrialMetrics> {
    let rows = replayed
        .iter()
        .filter_map(|replay| {
            replay.outcomes.iter().find(|outcome| {
                outcome.candidate_id == candidate_id
                    && outcome.cash_budget == cash_budget
                    && outcome.latency.stress_multiplier == latency_multiplier
            })
        })
        .collect::<Vec<_>>();
    let sample_count = u64::try_from(rows.len())
        .map_err(|error| methodology(format!("trial sample count does not fit u64: {error}")))?;
    if sample_count == 0 {
        return Err(methodology(format!(
            "candidate {candidate_id} has no replay rows"
        )));
    }
    let ratio = |count: usize| -> QuantResult<Decimal> {
        let count = u64::try_from(count)
            .map_err(|error| methodology(format!("trial count does not fit u64: {error}")))?;
        Ok(Decimal::from(count) / Decimal::from(sample_count))
    };
    let passive = rows
        .iter()
        .filter_map(|outcome| outcome.passive_reconciled_trade_covered)
        .collect::<Vec<_>>();
    let passive_reconciled_trade_coverage = (!passive.is_empty()).then(|| {
        Decimal::from(passive.iter().filter(|covered| **covered).count())
            / Decimal::from(passive.len())
    });
    Ok(CandidateTrialMetrics {
        sample_count,
        executable_coverage: ratio(
            rows.iter()
                .filter(|outcome| outcome.net_return_bps.is_some())
                .count(),
        )?,
        full_l2_coverage: ratio(rows.iter().filter(|outcome| outcome.full_l2).count())?,
        fee_catalog_coverage: ratio(rows.iter().filter(|outcome| outcome.fee_covered).count())?,
        ambiguous_touch_rate: ratio(
            rows.iter()
                .filter(|outcome| outcome.ambiguous_touch)
                .count(),
        )?,
        depth_failure_rate: ratio(
            rows.iter()
                .filter(|outcome| {
                    matches!(
                        outcome.gap,
                        Some(
                            TradePolicyReplayGap::EntryDepthInsufficient
                                | TradePolicyReplayGap::ExitDepthInsufficient
                        )
                    )
                })
                .count(),
        )?,
        passive_reconciled_trade_coverage,
    })
}

fn statistical_gate_passes(
    gate: &TradePolicyQualityGate,
    summary: &PolicyPerformanceSummary,
    metrics: &CandidateTrialMetrics,
    requires_passive_reconciliation: bool,
) -> bool {
    let Ok(min_cpcv_paths) = usize::try_from(gate.min_cpcv_paths) else {
        return false;
    };
    let passive_gate_passes = !requires_passive_reconciliation
        || metrics
            .passive_reconciled_trade_coverage
            .is_some_and(|coverage| coverage >= gate.min_passive_reconciled_trade_coverage);
    summary.effective_sample_size >= Decimal::from(gate.min_effective_sample_size)
        && summary.common_candidate_support >= gate.min_common_candidate_support
        && summary.cpcv_paths.len() >= min_cpcv_paths
        && summary.deflated_sharpe_ratio >= gate.min_deflated_sharpe_ratio
        && summary.probability_of_backtest_overfitting
            <= gate.max_probability_of_backtest_overfitting
        && summary.lower_confidence_utility_bps >= gate.min_lower_confidence_utility_bps.inner()
        && metrics.executable_coverage >= gate.min_eligible_market_coverage
        && metrics.full_l2_coverage >= gate.min_full_l2_coverage
        && metrics.fee_catalog_coverage >= gate.min_fee_catalog_coverage
        && passive_gate_passes
        && metrics.ambiguous_touch_rate <= gate.max_ambiguous_touch_rate
        && metrics.depth_failure_rate <= gate.max_depth_failure_rate
}

fn pooled_weather_cohort(
    profile: &ResearchProfileArtifact,
    cash_budget: Usd,
) -> QuantResult<TradePolicyCohortKey> {
    let methodology_hash = CanonicalDigest::content_hash_json(&(
        WEATHER_REPLAY_ORCHESTRATOR_VERSION,
        "pooled_weather",
        &profile.profile_ref,
        cash_budget,
    ))?;
    let dimension = |name: &str| TradePolicyCohortDimension {
        methodology_id: format!("weather_{name}_pooled_v1"),
        methodology_hash,
        bucket_id: "all".to_owned(),
    };
    Ok(TradePolicyCohortKey {
        profile_ref: profile.profile_ref.clone(),
        category: MarketCategory::Weather,
        horizon_secs: profile.spec.target_horizon_secs,
        entry_price_min: Price::ZERO,
        entry_price_max: Price::ONE,
        cash_budget_tier: cash_budget,
        liquidity: dimension("liquidity"),
        volatility: dimension("volatility"),
    })
}

struct FittedCohortRequest<'a> {
    key: TradePolicyCohortKey,
    cohort_hash: ContentHash,
    selected: &'a TradePolicyCandidateSpec,
    one_x: &'a PolicyPerformanceSummary,
    two_x: &'a PolicyPerformanceSummary,
    one_x_metrics: &'a CandidateTrialMetrics,
    two_x_metrics: &'a CandidateTrialMetrics,
    trial_count: u32,
}

fn fitted_cohort(request: FittedCohortRequest<'_>) -> QuantResult<TradePolicyCohort> {
    let FittedCohortRequest {
        key,
        cohort_hash,
        selected: candidate,
        one_x,
        two_x,
        one_x_metrics,
        two_x_metrics,
        trial_count,
    } = request;
    let (max_slippage_bps, max_book_age_ms) = match candidate.entry_execution {
        EntryOrderTemplate::Aggressive {
            max_slippage_bps,
            max_book_age_ms,
            ..
        } => (max_slippage_bps, max_book_age_ms),
        EntryOrderTemplate::PassivePostOnly {
            max_book_age_ms, ..
        } => (Bps::ZERO, max_book_age_ms),
    };
    let min_metric = |left: Decimal, right: Decimal| left.min(right);
    let max_metric = |left: Decimal, right: Decimal| left.max(right);
    let passive_reconciled_trade_coverage = match (
        one_x_metrics.passive_reconciled_trade_coverage,
        two_x_metrics.passive_reconciled_trade_coverage,
    ) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (None, None)
            if matches!(
                candidate.entry_execution,
                EntryOrderTemplate::Aggressive { .. }
            ) =>
        {
            None
        }
        _ => Some(Decimal::ZERO),
    };
    Ok(TradePolicyCohort {
        key,
        selected_candidate_id: candidate.candidate_id.clone(),
        entry_condition: candidate.entry_condition.clone(),
        entry_order: candidate.entry_execution.clone(),
        max_slippage_bps,
        max_book_age_ms,
        upper_barrier_bps: candidate.exit.upper_barrier_bps,
        lower_barrier_bps: candidate.exit.lower_barrier_bps,
        vertical_barrier_secs: candidate.exit.vertical_barrier_secs,
        scale_out_targets: candidate.exit.scale_out_targets.clone(),
        trailing_stop: candidate.exit.trailing_stop.clone(),
        min_score_retention: candidate.exit.min_score_retention,
        min_expected_return_bps: candidate.exit.min_expected_return_bps,
        require_route_gate_eligibility: candidate.exit.require_route_gate_eligibility,
        opportunistic_exit: candidate.exit.opportunistic_exit.clone(),
        settlement_mode: candidate.exit.settlement_mode,
        redeem_policy: candidate.exit.redeem_policy,
        sample_count: one_x.sample_count.min(two_x.sample_count),
        effective_sample_size: min_metric(one_x.effective_sample_size, two_x.effective_sample_size),
        executable_sample_count: one_x.common_sample_count.min(two_x.common_sample_count),
        executable_coverage: min_metric(
            one_x_metrics.executable_coverage,
            two_x_metrics.executable_coverage,
        ),
        full_l2_coverage: min_metric(
            one_x_metrics.full_l2_coverage,
            two_x_metrics.full_l2_coverage,
        ),
        common_candidate_support: min_metric(
            one_x.common_candidate_support,
            two_x.common_candidate_support,
        ),
        passive_reconciled_trade_coverage,
        fee_catalog_coverage: min_metric(
            one_x_metrics.fee_catalog_coverage,
            two_x_metrics.fee_catalog_coverage,
        ),
        cpcv_path_count: u32::try_from(one_x.cpcv_paths.len().min(two_x.cpcv_paths.len()))
            .map_err(|error| methodology(format!("cohort CPCV path count overflow: {error}")))?,
        trial_count,
        deflated_sharpe_ratio: min_metric(one_x.deflated_sharpe_ratio, two_x.deflated_sharpe_ratio),
        probability_of_backtest_overfitting: max_metric(
            one_x.probability_of_backtest_overfitting,
            two_x.probability_of_backtest_overfitting,
        ),
        ambiguous_touch_rate: max_metric(
            one_x_metrics.ambiguous_touch_rate,
            two_x_metrics.ambiguous_touch_rate,
        ),
        depth_failure_rate: max_metric(
            one_x_metrics.depth_failure_rate,
            two_x_metrics.depth_failure_rate,
        ),
        lower_confidence_utility_bps: Some(Bps::new(min_metric(
            one_x.lower_confidence_utility_bps,
            two_x.lower_confidence_utility_bps,
        ))),
        parameter_source: TradePolicyParameterSource {
            relaxed_dimensions: Vec::new(),
            source_sample_count: one_x.sample_count.min(two_x.sample_count),
            source_effective_sample_size: min_metric(
                one_x.effective_sample_size,
                two_x.effective_sample_size,
            ),
            source_selector_hash: cohort_hash,
        },
    })
}

pub(super) fn replay_weather_page(
    request: &WeatherReplayRequest<'_>,
) -> QuantResult<Vec<WeatherExampleReplay>> {
    if request.profile.spec.category != Some(MarketCategory::Weather) {
        return Err(methodology(format!(
            "{WEATHER_REPLAY_ORCHESTRATOR_VERSION} only accepts the Weather profile"
        )));
    }
    let first_cash_budget = request
        .profile
        .spec
        .allowed_cash_budget_tiers
        .first()
        .copied()
        .ok_or_else(|| methodology("Weather profile has no cash-budget tier".to_owned()))?;
    let base_delay_ms = shadow_action_delay_ms(request.latency_profile)?;
    let mut replayed = Vec::with_capacity(request.examples.len());
    for example in request.examples {
        let initial_signal = request
            .signals
            .exact(&example.market_id, example.decision_at());
        let Some(initial_signal) = initial_signal else {
            replayed.push(WeatherExampleReplay {
                example: example.clone(),
                token_id: example.token_id.clone(),
                weather_linkage_available: false,
                model_reinference_available: false,
                outcomes: request
                    .candidates
                    .iter()
                    .flat_map(|candidate| {
                        [Decimal::ONE, Decimal::TWO].map(|multiplier| {
                            replay_policy_candidate(
                                candidate,
                                OutcomeSide::Yes,
                                first_cash_budget,
                                TickSize::Hundredth,
                                PolicyReplayLatency {
                                    base_delay_ms,
                                    stress_multiplier: multiplier,
                                },
                                &[],
                            )
                        })
                    })
                    .collect::<QuantResult<Vec<_>>>()?,
            });
            continue;
        };
        let market_info = market_info_at(
            request.page,
            &example.market_id,
            &initial_signal.token_id,
            &example.decision_boundary,
        );
        let Some(market_info) = market_info else {
            return Err(methodology(format!(
                "market {} token {} has no decision-time CLOB market info",
                example.market_id, initial_signal.token_id
            )));
        };
        market_info
            .validate()
            .map_err(|detail| methodology(format!("invalid CLOB market info: {detail}")))?;
        let linkage = weather_linkage_at(request.page, example);
        let timeline = replay_timeline(
            example,
            request.page,
            request.profile.spec.target_horizon_secs,
            request.profile.spec.exit_heartbeat_secs,
            base_delay_ms,
        )?;
        let boundaries = timeline
            .iter()
            .map(|(at, _)| example.decision_boundary.rebased(*at))
            .collect::<QuantResult<Vec<_>>>()?;
        let books = request
            .page
            .books_at_boundaries(&initial_signal.token_id, &boundaries)?;
        let base_observations = base_observations(
            request,
            example,
            initial_signal,
            &timeline,
            &boundaries,
            books,
        )?;
        let mut outcomes = Vec::new();
        for candidate in request.candidates {
            let observations = condition_observations(
                request,
                example,
                initial_signal,
                market_info.tick_size,
                linkage,
                candidate,
                &base_observations,
            )?;
            for multiplier in [Decimal::ONE, Decimal::TWO] {
                for cash_budget in &request.profile.spec.allowed_cash_budget_tiers {
                    outcomes.push(replay_policy_candidate(
                        candidate,
                        initial_signal.outcome_side,
                        *cash_budget,
                        market_info.tick_size,
                        PolicyReplayLatency {
                            base_delay_ms,
                            stress_multiplier: multiplier,
                        },
                        &observations,
                    )?);
                }
            }
        }
        replayed.push(WeatherExampleReplay {
            example: example.clone(),
            token_id: initial_signal.token_id.clone(),
            weather_linkage_available: linkage.is_some(),
            model_reinference_available: true,
            outcomes,
        });
    }
    Ok(replayed)
}

fn shadow_action_delay_ms(profile: &ShadowLatencyProfileV1) -> QuantResult<u64> {
    [
        Some(profile.book_age_p95_ms),
        profile.decision_prepared_p95_ms,
        profile.endpoint_rtt_p95_ms,
        profile.market_delay_p95_ms,
    ]
    .into_iter()
    .try_fold(0_u64, |total, value| {
        total
            .checked_add(value.ok_or_else(|| {
                methodology("shadow latency profile has an incomplete p95 dimension".to_owned())
            })?)
            .ok_or_else(|| methodology("shadow latency p95 sum overflow".to_owned()))
    })
}

fn replay_timeline(
    example: &TrainingExample,
    page: &ReplayPage,
    horizon_secs: u64,
    heartbeat_secs: u64,
    base_delay_ms: u64,
) -> QuantResult<Vec<(DateTime<Utc>, bool)>> {
    if heartbeat_secs == 0 {
        return Err(methodology(
            "Weather replay heartbeat must be positive".to_owned(),
        ));
    }
    let horizon = Duration::seconds(i64::try_from(horizon_secs).map_err(|error| {
        methodology(format!(
            "Weather replay horizon does not fit chrono: {error}"
        ))
    })?);
    let end = example
        .decision_at()
        .checked_add_signed(horizon)
        .ok_or_else(|| methodology("Weather replay horizon overflows chrono".to_owned()))?;
    let heartbeat = Duration::seconds(i64::try_from(heartbeat_secs).map_err(|error| {
        methodology(format!(
            "Weather replay heartbeat does not fit chrono: {error}"
        ))
    })?);
    let one_x = Duration::milliseconds(i64::try_from(base_delay_ms).map_err(|error| {
        methodology(format!(
            "Weather replay latency does not fit chrono: {error}"
        ))
    })?);
    let two_x = one_x
        .checked_mul(2)
        .ok_or_else(|| methodology("Weather replay 2x latency overflows chrono".to_owned()))?;
    let mut timeline = BTreeMap::<DateTime<Utc>, bool>::new();
    let mut tick = example.decision_at();
    while tick <= end {
        timeline.insert(tick, true);
        for delay in [one_x, two_x] {
            if let Some(action_at) = tick.checked_add_signed(delay) {
                timeline.entry(action_at).or_insert(false);
            }
        }
        tick = tick
            .checked_add_signed(heartbeat)
            .ok_or_else(|| methodology("Weather replay heartbeat overflows chrono".to_owned()))?;
    }
    for resolution in page
        .resolutions
        .iter()
        .filter(|row| row.market_id == example.market_id)
    {
        if let Some(observed_at) = DateTime::from_timestamp_millis(resolution.observed_at)
            && observed_at >= example.decision_at()
            && observed_at <= end
        {
            timeline.entry(observed_at).or_insert(false);
        }
    }
    Ok(timeline.into_iter().collect())
}

fn base_observations(
    request: &WeatherReplayRequest<'_>,
    example: &TrainingExample,
    initial_signal: &SignalCandidate,
    timeline: &[(DateTime<Utc>, bool)],
    boundaries: &[DecisionBoundary],
    books: Vec<Option<BookSnapshotAt>>,
) -> QuantResult<Vec<PolicyReplayObservation>> {
    let mut previous_at = example.decision_at() - Duration::milliseconds(1);
    timeline
        .iter()
        .zip(boundaries)
        .zip(books)
        .map(|(((at, decision_tick), boundary), book)| {
            let book = book.and_then(policy_book);
            let fee_schedule = market_info_at(
                request.page,
                &example.market_id,
                &initial_signal.token_id,
                boundary,
            )
            .map(|market_info| {
                PitFeeSchedule::from_market_fee_schedule(&market_info.fee_schedule())
                    .map_err(|error| methodology(format!("invalid PIT fee schedule: {error:?}")))
            })
            .transpose()?;
            let signal = request
                .signals
                .at(
                    &example.market_id,
                    *at,
                    request.profile.spec.decision_cadence_secs,
                )
                .map(policy_signal);
            let (passive_trades, passive_trade_coverage) = passive_trades(
                request.page,
                &initial_signal.token_id,
                previous_at,
                *at,
                boundary,
            );
            previous_at = *at;
            Ok(PolicyReplayObservation {
                at: *at,
                decision_tick: *decision_tick,
                condition_truth: ConditionTruth::Satisfied,
                book,
                fee_schedule,
                signal,
                passive_trade_coverage,
                passive_trades,
                resolution: resolution_at(
                    request.page,
                    &example.market_id,
                    &initial_signal.token_id,
                    boundary,
                )?,
            })
        })
        .collect()
}

fn policy_book(snapshot: BookSnapshotAt) -> Option<PolicyReplayBook> {
    let source = snapshot.source_event?;
    Some(PolicyReplayBook {
        bids: snapshot.bids.iter().copied().collect(),
        asks: snapshot.asks.iter().copied().collect(),
        observed_at: DateTime::from_timestamp_millis(i64::try_from(snapshot.timestamp_ms).ok()?)?,
        available_at: snapshot.available_at,
        stream_session_id: source.stream_session_id,
        token_sequence: source.token_sequence,
        source_event_hash: source.source_event_hash,
    })
}

fn policy_signal(candidate: &SignalCandidate) -> PolicyReplaySignal {
    PolicyReplaySignal {
        token_id: candidate.token_id.clone(),
        outcome_side: candidate.outcome_side,
        composite_score: candidate.composite_score.inner(),
        expected_return_bps: candidate.expected_return_bps,
        route_gate_eligible: true,
        opportunistic_confidence: None,
        opportunistic_expected_alpha_bps: None,
        opportunistic_p_exit_better: None,
    }
}

fn market_info_at<'a>(
    page: &'a ReplayPage,
    market_id: &MarketId,
    token_id: &TokenId,
    boundary: &DecisionBoundary,
) -> Option<&'a ClobMarketInfoVersion> {
    page.clob_market_info
        .iter()
        .filter(|version| {
            &version.market_id == market_id
                && version
                    .tokens
                    .iter()
                    .any(|token| &token.token_id == token_id)
                && version.effective_at <= boundary.knowledge_cutoff()
                && version.available_at <= boundary.decision_at()
        })
        .max_by(|left, right| {
            (left.effective_at, left.available_at, &left.payload_hash).cmp(&(
                right.effective_at,
                right.available_at,
                &right.payload_hash,
            ))
        })
}

const fn passive_trades(
    _page: &ReplayPage,
    _token_id: &TokenId,
    _after: DateTime<Utc>,
    _at: DateTime<Utc>,
    _boundary: &DecisionBoundary,
) -> (Vec<PolicyReplayTrade>, bool) {
    // Finalized Polygon executions have exact economic identity but cannot be
    // bound retroactively to a historical CLOB stream session. Treating them
    // as queue-depleting prints would invent execution fidelity. Passive replay
    // therefore remains fail-closed; bootstrap profiles never build a
    // TradePolicy and use a separate reference-scenario contract.
    (Vec::new(), false)
}

fn resolution_at(
    page: &ReplayPage,
    market_id: &MarketId,
    token_id: &TokenId,
    boundary: &DecisionBoundary,
) -> QuantResult<Option<PolicyReplayResolution>> {
    let mut selected = None;
    for row in &page.resolutions {
        if &row.market_id != market_id {
            continue;
        }
        let resolved_at = DateTime::from_timestamp_millis(row.resolved_at).ok_or_else(|| {
            methodology(format!(
                "market resolution `{market_id}` has invalid resolved_at {}",
                row.resolved_at
            ))
        })?;
        let observed_at = DateTime::from_timestamp_millis(row.observed_at).ok_or_else(|| {
            methodology(format!(
                "market resolution `{market_id}` has invalid observed_at {}",
                row.observed_at
            ))
        })?;
        if resolved_at > boundary.knowledge_cutoff() || observed_at > boundary.decision_at() {
            continue;
        }
        let token_payout_ratio = row.payout_for(token_id).map_err(|error| {
            methodology(format!(
                "market resolution `{market_id}` cannot settle token `{token_id}`: {error}"
            ))
        })?;
        let candidate = PolicyReplayResolution {
            token_payout_ratio,
            resolved_at,
            observed_at,
        };
        if selected
            .as_ref()
            .is_none_or(|current: &PolicyReplayResolution| {
                (candidate.observed_at, candidate.resolved_at)
                    > (current.observed_at, current.resolved_at)
            })
        {
            selected = Some(candidate);
        }
    }
    Ok(selected)
}

fn weather_linkage_at<'a>(
    page: &'a ReplayPage,
    example: &TrainingExample,
) -> Option<&'a MarketLinkage> {
    let cutoff = example
        .decision_boundary
        .cutoff_for(DecisionSource::Linkage);
    page.linkages
        .iter()
        .filter(|linkage| {
            linkage.market_id == example.market_id
                && linkage.effective_at <= cutoff
                && linkage.available_at <= example.decision_at()
                && matches!(
                    &linkage.outcome,
                    LinkageOutcome::Resolved(binding)
                        if matches!(binding.subject, MarketSubject::Weather(_))
                )
        })
        .max_by(|left, right| {
            (left.effective_at, left.available_at, &left.content_hash).cmp(&(
                right.effective_at,
                right.available_at,
                &right.content_hash,
            ))
        })
}

fn condition_observations(
    request: &WeatherReplayRequest<'_>,
    example: &TrainingExample,
    signal: &SignalCandidate,
    tick_size: TickSize,
    linkage: Option<&MarketLinkage>,
    candidate: &TradePolicyCandidateSpec,
    base: &[PolicyReplayObservation],
) -> QuantResult<Vec<PolicyReplayObservation>> {
    let EntryConditionTemplate::Conditional {
        root,
        confirmation_ms,
        max_observation_gap_ms,
    } = &candidate.entry_condition
    else {
        return Ok(base.to_vec());
    };
    let Some(linkage) = linkage else {
        let mut unavailable = base.to_vec();
        for observation in &mut unavailable {
            observation.condition_truth =
                ConditionTruth::Unavailable(ConditionUnavailableReason::InputMissing);
        }
        return Ok(unavailable);
    };
    let artifact = materialize_condition_artifact(MaterializeConditionArtifactRequest {
        replay: request,
        example,
        signal,
        tick_size,
        linkage,
        template: root,
        confirmation_ms: *confirmation_ms,
        max_observation_gap_ms: *max_observation_gap_ms,
    })?;
    let mut fold_state = EntryConditionFoldState::default();
    let mut observations = base.to_vec();
    for observation in &mut observations {
        if !observation.decision_tick {
            continue;
        }
        let input = condition_inputs(
            request,
            example,
            observation,
            &artifact.binding,
            linkage,
            fold_state,
        )?;
        let evaluated = evaluate_entry_condition(&artifact, &input)?;
        observation.condition_truth = evaluated.truth;
        fold_state = evaluated.fold_state;
    }
    Ok(observations)
}

#[derive(Clone, Copy)]
struct MaterializeConditionArtifactRequest<'a> {
    replay: &'a WeatherReplayRequest<'a>,
    example: &'a TrainingExample,
    signal: &'a SignalCandidate,
    tick_size: TickSize,
    linkage: &'a MarketLinkage,
    template: &'a EntryConditionTemplateV1,
    confirmation_ms: u64,
    max_observation_gap_ms: u64,
}

fn materialize_condition_artifact(
    input: MaterializeConditionArtifactRequest<'_>,
) -> QuantResult<EntryConditionArtifactV1> {
    let MaterializeConditionArtifactRequest {
        replay: request,
        example,
        signal,
        tick_size,
        linkage,
        template,
        confirmation_ms,
        max_observation_gap_ms,
    } = input;
    let market = catalog_market_at(request.page, example)?;
    let root = materialize_condition_tree(
        template,
        tick_size,
        signal,
        example.decision_at(),
        &market,
        linkage,
        request.model_version_id,
    )?;
    let mut factor_bindings = Vec::new();
    let mut source_bindings = Vec::new();
    collect_bindings(&root, &mut factor_bindings, &mut source_bindings);
    factor_bindings.sort_by(|left, right| {
        left.definition_id
            .to_string()
            .cmp(&right.definition_id.to_string())
    });
    factor_bindings.dedup();
    source_bindings.sort();
    source_bindings.dedup();
    let capture = example.decision_capture.as_ref().ok_or_else(|| {
        methodology(format!(
            "Weather Dataset row {} has no decision capture",
            example.example_id
        ))
    })?;
    let catalog_snapshot_hash = CanonicalDigest::content_hash_json(&capture.snapshot.catalog)?;
    let recommendation_id = deterministic_recommendation_id(example);
    let market_selection_id = deterministic_market_selection_id(&catalog_snapshot_hash);
    EntryConditionArtifactV1 {
        schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
        evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
        binding: EntryConditionBinding {
            recommendation_id,
            market_id: example.market_id.clone(),
            token_id: signal.token_id.clone(),
            outcome_side: signal.outcome_side,
            market_linkage_id: Some(linkage.linkage_id),
            market_linkage_hash: Some(linkage.content_hash),
            catalog_snapshot_id: market_selection_id,
            catalog_snapshot_hash,
            model_version_id: *request.model_version_id,
            decision_policy_snapshot_id: *request.decision_policy_snapshot_id,
            factor_bindings,
            source_bindings,
        },
        confirmation: ConfirmationPolicy {
            required_continuous_ms: confirmation_ms,
            max_observation_gap_ms,
        },
        root,
    }
    .canonicalize()
    .map_err(|error| methodology(format!("Weather condition artifact is invalid: {error}")))
}

fn materialize_condition_tree(
    template: &EntryConditionTemplateV1,
    tick_size: TickSize,
    signal: &SignalCandidate,
    decision_at: DateTime<Utc>,
    market: &MarketRegistryInfo,
    linkage: &MarketLinkage,
    model_version_id: &ModelVersionId,
) -> QuantResult<EntryConditionV1> {
    match template {
        EntryConditionTemplateV1::Price {
            comparison,
            threshold,
            max_input_age_ms,
        } => Ok(EntryConditionV1::Price(PriceCondition {
            token_id: signal.token_id.clone(),
            comparison: *comparison,
            threshold: tick_aligned_price(*threshold, tick_size, *comparison),
            max_input_age_ms: *max_input_age_ms,
        })),
        EntryConditionTemplateV1::Clock { anchor, offset_ms } => {
            let anchor_at = match anchor {
                ClockAnchor::RecommendationDecision => decision_at,
                ClockAnchor::MarketStart => market.start_date.ok_or_else(|| {
                    methodology(format!("market {} has no start clock", market.market_id))
                })?,
                ClockAnchor::MarketEnd => market.end_date.ok_or_else(|| {
                    methodology(format!("market {} has no end clock", market.market_id))
                })?,
            };
            let deadline_at = anchor_at
                .checked_add_signed(Duration::milliseconds(*offset_ms))
                .ok_or_else(|| methodology("condition clock overflows chrono".to_owned()))?;
            Ok(EntryConditionV1::Clock(ClockCondition {
                anchor: *anchor,
                anchor_at,
                offset_ms: *offset_ms,
                deadline_at,
            }))
        }
        EntryConditionTemplateV1::Factor {
            definition_id,
            definition_hash,
            measure,
            comparison,
            threshold,
            minimum_confidence,
            max_input_age_ms,
        } => Ok(EntryConditionV1::Factor(FactorCondition {
            definition_id: *definition_id,
            definition_hash: *definition_hash,
            model_version_id: *model_version_id,
            measure: *measure,
            comparison: *comparison,
            threshold: *threshold,
            minimum_confidence: *minimum_confidence,
            max_input_age_ms: *max_input_age_ms,
        })),
        EntryConditionTemplateV1::MarketEvent { event } => Ok(EntryConditionV1::MarketEvent {
            event: materialize_weather_event(*event, signal, linkage)?,
        }),
        EntryConditionTemplateV1::All { children } => Ok(EntryConditionV1::All {
            children: children
                .iter()
                .map(|child| {
                    materialize_condition_tree(
                        child,
                        tick_size,
                        signal,
                        decision_at,
                        market,
                        linkage,
                        model_version_id,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        }),
        EntryConditionTemplateV1::Any { children } => Ok(EntryConditionV1::Any {
            children: children
                .iter()
                .map(|child| {
                    materialize_condition_tree(
                        child,
                        tick_size,
                        signal,
                        decision_at,
                        market,
                        linkage,
                        model_version_id,
                    )
                })
                .collect::<QuantResult<Vec<_>>>()?,
        }),
    }
}

fn materialize_weather_event(
    template: MarketEventTemplate,
    signal: &SignalCandidate,
    linkage: &MarketLinkage,
) -> QuantResult<MarketEventCondition> {
    let LinkageOutcome::Resolved(binding) = &linkage.outcome else {
        return Err(methodology("Weather linkage is unresolved".to_owned()));
    };
    let MarketSubject::Weather(subject) = &binding.subject else {
        return Err(methodology(
            "Weather candidate is bound to a non-Weather subject".to_owned(),
        ));
    };
    let source = binding
        .source_bindings
        .iter()
        .find(|source| source.role == LinkageSourceRole::LiveEvent)
        .map(|source| EntryConditionSourceBinding {
            source_id: source.source_id.clone(),
            instrument_key: source.instrument_key.clone(),
            binding_hash: source.binding_hash,
        })
        .ok_or_else(|| methodology("Weather linkage has no live-event source".to_owned()))?;
    let MarketEventTemplate::WeatherDailyTemperaturePredicate { max_input_age_ms } = template
    else {
        return Err(methodology(
            "Weather candidate contains a non-Weather event template".to_owned(),
        ));
    };
    Ok(match signal.outcome_side {
        OutcomeSide::Yes => MarketEventCondition::WeatherDailyTemperatureEnteredBand(
            WeatherDailyTemperatureEnteredBand {
                source,
                station: subject.decision_group.station.to_string(),
                local_date: subject.decision_group.local_date,
                temperature_statistic: subject.decision_group.temperature_statistic,
                unit: subject.decision_group.market_unit,
                band: subject.outcome_band.clone(),
                proxy_methodology_hash: subject.decision_group.proxy_methodology_hash,
                max_input_age_ms,
            },
        ),
        OutcomeSide::No => match match subject.decision_group.temperature_statistic {
            WeatherTemperatureStatistic::Maximum => subject.outcome_band.upper_inclusive,
            WeatherTemperatureStatistic::Minimum => subject.outcome_band.lower_inclusive,
        } {
            Some(terminal_bound) => {
                MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(
                    WeatherDailyTemperatureCrossedTerminalBound {
                        source,
                        station: subject.decision_group.station.to_string(),
                        local_date: subject.decision_group.local_date,
                        temperature_statistic: subject.decision_group.temperature_statistic,
                        unit: subject.decision_group.market_unit,
                        terminal_bound,
                        proxy_methodology_hash: subject.decision_group.proxy_methodology_hash,
                        max_input_age_ms,
                    },
                )
            }
            None => MarketEventCondition::WeatherObservationDayClosedOutsideBand(
                WeatherObservationDayClosedOutsideBand {
                    source,
                    station: subject.decision_group.station.to_string(),
                    local_date: subject.decision_group.local_date,
                    temperature_statistic: subject.decision_group.temperature_statistic,
                    unit: subject.decision_group.market_unit,
                    band: subject.outcome_band.clone(),
                    proxy_methodology_hash: subject.decision_group.proxy_methodology_hash,
                },
            ),
        },
    })
}

fn condition_inputs(
    request: &WeatherReplayRequest<'_>,
    example: &TrainingExample,
    observation: &PolicyReplayObservation,
    binding: &EntryConditionBinding,
    linkage: &MarketLinkage,
    fold_state: EntryConditionFoldState,
) -> QuantResult<EntryConditionInputSet> {
    let prices = observation
        .book
        .as_ref()
        .and_then(|book| book.asks.first().map(|level| (book, level.price_decimal())))
        .map(|(book, price)| {
            vec![ExecutablePriceInput {
                token_id: binding.token_id.clone(),
                price,
                observed_at: book.observed_at,
                available_at: book.available_at,
                gap_generation: 0,
            }]
        })
        .unwrap_or_default();
    let factors = factor_inputs(
        request.examples,
        &example.market_id,
        observation.at,
        request.profile.spec.decision_cadence_secs,
        binding,
    )?;
    let weather = weather_input(
        request.page,
        linkage,
        observation.at,
        &example.decision_boundary,
    )?
    .into_iter()
    .collect();
    Ok(EntryConditionInputSet {
        binding: binding.clone(),
        binding_revision: linkage.content_hash,
        binding_unavailable_reason: None,
        fold_state,
        evaluated_at: observation.at,
        prices,
        factors,
        crypto: Vec::new(),
        weather,
    })
}

fn factor_inputs(
    examples: &[TrainingExample],
    market_id: &MarketId,
    at: DateTime<Utc>,
    valid_for_secs: u64,
    binding: &EntryConditionBinding,
) -> QuantResult<Vec<FactorSnapshotInput>> {
    let Some(example) = examples
        .iter()
        .filter(|example| example.market_id == *market_id && example.decision_at() <= at)
        .max_by_key(|example| example.decision_at())
    else {
        return Ok(Vec::new());
    };
    let max_age =
        Duration::seconds(i64::try_from(valid_for_secs).map_err(|error| {
            methodology(format!("factor validity does not fit chrono: {error}"))
        })?);
    if at - example.decision_at() > max_age {
        return Ok(Vec::new());
    }
    binding
        .factor_bindings
        .iter()
        .filter_map(|factor_binding| {
            example
                .factor_values
                .iter()
                .find(|factor| factor.definition_id == factor_binding.definition_id)
                .map(|factor| (factor_binding, factor))
        })
        .filter_map(|(factor_binding, factor)| {
            let raw_value = factor.raw_value?;
            let normalized_value = factor.normalized_score()?.inner();
            Some((factor_binding, factor, raw_value, normalized_value))
        })
        .map(|(factor_binding, factor, raw_value, normalized_value)| {
            Ok(FactorSnapshotInput {
                definition_id: factor_binding.definition_id,
                definition_hash: factor_binding.definition_hash,
                model_version_id: binding.model_version_id,
                raw_value,
                normalized_value,
                confidence: factor.confidence.inner(),
                observed_at: example.decision_at(),
                available_at: example.decision_at(),
                snapshot_hash: CanonicalDigest::content_hash_json(&(
                    example.decision_at(),
                    factor,
                ))?,
            })
        })
        .collect()
}

fn weather_input(
    page: &ReplayPage,
    linkage: &MarketLinkage,
    at: DateTime<Utc>,
    initial_boundary: &DecisionBoundary,
) -> QuantResult<Option<WeatherDailyTemperatureInput>> {
    let LinkageOutcome::Resolved(binding) = &linkage.outcome else {
        return Ok(None);
    };
    let MarketSubject::Weather(subject) = &binding.subject else {
        return Ok(None);
    };
    let Some(source) = binding
        .source_bindings
        .iter()
        .find(|source| source.role == LinkageSourceRole::LiveEvent)
    else {
        return Ok(None);
    };
    let boundary = initial_boundary.rebased(at)?;
    let cutoff = boundary.cutoff_for(DecisionSource::DomainWeather);
    let mut latest = BTreeMap::<DateTime<Utc>, &WeatherObservationFact>::new();
    for fact in page.weather_observations.iter().filter(|fact| {
        fact.station().as_ref() == Some(&subject.decision_group.station)
            && fact.local_date == subject.decision_group.local_date
            && fact.report_kind != WeatherObservationReportKind::HistoricalGhcnh
            && fact.observed_at <= cutoff
            && fact.available_at <= at
            && fact.temperature_celsius().is_some()
    }) {
        let replace = latest.get(&fact.observed_at).is_none_or(|current| {
            (current.revision, current.available_at, &current.report_hash)
                < (fact.revision, fact.available_at, &fact.report_hash)
        });
        if replace {
            latest.insert(fact.observed_at, fact);
        }
    }
    if latest.is_empty() {
        return Ok(None);
    }
    let temperatures = latest
        .values()
        .filter_map(|fact| fact.temperature_celsius().map(TemperatureCelsius::value));
    let current_extreme = match subject.decision_group.temperature_statistic {
        WeatherTemperatureStatistic::Maximum => temperatures.max(),
        WeatherTemperatureStatistic::Minimum => temperatures.min(),
    }
    .ok_or_else(|| methodology("Weather daily-temperature fold is empty".to_owned()))?;
    let latest_fact = latest
        .values()
        .max_by_key(|fact| (fact.observed_at, fact.available_at, fact.revision))
        .ok_or_else(|| methodology("Weather daily-temperature has no latest fact".to_owned()))?;
    let report_hash = CanonicalDigest::content_hash_json(
        &latest
            .iter()
            .map(|(observation_time, fact)| (*observation_time, fact.revision, &fact.report_hash))
            .collect::<Vec<_>>(),
    )?;
    let timezone = subject
        .decision_group
        .timezone
        .parse::<Tz>()
        .map_err(|error| {
            methodology(format!(
                "Weather timezone {} is invalid: {error}",
                subject.decision_group.timezone
            ))
        })?;
    let next_date = subject
        .decision_group
        .local_date
        .succ_opt()
        .ok_or_else(|| methodology("Weather local date overflows".to_owned()))?;
    let local_midnight = timezone
        .from_local_datetime(
            &next_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| methodology("Weather local midnight is invalid".to_owned()))?,
        )
        .single()
        .ok_or_else(|| methodology("Weather local midnight is ambiguous".to_owned()))?;
    let grace = i64::try_from(WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS).map_err(|error| {
        methodology(format!(
            "Weather day-close grace does not fit chrono: {error}"
        ))
    })?;
    let close_at = (local_midnight + Duration::seconds(grace)).with_timezone(&Utc);
    let next_day_observed = page.weather_observations.iter().any(|fact| {
        fact.station().as_ref() == Some(&subject.decision_group.station)
            && fact.local_date >= next_date
            && fact.available_at <= at
            && fact.report_kind != WeatherObservationReportKind::HistoricalGhcnh
    });
    Ok(Some(WeatherDailyTemperatureInput {
        source: EntryConditionSourceBinding {
            source_id: source.source_id.clone(),
            instrument_key: source.instrument_key.clone(),
            binding_hash: source.binding_hash,
        },
        station: subject.decision_group.station.to_string(),
        local_date: subject.decision_group.local_date,
        temperature_statistic: subject.decision_group.temperature_statistic,
        current_extreme: TemperatureCelsius::new(current_extreme),
        observation_time: latest_fact.observed_at,
        available_at: latest_fact.available_at,
        revision: u64::try_from(latest.len())
            .map_err(|error| methodology(format!("Weather revision overflow: {error}")))?,
        day_closed: at >= close_at && next_day_observed,
        report_hash,
        gap_generation: 0,
        source_healthy: true,
    }))
}

fn catalog_market_at(
    page: &ReplayPage,
    example: &TrainingExample,
) -> QuantResult<MarketRegistryInfo> {
    let version = page
        .catalog_markets
        .iter()
        .filter(|version| {
            version.market_id == example.market_id
                && version.source_effective_at
                    <= example
                        .decision_boundary
                        .cutoff_for(DecisionSource::Catalog)
                && version.available_at <= example.decision_at()
        })
        .max_by(|left, right| catalog_change_order(left, right))
        .ok_or_else(|| {
            methodology(format!(
                "market {} has no PIT catalog row",
                example.market_id
            ))
        })?;
    serde_json::from_value(version.payload.clone().into_inner()).map_err(|error| {
        methodology(format!(
            "market {} catalog payload is invalid: {error}",
            version.market_id
        ))
    })
}

fn catalog_change_order(
    left: &CatalogMarketChangeInfo,
    right: &CatalogMarketChangeInfo,
) -> Ordering {
    (
        left.source_effective_at,
        left.available_at,
        &left.content_hash,
    )
        .cmp(&(
            right.source_effective_at,
            right.available_at,
            &right.content_hash,
        ))
}

fn collect_bindings(
    node: &EntryConditionV1,
    factors: &mut Vec<EntryConditionFactorBinding>,
    sources: &mut Vec<EntryConditionSourceBinding>,
) {
    match node {
        EntryConditionV1::Factor(condition) => factors.push(EntryConditionFactorBinding {
            definition_id: condition.definition_id,
            definition_hash: condition.definition_hash,
        }),
        EntryConditionV1::MarketEvent { event } => sources.push(match event {
            MarketEventCondition::CryptoSubjectPredicateEntered(condition) => {
                condition.source.clone()
            }
            MarketEventCondition::WeatherDailyTemperatureEnteredBand(condition) => {
                condition.source.clone()
            }
            MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(condition) => {
                condition.source.clone()
            }
            MarketEventCondition::WeatherObservationDayClosedOutsideBand(condition) => {
                condition.source.clone()
            }
        }),
        EntryConditionV1::All { children } | EntryConditionV1::Any { children } => {
            for child in children {
                collect_bindings(child, factors, sources);
            }
        }
        EntryConditionV1::Price(_) | EntryConditionV1::Clock(_) => {}
    }
}

fn tick_aligned_price(value: Price, tick_size: TickSize, comparison: PriceComparison) -> Price {
    let tick = tick_size.as_decimal();
    let units = value.inner().clamp(tick, Decimal::ONE - tick) / tick;
    let rounded = match comparison {
        PriceComparison::AtOrAbove => units.ceil(),
        PriceComparison::AtOrBelow => units.floor(),
    };
    Price::new((rounded * tick).clamp(tick, Decimal::ONE - tick))
}

fn deterministic_recommendation_id(example: &TrainingExample) -> RecommendationId {
    const NAMESPACE: Uuid = Uuid::from_u128(0x8b6a_7370_bef4_49e1_b135_2b40_43a1_32e1);
    RecommendationId::new(Uuid::new_v5(
        &NAMESPACE,
        example.example_id.to_string().as_bytes(),
    ))
}

fn deterministic_market_selection_id(hash: &ContentHash) -> MarketSelectionId {
    const NAMESPACE: Uuid = Uuid::from_u128(0x93ca_7564_5b4d_4788_a748_3114_b8df_3226);
    let canonical = hash.canonical_text();
    MarketSelectionId::new(Uuid::new_v5(&NAMESPACE, canonical.as_bytes()))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}
