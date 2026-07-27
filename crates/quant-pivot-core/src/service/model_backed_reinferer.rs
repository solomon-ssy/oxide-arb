//! Model-backed exit signal re-inference.
//!
//! Side-effect-free, single-market re-score for thesis-invalidation: reuses the
//! research feature / factor / model primitives without persisting model-run,
//! factor, or feature rows. Loads the **intent-frozen** model version and
//! runtime-config snapshot so exit evaluation matches the entry thesis.

use std::{slice, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    storage::{StorageError, entity::QUANT_FACTOR},
};
use quant_pivot_models::{
    domain::{
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
        quant::{
            LatestFactorSnapshotBundleInfo, LatestFactorSnapshotValueInfo, ModelVersionInfo,
            OrderIntentInfo, PositionInfo,
        },
    },
    enums::{
        factor::FactorValueState,
        quant::{DataQualityStatus, OutcomeSide, PublicationStatus},
    },
    runtime_config::{
        DataQualityConfig, DecisionPolicySnapshot, DomainConfig, FactorsConfig, FeaturesConfig,
    },
    types::{
        Bps, ContentHash, DecisionPolicySnapshotId, MarketId, ModelRunId, ModelVersionId, Price,
        Usd, stable_name::FactorName,
    },
};
use quant_pivot_repository::traits::{
    FactorRepository, ModelRegistryRepository, PolicyRepository, RecommendationRepository,
};
use quant_pivot_research::{
    factors::{
        FactorEligibility, FactorValue, MarketFactorOutcome, NormalizedFactor, ScoredFactor,
    },
    features::{
        ConfiguredFeatureBuilder, FeatureSourceWindows, FeatureVector, MarketWindowSnapshot,
        TradeTapeWindowSnapshot,
    },
    model::{ModelRuntimeOutput, QuantModelRuntime, SignalCandidate},
    pit::{PointInTimeSnapshotSource, ResolvedMarketSnapshot},
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use rust_decimal::Decimal;

use crate::{
    governance::quality_gate_load::quality_gate_passed_ok,
    prefetch::feature_window::FeatureWindowProvider,
    projection::inference_batch::build_runtime_input,
    service::{
        model_serving_preimage::ModelServingPreimageService,
        signal_reinference::{ExitSignalReinferer, FreshSignal},
    },
};

/// Dependencies for [`ModelBackedExitSignalReinferer`].
pub struct ModelBackedExitSignalReinfererDeps {
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
    pub config_versions: Arc<dyn PolicyRepository>,
    /// Source of the recommendation's exact governed factor-definition set.
    pub recommendations: Arc<dyn RecommendationRepository>,
    /// Coherent latest persisted factor snapshots for exit-side inference.
    pub factors: Arc<dyn FactorRepository>,
    pub pit_source: Arc<dyn PointInTimeSnapshotSource>,
    pub window_provider: FeatureWindowProvider,
}

/// Production [`ExitSignalReinferer`] that re-scores one lot via the frozen
/// entry model and runtime-config snapshot.
pub struct ModelBackedExitSignalReinferer {
    deps: ModelBackedExitSignalReinfererDeps,
}

impl ModelBackedExitSignalReinferer {
    #[must_use]
    pub const fn new(deps: ModelBackedExitSignalReinfererDeps) -> Self {
        Self { deps }
    }

    async fn load_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>> {
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version(model_version_id)
            .await
            .map_err(QuantError::from)?
        else {
            tracing::warn!(
                %model_version_id,
                "exit signal re-inference: frozen model version not found"
            );
            return Ok(None);
        };
        if let Err(reason) = exit_model_load_ok(&version) {
            tracing::warn!(
                %model_version_id,
                %reason,
                "exit signal re-inference: model load denied"
            );
            return Ok(None);
        }
        Ok(Some(version))
    }

    async fn load_runtime(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<Option<Arc<dyn QuantModelRuntime>>> {
        match self.deps.serving_preimages.load(version).await {
            Ok(source) => match source.buy_runtime() {
                Ok(runtime) => Ok(Some(runtime)),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        model_version_id = %version.model_version_id,
                        "exit signal re-inference: model runtime build failed"
                    );
                    Ok(None)
                }
            },
            Err(error) => {
                tracing::warn!(
                    %error,
                    model_version_id = %version.model_version_id,
                    "exit signal re-inference: model preimage load failed"
                );
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl ExitSignalReinferer for ModelBackedExitSignalReinferer {
    async fn reinfer(
        &self,
        intent: &OrderIntentInfo,
        lot: &PositionInfo,
        _mark_price: Option<Price>,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<FreshSignal>> {
        let Some(config) = resolve_frozen_config(
            self.deps.config_versions.as_ref(),
            &intent.decision_policy_snapshot_id,
        )
        .await?
        else {
            return Ok(None);
        };

        let Some(version) = self.load_model_version(&intent.model_version_id).await? else {
            return Ok(None);
        };
        let Some(runtime) = self.load_runtime(&version).await? else {
            return Ok(None);
        };

        let requirements = ModelFeatureRequirements::generic_only(runtime.required_features());
        let boundary = runtime_decision_boundary(&config, now)?;
        let Some(snapshot) = self
            .deps
            .pit_source
            .market_snapshot_at(&lot.market_id, &boundary)
            .await?
        else {
            tracing::debug!(
                market_id = %lot.market_id,
                "exit signal re-inference: durable market snapshot is unavailable"
            );
            return Ok(None);
        };
        let market = selected_market_for_lot(&snapshot, lot)?;
        let Some(liquidity_cap_usd) = liquidity_score_cap(&config) else {
            return Ok(None);
        };
        let request = LiveFeatureBuildRequest {
            pit: self.deps.pit_source.as_ref(),
            window_provider: &self.deps.window_provider,
            market: &market,
            features: &config.profile_artifacts.features.definition,
            domain: &config.profile_artifacts.domain.definition,
            data_quality: &config.recommendation.data_quality,
            requirements: &requirements,
            boundary: &boundary,
            liquidity_cap_usd,
        };
        let Some(vector) = build_live_feature_vector(&request).await? else {
            return Ok(None);
        };

        let Some(fresh) = fresh_exit_outcome(
            self.deps.recommendations.as_ref(),
            self.deps.factors.as_ref(),
            intent,
            vector.market_id.clone(),
            boundary.decision_at(),
            config
                .recommendation
                .data_quality
                .max_feature_bucket_age_secs,
            &config.profile_artifacts.scoring.definition,
        )
        .await?
        else {
            return Ok(None);
        };

        let output = infer_lot(
            runtime.as_ref(),
            &fresh.source_model_run_id,
            &market,
            &vector,
            &fresh.outcome,
            boundary.decision_at(),
        )
        .await?;
        let Some(candidate) = find_lot_candidate(&output.candidates, lot) else {
            tracing::debug!(
                market_id = %lot.market_id,
                token_id = %lot.token_id,
                "exit signal re-inference: model emitted no candidate for lot"
            );
            return Ok(None);
        };

        Ok(Some(fresh_signal_from_candidate(
            candidate,
            &config,
            &version.artifact_hash,
            &fresh.snapshot_hash,
        )))
    }
}

/// Inputs to build one lot's live feature vector for exit-side re-scoring.
/// Shared by thesis-invalidation re-inference and the opportunistic Sell scorer.
pub(crate) struct LiveFeatureBuildRequest<'a> {
    pub pit: &'a dyn PointInTimeSnapshotSource,
    pub window_provider: &'a FeatureWindowProvider,
    pub market: &'a SelectedMarket,
    pub features: &'a FeaturesConfig,
    pub domain: &'a DomainConfig,
    pub data_quality: &'a DataQualityConfig,
    pub requirements: &'a ModelFeatureRequirements,
    pub boundary: &'a DecisionBoundary,
    pub liquidity_cap_usd: Usd,
}

async fn infer_lot(
    runtime: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    market: &SelectedMarket,
    vector: &FeatureVector,
    outcome: &MarketFactorOutcome,
    as_of: DateTime<Utc>,
) -> QuantResult<ModelRuntimeOutput> {
    let input = build_runtime_input(
        runtime,
        model_run_id,
        as_of,
        slice::from_ref(market),
        slice::from_ref(vector),
        slice::from_ref(outcome),
    );
    runtime.infer_batch(input).await
}

/// Load the current coherent factor plane for exit-side inference.
///
/// Cross-sectional values remain meaningful because every value comes from one
/// persisted serving run. The recommendation contributes only its frozen set of
/// governed definition ids; its entry-time contribution breakdown is never
/// reused as a current signal.
pub(crate) async fn fresh_exit_outcome(
    recommendations: &dyn RecommendationRepository,
    factors: &dyn FactorRepository,
    intent: &OrderIntentInfo,
    market_id: MarketId,
    as_of: DateTime<Utc>,
    max_age_secs: u64,
    factor_config: &FactorsConfig,
) -> QuantResult<Option<FreshExitOutcome>> {
    let Some(recommendation) = recommendations
        .find_by_id(&intent.recommendation_id)
        .await
        .map_err(QuantError::from)?
    else {
        tracing::warn!(
            recommendation_id = %intent.recommendation_id,
            "exit signal re-inference: frozen recommendation not found"
        );
        return Ok(None);
    };
    let definition_ids = &recommendation.evidence_refs.factor_definition_versions;
    if definition_ids.is_empty() {
        tracing::warn!(
            recommendation_id = %intent.recommendation_id,
            "exit signal re-inference: recommendation has no governed factor definitions"
        );
        return Ok(None);
    }
    let Some(bundle) = factors
        .latest_snapshot_bundle(definition_ids, &market_id, &intent.model_version_id, as_of)
        .await
        .map_err(QuantError::from)?
    else {
        tracing::debug!(
            %market_id,
            model_version_id = %intent.model_version_id,
            "exit signal re-inference: coherent latest factor snapshot is unavailable"
        );
        return Ok(None);
    };
    if bundle.observed_at > as_of || bundle.available_at > as_of {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FACTOR),
            format!(
                "factor snapshot {} exceeded exit re-inference cutoff {as_of}",
                bundle.snapshot_hash
            ),
        )
        .into());
    }
    let max_age = ChronoDuration::seconds(i64::try_from(max_age_secs).map_err(|error| {
        QuantError::config(format!(
            "factor snapshot max age does not fit seconds: {error}"
        ))
    })?);
    if as_of - bundle.observed_at > max_age {
        tracing::debug!(
            %market_id,
            observed_at = %bundle.observed_at,
            max_age_secs,
            "exit signal re-inference: latest factor snapshot is stale"
        );
        return Ok(None);
    }
    let outcome = snapshot_bundle_outcome(bundle.clone(), as_of, factor_config)?;
    Ok(Some(FreshExitOutcome {
        outcome,
        source_model_run_id: bundle.model_run_id,
        snapshot_hash: bundle.snapshot_hash,
    }))
}

/// Current factor evidence and its real persisted serving-run identity.
pub(crate) struct FreshExitOutcome {
    pub outcome: MarketFactorOutcome,
    pub source_model_run_id: ModelRunId,
    pub snapshot_hash: ContentHash,
}

fn snapshot_bundle_outcome(
    bundle: LatestFactorSnapshotBundleInfo,
    as_of: DateTime<Utc>,
    factor_config: &FactorsConfig,
) -> QuantResult<MarketFactorOutcome> {
    let confidence_floor = factor_config.min_factor_confidence.value;
    let mut projected = Vec::with_capacity(bundle.values.len());
    for snapshot in bundle.values {
        let normalization = match snapshot.value_state {
            FactorValueState::Scored => {
                let score = snapshot.normalized_score.ok_or_else(|| {
                    QuantError::config(format!(
                        "scored factor {} has no normalized score",
                        snapshot.factor_definition_id
                    ))
                })?;
                let source = snapshot.normalization_source.ok_or_else(|| {
                    QuantError::config(format!(
                        "scored factor {} has no normalization source",
                        snapshot.factor_definition_id
                    ))
                })?;
                if snapshot.indeterminate_reason.is_some() {
                    return Err(QuantError::config(format!(
                        "scored factor {} carries an indeterminate reason",
                        snapshot.factor_definition_id
                    )));
                }
                NormalizedFactor::Scored {
                    score,
                    source,
                    clamp: None,
                }
            }
            FactorValueState::MissingInput => {
                require_unscored_snapshot(&snapshot)?;
                NormalizedFactor::MissingInput
            }
            FactorValueState::NotApplicable => {
                require_unscored_snapshot(&snapshot)?;
                NormalizedFactor::NotApplicable
            }
            FactorValueState::Indeterminate => {
                let reason = snapshot.indeterminate_reason.ok_or_else(|| {
                    QuantError::config(format!(
                        "indeterminate factor {} has no reason",
                        snapshot.factor_definition_id
                    ))
                })?;
                if snapshot.normalized_score.is_some() || snapshot.normalization_source.is_some() {
                    return Err(QuantError::config(format!(
                        "indeterminate factor {} carries a normalized value",
                        snapshot.factor_definition_id
                    )));
                }
                NormalizedFactor::Indeterminate { reason }
            }
        };
        let explanation = snapshot.explanation;
        let contributes = matches!(normalization, NormalizedFactor::Scored { .. })
            && snapshot.confidence.inner() >= confidence_floor;
        projected.push(ScoredFactor {
            below_confidence_floor: snapshot.confidence.inner() < confidence_floor,
            contributes,
            value: FactorValue {
                definition_id: snapshot.factor_definition_id,
                name: FactorName::new(snapshot.name),
                family: snapshot.family,
                raw_value: snapshot.raw_value,
                normalization,
                direction: snapshot.direction,
                confidence: snapshot.confidence,
                explanation,
                input_feature_refs: Vec::new(),
            },
        });
    }
    Ok(MarketFactorOutcome {
        market_id: bundle.market_id,
        decision_at: as_of,
        eligibility: FactorEligibility::Eligible,
        factors: projected,
    })
}

fn require_unscored_snapshot(snapshot: &LatestFactorSnapshotValueInfo) -> QuantResult<()> {
    if snapshot.normalized_score.is_some()
        || snapshot.normalization_source.is_some()
        || snapshot.indeterminate_reason.is_some()
    {
        return Err(QuantError::config(format!(
            "unscored factor {} carries scored/indeterminate fields",
            snapshot.factor_definition_id
        )));
    }
    Ok(())
}

/// Resolve the runtime-config snapshot frozen on the intent.
///
/// Fail-safe: a missing or unparseable snapshot yields `Ok(None)` (logged) so the
/// caller holds rather than scoring the forced-exit tier against drifted live
/// thresholds. Exit evaluation must reproduce the *entry* thesis or not run.
async fn resolve_frozen_config(
    config_versions: &dyn PolicyRepository,
    version_id: &DecisionPolicySnapshotId,
) -> QuantResult<Option<DecisionPolicySnapshot>> {
    let Some(info) = config_versions
        .load_snapshot(version_id)
        .await
        .map_err(QuantError::from)?
    else {
        tracing::warn!(
            %version_id,
            "frozen runtime config missing for exit re-inference; holding (fail-safe)"
        );
        return Ok(None);
    };
    Ok(Some(info.snapshot))
}

/// Load-time policy for a model version on the exit path (shared by
/// thesis-invalidation re-inference and the opportunistic Sell scorer).
pub(crate) fn exit_model_load_ok(version: &ModelVersionInfo) -> Result<(), String> {
    match version.publication_status {
        PublicationStatus::Published | PublicationStatus::Retired => Ok(()),
        PublicationStatus::Candidate | PublicationStatus::Shadow => quality_gate_passed_ok(version),
    }
}

/// Project a position lot into a [`SelectedMarket`] for feature / model scoring.
pub fn selected_market_for_lot(
    snapshot: &ResolvedMarketSnapshot,
    lot: &PositionInfo,
) -> QuantResult<SelectedMarket> {
    if snapshot.boundary.decision_at() < lot.opened_at {
        return Err(QuantError::config(format!(
            "exit snapshot decision {} predates lot open {}",
            snapshot.boundary.decision_at(),
            lot.opened_at
        )));
    }
    let entry = snapshot.market.as_ref();
    let yes = entry.token_yes.clone();
    let no = entry.token_no.clone();
    let (primary_token_id, secondary_token_id) = match lot.side {
        OutcomeSide::Yes => (yes, Some(no)),
        OutcomeSide::No => (no, Some(yes)),
    };
    if primary_token_id != lot.token_id {
        return Err(QuantError::config(format!(
            "lot token {} does not match {:?} token {} in durable market snapshot",
            lot.token_id, lot.side, primary_token_id
        )));
    }
    if lot
        .event_id
        .as_ref()
        .is_some_and(|event_id| event_id != &snapshot.event.event_id)
        || !entry.categories.contains(lot.category)
    {
        return Err(QuantError::config(format!(
            "lot market identity does not match durable snapshot for {}",
            lot.market_id
        )));
    }
    Ok(SelectedMarket {
        market_id: lot.market_id.clone(),
        event_id: snapshot.event.event_id.clone(),
        category: lot.category,
        primary_token_id,
        secondary_token_id,
        liquidity_usd: entry.liquidity_usd,
        volume_24h_usd: entry.volume_24h,
        source_refs: Vec::new(),
    })
}

pub(crate) async fn build_live_feature_vector(
    request: &LiveFeatureBuildRequest<'_>,
) -> QuantResult<Option<FeatureVector>> {
    let builder = ConfiguredFeatureBuilder::new(request.features, request.domain)?;
    let boundary = request.boundary;
    let window = load_window(
        request.window_provider,
        &builder,
        request.market,
        boundary,
        request.features,
    )
    .await?;
    let trade_tape = load_trade_tape_window(
        request.window_provider,
        &builder,
        request.market,
        boundary,
        request.features,
    )
    .await?;

    let bundle = builder
        .resolve_inputs(
            request.market,
            boundary,
            request.pit,
            FeatureSourceWindows {
                microstructure: &window,
                trade_tape: &trade_tape,
                // Exit-side factor truth is the FROZEN entry breakdown (domain
                // factors included); the live vector only supplies price /
                // liquidity context, so it carries no domain slice.
                domain: None,
            },
            request.liquidity_cap_usd,
        )
        .await?;

    let required = request.requirements.for_category(request.market.category);
    let vector =
        builder.compute_vector(&bundle, &required, request.features, request.data_quality)?;
    if vector.data_quality == DataQualityStatus::Insufficient {
        return Ok(None);
    }
    Ok(Some(vector))
}

async fn load_window(
    window_provider: &FeatureWindowProvider,
    builder: &ConfiguredFeatureBuilder,
    market: &SelectedMarket,
    boundary: &DecisionBoundary,
    features: &FeaturesConfig,
) -> QuantResult<MarketWindowSnapshot> {
    if !builder.schema().needs_window() {
        return Ok(MarketWindowSnapshot::empty(
            market.primary_token_id.clone(),
            boundary.decision_at(),
            boundary.cutoff_for(DecisionSource::Microstructure),
        ));
    }
    let lookback = Duration::from_secs(features.max_microstructure_lookback_secs());
    let mut windows = window_provider
        .load_windows(slice::from_ref(market), boundary, lookback)
        .await?;
    windows.remove(&market.primary_token_id).ok_or_else(|| {
        QuantError::config(format!(
            "missing prefetched window for token {}",
            market.primary_token_id.as_str()
        ))
    })
}

async fn load_trade_tape_window(
    window_provider: &FeatureWindowProvider,
    builder: &ConfiguredFeatureBuilder,
    market: &SelectedMarket,
    boundary: &DecisionBoundary,
    features: &FeaturesConfig,
) -> QuantResult<TradeTapeWindowSnapshot> {
    if !builder.needs_trade_tape() {
        return Ok(TradeTapeWindowSnapshot::empty(
            market.market_id.clone(),
            boundary.decision_at(),
            boundary.cutoff_for(DecisionSource::TradeTape),
        ));
    }
    let lookback = Duration::from_secs(features.structural.trade_tape_window_secs);
    let mut windows = window_provider
        .load_trade_tape_windows(slice::from_ref(market), boundary, lookback)
        .await?;
    windows.remove(&market.market_id).ok_or_else(|| {
        QuantError::config(format!(
            "missing prefetched trade-tape window for market {}",
            market.market_id.as_str()
        ))
    })
}

pub(crate) fn runtime_decision_boundary(
    config: &DecisionPolicySnapshot,
    decision_at: DateTime<Utc>,
) -> QuantResult<DecisionBoundary> {
    let knowledge_lag_secs = config.pit_knowledge_lag_secs().ok_or_else(|| {
        QuantError::config("enabled report schedules disagree on knowledge_lag_secs")
    })?;
    DecisionClock::new(knowledge_lag_secs).serving_boundary(
        decision_at,
        config
            .profile_artifacts
            .domain
            .definition
            .crypto
            .availability_lag_secs,
        config
            .profile_artifacts
            .domain
            .definition
            .weather
            .availability_lag_secs,
    )
}

pub(crate) fn liquidity_score_cap(config: &DecisionPolicySnapshot) -> Option<Usd> {
    let max_single = parse_config_decimal(
        "portfolio.budget.max_single_recommendation_usd",
        &config
            .execution_risk
            .portfolio
            .budget
            .max_single_recommendation_usd
            .value,
    );
    let usage_cap = parse_config_decimal(
        "portfolio.constraints.liquidity_usage_cap_pct",
        &config
            .execution_risk
            .portfolio
            .constraints
            .liquidity_usage_cap_pct
            .value,
    );
    if usage_cap > Decimal::ZERO && max_single > Decimal::ZERO {
        return Some(Usd::new(max_single / usage_cap));
    }
    // No positive cap to normalize the liquidity feature against — hold rather
    // than guess a magic cap that would silently shift the composite score.
    tracing::warn!(
        %max_single,
        %usage_cap,
        "liquidity score cap unavailable (non-positive budget/usage); holding (fail-safe)"
    );
    None
}

/// Find the model candidate matching the lot's outcome token and side.
#[must_use]
pub fn find_lot_candidate<'a>(
    candidates: &'a [SignalCandidate],
    lot: &PositionInfo,
) -> Option<&'a SignalCandidate> {
    let token = lot.token_id.as_str();
    let side = lot.side;
    candidates
        .iter()
        .find(|candidate| candidate.token_id.as_str() == token && candidate.outcome_side == side)
}

fn fresh_signal_from_candidate(
    candidate: &SignalCandidate,
    config: &DecisionPolicySnapshot,
    model_artifact_hash: &ContentHash,
    factor_snapshot_hash: &ContentHash,
) -> FreshSignal {
    FreshSignal {
        model_artifact_hash: *model_artifact_hash,
        factor_snapshot_hash: *factor_snapshot_hash,
        composite_score: candidate.composite_score,
        expected_return_bps: Bps::new(candidate.expected_return_bps),
        auto_exec_eligible: fresh_auto_exec_eligible(candidate, config),
    }
}

fn fresh_auto_exec_eligible(candidate: &SignalCandidate, config: &DecisionPolicySnapshot) -> bool {
    // Thesis eligibility is the frozen score/confidence policy. Runtime mode is
    // an independent operator control and must not retrospectively rewrite the
    // thesis of an already-created auto-execution intent.
    let policy = &config.execution_authorization.auto_execution;
    let min_score = parse_config_decimal(
        "execution.auto_execution.min_score",
        &policy.min_score.value,
    );
    let min_confidence = parse_config_decimal(
        "execution.auto_execution.min_confidence",
        &policy.min_confidence.value,
    );
    candidate.composite_score.inner() >= min_score && candidate.confidence.inner() >= min_confidence
}

const fn parse_config_decimal(_field: &str, value: &Decimal) -> Decimal {
    *value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration as ChronoDuration, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::{DecisionClock, DecisionSource},
            market::registry::{EventRegistryInfo, MarketRegistryInfo, NegRiskLegSet},
            quant::PositionInfo,
        },
        enums::{
            catalog::{CatalogFilterReasonSet, CatalogTimestampQuality},
            common::{CategorySet, MarketCategory, TickSize},
            execution::PositionLedgerState,
            market::{EventStatus, MarketStatus},
            quant::{AccountSource, OutcomeSide},
        },
        runtime_config::DecisionPolicySnapshot,
        types::{
            CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId, ContentHash, EventId,
            ExecutionAccountId, MarketId, ModelRunId, OrderIntentId, PositionId, Price,
            Probability, Shares, SignalCandidateId, TokenId, Usd,
        },
    };
    use quant_pivot_research::{
        model::{ModelExplanation, SignalCandidate},
        pit::{MarketContextAt, ResolvedMarketSnapshot},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        find_lot_candidate, liquidity_score_cap, runtime_decision_boundary, selected_market_for_lot,
    };

    fn candidate(token: &str, side: OutcomeSide) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("m1"),
            token_id: TokenId::new(token),
            outcome_side: side,
            composite_score: Probability::new(dec!(0.7)),
            confidence: Probability::new(dec!(0.8)),
            expected_return_bps: dec!(120),
            downside_bps: dec!(50),
            win_probability: None,
            entry_price_ref: Price::new(dec!(0.55)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "test".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 1,
            liquidity_score: Probability::new(dec!(0.5)),
            data_quality_score: Probability::new(dec!(0.9)),
            model_score_percentile: Probability::new(dec!(0.8)),
            decision_at: Utc::now(),
        }
    }

    #[test]
    fn find_lot_matches_side() {
        let lot = PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            execution_account_id: ExecutionAccountId::from_v7(),
            token_id: TokenId::new("yes"),
            market_id: MarketId::new("m1"),
            event_id: None,
            category: MarketCategory::Sports,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Open,
            shares: Shares::new(dec!(100)),
            avg_price: Price::new(dec!(0.5)),
            cost_usd: Usd::new(dec!(50)),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        };
        let candidates = vec![
            candidate("no", OutcomeSide::No),
            candidate("yes", OutcomeSide::Yes),
        ];
        let found = find_lot_candidate(&candidates, &lot).expect("match");
        assert_eq!(found.token_id.as_str(), "yes");
    }

    #[test]
    fn selected_market_uses_primary() {
        let now = Utc::now();
        let market = MarketRegistryInfo {
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_yes: TokenId::new("yes"),
            token_no: TokenId::new("no"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            description: None,
            categories: CategorySet::from(MarketCategory::Sports),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: Vec::new(),
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            liquidity_usd: None,
            volume_24h: None,
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(now - ChronoDuration::days(1)),
            updated_at: now,
        };
        let event = EventRegistryInfo {
            event_id: EventId::new("e1"),
            title: "event".to_owned(),
            slug: "event".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: vec![MarketId::new("m1")],
            categories: CategorySet::from(MarketCategory::Sports),
            tags: Vec::new(),
            neg_risk: false,
            end_date: None,
            created_at: now - ChronoDuration::days(1),
            updated_at: now,
        };
        let boundary = DecisionClock::new(0).boundary(now).expect("boundary");
        let snapshot = ResolvedMarketSnapshot {
            boundary,
            market: Arc::new(market),
            event: Arc::new(event),
            context: MarketContextAt {
                market_id: MarketId::new("m1"),
                effective_at: now,
                available_at: now,
                status: MarketStatus::Active,
                neg_risk: false,
                start_date: None,
                end_date: None,
                created_at: Some(now - ChronoDuration::days(1)),
                fee_schedule: None,
            },
            neg_risk_leg_set: NegRiskLegSet::empty(),
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            market_change_id: CatalogMarketChangeId::from_v7(),
            event_change_id: CatalogEventChangeId::from_v7(),
            market_content_hash: ContentHash::parse(&format!("blake3:{}", "a".repeat(64)))
                .expect("hash"),
            event_content_hash: ContentHash::parse(&format!("blake3:{}", "b".repeat(64)))
                .expect("hash"),
            membership_hash: ContentHash::parse(&format!("blake3:{}", "c".repeat(64)))
                .expect("hash"),
            market_timestamp_quality: CatalogTimestampQuality::Source,
            event_timestamp_quality: CatalogTimestampQuality::Source,
            market_effective_at: now,
            market_available_at: now,
            event_effective_at: now,
            event_available_at: now,
        };
        let lot = PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            execution_account_id: ExecutionAccountId::from_v7(),
            token_id: TokenId::new("no"),
            market_id: MarketId::new("m1"),
            event_id: Some(EventId::new("e1")),
            category: MarketCategory::Sports,
            side: OutcomeSide::No,
            state: PositionLedgerState::Open,
            shares: Shares::new(dec!(100)),
            avg_price: Price::new(dec!(0.5)),
            cost_usd: Usd::new(dec!(50)),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: now - ChronoDuration::hours(1),
            updated_at: now,
            closed_at: None,
        };
        let selected = selected_market_for_lot(&snapshot, &lot).expect("market");
        assert_eq!(selected.primary_token_id.as_str(), "no");
        assert_eq!(
            selected.secondary_token_id.as_ref().map(TokenId::as_str),
            Some("yes")
        );
    }

    #[test]
    fn liquidity_score_non_positive() {
        let mut config = DecisionPolicySnapshot::default();
        // Positive budget + usage cap resolve a usable normalization cap.
        config
            .execution_risk
            .portfolio
            .budget
            .max_single_recommendation_usd
            .value = dec!(1000);
        config
            .execution_risk
            .portfolio
            .constraints
            .liquidity_usage_cap_pct
            .value = dec!(0.1);
        assert!(liquidity_score_cap(&config).is_some());
        // A zero single-recommendation budget cannot normalize the liquidity
        // feature → fail-safe None (hold) rather than a guessed magic cap.
        config
            .execution_risk
            .portfolio
            .budget
            .max_single_recommendation_usd
            .value = Decimal::ZERO;
        assert!(liquidity_score_cap(&config).is_none());
    }

    #[test]
    fn runtime_boundary_derives_time() {
        let mut config = DecisionPolicySnapshot::default();
        config.report_schedule.schedules[0].knowledge_lag_secs = 10;
        config
            .profile_artifacts
            .domain
            .definition
            .crypto
            .availability_lag_secs = 30;
        let decision_at = Utc::now();

        let boundary = runtime_decision_boundary(&config, decision_at).expect("boundary");

        assert_eq!(boundary.decision_at(), decision_at);
        assert_eq!(
            boundary.knowledge_cutoff(),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Microstructure),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Catalog),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Book),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::TradeTape),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Linkage),
            decision_at - ChronoDuration::seconds(10)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::DomainCrypto),
            decision_at - ChronoDuration::seconds(30)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::DomainWeather),
            decision_at - ChronoDuration::seconds(300)
        );
        assert_eq!(boundary.per_source_cutoffs().len(), 7);
    }
}
