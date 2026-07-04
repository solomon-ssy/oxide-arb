//! Model-backed exit signal re-inference (Phase 06.0).
//!
//! Side-effect-free, single-market re-score for thesis-invalidation: reuses the
//! research feature / factor / model primitives without persisting model-run,
//! factor, or feature rows. Loads the **intent-frozen** model version and
//! runtime-config snapshot so exit evaluation matches the entry thesis.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{ModelVersionInfo, OrderIntentInfo, PointInTimeDataSource, PositionInfo},
    enums::quant::{DataQualityStatus, OutcomeSide, PublicationStatus},
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, RuntimeConfig},
    types::{Bps, ModelRunId, ModelVersionId, Price, RuntimeConfigVersionId, Usd},
};
use quant_pivot_repository::traits::{ModelRegistryRepository, RuntimeConfigVersionRepository};
use quant_pivot_research::{
    factors::{FactorEngine, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, FeatureSchema, FeatureVector, MarketWindowSnapshot, PitView,
        merged_required_features,
    },
    hashing::ResearchHasher,
    model::{
        ActiveSchemaBinding, ModelRuntimeFactoryBuilder, ModelRuntimeOutput, QuantModelRuntime,
        SignalCandidate,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use rust_decimal::Decimal;

use crate::{
    governance::quality_gate_load::quality_gate_passed_ok,
    pipeline::{
        feature_window_provider::FeatureWindowProvider, inference_batch::build_runtime_input,
        market_registry::MarketRegistry,
    },
    service::signal_reinference::{ExitSignalReinferer, FreshSignal},
};

/// Dependencies for [`ModelBackedExitSignalReinferer`].
pub struct ModelBackedExitSignalReinfererDeps {
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    pub weight_overlay: Arc<crate::governance::WeightOverlayApplicator>,
    pub config_versions: Arc<dyn RuntimeConfigVersionRepository>,
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    pub market_registry: Arc<MarketRegistry>,
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
            .find_model_version_by_id(model_version_id)
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
        config: &RuntimeConfig,
    ) -> QuantResult<Option<Box<dyn QuantModelRuntime>>> {
        let binding = schema_binding(&config.features, &config.factors)?;
        let factory = self.deps.factory_builder.build(binding);
        let overlay = resolve_overlay(&self.deps.weight_overlay, version);
        match factory.load(version, overlay).await {
            Ok(runtime) => Ok(Some(runtime)),
            Err(error) => {
                tracing::warn!(
                    %error,
                    model_version_id = %version.model_version_id,
                    "exit signal re-inference: model runtime load failed"
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
            &intent.runtime_config_version_id,
        )
        .await?
        else {
            return Ok(None);
        };

        let Some(market) = selected_market_for_lot(&self.deps.market_registry, lot) else {
            tracing::debug!(
                market_id = %lot.market_id,
                "exit signal re-inference: market not in registry"
            );
            return Ok(None);
        };

        let Some(version) = self.load_model_version(&intent.model_version_id).await? else {
            return Ok(None);
        };
        let Some(runtime) = self.load_runtime(&version, &config).await? else {
            return Ok(None);
        };

        let requirements = ModelFeatureRequirements {
            required_features: runtime.required_features(),
        };
        // Exit re-inference has no schedule/request source delay; fall back the
        // live feature `as_of` by the maximum tolerated feature-bucket age so the
        // window end lands on the freshest guaranteed-settled fact boundary.
        let source_delay = Duration::from_secs(config.data_quality.max_feature_bucket_age_secs);
        let as_of = now - source_delay;
        let Some(liquidity_cap_usd) = liquidity_score_cap(&config)? else {
            return Ok(None);
        };
        let request = LiveFeatureBuildRequest {
            pit: &self.deps.pit_source,
            window_provider: &self.deps.window_provider,
            market: &market,
            features: &config.features,
            data_quality: &config.data_quality,
            requirements: &requirements,
            as_of,
            source_delay,
            liquidity_cap_usd,
        };
        let Some(vector) = build_live_feature_vector(&request).await? else {
            return Ok(None);
        };

        let outcome = factor_outcome(&vector, &config)?;
        if !outcome.eligibility.is_eligible() {
            tracing::debug!(
                market_id = %lot.market_id,
                "exit signal re-inference: factor-ineligible market"
            );
            return Ok(None);
        }

        let output = infer_lot(runtime.as_ref(), &market, &vector, &outcome, as_of).await?;
        let Some(candidate) = find_lot_candidate(&output.candidates, lot) else {
            tracing::debug!(
                market_id = %lot.market_id,
                token_id = %lot.token_id,
                "exit signal re-inference: model emitted no candidate for lot"
            );
            return Ok(None);
        };

        Ok(Some(fresh_signal_from_candidate(candidate, &config)?))
    }
}

/// Inputs to build one lot's live feature vector for exit-side re-scoring.
/// Shared by thesis-invalidation re-inference and the opportunistic Sell scorer.
pub(crate) struct LiveFeatureBuildRequest<'a> {
    pub pit: &'a Arc<dyn PointInTimeDataSource>,
    pub window_provider: &'a FeatureWindowProvider,
    pub market: &'a SelectedMarket,
    pub features: &'a FeaturesConfig,
    pub data_quality: &'a DataQualityConfig,
    pub requirements: &'a ModelFeatureRequirements,
    pub as_of: DateTime<Utc>,
    pub source_delay: Duration,
    pub liquidity_cap_usd: Usd,
}

async fn infer_lot(
    runtime: &dyn QuantModelRuntime,
    market: &SelectedMarket,
    vector: &FeatureVector,
    outcome: &MarketFactorOutcome,
    as_of: DateTime<Utc>,
) -> QuantResult<ModelRuntimeOutput> {
    let model_run_id = ModelRunId::from_v7();
    let input = build_runtime_input(
        runtime,
        &model_run_id,
        as_of,
        std::slice::from_ref(market),
        std::slice::from_ref(vector),
        std::slice::from_ref(outcome),
    );
    runtime.infer_batch(input).await
}

pub(crate) fn factor_outcome(
    vector: &FeatureVector,
    config: &RuntimeConfig,
) -> QuantResult<MarketFactorOutcome> {
    let factor_engine = FactorEngine::new(&config.factors, &config.features);
    let outcomes =
        factor_engine.compute_all_batch(std::slice::from_ref(vector), &config.factors)?;
    outcomes.into_iter().next().ok_or_else(|| {
        QuantError::config("exit signal re-inference: factor engine produced no outcome")
    })
}

/// Resolve the runtime-config snapshot frozen on the intent.
///
/// Fail-safe: a missing or unparseable snapshot yields `Ok(None)` (logged) so the
/// caller holds rather than scoring the forced-exit tier against drifted live
/// thresholds. Exit evaluation must reproduce the *entry* thesis or not run.
async fn resolve_frozen_config(
    config_versions: &dyn RuntimeConfigVersionRepository,
    version_id: &RuntimeConfigVersionId,
) -> QuantResult<Option<RuntimeConfig>> {
    let Some(info) = config_versions
        .load_version(version_id)
        .await
        .map_err(QuantError::from)?
    else {
        tracing::warn!(
            %version_id,
            "frozen runtime config missing for exit re-inference; holding (fail-safe)"
        );
        return Ok(None);
    };
    match RuntimeConfig::from_json(&info.config_json) {
        Ok(config) => Ok(Some(config)),
        Err(error) => {
            tracing::warn!(
                %version_id,
                %error,
                "frozen runtime config invalid for exit re-inference; holding (fail-safe)"
            );
            Ok(None)
        }
    }
}

/// Build the [`ActiveSchemaBinding`] from a config snapshot.
pub(crate) fn schema_binding(
    features: &FeaturesConfig,
    factors: &FactorsConfig,
) -> QuantResult<ActiveSchemaBinding> {
    Ok(ActiveSchemaBinding {
        feature_schema_hash: ResearchHasher::feature_schema(&FeatureSchema::build(features))?,
        factor_schema_hash: FactorEngine::new(factors, features).factor_schema_hash()?,
    })
}

/// Load-time policy for a model version on the exit path (shared by
/// thesis-invalidation re-inference and the opportunistic Sell scorer).
pub(crate) fn exit_model_load_ok(version: &ModelVersionInfo) -> Result<(), String> {
    match version.publication_status {
        PublicationStatus::Published | PublicationStatus::Retired => Ok(()),
        PublicationStatus::Candidate | PublicationStatus::Shadow => quality_gate_passed_ok(version),
        PublicationStatus::Draft | PublicationStatus::Rejected => Err(format!(
            "model {} publication status {} cannot score exit signal",
            version.model_version_id,
            version.publication_status.as_str()
        )),
    }
}

fn resolve_overlay(
    applicator: &crate::governance::WeightOverlayApplicator,
    version: &ModelVersionInfo,
) -> Option<quant_pivot_research::model::WeightOverlay> {
    if version.publication_status == PublicationStatus::Published {
        return None;
    }
    applicator.overlay_for(&version.model_version_id)
}

/// Project a position lot into a [`SelectedMarket`] for feature / model scoring.
#[must_use]
pub fn selected_market_for_lot(
    registry: &MarketRegistry,
    lot: &PositionInfo,
) -> Option<SelectedMarket> {
    let entry = registry.get_market(&lot.market_id)?;
    let yes = entry.token_yes.clone();
    let no = entry.token_no.clone();
    let (primary_token_id, secondary_token_id) = match lot.side {
        OutcomeSide::Yes => (yes, Some(no)),
        OutcomeSide::No => (no, Some(yes)),
    };
    Some(SelectedMarket {
        market_id: lot.market_id.clone(),
        event_id: lot
            .event_id
            .clone()
            .unwrap_or_else(|| entry.event_id.clone()),
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
    let builder = ConfiguredFeatureBuilder::new(request.features);
    let window = load_window(
        request.window_provider,
        &builder,
        request.market,
        request.as_of,
        request.source_delay,
        request.features,
    )
    .await?;

    let bundle = builder
        .resolve_inputs(
            request.market,
            request.as_of,
            PitView::Live(request.pit.as_ref()),
            &window,
            request.liquidity_cap_usd,
        )
        .await?;

    let required_set =
        merged_required_features(&request.requirements.required_features, request.features);
    let mut required: Vec<_> = required_set.into_iter().collect();
    required.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let vector = builder.compute_vector(&bundle, &required, request.features, request.data_quality);
    if vector.data_quality == DataQualityStatus::Insufficient {
        return Ok(None);
    }
    Ok(Some(vector))
}

async fn load_window(
    window_provider: &FeatureWindowProvider,
    builder: &ConfiguredFeatureBuilder,
    market: &SelectedMarket,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    features: &FeaturesConfig,
) -> QuantResult<MarketWindowSnapshot> {
    if !builder.schema().needs_window() {
        return Ok(MarketWindowSnapshot {
            token_id: market.primary_token_id.clone(),
            as_of,
            source_delay,
            buckets: Vec::new(),
        });
    }
    let lookback = Duration::from_secs(features.max_lookback_secs());
    let mut windows = window_provider
        .load_windows(std::slice::from_ref(market), as_of, lookback, source_delay)
        .await?;
    windows.remove(&market.primary_token_id).ok_or_else(|| {
        QuantError::config(format!(
            "missing prefetched window for token {}",
            market.primary_token_id.as_str()
        ))
    })
}

pub(crate) fn liquidity_score_cap(config: &RuntimeConfig) -> QuantResult<Option<Usd>> {
    let max_single = parse_config_decimal(
        "portfolio.budget.max_single_recommendation_usd",
        &config.portfolio.budget.max_single_recommendation_usd.value,
    )?;
    let usage_cap = parse_config_decimal(
        "portfolio.constraints.liquidity_usage_cap_pct",
        &config.portfolio.constraints.liquidity_usage_cap_pct.value,
    )?;
    if usage_cap > Decimal::ZERO && max_single > Decimal::ZERO {
        return Ok(Some(Usd::new(max_single / usage_cap)));
    }
    // No positive cap to normalize the liquidity feature against — hold rather
    // than guess a magic cap that would silently shift the composite score.
    tracing::warn!(
        %max_single,
        %usage_cap,
        "liquidity score cap unavailable (non-positive budget/usage); holding (fail-safe)"
    );
    Ok(None)
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
    config: &RuntimeConfig,
) -> QuantResult<FreshSignal> {
    Ok(FreshSignal {
        composite_score: candidate.composite_score,
        expected_return_bps: Bps::new(candidate.expected_return_bps),
        auto_exec_eligible: fresh_auto_exec_eligible(candidate, config)?,
    })
}

fn fresh_auto_exec_eligible(
    candidate: &SignalCandidate,
    config: &RuntimeConfig,
) -> QuantResult<bool> {
    // Thesis eligibility is purely the score/confidence bar — NOT the
    // `auto_execution.enabled` admission toggle. The evaluator already gates
    // forced exits to auto-execution intents (see `ReinferenceSignalEvaluator`),
    // so keying invalidation off `enabled` here would only mis-fire.
    let policy = &config.execution.auto_execution;
    let min_score = parse_config_decimal(
        "execution.auto_execution.min_score",
        &policy.min_score.value,
    )?;
    let min_confidence = parse_config_decimal(
        "execution.auto_execution.min_confidence",
        &policy.min_confidence.value,
    )?;
    Ok(candidate.composite_score.inner() >= min_score
        && candidate.confidence.inner() >= min_confidence)
}

fn parse_config_decimal(field: &str, value: &str) -> QuantResult<Decimal> {
    value
        .trim()
        .parse::<Decimal>()
        .map_err(|error| QuantError::config(format!("{field} is not a valid decimal: {error}")))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::runtime_config::RuntimeConfig;
    use quant_pivot_models::{
        domain::PositionInfo,
        enums::{
            common::MarketCategory,
            execution::PositionLedgerState,
            quant::{AccountSource, OutcomeSide},
        },
        types::{
            MarketId, ModelRunId, OrderIntentId, PositionId, Price, Probability, Shares,
            SignalCandidateId, TokenId, Usd,
        },
    };
    use quant_pivot_research::model::{ModelExplanation, SignalCandidate};
    use rust_decimal_macros::dec;

    use super::{find_lot_candidate, liquidity_score_cap, selected_market_for_lot};
    use crate::pipeline::market_registry::MarketRegistry;

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
            as_of: Utc::now(),
        }
    }

    #[test]
    fn find_lot_candidate_matches_token_and_side() {
        let lot = PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
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
    fn selected_market_uses_lot_outcome_token_as_primary() {
        use quant_pivot_models::{
            domain::market::registry::MarketRegistryInfo,
            enums::common::{CategorySet, TickSize},
            enums::market::MarketStatus,
            types::EventId,
        };

        let registry = MarketRegistry::new();
        registry.register_market(MarketRegistryInfo {
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_yes: TokenId::new("yes"),
            token_no: TokenId::new("no"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            categories: CategorySet::default(),
            status: MarketStatus::Active,
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
            fee_schedule: None,
            end_date: None,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let lot = PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
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
            opened_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        };
        let selected = selected_market_for_lot(&registry, &lot).expect("market");
        assert_eq!(selected.primary_token_id.as_str(), "no");
        assert_eq!(
            selected.secondary_token_id.as_ref().map(TokenId::as_str),
            Some("yes")
        );
    }

    #[test]
    fn liquidity_score_cap_none_when_budget_non_positive() {
        let mut config = RuntimeConfig::default();
        // Positive budget + usage cap resolve a usable normalization cap.
        "1000".clone_into(&mut config.portfolio.budget.max_single_recommendation_usd.value);
        "0.1".clone_into(&mut config.portfolio.constraints.liquidity_usage_cap_pct.value);
        assert!(liquidity_score_cap(&config).expect("ok").is_some());
        // A zero single-recommendation budget cannot normalize the liquidity
        // feature → fail-safe None (hold) rather than a guessed magic cap.
        "0".clone_into(&mut config.portfolio.budget.max_single_recommendation_usd.value);
        assert!(liquidity_score_cap(&config).expect("ok").is_none());
    }
}
