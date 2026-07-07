//! Opportunistic-Sell exit-signal evaluator (Phase 06.1).
//!
//! Closes the advisory branch of the [`ExitSignalEvaluator`] seam: when the
//! thesis still holds but the Sell scorer ranks exiting now above holding, this
//! evaluator emits [`ExitSignalVerdict::OpportunisticSell`] with a **target
//! cumulative exit fraction**. It is composed *behind* thesis-invalidation
//! re-inference (see [`CompositeExitSignalEvaluator`](crate::execution::CompositeExitSignalEvaluator)),
//! so it only ever runs when the thesis is checkable and holding.
//!
//! Fail-safe throughout: a disabled config, a non-auto-execution intent, an
//! unavailable scorer, a low-confidence / low-alpha score, or `shadow_mode` all
//! resolve to [`ExitSignalVerdict::Holds`] (never a forced or accidental exit).
//! Every evaluation — including shadow — is mirrored to the
//! `quant_exit_signal_evaluation_event` audit fact for ex-post shadow analysis.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, QuantExitSignalEvaluationEventRow},
    domain::{OrderIntentInfo, PointInTimeDataSource, PositionInfo},
    enums::{
        clickhouse::{ChExitSignalEvaluatorKind, ChExitSignalVerdict},
        quant::QuantRuntimeMode,
    },
    types::{ModelVersionId, Price},
};
use quant_pivot_repository::traits::{ModelRegistryRepository, RecommendationRepository};
use quant_pivot_research::{
    model::{
        LotStateInput, ModelRuntimeFactoryBuilder, SellScore, SellScoreInput,
        position_state_features,
    },
    selection::ModelFeatureRequirements,
};
use rust_decimal::Decimal;

use crate::{
    execution::{ExitSignalContext, ExitSignalEvaluator, ExitSignalVerdict},
    observability::{
        exit_signal_fact_writer::ExitSignalEvaluationEventWriter, metrics_hub::MetricsHub,
    },
    pipeline::{feature_window_provider::FeatureWindowProvider, market_registry::MarketRegistry},
    runtime_config::RuntimeConfigStore,
    service::model_backed_reinferer::{
        LiveFeatureBuildRequest, build_live_feature_vector, exit_model_load_ok,
        frozen_exit_outcome, liquidity_score_cap, schema_binding, selected_market_for_lot,
    },
};

/// Default hold horizon (secs) for the `time_in_trade` position feature when the
/// intent's exit policy carries no `max_hold_secs`.
const DEFAULT_HOLD_HORIZON_SECS: u64 = 86_400;

/// Side-effect-free Sell-side hold-vs-exit scorer over one open lot.
///
/// Returning `Ok(None)` is the fail-safe path (missing model / features / stale)
/// — the evaluator maps it to Hold.
#[async_trait]
pub trait OpportunisticSellScorer: Send + Sync {
    async fn score(
        &self,
        intent: &OrderIntentInfo,
        lot: &PositionInfo,
        mark_price: Option<Price>,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<SellScore>>;
}

/// Dependencies for [`ModelBackedOpportunisticSellScorer`].
pub struct ModelBackedOpportunisticSellScorerDeps {
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    pub config: Arc<RuntimeConfigStore>,
    /// Source of the entry recommendation's frozen factor breakdown, replayed as
    /// the exit-side factor plane (reproducing the entry thesis).
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    pub market_registry: Arc<MarketRegistry>,
    pub window_provider: FeatureWindowProvider,
}

/// Production [`OpportunisticSellScorer`] backed by the active exit scorer.
///
/// Loads `model.active_exit_model_version_id`, builds the lot's live market
/// factors and position-state features, then scores the hold-vs-exit decision
/// against the live runtime-config schema (matching the model's binding).
pub struct ModelBackedOpportunisticSellScorer {
    deps: ModelBackedOpportunisticSellScorerDeps,
}

impl ModelBackedOpportunisticSellScorer {
    #[must_use]
    pub const fn new(deps: ModelBackedOpportunisticSellScorerDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl OpportunisticSellScorer for ModelBackedOpportunisticSellScorer {
    async fn score(
        &self,
        intent: &OrderIntentInfo,
        lot: &PositionInfo,
        mark_price: Option<Price>,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<SellScore>> {
        let config = self.deps.config.current();
        let Some(reference) = config.model.active_exit_model_version_id.as_ref() else {
            return Ok(None);
        };
        let version_id = ModelVersionId::try_from(reference)?;
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version_by_id(&version_id)
            .await
            .map_err(QuantError::from)?
        else {
            tracing::warn!(%version_id, "opportunistic sell: active exit model version not found");
            return Ok(None);
        };
        if let Err(reason) = exit_model_load_ok(&version) {
            tracing::warn!(%version_id, %reason, "opportunistic sell: exit model load denied");
            return Ok(None);
        }

        let binding = schema_binding(&config.features, &config.factors, &config.domain, None)?;
        let factory = self.deps.factory_builder.build(binding);
        let runtime = match factory.load_sell_scorer(&version).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(%error, %version_id, "opportunistic sell: sell scorer load failed");
                return Ok(None);
            }
        };

        let Some(market) = selected_market_for_lot(&self.deps.market_registry, lot) else {
            return Ok(None);
        };
        let requirements = ModelFeatureRequirements {
            required_features: runtime.required_features(),
        };
        let source_delay = Duration::from_secs(config.data_quality.max_feature_bucket_age_secs);
        let as_of =
            now - chrono::Duration::from_std(source_delay).unwrap_or(chrono::Duration::zero());
        let Some(liquidity_cap_usd) = liquidity_score_cap(config.as_ref())? else {
            return Ok(None);
        };
        let request = LiveFeatureBuildRequest {
            pit: &self.deps.pit_source,
            window_provider: &self.deps.window_provider,
            market: &market,
            features: &config.features,
            domain: &config.domain,
            data_quality: &config.data_quality,
            requirements: &requirements,
            as_of,
            source_delay,
            liquidity_cap_usd,
        };
        let Some(vector) = build_live_feature_vector(&request).await? else {
            return Ok(None);
        };
        // Reproduce the entry factor thesis from the recommendation's frozen
        // breakdown rather than recompute on a peerless single market.
        let Some(outcome) = frozen_exit_outcome(
            self.deps.recommendations.as_ref(),
            intent,
            vector.market_id.clone(),
            as_of,
        )
        .await?
        else {
            return Ok(None);
        };
        let market_factors = outcome
            .factors
            .iter()
            .map(|scored| scored.value.clone())
            .collect();
        let position_state = position_state_features(LotStateInput {
            avg_price: lot.avg_price.inner(),
            mark: mark_price.map(Price::inner),
            opened_at: lot.opened_at,
            now,
            max_hold_secs: intent
                .exit_policy_json
                .max_hold_secs
                .unwrap_or(DEFAULT_HOLD_HORIZON_SECS),
            peak_mark: intent.peak_mark_price.map(Price::inner),
        });
        let score = runtime.score(&SellScoreInput {
            market_factors,
            position_state,
        })?;
        Ok(Some(score))
    }
}

/// Dependencies for [`OpportunisticSellSignalEvaluator`].
pub struct OpportunisticSellSignalEvaluatorDeps<S> {
    pub scorer: S,
    pub config: Arc<RuntimeConfigStore>,
    pub metrics: Arc<MetricsHub>,
    pub audit: Arc<ExitSignalEvaluationEventWriter>,
}

/// The opportunistic-Sell branch of the [`ExitSignalEvaluator`] seam.
pub struct OpportunisticSellSignalEvaluator<S> {
    scorer: S,
    config: Arc<RuntimeConfigStore>,
    metrics: Arc<MetricsHub>,
    audit: Arc<ExitSignalEvaluationEventWriter>,
}

impl<S> OpportunisticSellSignalEvaluator<S> {
    #[must_use]
    pub fn new(deps: OpportunisticSellSignalEvaluatorDeps<S>) -> Self {
        Self {
            scorer: deps.scorer,
            config: deps.config,
            metrics: deps.metrics,
            audit: deps.audit,
        }
    }
}

impl<S: OpportunisticSellScorer> OpportunisticSellSignalEvaluator<S> {
    /// Score the lot, or `None` when the enabled scorer could not evaluate. The
    /// couldn't-score paths record an `Indeterminate` audit row (so shadow-period
    /// coverage is not silently biased) and the eval metric before returning.
    async fn fetch_score(
        &self,
        ctx: &ExitSignalContext<'_>,
        shadow: bool,
        model_version_id: Option<&ModelVersionId>,
    ) -> Option<SellScore> {
        match self
            .scorer
            .score(ctx.intent, ctx.lot, ctx.mark_price, ctx.now)
            .await
        {
            Ok(Some(score)) => Some(score),
            Ok(None) => {
                self.audit.write(audit_row(
                    ctx,
                    None,
                    None,
                    ChExitSignalVerdict::Indeterminate,
                    model_version_id,
                    shadow,
                ));
                self.metrics.inc_opportunistic_sell_eval("unavailable");
                None
            }
            Err(error) => {
                self.audit.write(audit_row(
                    ctx,
                    None,
                    None,
                    ChExitSignalVerdict::Indeterminate,
                    model_version_id,
                    shadow,
                ));
                self.metrics.inc_opportunistic_sell_eval("error");
                tracing::warn!(%error, "opportunistic sell scoring failed; holding (fail-safe)");
                None
            }
        }
    }
}

#[async_trait]
impl<S: OpportunisticSellScorer> ExitSignalEvaluator for OpportunisticSellSignalEvaluator<S> {
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalVerdict {
        let snapshot = self.config.current();
        let policy = snapshot.execution.exit_monitor.opportunistic_sell.clone();
        if !policy.enabled {
            self.metrics.inc_opportunistic_sell_eval("disabled");
            return ExitSignalVerdict::Holds;
        }
        // Opportunistic exits are auto-submitted advisory scale-outs; a human owns
        // the exit for non-auto-execution intents.
        if ctx.intent.runtime_mode != QuantRuntimeMode::AutoExecution {
            self.metrics.inc_opportunistic_sell_eval("skipped_non_auto");
            return ExitSignalVerdict::Holds;
        }

        // Resolve the active exit-model version up front so the enabled-path
        // audit rows (including the couldn't-score paths below) carry it.
        let model_version_id = snapshot
            .model
            .active_exit_model_version_id
            .as_ref()
            .and_then(|reference| ModelVersionId::try_from(reference).ok());

        let Some(score) = self
            .fetch_score(&ctx, policy.shadow_mode, model_version_id.as_ref())
            .await
        else {
            return ExitSignalVerdict::Holds;
        };

        let min_confidence = parse_decimal(&policy.min_confidence.value);
        let min_p_exit_better =
            parse_decimal_or(&policy.min_p_exit_better.value, Decimal::new(5, 1));
        let max_sell_pct = parse_decimal_or(&policy.max_sell_pct.value, Decimal::ONE);
        let min_alpha = Decimal::from(policy.min_expected_alpha_bps);

        let target = score
            .recommended_cumulative_exit_pct
            .min(max_sell_pct)
            .clamp(Decimal::ZERO, Decimal::ONE);

        let fires = score.confidence.inner() >= min_confidence
            && score.p_exit_better.inner() >= min_p_exit_better
            && score.exit_alpha_bps.inner() >= min_alpha;

        if !fires {
            self.audit.write(audit_row(
                &ctx,
                Some(&score),
                Some(target),
                ChExitSignalVerdict::Holds,
                model_version_id.as_ref(),
                false,
            ));
            self.metrics.inc_opportunistic_sell_eval("hold");
            return ExitSignalVerdict::Holds;
        }

        // Shadow: record the would-be opportunistic exit, but never submit.
        if policy.shadow_mode {
            self.audit.write(audit_row(
                &ctx,
                Some(&score),
                Some(target),
                ChExitSignalVerdict::OpportunisticSell,
                model_version_id.as_ref(),
                true,
            ));
            self.metrics
                .inc_opportunistic_sell_eval("shadow_would_sell");
            return ExitSignalVerdict::Holds;
        }

        self.audit.write(audit_row(
            &ctx,
            Some(&score),
            Some(target),
            ChExitSignalVerdict::OpportunisticSell,
            model_version_id.as_ref(),
            false,
        ));
        self.metrics
            .inc_opportunistic_sell_eval("opportunistic_sell");
        ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct: target,
            detail: format!(
                "sell scorer: exit_alpha {} bps, p_exit {}, confidence {}",
                score.exit_alpha_bps.inner(),
                score.p_exit_better.inner(),
                score.confidence.inner()
            ),
        }
    }
}

/// Project one opportunistic evaluation into the audit fact row. `score` /
/// `target_cumulative_exit_pct` are `None` on the couldn't-score paths (the
/// scorer was unavailable or errored), which record an Indeterminate row.
fn audit_row(
    ctx: &ExitSignalContext<'_>,
    score: Option<&SellScore>,
    target_cumulative_exit_pct: Option<Decimal>,
    verdict: ChExitSignalVerdict,
    model_version_id: Option<&ModelVersionId>,
    shadow: bool,
) -> QuantExitSignalEvaluationEventRow {
    let now_ms = ctx.now.timestamp_millis();
    QuantExitSignalEvaluationEventRow {
        event_time: now_ms,
        order_intent_id: ctx.lot.order_intent_id.clone(),
        position_id: ctx.lot.position_id.clone(),
        market_id: ctx.lot.market_id.clone(),
        token_id: ctx.lot.token_id.clone(),
        evaluator_kind: ChExitSignalEvaluatorKind::Opportunistic,
        verdict,
        model_version_id: model_version_id.cloned(),
        mark_price: ctx.mark_price.map(ChPrice::from),
        entry_composite_score: ChProbability::from(
            ctx.intent.exit_policy_json.entry_composite_score,
        ),
        fresh_composite_score: None,
        exit_alpha_bps: score.map(|score| ChDecimal64::from(score.exit_alpha_bps.inner())),
        confidence: score.map(|score| ChProbability::from(score.confidence)),
        target_cumulative_exit_pct: target_cumulative_exit_pct.map(ChDecimal64::from),
        shadow: u8::from(shadow),
        detail: verdict.as_str().to_owned(),
        ingestion_time: now_ms,
    }
}

/// Parse a config decimal, defaulting to the most conservative bar (`1.0`, i.e.
/// unreachable confidence) on a malformed value so a corrupted snapshot never
/// silently opens the opportunistic gate.
fn parse_decimal(value: &str) -> Decimal {
    value.trim().parse::<Decimal>().unwrap_or(Decimal::ONE)
}

/// Parse a config decimal with an explicit fallback.
fn parse_decimal_or(value: &str, fallback: Decimal) -> Decimal {
    value.trim().parse::<Decimal>().unwrap_or(fallback)
}
