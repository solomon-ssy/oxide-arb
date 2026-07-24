//! Opportunistic-Sell exit-signal evaluator.
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

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, QuantExitSignalEvaluationEventRow},
    domain::quant::{OrderIntentInfo, PositionInfo},
    enums::{
        clickhouse::{ChExitSignalEvaluatorKind, ChExitSignalVerdict},
        quant::QuantRuntimeMode,
    },
    types::{ModelVersionId, Price},
};
use quant_pivot_repository::traits::{
    FactorRepository, ModelRegistryRepository, RecommendationRepository,
};
use quant_pivot_research::{
    model::{
        LotStateInput, ModelRuntimeFactoryBuilder, SellScore, SellScoreInput, SellSignalPolicy,
        sell_signal_fires, sell_signal_target,
    },
    pit::PointInTimeSnapshotSource,
    selection::ModelFeatureRequirements,
};
use rust_decimal::Decimal;

use crate::{
    execution::{ExitSignalContext, ExitSignalEvaluation, ExitSignalEvaluator, ExitSignalVerdict},
    observability::{
        exit_signal_fact_writer::ExitSignalEvaluationEventWriter, metrics_hub::MetricsHub,
    },
    prefetch::feature_window::FeatureWindowProvider,
    runtime_config::DecisionPolicyStore,
    service::model_backed_reinferer::{
        LiveFeatureBuildRequest, build_live_feature_vector, exit_model_load_ok, fresh_exit_outcome,
        liquidity_score_cap, runtime_decision_boundary, schema_binding, selected_market_for_lot,
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
    pub config: Arc<DecisionPolicyStore>,
    /// Source of the recommendation's governed factor-definition set.
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub factors: Arc<dyn FactorRepository>,
    pub pit_source: Arc<dyn PointInTimeSnapshotSource>,
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
        let Some(reference) = config
            .model_routing
            .model
            .active_exit_model_version_id
            .as_ref()
        else {
            return Ok(None);
        };
        let version_id = reference.id;
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version(&version_id)
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

        let binding = schema_binding(
            &config.profile_artifacts.features.definition,
            &config.profile_artifacts.scoring.definition,
            &config.profile_artifacts.domain.definition,
            None,
        )?;
        let factory = self.deps.factory_builder.build(binding);
        let runtime = match factory.load_sell_scorer(&version).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(%error, %version_id, "opportunistic sell: sell scorer load failed");
                return Ok(None);
            }
        };

        let boundary = runtime_decision_boundary(&config, now)?;
        let Some(snapshot) = self
            .deps
            .pit_source
            .market_snapshot_at(&lot.market_id, &boundary)
            .await?
        else {
            tracing::debug!(
                market_id = %lot.market_id,
                "opportunistic sell: durable market snapshot is unavailable"
            );
            return Ok(None);
        };
        let market = selected_market_for_lot(&snapshot, lot)?;
        let requirements = ModelFeatureRequirements::generic_only(runtime.required_features());
        let Some(liquidity_cap_usd) = liquidity_score_cap(config.as_ref()) else {
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
        let market_factors = fresh
            .outcome
            .factors
            .iter()
            .map(|scored| scored.value.clone())
            .collect();
        let position_state = LotStateInput {
            avg_price: lot.avg_price.inner(),
            mark: mark_price.map(Price::inner),
            opened_at: lot.opened_at,
            now,
            max_hold_secs: intent
                .exit_policy_json
                .max_hold_secs
                .unwrap_or(DEFAULT_HOLD_HORIZON_SECS),
            peak_mark: intent.peak_mark_price.map(Price::inner),
        }
        .position_state_features()?;
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
    pub config: Arc<DecisionPolicyStore>,
    pub metrics: Arc<MetricsHub>,
    pub audit: Arc<ExitSignalEvaluationEventWriter>,
}

/// The opportunistic-Sell branch of the [`ExitSignalEvaluator`] seam.
pub struct OpportunisticSellSignalEvaluator<S> {
    scorer: S,
    config: Arc<DecisionPolicyStore>,
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
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalEvaluation {
        let snapshot = self.config.current();
        let policy = snapshot
            .execution_risk
            .exit_monitor
            .opportunistic_sell
            .clone();
        if !policy.enabled {
            self.metrics.inc_opportunistic_sell_eval("disabled");
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Holds);
        }
        // Opportunistic exits are auto-submitted advisory scale-outs; a human owns
        // the exit for non-auto-execution intents.
        if ctx.intent.runtime_mode != QuantRuntimeMode::AutoExecution {
            self.metrics.inc_opportunistic_sell_eval("skipped_non_auto");
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Holds);
        }

        // Resolve the active exit-model version up front so the enabled-path
        // audit rows (including the couldn't-score paths below) carry it.
        let model_version_id = snapshot
            .model_routing
            .model
            .active_exit_model_version_id
            .as_ref()
            .map(|reference| reference.id);

        let Some(score) = self
            .fetch_score(&ctx, policy.shadow_mode, model_version_id.as_ref())
            .await
        else {
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Holds);
        };

        let signal_policy =
            SellSignalPolicy::from_frozen(&ctx.intent.exit_policy_json.opportunistic_exit);
        let target = sell_signal_target(&score, &signal_policy);
        let fires = sell_signal_fires(&score, &signal_policy);

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
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Holds);
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
            return ExitSignalEvaluation::verdict(ExitSignalVerdict::Holds);
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
        ExitSignalEvaluation::verdict(ExitSignalVerdict::OpportunisticSell {
            target_cumulative_exit_pct: target,
            detail: format!(
                "sell scorer: exit_alpha {} bps, p_exit {}, confidence {}",
                score.exit_alpha_bps.inner(),
                score.p_exit_better.inner(),
                score.confidence.inner()
            ),
        })
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
        order_intent_id: ctx.lot.order_intent_id,
        position_id: ctx.lot.position_id,
        market_id: ctx.lot.market_id.clone(),
        token_id: ctx.lot.token_id.clone(),
        evaluator_kind: ChExitSignalEvaluatorKind::Opportunistic,
        verdict,
        model_version_id: model_version_id.copied(),
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
