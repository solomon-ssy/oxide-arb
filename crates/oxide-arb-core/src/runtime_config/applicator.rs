//! Runtime-config activation applicator — the single write path from a
//! durable, audited activation to the live trading system.

use crate::{
    bridge::{CoreOpportunityPipeline, execution_mode::ExecutionModeHandle},
    detection::{coalescer::Coalescer, funnel::Funnel},
    execution::{
        capital_manager::CapitalManager, fok_strategy::FokOrderStrategy,
        settlement::dedup::SettlementDedup, settlement::service::MarketSettlementService,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::alert_dispatcher::AlertDispatcher,
    pipeline::{
        market_cache::MarketCache, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier, universe_filter::MarketUniverseFilter,
    },
    runtime_config::RuntimeConfigStore,
    service::{risk_metrics::RiskMetricsState, ws_subscription::WsSubscriptionCoordinator},
};
use async_trait::async_trait;
use oxide_arb_algorithm::calibration::CalibrationUpdater;
use oxide_arb_api::{VotingOracle, ctf::client::CtfRedeemClient};
use oxide_arb_models::{
    domain::{RuntimeConfigPort, RuntimeControlError},
    enums::common::CategorySet,
    runtime_config::{
        RuntimeConfig,
        validation::{RuntimePreflightContext, preflight_runtime_config, validate_runtime_config},
    },
};
use oxide_arb_risk::engine::RiskEngine;
use std::sync::Arc;

/// Every live subscriber of the runtime configuration, held as typed handles.
///
/// Propagation is compile-time complete: adding a subscriber means adding a
/// field here, not remembering to register a callback somewhere.
pub struct RuntimeConfigSubscribers {
    /// R1 — risk limits, circuit breaker, sizing (risk first, always).
    pub risk_engine: Arc<RiskEngine>,
    /// R1 — simulated cash baseline (rebased on `bankroll_usd` activation in
    /// `DryRun`/`Paper`; never touched on the authoritative Live source).
    pub metrics_state: Arc<RiskMetricsState>,
    /// E6 — exposure reservation ceilings (shared backend).
    pub exposure: Arc<InMemoryExposureReservation>,
    /// E6 — reservation TTL used when reserving capital.
    pub capital: Arc<CapitalManager>,
    /// D2–D5 — detector, scorer, emission cooldown, and emit gates.
    pub opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    /// D6/D7 — calibration config (updater + shared calibrator + fusion).
    pub calibration_updater: Arc<CalibrationUpdater>,
    /// D1/E1 — staleness ladder shared by scanner and validator.
    pub staleness: StalenessClassifier,
    /// D1 — tradeable-universe category filter.
    pub universe: Arc<MarketUniverseFilter>,
    /// D1 — registry backing the universe recomputation.
    pub market_registry: Arc<MarketRegistry>,
    /// D1 — scanner cache rebuilt after a universe change.
    pub market_cache: Arc<MarketCache>,
    /// D1 — CLOB websocket subscriptions resynced after a universe change.
    pub ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    /// E1 — validation slippage / book-age budgets.
    pub validator: Arc<Validator>,
    /// E2 — FOK dispatch timeout.
    pub order_strategy: Arc<FokOrderStrategy>,
    /// E3 — coalesce window.
    pub coalescer: Arc<Coalescer>,
    /// E4 — funnel queue capacity + sweep cadence.
    pub funnel: Arc<Funnel>,
    /// S1 — settlement oracle policy + lifecycle limits.
    pub settlement_service: Arc<MarketSettlementService>,
    /// S2 — settlement trigger dedup window.
    pub settlement_dedup: Arc<SettlementDedup>,
    /// S1 — oracle voting policy + UMA source.
    pub voting_oracle: Arc<VotingOracle>,
    /// S3 — redeem route (absent when no keystore was loaded at boot).
    pub ctf_redeem: Option<Arc<CtfRedeemClient>>,
    /// N1 — alert channels + cooldown.
    pub alerts: Arc<AlertDispatcher>,
}

/// Applies a durably-activated [`RuntimeConfig`] to the live system.
///
/// Swaps the store and propagates to every subscriber in a fixed order (risk
/// before execution), so tightened limits are active before any dispatch
/// decision observes the new detection/execution parameters.
pub struct RuntimeConfigApplicator {
    store: Arc<RuntimeConfigStore>,
    execution_mode: ExecutionModeHandle,
    subscribers: RuntimeConfigSubscribers,
}

impl RuntimeConfigApplicator {
    #[must_use]
    pub const fn new(
        store: Arc<RuntimeConfigStore>,
        execution_mode: ExecutionModeHandle,
        subscribers: RuntimeConfigSubscribers,
    ) -> Self {
        Self {
            store,
            execution_mode,
            subscribers,
        }
    }

    /// Full fail-closed gate: semantic validation + mode-aware validation +
    /// live money-state preflight against in-flight reservations.
    fn preflight_internal(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        let report = validate_runtime_config(candidate);
        if report.has_errors() {
            return Err(RuntimeControlError::Precondition(report.to_string()));
        }
        let ctx = RuntimePreflightContext {
            mode: self.execution_mode.current(),
            reserved_total_usd: self.subscribers.exposure.total_reserved_usd_sync().inner(),
            max_market_reserved_usd: self
                .subscribers
                .exposure
                .max_market_reserved_usd_sync()
                .inner(),
        };
        let report = preflight_runtime_config(candidate, &ctx);
        if report.has_errors() {
            return Err(RuntimeControlError::Precondition(report.to_string()));
        }
        Ok(())
    }

    /// Propagate `config` to all subscribers.
    ///
    /// The two fallible reloads (oracle source rebuild, redeem route holder
    /// resolution) are **staged first** without publishing anything: if either
    /// fails the activation aborts with no live mutation at all. Both staged
    /// states are then committed, followed by the infallible reloads, so the
    /// whole propagation is fail-closed and atomic.
    fn propagate(&self, config: &Arc<RuntimeConfig>) -> Result<(), RuntimeControlError> {
        let subs = &self.subscribers;

        let staged_oracle = subs
            .voting_oracle
            .stage_reload(&config.settlement.oracle)
            .map_err(|error| {
                RuntimeControlError::Activation(format!("oracle reload failed: {error}"))
            })?;
        let staged_redeem = subs
            .ctf_redeem
            .as_ref()
            .map(|ctf_redeem| ctf_redeem.stage_reload(config.settlement.redeem.clone()))
            .transpose()
            .map_err(|error| {
                RuntimeControlError::Activation(format!("redeem reload failed: {error}"))
            })?;

        // Every fallible step has succeeded — from here on the activation
        // cannot partially fail. Commit the staged states, then the
        // infallible reloads in order: risk → exposure → detection →
        // execution → settlement → notification.
        staged_oracle.commit();
        if let Some(staged_redeem) = staged_redeem {
            staged_redeem.commit();
        }

        subs.risk_engine.reload(config.risk.clone());
        subs.metrics_state
            .reload(&self.store.load().risk, &config.risk);
        let reservation = config.risk.exposure_reservation_config();
        subs.exposure.reload(reservation.clone());
        subs.capital.reload(&reservation);

        subs.opportunity_pipeline.reload(&config.detection);
        subs.calibration_updater
            .reload(config.detection.calibration.clone());
        subs.staleness.reload(&config.market_data);

        // Universe filter: swap the enabled set, rebuild the scanner cache,
        // and resync websocket subscriptions to the new tradeable set.
        let universe_changed = subs.universe.enabled()
            != CategorySet::from(config.market_data.enabled_categories.as_slice());
        if universe_changed {
            subs.universe.reload(&config.market_data.enabled_categories);
            subs.market_cache.rebuild();
            if let Some(ws_subscription) = &subs.ws_subscription {
                let tokens = subs
                    .market_registry
                    .active_subscribable_tokens(&subs.universe);
                let token_count = tokens.len();
                ws_subscription.sync_to_tokens(&tokens);
                tracing::info!(
                    enabled = ?subs.universe.enabled().iter().collect::<Vec<_>>(),
                    tokens = token_count,
                    subscribed = ws_subscription.subscribed_count(),
                    "tradeable universe filter reloaded; websocket subscriptions resynced"
                );
            }
        }

        subs.validator.reload(&config.execution);
        subs.order_strategy.reload(&config.execution.timeout);
        subs.coalescer.reload(&config.execution.coalescer);
        subs.funnel.reload(&config.execution.funnel);

        subs.settlement_service.reload(config.settlement.clone());
        subs.settlement_dedup.reload(&config.settlement.lifecycle);

        subs.alerts.reload(&config.notification);

        // The store swaps last: tasks that read whole sections from the store
        // (scanner fast-lane threshold, dynamic periodic intervals, bankroll
        // seeds) observe the new snapshot only after every push-subscriber is
        // already consistent with it.
        self.store.swap(Arc::clone(config));
        tracing::info!("runtime config activated and propagated to all subscribers");
        Ok(())
    }
}

#[async_trait]
impl RuntimeConfigPort for RuntimeConfigApplicator {
    fn current(&self) -> Arc<RuntimeConfig> {
        self.store.current()
    }

    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        self.preflight_internal(candidate)
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError> {
        // Defense in depth: the web handler preflights before the durable
        // activation, but reservations may have moved in between — re-check
        // against the live state at the moment of application.
        self.preflight_internal(&config)?;
        self.propagate(&Arc::new(config))
    }
}
