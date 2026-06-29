//! [`AdmissionInputBuilder`]: read-only, fail-closed aggregation of all decision
//! inputs into a frozen [`AdmissionInput`].
//!
//! This is the *only* place admission performs I/O. A failure to read any
//! mandatory source (recommendation, report, account, active config version) is
//! a `QuantError` — the 05.4 dispatcher treats it as not-executable (fail
//! closed). Optional state (book snapshot) maps to `None` and is handled by the
//! checks. The resulting input is immutable; the checks are pure over it.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{DataQualityPort, OrderIntentInfo},
    enums::{market::MarketStatus, quant::PublicationStatus},
    types::Usd,
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ExecutionOrderRepository, MarketRepository,
    ModelRegistryRepository, RecommendationReportRepository, RecommendationRepository,
    ReconciliationRepository, RuntimeConfigVersionRepository,
};
use rust_decimal::Decimal;

use super::{AdmissionInput, AdmissionSeams, StateVersion};
use crate::{
    execution::{breaker::VenueHealthHandle, exit_monitor::ExitMonitorHealthHandle},
    governance::{KillSwitchHandle, RuntimeModeHandle},
    pipeline::book_store::BookStore,
    runtime_config::RuntimeConfigStore,
    service::account::AccountProviderFactory,
};

/// Dependencies for [`AdmissionInputBuilder`].
pub struct AdmissionInputBuilderDeps {
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub execution_orders: Arc<dyn ExecutionOrderRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub config_versions: Arc<dyn RuntimeConfigVersionRepository>,
    pub account_factory: Arc<AccountProviderFactory>,
    pub book_store: Arc<BookStore>,
    pub data_quality: Arc<dyn DataQualityPort>,
    pub config: Arc<RuntimeConfigStore>,
    pub runtime_mode: RuntimeModeHandle,
    pub kill_switch: KillSwitchHandle,
    /// Venue-health hot read published by the 05.4 execution breaker (seam #18).
    pub venue_health: VenueHealthHandle,
    /// Exit-monitor health hot read published by the 05.6 worker (seam #20).
    pub exit_monitor_health: ExitMonitorHealthHandle,
}

/// Builds the frozen [`AdmissionInput`] for an intent at decision time.
pub struct AdmissionInputBuilder {
    deps: AdmissionInputBuilderDeps,
}

impl AdmissionInputBuilder {
    /// Assemble the builder from its dependencies.
    #[must_use]
    pub const fn new(deps: AdmissionInputBuilderDeps) -> Self {
        Self { deps }
    }

    /// Aggregate every admission input at `now`, or fail closed.
    ///
    /// # Errors
    ///
    /// Returns a `QuantError` when a mandatory source is missing (recommendation
    /// / report / active config version) or the real account is unavailable
    /// (missing credentials, venue read failure). Deny/defer outcomes are *not*
    /// errors — they are produced by the engine over a successfully built input.
    pub async fn build(
        &self,
        intent: &OrderIntentInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<AdmissionInput> {
        let deps = &self.deps;

        let recommendation = deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| not_found("recommendation", intent.recommendation_id.to_string()))?;

        let config = deps.config.current();
        let budget_total_usd = parse_budget_usd(&config.portfolio.budget.total_budget_usd.value)?;
        let max_stale_book_ratio_bps = config.data_quality.max_stale_book_ratio_bps;

        let report_id = recommendation.recommendation_report_id.clone();
        let market_id = recommendation.market_id.clone();
        let order_intent_id = intent.order_intent_id.clone();
        let model_version_id = intent.model_version_id.clone();
        let account_factory = Arc::clone(&deps.account_factory);

        let (
            report_result,
            model_version_result,
            unresolvable_result,
            ambiguous_inflight_result,
            allocation_result,
            active_version_result,
            account_result,
            market_result,
        ) = tokio::join!(
            deps.reports.find_by_id(&report_id),
            deps.model_registry
                .find_model_version_by_id(&model_version_id),
            deps.reconciliation.has_unresolvable(),
            deps.execution_orders.has_ambiguous_inflight(),
            deps.capital.find_by_intent(&order_intent_id),
            deps.config_versions.load_current(),
            async move {
                account_factory
                    .create(budget_total_usd)?
                    .snapshot(now)
                    .await
            },
            deps.markets.find_by_id(&market_id)
        );

        let report = report_result?
            .ok_or_else(|| not_found("recommendation_report", report_id.to_string()))?;
        let model_published = model_version_result?
            .is_some_and(|version| version.publication_status == PublicationStatus::Published);
        // Truth-unknown in-flight exposure blocks new auto entries (fail-closed):
        // an `Ambiguous` order (capital held, venue truth unknown) or a recon
        // worker's terminal `Unresolvable` verdict (05.5). Resting open orders and
        // `Pending` (not-yet-reconciled) rows do not block.
        let has_blocking_inflight = unresolvable_result? || ambiguous_inflight_result?;
        let allocation = allocation_result?;
        let account = account_result?;
        let manual_block = market_result?
            .ok_or_else(|| not_found("market", recommendation.market_id.to_string()))?
            .status
            == MarketStatus::ManuallyBlocked;
        let active_version = active_version_result?
            .ok_or_else(|| not_found("runtime_config_version", "current".to_owned()))?;

        let book = deps.book_store.load(&intent.entry_order_json.token_id);
        let data_quality = deps.data_quality.snapshot();
        let mode = deps.runtime_mode.current();
        let kill_switch = deps.kill_switch.current();

        let state_version = StateVersion {
            config_version_id: active_version.runtime_config_version_id,
            account_as_of: account.as_of,
            book_version: book.as_ref().map(|snapshot| snapshot.version),
            book_as_of_ms: book.as_ref().map(|snapshot| snapshot.timestamp_ms),
            kill_switch_state: kill_switch,
        };

        Ok(AdmissionInput {
            intent: intent.clone(),
            recommendation,
            report,
            mode,
            kill_switch,
            account,
            allocation,
            book,
            budget_total_usd,
            model_published,
            data_quality,
            max_stale_book_ratio_bps,
            has_blocking_inflight,
            manual_block,
            // Real readiness seams: the 05.4 breaker supplies venue health, the
            // 05.6 exit-monitor worker supplies its heartbeat, and credentials are
            // real (the same keystore signs and reads).
            seams: AdmissionSeams {
                venue_health: deps.venue_health.current(),
                credentials_ready: deps.account_factory.credentials_ready(),
                exit_monitor_ready: deps.exit_monitor_health.is_ready(now),
            },
            now,
            now_ms: u64::try_from(now.timestamp_millis()).unwrap_or(0),
            state_version,
        })
    }
}

fn not_found(entity: &'static str, id: String) -> QuantError {
    StorageError::NotFound { entity, id }.into()
}

/// Parse the governed total budget, failing closed on a malformed value.
fn parse_budget_usd(value: &str) -> QuantResult<Usd> {
    value
        .trim()
        .parse::<Decimal>()
        .map(Usd::new)
        .map_err(|error| {
            QuantError::config(format!(
                "portfolio.budget.total_budget_usd is not a valid decimal: {error}"
            ))
        })
}
