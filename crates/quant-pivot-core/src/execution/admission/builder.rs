//! [`AdmissionInputBuilder`]: read-only, fail-closed aggregation of all decision
//! inputs into a frozen [`AdmissionInput`].
//!
//! This is the *only* place admission performs I/O. A failure to read any
//! mandatory source (recommendation, report, account, active config version) is
//! a `QuantError` — the dispatcher treats it as not-executable (fail
//! closed). Optional state (book snapshot) maps to `None` and is handled by the
//! checks. The resulting input is immutable; the checks are pure over it.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::clob::{ClobClient, VenueFundingEvidence, VenueOrderMetadata};
use quant_pivot_error::{
    QuantError, QuantResult, execution::ExecutionError, storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionClock,
        governance::{DecisionPolicySnapshotInfo, RuntimeControlSnapshot},
        market::{BookSnapshot, CatalogMarketChangeInfo, MarketRegistryInfo},
        order::OrderRequest,
        ports::DataQualityPort,
        quant::{
            CapitalAllocationInfo, ModelVersionInfo, OrderIntentInfo, RecommendationInfo,
            RecommendationReportInfo, RouteEconomicHealthIdentity,
        },
    },
    enums::{market::MarketStatus, settlement::SettlementRoute},
    types::{ClobMarketInfoVersion, PreparedVenueOrder, ResearchProfileRef, Usd},
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    EntryConditionRepository, ExecutionOrderRepository, MarketRepository, ModelRegistryRepository,
    OrderIntentRepository, PolicyRepository, RecommendationReportRepository,
    RecommendationRepository, ReconciliationRepository, RouteEconomicHealthRepository,
    TradePolicyRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    execution_semantics::{PitFeeSchedule, PitMarketExecutionEconomics},
    model::{CalibrationArtifactLoader, ModelArtifact},
    portfolio::AccountSnapshot,
};

use super::{
    AdmissionEconomicHealth, AdmissionExecutionEvidence, AdmissionExposureState, AdmissionInput,
    AdmissionModelState, AdmissionSeams, AdmissionVenueMetadata, EntryOrderPreparation,
    StateVersion,
};
use crate::{
    execution::{
        breaker::VenueHealthHandle,
        exit_monitor::ExitMonitorHealthHandle,
        settlement_recovery_admission::{
            SettlementRecoveryAdmission, SettlementRecoveryAdmissionPort,
            SettlementRecoveryAdmissionRequest, requires_automatic_settlement_recovery,
        },
        trade_policy_guard::require_frozen_trade_policy,
    },
    governance::{RuntimeControlsHandle, resolve_return_model_calibration},
    ingest::book_store::{BookStore, FreshBook},
    runtime_config::DecisionPolicyStore,
    service::account::AccountProviderFactory,
};

/// Dependencies for [`AdmissionInputBuilder`].
pub struct AdmissionInputBuilderDeps {
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub trade_policies: Arc<dyn TradePolicyRepository>,
    pub economic_health: Arc<dyn RouteEconomicHealthRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Re-verifies a bound `model_score` calibrator's liveness (hash + `active`)
    /// at submit time — the enum tag alone (`ReturnModelSpec::Calibrated`) only
    /// proves a calibrator was bound at publish time, not that it still
    /// resolves today (the deep calibrator-verification check).
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub execution_orders: Arc<dyn ExecutionOrderRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub conditions: Arc<dyn EntryConditionRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub catalog_ledger: Arc<dyn CatalogLedgerRepository>,
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub config_versions: Arc<dyn PolicyRepository>,
    pub account_factory: Arc<AccountProviderFactory>,
    pub book_store: Arc<BookStore>,
    pub clob: Arc<ClobClient>,
    pub data_quality: Arc<dyn DataQualityPort>,
    pub config: Arc<DecisionPolicyStore>,
    pub runtime_controls: RuntimeControlsHandle,
    /// Venue-health hot read published by the execution breaker (seam #18).
    pub venue_health: VenueHealthHandle,
    /// Exit-monitor health hot read published by the worker (seam #20).
    pub exit_monitor_health: ExitMonitorHealthHandle,
    /// Fresh signer-free settlement capability gate for automatic resolution recovery.
    pub settlement_recovery: Arc<dyn SettlementRecoveryAdmissionPort>,
}

/// Builds the frozen [`AdmissionInput`] for an intent at decision time.
pub struct AdmissionInputBuilder {
    deps: AdmissionInputBuilderDeps,
}

struct EconomicHealthLookup {
    identity: RouteEconomicHealthIdentity,
    freshness_secs: u64,
    now: DateTime<Utc>,
}

#[derive(Clone, Copy)]
struct AdmissionPrepareInput<'a> {
    recommendation: &'a RecommendationInfo,
    intent: &'a OrderIntentInfo,
    fetched: &'a ParallelAdmissionFetch,
    budget_total_usd: Usd,
    now: DateTime<Utc>,
}

struct PreparedAdmissionContext {
    profile_ref: ResearchProfileRef,
    venue_metadata: AdmissionVenueMetadata,
    prepared_order: PreparedVenueOrder,
    venue_funding: VenueFundingEvidence,
    account: AccountSnapshot,
    book: Arc<BookSnapshot>,
    execution_economics: PitMarketExecutionEconomics,
    economic_health: AdmissionEconomicHealth,
    model_state: AdmissionModelState,
    exposure: AdmissionExposureState,
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
        let condition = deps
            .conditions
            .find_instance(&intent.condition_instance_id)
            .await?
            .ok_or_else(|| {
                not_found(
                    "entry_condition_instance",
                    intent.condition_instance_id.to_string(),
                )
            })?;

        let config = deps.config.current();
        let budget_total_usd = Usd::new(
            config
                .execution_risk
                .portfolio
                .budget
                .total_budget_usd
                .value,
        );
        let max_stale_book_ratio_bps = config.recommendation.data_quality.max_stale_book_ratio_bps;
        let max_open_intents = config
            .execution_risk
            .portfolio
            .exposure_limits
            .max_open_recommendations;
        let max_reserved_usd = Usd::new(
            config
                .execution_risk
                .portfolio
                .budget
                .max_open_capital_usd
                .value,
        );

        let fetched = self
            .fetch_parallel_sources(&recommendation, intent, now)
            .await?;
        let prepared = self
            .prepare_execution_context(AdmissionPrepareInput {
                recommendation: &recommendation,
                intent,
                fetched: &fetched,
                budget_total_usd,
                now,
            })
            .await?;
        let data_quality = deps.data_quality.snapshot();
        let controls = deps.runtime_controls.snapshot();
        let authorization_policy = controls.entry_authorization_policy;
        let kill_switch = controls.kill_switch_state;
        let settlement_recovery = self
            .resolve_settlement_recovery(intent, &prepared.venue_metadata, &controls, now)
            .await?;
        let now_ms = Self::timestamp_ms(now)?;
        let admission_evidence_hash = AdmissionExecutionEvidence {
            venue_metadata: &prepared.venue_metadata,
            venue_funding: &prepared.venue_funding,
            prepared_order: &prepared.prepared_order,
        }
        .content_hash()?;
        let PreparedAdmissionContext {
            profile_ref,
            venue_metadata,
            prepared_order,
            venue_funding,
            account,
            book,
            execution_economics,
            economic_health,
            model_state,
            exposure,
        } = prepared;
        let fee_schedule = execution_economics.fee_schedule;
        let maker_rebate_evidence = execution_economics.maker_rebate_evidence;
        let state_version = StateVersion {
            config_version_id: fetched.active_version.decision_policy_snapshot_id,
            account_as_of: account.as_of,
            book_version: Some(book.version),
            book_as_of_ms: Some(book.timestamp_ms),
            fee_schedule_hash: fee_schedule.schedule_hash,
            maker_rebate_terms_hash: maker_rebate_evidence.terms_hash(),
            maker_rebate_decidable: maker_rebate_evidence.is_decidable(),
            kill_switch_state: kill_switch,
            settlement_write_policy: controls.settlement_write_policy,
            settlement_deployment_digest: settlement_recovery.deployment_digest(),
            settlement_verified_block_hash: settlement_recovery.verified_block_hash(),
            route_economic_health_id: economic_health.evidence_id(),
            route_economic_health_hash: economic_health.evidence_hash(),
            route_economic_health_state: economic_health.state(),
            admission_evidence_hash,
        };
        Ok(AdmissionInput {
            profile_ref,
            intent: intent.clone(),
            condition,
            recommendation,
            report: fetched.report,
            authorization_policy,
            kill_switch,
            account,
            allocation: fetched.allocation,
            book: Some(book),
            fee_schedule,
            maker_rebate_evidence,
            budget_total_usd,
            open_intent_count: fetched.open_intent_count,
            max_open_intents,
            max_reserved_usd,
            model_state,
            economic_health,
            data_quality,
            max_stale_book_ratio_bps,
            exposure,
            venue_metadata,
            prepared_order,
            venue_funding,
            seams: AdmissionSeams {
                venue_health: deps.venue_health.current(),
                credentials_ready: deps.account_factory.credentials_ready(),
                exit_monitor_ready: deps.exit_monitor_health.is_ready(now),
            },
            settlement_recovery,
            now,
            now_ms,
            state_version,
        })
    }

    async fn prepare_execution_context(
        &self,
        input: AdmissionPrepareInput<'_>,
    ) -> QuantResult<PreparedAdmissionContext> {
        let AdmissionPrepareInput {
            recommendation,
            intent,
            fetched,
            budget_total_usd,
            now,
        } = input;
        let catalog_market = fetched.catalog_market.verified_payload().map_err(|error| {
            ExecutionError::IntentDenied {
                reason: format!("current Gamma catalog evidence is invalid: {error}"),
            }
        })?;
        let venue_metadata = Self::venue_metadata(intent, fetched, &catalog_market);
        venue_metadata
            .validate(&recommendation.market_id, &intent.entry_order_json.token_id)
            .map_err(|reason| ExecutionError::IntentDenied { reason })?;
        let execution_economics = Self::resolve_execution_economics(fetched, &catalog_market, now)?;
        let model_version =
            fetched
                .model_version
                .as_ref()
                .ok_or_else(|| ExecutionError::IntentDenied {
                    reason: "intent model version no longer exists".to_owned(),
                })?;
        let profile_ref = require_frozen_trade_policy(
            self.deps.trade_policies.as_ref(),
            model_version,
            recommendation,
        )
        .await?;
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(QuantError::config)?;
        let trade_policy_artifact_id =
            model_version
                .trade_policy_artifact_id
                .ok_or_else(|| ExecutionError::IntentDenied {
                    reason: "intent model has no frozen trade policy".to_owned(),
                })?;
        let economic_health = self
            .resolve_economic_health(EconomicHealthLookup {
                identity: RouteEconomicHealthIdentity {
                    route: recommendation.route,
                    research_profile_artifact_id: profile_ref.artifact_id(),
                    model_version_id: model_version.model_version_id,
                    trade_policy_artifact_id,
                },
                freshness_secs: profile.spec.feedback_policy.feedback_cadence_secs,
                now,
            })
            .await?;
        let model_state = self
            .resolve_model_state(fetched.model_version.as_ref(), &fetched.active_version)
            .await?;
        let exposure = AdmissionExposureState {
            has_blocking_inflight: fetched.has_blocking_inflight,
            manual_block: fetched.manual_block,
        };
        let book = self
            .deps
            .book_store
            .load_fresh_by_id(&intent.entry_order_json.token_id)
            .map(FreshBook::into_snapshot)
            .map_err(|unavailable| ExecutionError::IntentDenied {
                reason: format!(
                    "cannot prepare entry without a fresh PIT L2 book: {unavailable:?}"
                ),
            })?;
        let prepared_order = EntryOrderPreparation {
            profile_ref: &profile_ref,
            spec: &intent.entry_order_json,
            book: &book,
            fee_schedule: &execution_economics.fee_schedule,
            maker_rebate_evidence: &execution_economics.maker_rebate_evidence,
            venue_metadata: &venue_metadata,
            now,
        }
        .prepare()?;
        let funding_request = OrderRequest {
            market_id: prepared_order.market_id.clone(),
            token_id: prepared_order.token_id.clone(),
            expected_tick_size: prepared_order.tick_size,
            expected_minimum_order_size: prepared_order.minimum_order_size,
            expected_neg_risk: prepared_order.neg_risk,
            expected_clob_market_info_payload_hash: prepared_order.clob_market_info_payload_hash,
            side: prepared_order.side,
            amount: prepared_order.venue_amount,
            expected_fee: prepared_order.expected_fee,
            price: prepared_order.limit_price,
            order_type: prepared_order.order_type,
            post_only: prepared_order.post_only,
        };
        let venue_funding = self
            .deps
            .clob
            .order_funding_evidence(&funding_request, &fetched.live_venue_metadata)
            .await?;
        let collateral = venue_funding
            .snapshot()
            .human_balance
            .collateral()
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "entry funding evidence does not carry collateral balance".to_owned(),
            })?;
        let account = self
            .deps
            .account_factory
            .create(budget_total_usd)?
            .snapshot_with_collateral(now, collateral)
            .await?;
        Ok(PreparedAdmissionContext {
            profile_ref,
            venue_metadata,
            prepared_order,
            venue_funding,
            account,
            book,
            execution_economics,
            economic_health,
            model_state,
            exposure,
        })
    }

    async fn resolve_economic_health(
        &self,
        lookup: EconomicHealthLookup,
    ) -> QuantResult<AdmissionEconomicHealth> {
        let identity_hash = lookup.identity.content_hash().map_err(QuantError::config)?;
        let Some(health) = self
            .deps
            .economic_health
            .latest(
                &identity_hash,
                &lookup.identity.research_profile_artifact_id,
                lookup.now,
            )
            .await?
        else {
            return Ok(AdmissionEconomicHealth::Missing);
        };
        let freshness =
            Duration::seconds(i64::try_from(lookup.freshness_secs).map_err(|error| {
                ExecutionError::TimeConversion {
                    field: "admission.economic_health_freshness",
                    value: lookup.freshness_secs.to_string(),
                    detail: error.to_string(),
                }
            })?);
        Ok(AdmissionEconomicHealth::Present {
            fresh: lookup.now - health.assessed_through <= freshness,
            route_economic_health_id: health.route_economic_health_id,
            evidence_hash: health.evidence_hash,
            state: health.state,
            assessed_through: health.assessed_through,
        })
    }

    fn timestamp_ms(now: DateTime<Utc>) -> QuantResult<u64> {
        u64::try_from(now.timestamp_millis()).map_err(|error| {
            ExecutionError::TimeConversion {
                field: "admission.now_ms",
                value: now.timestamp_millis().to_string(),
                detail: error.to_string(),
            }
            .into()
        })
    }

    async fn resolve_settlement_recovery(
        &self,
        intent: &OrderIntentInfo,
        metadata: &AdmissionVenueMetadata,
        controls: &RuntimeControlSnapshot,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementRecoveryAdmission> {
        if !requires_automatic_settlement_recovery(&intent.exit_policy_json) {
            return Ok(SettlementRecoveryAdmission::NotRequired);
        }
        let route = if metadata.current_neg_risk {
            SettlementRoute::NegRiskV2
        } else {
            SettlementRoute::StandardV2
        };
        self.deps
            .settlement_recovery
            .evaluate_recovery_admission(
                SettlementRecoveryAdmissionRequest {
                    execution_account_id: intent.execution_account_id,
                    route,
                    authorization_policy: controls.entry_authorization_policy,
                    write_policy: controls.settlement_write_policy,
                },
                now,
            )
            .await
    }

    fn resolve_execution_economics(
        fetched: &ParallelAdmissionFetch,
        catalog_market: &MarketRegistryInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<PitMarketExecutionEconomics> {
        PitMarketExecutionEconomics::resolve(
            &fetched.current_clob_market_info.fee_schedule(),
            &catalog_market.maker_rebate_evidence,
            fetched.catalog_market.available_at,
            now,
        )
        .map_err(|error| {
            ExecutionError::IntentDenied {
                reason: format!("current execution economics are invalid: {error:?}"),
            }
            .into()
        })
    }

    fn venue_metadata(
        intent: &OrderIntentInfo,
        fetched: &ParallelAdmissionFetch,
        catalog_market: &MarketRegistryInfo,
    ) -> AdmissionVenueMetadata {
        let token_id = &intent.entry_order_json.token_id;
        let frozen_token_id = fetched
            .frozen_clob_market_info
            .tokens
            .iter()
            .find(|token| &token.token_id == token_id)
            .map(|token| token.token_id.clone());
        let current_token_id = fetched
            .current_clob_market_info
            .tokens
            .iter()
            .find(|token| &token.token_id == token_id)
            .map(|token| token.token_id.clone());
        let catalog_token_id = [&catalog_market.token_yes, &catalog_market.token_no]
            .into_iter()
            .find(|candidate| *candidate == token_id)
            .cloned();
        AdmissionVenueMetadata {
            catalog_market_id: catalog_market.market_id.clone(),
            catalog_token_id,
            frozen_version_id: fetched.frozen_clob_market_info.version_id,
            frozen_market_id: fetched.frozen_clob_market_info.market_id.clone(),
            frozen_token_id,
            frozen_tick_size: fetched.frozen_clob_market_info.tick_size,
            frozen_minimum_order_size: fetched.frozen_clob_market_info.minimum_order_size,
            frozen_neg_risk: fetched.frozen_clob_market_info.neg_risk,
            frozen_payload_hash: fetched.frozen_clob_market_info.payload_hash,
            current_version_id: fetched.current_clob_market_info.version_id,
            current_market_id: fetched.current_clob_market_info.market_id.clone(),
            current_token_id,
            current_tick_size: fetched.current_clob_market_info.tick_size,
            current_minimum_order_size: fetched.current_clob_market_info.minimum_order_size,
            current_neg_risk: fetched.current_clob_market_info.neg_risk,
            current_payload_hash: fetched.current_clob_market_info.payload_hash,
            live_market_id: fetched.live_venue_metadata.market_id.clone(),
            live_token_id: fetched.live_venue_metadata.token_id.clone(),
            live_tick_size: fetched.live_venue_metadata.tick_size,
            live_minimum_order_size: fetched.live_venue_metadata.minimum_order_size,
            live_neg_risk: fetched.live_venue_metadata.neg_risk,
            catalog_status: catalog_market.status,
            catalog_filter_reasons: catalog_market.filter_reasons,
        }
    }

    async fn fetch_parallel_sources(
        &self,
        recommendation: &RecommendationInfo,
        intent: &OrderIntentInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<ParallelAdmissionFetch> {
        let deps = &self.deps;
        let report_id = recommendation.recommendation_report_id;
        let report = deps
            .reports
            .find_by_id(&report_id)
            .await?
            .ok_or_else(|| not_found("recommendation_report", report_id.to_string()))?;
        let market_id = recommendation.market_id.clone();
        let order_intent_id = intent.order_intent_id;
        let model_version_id = intent.model_version_id;
        let clob = Arc::clone(&deps.clob);
        let token_id = intent.entry_order_json.token_id.clone();
        let catalog_boundary = DecisionClock::new(0).boundary(now)?;

        let (
            model_version_result,
            unresolvable_result,
            ambiguous_inflight_result,
            allocation_result,
            active_version_result,
            market_result,
            frozen_clob_market_info_result,
            current_clob_market_info_result,
            open_intent_result,
            venue_metadata_result,
            catalog_market_result,
        ) = tokio::join!(
            deps.model_registry.find_model_version(&model_version_id),
            deps.reconciliation.has_unresolvable(),
            deps.execution_orders.has_ambiguous_inflight(),
            deps.capital.find_by_intent(&order_intent_id),
            deps.config_versions.load_current(),
            deps.markets.find_by_id(&market_id),
            deps.clob_market_info
                .at(&market_id, report.decision_at, report.created_at),
            deps.clob_market_info.at(&market_id, now, now),
            deps.intents.count_open(),
            async move { clob.order_metadata(&token_id).await },
            deps.catalog_ledger.market_at(&market_id, &catalog_boundary),
        );

        let market = market_result?
            .ok_or_else(|| not_found("market", recommendation.market_id.to_string()))?;
        let manual_block = market.status == MarketStatus::ManuallyBlocked;
        let frozen_clob_market_info =
            frozen_clob_market_info_result?.ok_or_else(|| ExecutionError::IntentDenied {
                reason: "no report-time CLOB market-info observation is available".to_owned(),
            })?;
        let current_clob_market_info =
            current_clob_market_info_result?.ok_or_else(|| ExecutionError::IntentDenied {
                reason: "no current CLOB market-info observation is available".to_owned(),
            })?;
        let live_venue_metadata = venue_metadata_result?;
        let active_version = active_version_result?
            .ok_or_else(|| not_found("decision_policy_snapshot", "current".to_owned()))?;
        let catalog_market =
            catalog_market_result?.ok_or_else(|| ExecutionError::IntentDenied {
                reason: "no current Gamma catalog evidence is available".to_owned(),
            })?;

        Ok(ParallelAdmissionFetch {
            report,
            model_version: model_version_result?,
            has_blocking_inflight: unresolvable_result? || ambiguous_inflight_result?,
            allocation: allocation_result?,
            manual_block,
            frozen_clob_market_info,
            current_clob_market_info,
            active_version,
            open_intent_count: open_intent_result?,
            live_venue_metadata,
            catalog_market,
        })
    }

    async fn resolve_model_state(
        &self,
        model_version: Option<&ModelVersionInfo>,
        active_version: &DecisionPolicySnapshotInfo,
    ) -> QuantResult<AdmissionModelState> {
        let route_bound = model_version.is_some_and(|version| {
            let routing = &active_version.snapshot.model_routing.model;
            routing
                .buy_routes
                .values()
                .any(|binding| binding.champion.model_version_id == version.model_version_id)
                || routing
                    .active_exit_model_version_id
                    .as_ref()
                    .is_some_and(|reference| reference.id == version.model_version_id)
        });
        let return_model_calibrated = match model_version {
            Some(version) => {
                let artifact =
                    ModelArtifact::load_verified(self.deps.artifact_store.as_ref(), version)
                        .await?;
                // The enum tag alone only proves a calibrator was bound at
                // publish time; `resolve_return_model_calibration` re-resolves
                // it now (the same shared deep check publish / report /
                // intent-creation use) so a calibrator that has since been
                // deactivated/superseded denies at submit time (TOCTOU close
                // — admission check #23 deep verification) rather than
                // silently continuing to trust a stale reference.
                resolve_return_model_calibration(self.deps.calibration_loader.as_ref(), &artifact)
                    .await
                    .is_ok_and(|resolved| resolved.is_some())
            }
            None => false,
        };
        Ok(AdmissionModelState {
            route_bound,
            return_model_calibrated,
        })
    }
}

struct ParallelAdmissionFetch {
    report: RecommendationReportInfo,
    model_version: Option<ModelVersionInfo>,
    has_blocking_inflight: bool,
    allocation: Option<CapitalAllocationInfo>,
    manual_block: bool,
    frozen_clob_market_info: ClobMarketInfoVersion,
    current_clob_market_info: ClobMarketInfoVersion,
    active_version: DecisionPolicySnapshotInfo,
    open_intent_count: u64,
    live_venue_metadata: VenueOrderMetadata,
    catalog_market: CatalogMarketChangeInfo,
}

pub fn pit_fee_schedule(
    market_info: &ClobMarketInfoVersion,
    decision_at: DateTime<Utc>,
) -> QuantResult<PitFeeSchedule> {
    if market_info.effective_at > decision_at || market_info.available_at > decision_at {
        return Err(ExecutionError::IntentDenied {
            reason: "market fee schedule was not point-in-time visible".to_owned(),
        }
        .into());
    }
    PitFeeSchedule::from_market_fee_schedule(&market_info.fee_schedule()).map_err(|error| {
        ExecutionError::IntentDenied {
            reason: format!("market fee schedule is invalid: {error:?}"),
        }
        .into()
    })
}

fn not_found(entity: &'static str, id: String) -> QuantError {
    StorageError::NotFound { entity, id }.into()
}
