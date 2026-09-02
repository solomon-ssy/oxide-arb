//! Observe the real composer boundary and preserve its risk decisions.

use std::{
    collections::{BTreeMap, BTreeSet},
    slice,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use quant_pivot_core::report::{
    ComposeReportInput, ComposedReport, DefaultRecommendationComposer, RecommendationComposer,
};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::quant::{
        ExecutableEconomicTier, ModelVersionInfo, RepresentedRouteSet, TradePolicyArtifactInfo,
    },
    enums::quant::OutcomeSide,
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioAdmission},
    types::{EntryOrderTemplate, MarketId, ReportFunnelReason, TokenId, Usd},
};
use quant_pivot_repository::{postgres::PgTradePolicyRepository, traits::TradePolicyRepository};
use quant_pivot_research::{
    execution_semantics::PitMarketExecutionEconomics,
    portfolio::{
        EconomicTierFactory, ExecutableCashTierSeedFactory, ExecutableCashTierSeedInput,
        ExistingPortfolioFactory, GlobalPortfolioInput, GlobalPortfolioPlanner,
        SealedPortfolioScenarioArtifact, TierAdmissionRejectionCode, TierSeedBuild,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use serde_json::json;

use super::super::super::{closure_market_identity, closure_no_token, closure_token};

pub(super) struct SourceSideComposer {
    crypto_policy: TradePolicyArtifactInfo,
}

impl SourceSideComposer {
    pub(super) async fn load(db: &DatabaseConnection, model: &ModelVersionInfo) -> Result<Self> {
        let id = model
            .trade_policy_artifact_id
            .context("Crypto policy binding")?;
        let crypto_policy = PgTradePolicyRepository::new(db.clone())
            .find(&id)
            .await?
            .context("Crypto published policy")?;
        ensure!(
            Some(crypto_policy.content_hash) == model.trade_policy_hash,
            "Crypto policy hash differs from model"
        );
        Ok(Self { crypto_policy })
    }

    fn inspect(
        &self,
        input: &ComposeReportInput<'_>,
    ) -> Result<BTreeMap<MarketId, Option<ReportFunnelReason>>> {
        ensure!(
            input.candidate_count == 10
                && input.tiers.len() == 10
                && input.tier_build_rejections.is_empty(),
            "all ten markets must reach real executable tiers: candidate_count={} tier_count={} tier_build_rejections={:?} model_decisions={:?} feature_rejected={:?}",
            input.candidate_count,
            input.tiers.len(),
            input.tier_build_rejections,
            input
                .model_decisions
                .iter()
                .map(|decision| (
                    &decision.market_id,
                    decision.gate_passed,
                    &decision.primary_reason
                ))
                .collect::<Vec<_>>(),
            input
                .feature_rejected
                .iter()
                .map(|rejection| (
                    &rejection.market_id,
                    rejection.data_quality,
                    &rejection.missing_required
                ))
                .collect::<Vec<_>>(),
        );
        let admission = &input.runtime_config.execution_risk.portfolio.admission;
        ensure!(
            admission.min_nominal_expected_net_usd.value == dec!(1)
                && admission.min_robust_expected_net_usd.value == dec!(0.5)
                && admission.min_profit_probability_bps == 5_200,
            "source-side regression must retain the original risk floors"
        );
        let scenario = SealedPortfolioScenarioArtifact::verify(
            input
                .portfolio_plan
                .scenario_artifact_json
                .clone()
                .context("real mixed scenario")?,
        )?;
        let mut reasons = BTreeMap::new();
        let mut crypto_sides = BTreeMap::new();
        for tier in input.tiers {
            let expected = Self::risk_reason(tier, admission);
            let actual = input
                .tier_rejections
                .iter()
                .find(|rejection| rejection.economic_tier_id == tier.economic_tier_id)
                .map(|rejection| rejection.code);
            ensure!(
                actual == expected.map(|(code, _)| code),
                "market {} typed rejection differs from its actual unmodified risk inputs: actual={actual:?} expected={expected:?}",
                tier.market_id
            );
            ensure!(
                reasons
                    .insert(tier.market_id.clone(), expected.map(|(_, reason)| reason))
                    .is_none(),
                "fixture emitted duplicate market tiers"
            );
            let capture = input
                .captures
                .get(&tier.market_id)
                .context("market capture")?;
            let book = capture
                .book_for(&tier.token_id)
                .context("candidate-side book")?;
            let reference = capture
                .book_snapshot_ref_for(&tier.token_id)
                .context("candidate-side snapshot")?;
            ensure!(
                book.token_id == tier.token_id && reference == &book.snapshot_ref()?,
                "candidate-side book/reference identity drifted"
            );
            if tier.route != BuyModelRoute::Crypto {
                continue;
            }
            let (scope, ordinal) = closure_market_identity(&tier.market_id)?;
            let expected_side = if ordinal.is_multiple_of(2) {
                OutcomeSide::No
            } else {
                OutcomeSide::Yes
            };
            let expected_token = if expected_side == OutcomeSide::Yes {
                TokenId::new(closure_token(scope, ordinal))
            } else {
                closure_no_token(scope, ordinal)
            };
            ensure!(
                tier.outcome_side == expected_side && tier.token_id == expected_token,
                "Crypto source-side identity is not Yes/No/Yes/No/Yes"
            );
            crypto_sides.insert(ordinal, expected_side);
            if expected_side == OutcomeSide::No {
                ensure!(
                    tier.economics.nominal_expected_net_usd.inner()
                        > admission.min_nominal_expected_net_usd.value,
                    "fixed No quote did not clear the original nominal floor"
                );
                ensure!(
                    matches!(
                        actual,
                        Some(
                            TierAdmissionRejectionCode::RobustExpectedNetFloor
                                | TierAdmissionRejectionCode::ProfitProbabilityFloor
                        )
                    ),
                    "the underpowered local panel must still fail its actual robust/probability gate"
                );
                let expensive = self.expensive_no(input, tier, &scenario)?;
                ensure!(
                    expensive.economics.nominal_expected_net_usd
                        < tier.economics.nominal_expected_net_usd
                        && expensive.economics.nominal_expected_net_usd.inner()
                            < admission.min_nominal_expected_net_usd.value,
                    "the same frozen scenario must reject the higher-cost No counterfactual"
                );
                ensure!(
                    Self::risk_reason(&expensive, admission).map(|(code, _)| code)
                        == Some(TierAdmissionRejectionCode::NominalExpectedNetFloor),
                    "high-cost No counterfactual must fail nominal admission"
                );
                Self::verify_negative(input, &expensive, &scenario)?;
                println!(
                    "source-side-economics {}",
                    json!({"market_id": tier.market_id, "token_id": tier.token_id,
                    "quote": book.best_ask(), "nominal": tier.economics.nominal_expected_net_usd,
                    "robust": tier.economics.robust_expected_net_usd, "probability_lower_bps": tier.profit_probability_lower_bps,
                    "retained_rejection": format!("{actual:?}"), "high_quote": expensive.entry_execution.limit_price(),
                    "high_quote_nominal": expensive.economics.nominal_expected_net_usd})
                );
            }
        }
        ensure!(
            crypto_sides
                == BTreeMap::from([
                    (1, OutcomeSide::Yes),
                    (2, OutcomeSide::No),
                    (3, OutcomeSide::Yes),
                    (4, OutcomeSide::No),
                    (5, OutcomeSide::Yes)
                ]),
            "Crypto source population differs"
        );
        Ok(reasons)
    }

    fn risk_reason(
        tier: &ExecutableEconomicTier,
        admission: &PortfolioAdmission,
    ) -> Option<(TierAdmissionRejectionCode, ReportFunnelReason)> {
        if tier.economics.nominal_expected_net_usd.inner()
            < admission.min_nominal_expected_net_usd.value
        {
            Some((
                TierAdmissionRejectionCode::NominalExpectedNetFloor,
                ReportFunnelReason::NominalExpectedNetBelowFloor,
            ))
        } else if tier.economics.robust_expected_net_usd.inner()
            < admission.min_robust_expected_net_usd.value
        {
            Some((
                TierAdmissionRejectionCode::RobustExpectedNetFloor,
                ReportFunnelReason::RobustExpectedNetBelowFloor,
            ))
        } else if tier.profit_probability_lower_bps < admission.min_profit_probability_bps {
            Some((
                TierAdmissionRejectionCode::ProfitProbabilityFloor,
                ReportFunnelReason::ProfitProbabilityBelowFloor,
            ))
        } else if tier.probability_interval_width_bps > admission.max_probability_interval_width_bps
        {
            Some((
                TierAdmissionRejectionCode::ProbabilityIntervalWidth,
                ReportFunnelReason::ProbabilityIntervalTooWide,
            ))
        } else {
            None
        }
    }

    fn expensive_no(
        &self,
        input: &ComposeReportInput<'_>,
        tier: &ExecutableEconomicTier,
        scenario: &SealedPortfolioScenarioArtifact,
    ) -> Result<ExecutableEconomicTier> {
        let capture = input.captures.get(&tier.market_id).context("No capture")?;
        let original = capture.book_for(&tier.token_id).context("No book")?;
        // Freeze a test-only alternative quote at the complementary high price.
        // The No identity, model outcomes, policy, fee schedule and scenario
        // remain unchanged; no stored or production book is rewritten.
        let high = &capture.book;
        ensure!(
            high.token_id != tier.token_id && high.best_ask() > original.best_ask(),
            "No counterfactual must isolate higher entry cost"
        );
        let index = usize::try_from(
            tier.tier_ordinal
                .checked_sub(1)
                .context("positive tier ordinal")?,
        )?;
        let cohort = self
            .crypto_policy
            .payload_json
            .cohorts
            .get(index)
            .context("exact Crypto cohort")?;
        let EntryOrderTemplate::Aggressive {
            fill_requirement,
            max_slippage_bps,
            ..
        } = cohort.entry_order
        else {
            bail!("source-side fixture requires the real aggressive cohort");
        };
        let rules = capture.market.order_rules.context("frozen order rules")?;
        let limit_price = rules
            .aggressive_buy_limit(high.best_ask().context("high quote ask")?, max_slippage_bps)?;
        let execution_economics = PitMarketExecutionEconomics::resolve(
            capture
                .market
                .fee_schedule
                .as_ref()
                .context("actual market fee schedule")?,
            &capture.market.maker_rebate_evidence,
            capture.market.available_at,
            input.decision_at,
        )
        .map_err(|error| anyhow!("frozen execution economics could not resolve: {error:?}"))?;
        let build = ExecutableCashTierSeedFactory::build(ExecutableCashTierSeedInput {
            report_route_run_id: tier.report_route_run_id,
            candidate_id: tier.candidate_id,
            tier_ordinal: tier.tier_ordinal,
            route: tier.route,
            market_id: tier.market_id.clone(),
            event_id: tier.event_id.clone(),
            category: tier.category,
            token_id: tier.token_id.clone(),
            outcome_side: tier.outcome_side,
            bids: &high.bids,
            asks: &high.asks,
            fee_schedule: &execution_economics.fee_schedule,
            fill_at: input.decision_at,
            limit_price,
            cash_budget: cohort.key.cash_budget_tier,
            fill_requirement,
            order_rules: rules,
            source_lineage_hash: CanonicalDigest::content_hash_json(&(
                "source-side-high-quote-counterfactual",
                tier.lineage_hash,
                high.snapshot_ref()?,
            ))?,
        })?;
        let TierSeedBuild::Ready(seed) = build else {
            bail!("high-price No quote must remain executable before economic rejection");
        };
        Ok(EconomicTierFactory::build(*seed, scenario)?)
    }

    fn verify_negative(
        input: &ComposeReportInput<'_>,
        tier: &ExecutableEconomicTier,
        scenario: &SealedPortfolioScenarioArtifact,
    ) -> Result<()> {
        ensure!(
            input.account.positions.is_empty()
                && input.account.reserved_usd == Usd::ZERO
                && input.equity_snapshot.drawdown_pct.is_zero(),
            "counterfactual requires the actual empty-position, unreserved, zero-drawdown fixture account"
        );
        let existing = ExistingPortfolioFactory::build(input.account, Usd::ZERO, scenario)?;
        let represented = RepresentedRouteSet::from_routes(scenario.ordered_routes.clone())?;
        let binding = input
            .runtime_config
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .iter()
            .find(|binding| {
                let identity_matches = binding.portfolio_scenario_model_artifact_id
                    == scenario.portfolio_scenario_model_artifact_id;
                let content_matches =
                    binding.model_content_hash == scenario.scenario_model_content_hash;
                identity_matches && content_matches
            })
            .context("actual frozen scenario binding")?;
        let result = GlobalPortfolioPlanner::solve_and_verify(GlobalPortfolioInput {
            portfolio_plan_id: input.portfolio_plan.portfolio_plan_id,
            account: input.account,
            existing: &existing,
            represented_routes: &represented,
            scenario_model_binding: binding,
            scenario_artifact: scenario,
            policy: &input.runtime_config.execution_risk.portfolio,
            solver: &PortfolioSolverDeployConfig::default(),
            tiers: slice::from_ref(tier),
            top_n: input.top_n,
        })?;
        ensure!(
            result.selected.is_empty()
                && result.rejected.len() == 1
                && result.rejected[0].economic_tier_id == tier.economic_tier_id
                && result.rejected[0].code == TierAdmissionRejectionCode::NominalExpectedNetFloor,
            "production planner did not reject the expensive No tier at its unchanged nominal floor"
        );
        Ok(())
    }

    fn verify_funnel(
        report: &ComposedReport,
        reasons: &BTreeMap<MarketId, Option<ReportFunnelReason>>,
    ) -> Result<()> {
        let mut observed = BTreeSet::new();
        let published = report
            .transaction
            .recommendations
            .iter()
            .map(|row| row.market_id.clone())
            .collect::<BTreeSet<_>>();
        for row in &report.funnel_rows {
            ensure!(
                observed.insert(row.market_id.clone()),
                "market funnel repeats an identity"
            );
            let expected = reasons
                .get(&row.market_id)
                .context("funnel outside the ten-market tier population")?;
            if let Some(reason) = expected {
                ensure!(
                    row.terminal_stage == "sizing_eligible"
                        && row.primary_reason == reason.as_str()
                        && !published.contains(&row.market_id),
                    "economic rejection was lost in the durable funnel"
                );
            } else if published.contains(&row.market_id) {
                ensure!(
                    row.primary_reason == ReportFunnelReason::Published.as_str(),
                    "selected market lacks its published funnel fact"
                );
            } else {
                ensure!(
                    row.primary_reason == ReportFunnelReason::NotSelectedByGlobalOptimum.as_str(),
                    "unselected admitted market lacks optimizer evidence"
                );
            }
        }
        ensure!(
            observed == reasons.keys().cloned().collect(),
            "ten-market funnel is not conserved"
        );
        Ok(())
    }
}

impl RecommendationComposer for SourceSideComposer {
    fn compose(&self, input: ComposeReportInput<'_>) -> QuantResult<ComposedReport> {
        let reasons =
            self.inspect(&input)
                .map_err(|error| ResearchError::ValidationMethodology {
                    detail: format!("source-side regression: {error:#}"),
                })?;
        let report = DefaultRecommendationComposer::new().compose(input)?;
        Self::verify_funnel(&report, &reasons).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("source-side funnel regression: {error:#}"),
            }
        })?;
        Ok(report)
    }
}
