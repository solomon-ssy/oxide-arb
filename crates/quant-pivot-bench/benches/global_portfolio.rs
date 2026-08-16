//! Production-path benchmark for the exact-verified 100-tier global MILP.

use chrono::{DateTime, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        market::fee::ImmediateExecutionCost,
        quant::{
            AggressiveEntryEconomics, CapitalOccupancyBucket, DiscountCurvePoint,
            EntryExecutionEconomics, ExecutableEconomicTier, ExistingPortfolioState,
            HardReservationBucket, PortfolioScenario, PortfolioScenarioArtifact,
            PortfolioScenarioEvidenceRegime, PortfolioScenarioKind, PortfolioScenarioVisibility,
            RecommendationEconomics, RepresentedRouteSet, ScenarioCapitalOccupancySlice,
            ScenarioCashflow, ScenarioDistribution, ScenarioEntryExecution,
            ScenarioExecutionCashflow, ScenarioWeight,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{AccountSource, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
    types::{
        Bps, ContentHash, EconomicTierId, EventId, MarketId, PortfolioPlanId,
        PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId, Price, ReportRouteRunId,
        SchemaVersion, Shares, SignalCandidateId, TokenId, Usd, UsdHours,
    },
};
use quant_pivot_research::portfolio::{
    AccountSnapshot, CapitalTimeBucketContract, GlobalPortfolioInput, GlobalPortfolioPlanner,
    SealedPortfolioScenarioArtifact,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

const CANDIDATE_COUNT: u128 = 100;
const TOP_N: u32 = 20;

struct BenchmarkFixture {
    portfolio_plan_id: PortfolioPlanId,
    account: AccountSnapshot,
    existing: ExistingPortfolioState,
    represented_routes: RepresentedRouteSet,
    scenario_model_binding: PortfolioScenarioModelArtifactBinding,
    scenario_artifact: SealedPortfolioScenarioArtifact,
    policy: PortfolioConfig,
    solver: PortfolioSolverDeployConfig,
    tiers: Vec<ExecutableEconomicTier>,
}

impl BenchmarkFixture {
    fn build() -> Self {
        let decision_at = DateTime::<Utc>::UNIX_EPOCH;
        let represented_routes =
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled]).expect("Route set");
        let mut policy = PortfolioConfig::default();
        policy.budget.total_budget_usd.value = dec!(10000);
        policy.budget.cash_reserve_usd.value = Decimal::ZERO;
        policy.budget.max_open_capital_usd.value = dec!(10000);
        policy.exposure_limits.max_single_recommendation_usd.value = dec!(100);
        policy.exposure_limits.max_market_exposure_usd.value = dec!(100);
        policy.exposure_limits.max_event_exposure_usd.value = dec!(10000);
        policy.exposure_limits.max_category_exposure_usd.value = dec!(10000);
        policy.exposure_limits.max_route_exposure_usd.value = dec!(10000);
        policy.exposure_limits.max_open_recommendations = TOP_N;
        policy.tail_risk.max_cvar_usd.value = dec!(10000);
        policy.tail_risk.max_scenario_loss_usd.value = dec!(10000);
        policy.tail_risk.max_drawdown_usd.value = dec!(10000);
        policy.admission.min_nominal_expected_net_usd.value = Decimal::ZERO;
        policy.admission.min_robust_expected_net_usd.value = Decimal::ZERO;
        policy.admission.min_profit_probability_bps = 5_000;
        policy.admission.max_probability_interval_width_bps = 2_000;
        policy.admission.liquidity_buffer_bps = 1_000;

        let capital_time_bucket_contract_digest =
            CapitalTimeBucketContract::try_from(policy.tail_risk.capital_time_buckets.as_slice())
                .expect("capital-time grid")
                .content_hash()
                .expect("capital-time contract hash");
        let serving_contract_digest = hash("serving");
        let calibration_contract_digest = hash("calibration");
        let recommendation_contract_digest = hash("trade-policy");
        let scenario_model_content_hash = hash("scenario-model");
        let scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&scenario_model_content_hash);
        let mut scenario_artifact = PortfolioScenarioArtifact {
            portfolio_scenario_artifact_id: PortfolioScenarioArtifactId::new(Uuid::from_u128(1)),
            portfolio_scenario_model_artifact_id: scenario_model_artifact_id,
            scenario_model_content_hash,
            schema_version: SchemaVersion::FIRST,
            decision_at,
            visibility: PortfolioScenarioVisibility::PointInTime,
            evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
            input_universe_hash: hash("input-universe"),
            ordered_routes: represented_routes.routes.clone(),
            route_set_digest: represented_routes.digest,
            serving_contract_digest,
            calibration_contract_digest,
            recommendation_contract_digest,
            capital_time_bucket_contract_digest,
            scenarios: vec![
                scenario(0, PortfolioScenarioKind::PitBootstrap, "pit"),
                scenario(
                    1,
                    PortfolioScenarioKind::CalibrationUncertainty,
                    "calibration",
                ),
                scenario(2, PortfolioScenarioKind::StructuralStress, "stress"),
            ],
            distributions: vec![
                distribution("nominal", true, [5_000, 3_000, 2_000]),
                distribution("robust", false, [3_000, 3_000, 4_000]),
            ],
            structural_exclusivity: Vec::new(),
            discount_curve: policy
                .tail_risk
                .capital_time_buckets
                .iter()
                .map(|bucket| DiscountCurvePoint {
                    end_secs: bucket.end_secs,
                    annualized_cost_bps: 500,
                })
                .collect(),
            content_hash: hash("pending-artifact"),
        };
        scenario_artifact.content_hash = scenario_artifact
            .recomputed_hash()
            .expect("scenario artifact hash");
        scenario_artifact.portfolio_scenario_artifact_id =
            PortfolioScenarioArtifactId::from_content_hash(&scenario_artifact.content_hash);
        let scenario_model_binding = PortfolioScenarioModelArtifactBinding {
            portfolio_scenario_model_artifact_id: scenario_model_artifact_id,
            ordered_routes: represented_routes.routes.clone(),
            route_set_digest: represented_routes.digest,
            serving_contract_digest,
            calibration_contract_digest,
            recommendation_contract_digest,
            scenario_model_schema_version: SchemaVersion::FIRST,
            capital_time_bucket_contract_digest,
            model_content_hash: scenario_model_content_hash,
            bound_at: decision_at,
        };
        let bucket_ends = scenario_artifact
            .discount_curve
            .iter()
            .map(|point| point.end_secs)
            .collect::<Vec<_>>();
        let scenario_artifact = SealedPortfolioScenarioArtifact::verify(scenario_artifact)
            .expect("sealed scenario artifact");
        let tiers = (1..=CANDIDATE_COUNT)
            .map(|identity| tier(identity, &bucket_ends))
            .collect();
        let existing = ExistingPortfolioState {
            existing_open_capital_usd: Usd::ZERO,
            existing_open_recommendations: 0,
            current_drawdown_usd: Usd::ZERO,
            scenario_cashflows: zero_cashflows(),
            capital_occupancy: bucket_ends
                .iter()
                .map(|end_secs| CapitalOccupancyBucket {
                    end_secs: *end_secs,
                    locked_usd: Usd::ZERO,
                })
                .collect(),
        };
        Self {
            portfolio_plan_id: PortfolioPlanId::new(Uuid::from_u128(1_000)),
            account: AccountSnapshot::new(
                decision_at,
                AccountSource::Polymarket,
                Usd::new(dec!(10000)),
                Usd::new(dec!(10000)),
                Usd::new(dec!(10000)),
                Usd::ZERO,
                Vec::new(),
            ),
            existing,
            represented_routes,
            scenario_model_binding,
            scenario_artifact,
            policy,
            solver: PortfolioSolverDeployConfig {
                deadline_secs: 30,
                threads: 1,
                max_tiers: 100,
                max_scenarios: 3,
                max_top_n: 1,
            },
            tiers,
        }
    }

    fn input(&self) -> GlobalPortfolioInput<'_> {
        GlobalPortfolioInput {
            portfolio_plan_id: self.portfolio_plan_id,
            account: &self.account,
            existing: &self.existing,
            represented_routes: &self.represented_routes,
            scenario_model_binding: &self.scenario_model_binding,
            scenario_artifact: &self.scenario_artifact,
            policy: &self.policy,
            solver: &self.solver,
            tiers: &self.tiers,
            top_n: TOP_N,
        }
    }
}

fn tier(identity: u128, bucket_ends: &[u64]) -> ExecutableEconomicTier {
    let upside = 30 + i64::try_from(identity % 20).expect("bounded upside");
    let cashflows = [upside, 10, -5];
    let nominal = weighted_expected(cashflows, [5_000, 3_000, 2_000]);
    let robust = weighted_expected(cashflows, [3_000, 3_000, 4_000]);
    let filled_shares = Shares::new(dec!(200));
    let immediate_cost =
        ImmediateExecutionCost::new(Usd::new(dec!(100)), Usd::new(dec!(1)), Usd::ZERO)
            .expect("valid benchmark cost");
    let occupancy_secs = bucket_ends.first().copied().expect("capital bucket");
    ExecutableEconomicTier {
        economic_tier_id: EconomicTierId::new(Uuid::from_u128(identity)),
        report_route_run_id: ReportRouteRunId::new(Uuid::from_u128(500)),
        candidate_id: SignalCandidateId::new(Uuid::from_u128(identity)),
        tier_ordinal: 1,
        route: BuyModelRoute::Pooled,
        market_id: MarketId::new(format!("market-{identity}")),
        event_id: EventId::new(format!("event-{}", identity % 10)),
        category: MarketCategory::Sports,
        token_id: TokenId::new(format!("token-{identity}")),
        outcome_side: OutcomeSide::Yes,
        entry_execution: EntryExecutionEconomics::Aggressive(AggressiveEntryEconomics {
            requested_shares: filled_shares,
            filled_shares,
            limit_price: Price::new(dec!(0.5)),
            entry_vwap: Price::new(dec!(0.5)),
            immediate_cost,
            slippage_usd: Usd::new(dec!(1)),
            visible_liquidity_usd: Usd::new(dec!(10000)),
        }),
        profit_probability_lower_bps: 7_500,
        probability_interval_width_bps: 1_000,
        scenario_cashflows: cashflows
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let discounted_net_usd = Usd::new(Decimal::from(value));
                ScenarioExecutionCashflow {
                    scenario_index: u32::try_from(index).expect("three scenarios"),
                    entry_execution: ScenarioEntryExecution::AggressiveFill,
                    filled_shares,
                    immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
                    discounted_exit_cash_usd: immediate_cost.cash_outlay_usd + discounted_net_usd,
                    delayed_maker_rebate_usd: Usd::ZERO,
                    discounted_maker_rebate_usd: Usd::ZERO,
                    capital_cost_usd: Usd::ZERO,
                    capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                        locked_cash_usd: immediate_cost.cash_outlay_usd,
                        duration_secs: occupancy_secs,
                    }],
                    discounted_net_usd,
                    risk_net_usd: discounted_net_usd,
                }
            })
            .collect(),
        hard_reservation_envelope: bucket_ends
            .iter()
            .enumerate()
            .map(|(index, end_secs)| HardReservationBucket {
                end_secs: *end_secs,
                reserved_cash_usd: if index == 0 {
                    immediate_cost.cash_outlay_usd
                } else {
                    Usd::ZERO
                },
            })
            .collect(),
        economics: RecommendationEconomics {
            profit_probability_bps: Bps::new(dec!(8000)),
            nominal_expected_net_usd: Usd::new(nominal),
            robust_expected_net_usd: Usd::new(robust),
            max_loss_usd: Usd::new(dec!(5)),
            cvar_contribution_usd: Usd::ZERO,
            capital_occupancy_usd_hours: UsdHours::new(dec!(100)),
            marginal_portfolio_value_usd: Usd::ZERO,
        },
        lineage_hash: hash(&format!("tier-{identity}")),
    }
}

fn weighted_expected(cashflows: [i64; 3], weights: [i64; 3]) -> Decimal {
    cashflows
        .into_iter()
        .zip(weights)
        .map(|(cashflow, weight)| Decimal::from(cashflow * weight))
        .sum::<Decimal>()
        / dec!(10000)
}

fn scenario(index: u32, kind: PortfolioScenarioKind, label: &str) -> PortfolioScenario {
    let mut value = PortfolioScenario {
        scenario_index: index,
        kind,
        label: label.to_owned(),
        scenario_model_state_hash: hash(&format!("scenario-model-state-{index}")),
        scenario_state_hash: hash("pending-scenario"),
        market_outcomes: Vec::new(),
    };
    value.scenario_state_hash = value.recomputed_state_hash().expect("scenario state hash");
    value
}

fn distribution(id: &str, nominal: bool, weights: [u32; 3]) -> ScenarioDistribution {
    ScenarioDistribution {
        distribution_id: id.to_owned(),
        nominal,
        weights: weights
            .into_iter()
            .enumerate()
            .map(|(index, probability_bps)| ScenarioWeight {
                scenario_index: u32::try_from(index).expect("three scenarios"),
                probability_bps,
            })
            .collect(),
    }
}

fn zero_cashflows() -> Vec<ScenarioCashflow> {
    (0_u32..3)
        .map(|scenario_index| ScenarioCashflow {
            scenario_index,
            discounted_net_usd: Usd::ZERO,
        })
        .collect()
}

fn hash(label: &str) -> ContentHash {
    CanonicalDigest::content_hash_typed("quant-pivot/global-benchmark", 1, &label)
        .expect("benchmark hash")
}

fn bench_global_portfolio(criterion: &mut Criterion) {
    let fixture = BenchmarkFixture::build();
    let mut group = criterion.benchmark_group("global_portfolio_100_tiers");
    group.sample_size(10);
    group.bench_function("highs_exact_verified", |bencher| {
        bencher.iter(|| {
            GlobalPortfolioPlanner::solve_and_verify(fixture.input())
                .expect("global portfolio solve must succeed");
        });
    });
    group.finish();
}

criterion_group!(benches, bench_global_portfolio);
criterion_main!(benches);
