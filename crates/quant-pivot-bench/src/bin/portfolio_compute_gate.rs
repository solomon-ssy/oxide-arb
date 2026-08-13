//! Parameterized production-path gate for report-time portfolio finance compute.

use std::{env, error::Error, str::FromStr, time::Instant};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_bench::{self as _, enforce_linux_peak_rss, peak_rss_bytes};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        market::{book::BookLevel, fee::BuilderFeeAttribution},
        quant::{
            DiscountCurvePoint, GlobalPortfolioPlan, PortfolioScenarioFitEvidence,
            PortfolioScenarioKind, PortfolioScenarioModelArtifact, PortfolioScenarioModelState,
            PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFactor,
            PortfolioScenarioRouteFitLineage, PortfolioScenarioRouteModelLineage,
            PortfolioScenarioVisibility, RepresentedRouteSet, ScenarioDistribution, ScenarioWeight,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{AccountSource, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
    types::{
        BacktestPathSetId, Bps, CalibrationArtifactId, ContentHash, EventId, MarketId,
        ModelVersionId, PayoutRatio, PortfolioPlanId, PortfolioScenarioModelArtifactId, Price,
        Probability, ReportRouteRunId, SchemaVersion, Shares, SignalCandidateId, TokenId, Usd,
        calibration::CalibratedPayoutDistribution,
    },
};
use quant_pivot_research::{
    execution_semantics::PitFeeSchedule,
    portfolio::{
        AccountSnapshot, CapitalTimeBucketContract, EconomicTierFactory,
        ExecutableTierLadderSeedFactory, ExecutableTierLadderSeedInput, ExecutableTierSeed,
        ExistingPortfolioFactory, GlobalPortfolioInput, GlobalPortfolioPlanner,
        GlobalPortfolioResult, PortfolioScenarioGenerationInput, PortfolioScenarioGenerator,
        PortfolioScenarioLegInput, VerifiedPortfolioScenarioModel,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

const ASK_LEVEL_COUNT: usize = 5;
const DEFAULT_SCENARIO_COUNT: usize = 10;
const DEFAULT_TIER_COUNT: usize = 100;
const DEFAULT_TOP_N: u32 = 20;
const DEFAULT_DEADLINE_SECS: u64 = 30;
const MAX_RSS_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;

struct GateArgs {
    tier_count: usize,
    scenario_count: usize,
    top_n: u32,
    deadline_secs: u64,
}

#[derive(Default)]
struct GateTimings {
    scenario: u128,
    ladder: u128,
    economics: u128,
    solver: u128,
}

impl GateTimings {
    fn failure(
        &self,
        args: &GateArgs,
        input_hash: ContentHash,
        phase: &'static str,
        total_micros: u128,
        error: QuantError,
    ) -> Box<dyn Error> {
        eprintln!(
            "portfolio_compute_gate status=failed phase={phase} tiers={} scenarios={} top_n={} deadline_secs={} input_hash={} scenario_micros={} ladder_micros={} economics_micros={} solver_micros={} total_micros={} error={error:?}",
            args.tier_count,
            args.scenario_count,
            args.top_n,
            args.deadline_secs,
            input_hash,
            self.scenario,
            self.ladder,
            self.economics,
            self.solver,
            total_micros,
        );
        Box::new(error)
    }
}

impl GateArgs {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let values = env::args().skip(1).collect::<Vec<_>>();
        if values.len() > 4 {
            return Err(
                "usage: portfolio_compute_gate [tiers] [scenarios] [top_n] [deadline_secs]".into(),
            );
        }
        let tier_count = parse_arg(values.first(), DEFAULT_TIER_COUNT, "tiers")?;
        let scenario_count = parse_arg(values.get(1), DEFAULT_SCENARIO_COUNT, "scenarios")?;
        let top_n = parse_arg(values.get(2), DEFAULT_TOP_N, "top_n")?;
        let deadline_secs = parse_arg(values.get(3), DEFAULT_DEADLINE_SECS, "deadline_secs")?;
        let candidate_count = tier_count.div_ceil(ASK_LEVEL_COUNT);
        let top_n_usize = usize::try_from(top_n)?;
        if tier_count == 0
            || !(3..=10_000).contains(&scenario_count)
            || top_n == 0
            || top_n_usize > candidate_count
            || !(1..=600).contains(&deadline_secs)
        {
            return Err(format!(
                "invalid workload: tiers={tier_count}, scenarios={scenario_count}, top_n={top_n}, deadline_secs={deadline_secs}, candidates={candidate_count}"
            )
            .into());
        }
        Ok(Self {
            tier_count,
            scenario_count,
            top_n,
            deadline_secs,
        })
    }
}

struct GateFixture {
    decision_at: DateTime<Utc>,
    represented_routes: RepresentedRouteSet,
    scenario_model: PortfolioScenarioModelArtifact,
    scenario_binding: PortfolioScenarioModelArtifactBinding,
    scenario_legs: Vec<PortfolioScenarioLegInput>,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    fee_schedule: PitFeeSchedule,
    policy: PortfolioConfig,
    solver: PortfolioSolverDeployConfig,
    account: AccountSnapshot,
    input_hash: ContentHash,
}

impl GateFixture {
    fn build(args: &GateArgs) -> QuantResult<Self> {
        let decision_at = DateTime::<Utc>::from_timestamp(1_750_000_000, 0)
            .ok_or_else(|| QuantError::config("portfolio gate timestamp is invalid"))?;
        let represented_routes = RepresentedRouteSet::from_routes([
            BuyModelRoute::Pooled,
            BuyModelRoute::Crypto,
            BuyModelRoute::Weather,
        ])?;
        let policy = policy(args.top_n);
        let (scenario_model, scenario_binding) = scenario_contract(
            decision_at,
            &represented_routes,
            &policy,
            args.scenario_count,
        )?;
        let candidate_count = args.tier_count.div_ceil(ASK_LEVEL_COUNT);
        let scenario_legs = (0..candidate_count)
            .map(|index| scenario_leg(index, &represented_routes))
            .collect::<QuantResult<Vec<_>>>()?;
        let bids = bid_levels();
        let asks = ask_levels();
        let fee_schedule = fee_schedule(decision_at)?;
        let solver = PortfolioSolverDeployConfig {
            deadline_secs: args.deadline_secs,
            threads: 1,
            max_tiers: u32::try_from(args.tier_count)
                .map_err(|error| QuantError::config(error.to_string()))?,
            max_scenarios: u32::try_from(args.scenario_count)
                .map_err(|error| QuantError::config(error.to_string()))?,
            max_top_n: args.top_n,
        };
        let capital = Usd::new(dec!(10000));
        let account = AccountSnapshot::new(
            decision_at,
            AccountSource::Polymarket,
            capital,
            capital,
            capital,
            Usd::ZERO,
            Vec::new(),
        );
        let input_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-compute-gate-input",
            1,
            &(
                args.tier_count,
                args.scenario_count,
                args.top_n,
                scenario_model.content_hash,
                &scenario_legs,
                &bids,
                &asks,
                &fee_schedule,
                &policy,
                &solver,
                &account,
            ),
        )?;
        Ok(Self {
            decision_at,
            represented_routes,
            scenario_model,
            scenario_binding,
            scenario_legs,
            bids,
            asks,
            fee_schedule,
            policy,
            solver,
            account,
            input_hash,
        })
    }

    fn ladder_seeds(&self, tier_count: usize) -> QuantResult<Vec<ExecutableTierSeed>> {
        let mut tiers = Vec::with_capacity(tier_count);
        for (index, leg) in self.scenario_legs.iter().enumerate() {
            let route_index = self
                .represented_routes
                .routes
                .iter()
                .position(|route| *route == leg.route)
                .ok_or_else(|| QuantError::config("scenario leg Route is not represented"))?;
            let route_run_id = ReportRouteRunId::new(Uuid::from_u128(
                10_000_u128
                    .checked_add(
                        u128::try_from(route_index)
                            .map_err(|error| QuantError::config(error.to_string()))?,
                    )
                    .ok_or_else(|| QuantError::config("route-run id overflow"))?,
            ));
            let candidate_id = SignalCandidateId::new(Uuid::from_u128(
                100_000_u128
                    .checked_add(
                        u128::try_from(index)
                            .map_err(|error| QuantError::config(error.to_string()))?,
                    )
                    .ok_or_else(|| QuantError::config("candidate id overflow"))?,
            ));
            let category = match leg.route {
                BuyModelRoute::Pooled => MarketCategory::Sports,
                BuyModelRoute::Crypto => MarketCategory::Crypto,
                BuyModelRoute::Weather => MarketCategory::Weather,
            };
            let event_id = EventId::new(format!("event-{}", index / 10));
            tiers.extend(ExecutableTierLadderSeedFactory::build(
                &ExecutableTierLadderSeedInput {
                    report_route_run_id: route_run_id,
                    candidate_id,
                    route: leg.route,
                    market_id: leg.market_id.clone(),
                    event_id,
                    category,
                    token_id: leg.token_id.clone(),
                    outcome_side: leg.outcome_side,
                    bids: &self.bids,
                    asks: &self.asks,
                    fee_schedule: &self.fee_schedule,
                    fill_at: self.decision_at,
                    limit_price: Price::new(dec!(0.09)),
                    max_notional_usd: Usd::new(dec!(100)),
                    source_lineage_hash: leg.lineage_hash,
                },
            )?);
            if tiers.len() >= tier_count {
                tiers.truncate(tier_count);
                break;
            }
        }
        if tiers.len() != tier_count {
            return Err(QuantError::config(format!(
                "portfolio gate built {} tiers, expected {tier_count}",
                tiers.len()
            )));
        }
        Ok(tiers)
    }
}

fn parse_arg<T>(value: Option<&String>, default: T, name: &str) -> Result<T, Box<dyn Error>>
where
    T: FromStr,
    T::Err: Error + 'static,
{
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("invalid {name} `{value}`: {error}").into())
    })
}

fn policy(top_n: u32) -> PortfolioConfig {
    let mut policy = PortfolioConfig::default();
    policy.budget.total_budget_usd.value = dec!(10000);
    policy.budget.cash_reserve_usd.value = Decimal::ONE;
    policy.budget.max_open_capital_usd.value = dec!(9999);
    policy.exposure_limits.max_single_recommendation_usd.value = dec!(100);
    policy.exposure_limits.max_market_exposure_usd.value = dec!(100);
    policy.exposure_limits.max_event_exposure_usd.value = dec!(9999);
    policy.exposure_limits.max_category_exposure_usd.value = dec!(9999);
    policy.exposure_limits.max_route_exposure_usd.value = dec!(9999);
    policy.exposure_limits.max_open_recommendations = top_n;
    policy.tail_risk.max_cvar_usd.value = dec!(9999);
    policy.tail_risk.max_scenario_loss_usd.value = dec!(9999);
    policy.tail_risk.max_drawdown_usd.value = dec!(9999);
    for bucket in &mut policy.tail_risk.capital_time_buckets {
        bucket.max_capital_usd.value = dec!(9999);
    }
    policy.admission.min_nominal_expected_net_usd.value = Decimal::ZERO;
    policy.admission.min_robust_expected_net_usd.value = Decimal::ZERO;
    policy.admission.min_profit_probability_bps = 0;
    policy.admission.max_probability_interval_width_bps = 10_000;
    policy.admission.liquidity_buffer_bps = 1_000;
    policy
}

fn scenario_contract(
    decision_at: DateTime<Utc>,
    routes: &RepresentedRouteSet,
    policy: &PortfolioConfig,
    scenario_count: usize,
) -> QuantResult<(
    PortfolioScenarioModelArtifact,
    PortfolioScenarioModelArtifactBinding,
)> {
    let serving_contract_digest = hash("serving-contract")?;
    let calibration_contract_digest = hash("calibration-contract")?;
    let trade_policy_contract_digest = hash("trade-policy-contract")?;
    let discount_curve = policy
        .tail_risk
        .capital_time_buckets
        .iter()
        .map(|bucket| DiscountCurvePoint {
            end_secs: bucket.end_secs,
            annualized_cost_bps: 500,
        })
        .collect::<Vec<_>>();
    let capital_time_bucket_contract_digest =
        CapitalTimeBucketContract::try_from(discount_curve.as_slice())
            .map_err(|error| QuantError::config(error.to_string()))?
            .content_hash()?;
    let states = (0..scenario_count)
        .map(|index| scenario_state(index, routes))
        .collect::<QuantResult<Vec<_>>>()?;
    let distributions = vec![
        ScenarioDistribution {
            distribution_id: "nominal_uniform".to_owned(),
            nominal: true,
            weights: nominal_weights(scenario_count)?,
        },
        ScenarioDistribution {
            distribution_id: "structural_stress".to_owned(),
            nominal: false,
            weights: stress_weights(scenario_count)?,
        },
    ];
    let route_fit_lineage = routes
        .routes
        .iter()
        .copied()
        .enumerate()
        .map(|(index, route)| {
            route_lineage(index, route, decision_at, trade_policy_contract_digest)
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let pending_hash = hash("pending-model")?;
    let mut model = PortfolioScenarioModelArtifact {
        portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId::from_content_hash(
            &pending_hash,
        ),
        schema_version: SchemaVersion::FIRST,
        as_of: decision_at,
        fit_window_start: decision_at - Duration::days(30),
        time_bucket_secs: 3_600,
        ordered_routes: routes.routes.clone(),
        route_set_digest: routes.digest,
        serving_contract_digest,
        calibration_contract_digest,
        trade_policy_contract_digest,
        capital_time_bucket_contract_digest,
        scenario_random_stream_hash: hash("scenario-random-stream")?,
        pit_residual_panel_hash: hash("pit-residual-panel")?,
        calibration_uncertainty_model_hash: hash("calibration-uncertainty")?,
        stress_catalog_hash: hash("stress-catalog")?,
        resampling_method: PortfolioScenarioResamplingMethod::StationaryBootstrap {
            expected_block_length: 8,
            scenario_horizon_buckets: 24,
        },
        route_fit_lineage,
        states,
        distributions,
        discount_curve,
        content_hash: pending_hash,
    };
    model.content_hash = model.recomputed_hash()?;
    model.portfolio_scenario_model_artifact_id =
        PortfolioScenarioModelArtifactId::from_content_hash(&model.content_hash);
    let binding = PortfolioScenarioModelArtifactBinding {
        portfolio_scenario_model_artifact_id: model.portfolio_scenario_model_artifact_id,
        ordered_routes: routes.routes.clone(),
        route_set_digest: routes.digest,
        serving_contract_digest,
        calibration_contract_digest,
        trade_policy_contract_digest,
        scenario_model_schema_version: SchemaVersion::FIRST,
        capital_time_bucket_contract_digest,
        model_content_hash: model.content_hash,
        bound_at: decision_at,
    };
    Ok((model, binding))
}

fn scenario_state(
    index: usize,
    routes: &RepresentedRouteSet,
) -> QuantResult<PortfolioScenarioModelState> {
    let scenario_index =
        u32::try_from(index).map_err(|error| QuantError::config(error.to_string()))?;
    let kind = match index % 3 {
        0 => PortfolioScenarioKind::PitBootstrap,
        1 => PortfolioScenarioKind::CalibrationUncertainty,
        _ => PortfolioScenarioKind::StructuralStress,
    };
    let systematic_quantile_bps = match kind {
        PortfolioScenarioKind::PitBootstrap => 1_000,
        PortfolioScenarioKind::CalibrationUncertainty => 5_000,
        PortfolioScenarioKind::StructuralStress => 9_000,
    };
    let split_probability_quantile_bps = match kind {
        PortfolioScenarioKind::PitBootstrap => 5_000,
        PortfolioScenarioKind::CalibrationUncertainty => 0,
        PortfolioScenarioKind::StructuralStress => 10_000,
    };
    let route_factors = routes
        .routes
        .iter()
        .copied()
        .map(|route| {
            Ok(PortfolioScenarioRouteFactor {
                route,
                systematic_quantile_bps,
                systematic_weight_bps: 7_500,
                calibrated_probability_shift_bps: 0,
                split_probability_quantile_bps,
                win_cash_recovery_bps: 10_000,
                split_cash_recovery_bps: 5_000,
                loss_cash_recovery_bps: 2_500,
                executable_share_bps: 10_000,
                capital_release_multiplier_bps: 10_000,
                factor_lineage_hash: hash(&format!("factor-{scenario_index}-{route:?}"))?,
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let pending_hash = hash("pending-state")?;
    let mut state = PortfolioScenarioModelState {
        scenario_index,
        kind,
        label: format!("{kind:?}-{scenario_index}"),
        scenario_state_hash: pending_hash,
        route_factors,
    };
    state.scenario_state_hash = state.recomputed_state_hash()?;
    Ok(state)
}

fn route_lineage(
    index: usize,
    route: BuyModelRoute,
    decision_at: DateTime<Utc>,
    trade_policy_contract_hash: ContentHash,
) -> QuantResult<PortfolioScenarioRouteFitLineage> {
    let offset = u128::try_from(index).map_err(|error| QuantError::config(error.to_string()))?;
    let evaluated_id = ModelVersionId::new(Uuid::from_u128(1_000 + offset * 10));
    let source_id = ModelVersionId::new(Uuid::from_u128(1_001 + offset * 10));
    Ok(PortfolioScenarioRouteFitLineage {
        route,
        model_lineage: PortfolioScenarioRouteModelLineage {
            evaluated_model_version_id: evaluated_id,
            evaluated_model_artifact_hash: hash(&format!("evaluated-model-{route:?}"))?,
            evaluated_serving_contract_hash: hash(&format!("evaluated-serving-{route:?}"))?,
            calibration_source_model_version_id: source_id,
            calibration_source_model_artifact_hash: hash(&format!("source-model-{route:?}"))?,
            calibration_source_serving_contract_hash: hash(&format!("source-serving-{route:?}"))?,
        },
        fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
            backtest_path_set_id: BacktestPathSetId::new(Uuid::from_u128(2_000 + offset)),
            backtest_path_set_hash: hash(&format!("path-set-{route:?}"))?,
            representative_path_index: 0,
        },
        calibration_artifact_id: CalibrationArtifactId::new(Uuid::from_u128(3_000 + offset)),
        calibration_artifact_hash: hash(&format!("calibration-{route:?}"))?,
        trade_policy_contract_hash,
        fit_window_start: decision_at - Duration::days(30),
        fit_window_end: decision_at,
    })
}

fn nominal_weights(scenario_count: usize) -> QuantResult<Vec<ScenarioWeight>> {
    let count =
        u32::try_from(scenario_count).map_err(|error| QuantError::config(error.to_string()))?;
    let quotient = 10_000 / count;
    let remainder = 10_000 % count;
    (0..scenario_count)
        .map(|index| {
            let scenario_index =
                u32::try_from(index).map_err(|error| QuantError::config(error.to_string()))?;
            Ok(ScenarioWeight {
                scenario_index,
                probability_bps: quotient + u32::from(scenario_index < remainder),
            })
        })
        .collect()
}

fn stress_weights(scenario_count: usize) -> QuantResult<Vec<ScenarioWeight>> {
    let count =
        u32::try_from(scenario_count).map_err(|error| QuantError::config(error.to_string()))?;
    let structural_count =
        u32::try_from((0..scenario_count).filter(|index| index % 3 == 2).count())
            .map_err(|error| QuantError::config(error.to_string()))?;
    let remaining = 10_000_u32
        .checked_sub(count)
        .ok_or_else(|| QuantError::config("scenario count exceeds basis-point mass"))?;
    let quotient = remaining / structural_count;
    let remainder = remaining % structural_count;
    let mut structural_offset = 0_u32;
    (0..scenario_count)
        .map(|index| {
            let scenario_index =
                u32::try_from(index).map_err(|error| QuantError::config(error.to_string()))?;
            let mut probability_bps = 1;
            if index % 3 == 2 {
                probability_bps += quotient + u32::from(structural_offset < remainder);
                structural_offset = structural_offset
                    .checked_add(1)
                    .ok_or_else(|| QuantError::config("structural state count overflow"))?;
            }
            Ok(ScenarioWeight {
                scenario_index,
                probability_bps,
            })
        })
        .collect()
}

fn scenario_leg(
    index: usize,
    routes: &RepresentedRouteSet,
) -> QuantResult<PortfolioScenarioLegInput> {
    let route = routes.routes[index % routes.routes.len()];
    Ok(PortfolioScenarioLegInput {
        route,
        market_id: MarketId::new(format!("0x{index:064x}")),
        token_id: TokenId::new((index + 1).to_string()),
        outcome_side: OutcomeSide::Yes,
        calibrated_payout_distribution: CalibratedPayoutDistribution {
            winner_take_all_win_probability: Probability::new(dec!(0.80)),
            split_probability: Probability::new(dec!(0.02)),
            split_probability_interval: (
                Probability::new(dec!(0.01)),
                Probability::new(dec!(0.04)),
            ),
            split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                .map_err(|error| QuantError::config(error.to_string()))?,
        },
        observed_exit_capacity_shares: Shares::new(dec!(400)),
        base_capital_release_secs: 3_600,
        lineage_hash: hash(&format!("scenario-leg-{index}"))?,
    })
}

fn bid_levels() -> Vec<BookLevel> {
    [dec!(0.04), dec!(0.03), dec!(0.02), dec!(0.01)]
        .into_iter()
        .map(|price| BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(dec!(100))))
        .collect()
}

fn ask_levels() -> Vec<BookLevel> {
    [dec!(0.05), dec!(0.06), dec!(0.07), dec!(0.08), dec!(0.09)]
        .into_iter()
        .map(|price| BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(dec!(10))))
        .collect()
}

fn fee_schedule(decision_at: DateTime<Utc>) -> QuantResult<PitFeeSchedule> {
    Ok(PitFeeSchedule {
        schedule_hash: hash("fee-schedule")?,
        effective_at: decision_at,
        available_at: decision_at,
        platform_rate: Decimal::ZERO,
        exponent: dec!(2),
        taker_only: true,
        builder_maker_fee_bps: Bps::ZERO,
        builder_taker_fee_bps: Bps::ZERO,
        builder_attribution: BuilderFeeAttribution::NoBuilderCode,
    })
}

fn hash(label: &str) -> QuantResult<ContentHash> {
    Ok(CanonicalDigest::content_hash_typed(
        "quant-pivot/portfolio-compute-gate",
        1,
        &label,
    )?)
}

fn validated_plan<'a>(
    args: &GateArgs,
    tier_count: usize,
    scenario_count: usize,
    result: &'a GlobalPortfolioResult,
) -> Result<&'a GlobalPortfolioPlan, Box<dyn Error>> {
    let plan = result
        .plan
        .as_ref()
        .ok_or("portfolio compute gate produced no optimized plan")?;
    let expected_selected = usize::try_from(args.top_n)?;
    if tier_count != args.tier_count
        || scenario_count != args.scenario_count
        || result.selected.len() != expected_selected
        || plan.selected_tier_ids.len() != expected_selected
        || !plan.solver.optimal
        || !plan.exact_verification.passed
        || plan.solver.lexicographic_model_build_count != 1
        || plan.solver.marginal_model_build_count != 0
        || plan.solver.marginal_solve_count != args.top_n
        || plan.solver.marginal_model_reuse_count != args.top_n
        || plan.solver.tie_break_proof_count == 0
        || plan.solver.lexicographic_solve_count
            != 4 + plan.objectives.stable_tie_break_stages + plan.solver.tie_break_proof_count
        || plan.solver.lexicographic_warm_start_count + 1 != plan.solver.lexicographic_solve_count
    {
        return Err("portfolio compute gate output contract mismatch".into());
    }
    Ok(plan)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = GateArgs::parse()?;
    let fixture = GateFixture::build(&args)?;
    let total_started = Instant::now();
    let mut timings = GateTimings::default();

    let scenario_started = Instant::now();
    let scenario_artifact = (|| -> QuantResult<_> {
        let model_contract = VerifiedPortfolioScenarioModel::verify(
            &fixture.scenario_binding,
            &fixture.scenario_model,
            &fixture.represented_routes,
        )?;
        PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
            model_contract: &model_contract,
            decision_at: fixture.decision_at,
            visibility: PortfolioScenarioVisibility::PointInTime,
            input_universe_hash: fixture.input_hash,
            legs: &fixture.scenario_legs,
        })
    })();
    timings.scenario = scenario_started.elapsed().as_micros();
    let scenario_artifact = match scenario_artifact {
        Ok(artifact) => artifact,
        Err(error) => {
            return Err(timings.failure(
                &args,
                fixture.input_hash,
                "scenario",
                total_started.elapsed().as_micros(),
                error,
            ));
        }
    };

    let ladder_started = Instant::now();
    let seeds = fixture.ladder_seeds(args.tier_count);
    timings.ladder = ladder_started.elapsed().as_micros();
    let seeds = match seeds {
        Ok(seeds) => seeds,
        Err(error) => {
            return Err(timings.failure(
                &args,
                fixture.input_hash,
                "ladder",
                total_started.elapsed().as_micros(),
                error,
            ));
        }
    };

    let economics_started = Instant::now();
    let economics = (|| -> QuantResult<_> {
        let tiers = seeds
            .into_iter()
            .map(|seed| EconomicTierFactory::build(seed, &scenario_artifact))
            .collect::<QuantResult<Vec<_>>>()?;
        let existing =
            ExistingPortfolioFactory::build(&fixture.account, Usd::ZERO, &scenario_artifact)?;
        Ok((tiers, existing))
    })();
    timings.economics = economics_started.elapsed().as_micros();
    let (tiers, existing) = match economics {
        Ok(economics) => economics,
        Err(error) => {
            return Err(timings.failure(
                &args,
                fixture.input_hash,
                "economics",
                total_started.elapsed().as_micros(),
                error,
            ));
        }
    };

    let solver_started = Instant::now();
    let result = GlobalPortfolioPlanner::solve_and_verify(GlobalPortfolioInput {
        portfolio_plan_id: PortfolioPlanId::new(Uuid::from_u128(9_000)),
        account: &fixture.account,
        existing: &existing,
        represented_routes: &fixture.represented_routes,
        scenario_model_binding: &fixture.scenario_binding,
        scenario_artifact: &scenario_artifact,
        policy: &fixture.policy,
        solver: &fixture.solver,
        tiers: &tiers,
        top_n: args.top_n,
    });
    timings.solver = solver_started.elapsed().as_micros();
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return Err(timings.failure(
                &args,
                fixture.input_hash,
                "solver",
                total_started.elapsed().as_micros(),
                error,
            ));
        }
    };
    let total_micros = total_started.elapsed().as_micros();

    let plan = validated_plan(
        &args,
        tiers.len(),
        scenario_artifact.scenarios.len(),
        &result,
    )?;
    let peak_rss = peak_rss_bytes()?;
    let total_limit_micros = u128::from(args.deadline_secs) * 1_000_000;
    if total_micros > total_limit_micros {
        return Err(format!(
            "portfolio full-path gate exceeded {}s: {total_micros}us",
            args.deadline_secs
        )
        .into());
    }
    enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, "global portfolio")?;
    let peak_rss_label = peak_rss.map_or_else(|| "unavailable".to_owned(), |rss| rss.to_string());
    println!(
        "portfolio_compute_gate status=passed tiers={} scenarios={} top_n={} deadline_secs={} candidates={} input_hash={} scenario_micros={} ladder_micros={} economics_micros={} solver_micros={} total_micros={} selected={} lexicographic_solves={} tie_stages={} tie_proof_stages={} marginal_solves={} marginal_model_reuses={} plan_hash={} peak_rss_bytes={}",
        args.tier_count,
        args.scenario_count,
        args.top_n,
        args.deadline_secs,
        fixture.scenario_legs.len(),
        fixture.input_hash,
        timings.scenario,
        timings.ladder,
        timings.economics,
        timings.solver,
        total_micros,
        result.selected.len(),
        plan.solver.lexicographic_solve_count,
        plan.objectives.stable_tie_break_stages,
        plan.solver.tie_break_proof_count,
        plan.solver.marginal_solve_count,
        plan.solver.marginal_model_reuse_count,
        plan.content_hash,
        peak_rss_label,
    );
    Ok(())
}
