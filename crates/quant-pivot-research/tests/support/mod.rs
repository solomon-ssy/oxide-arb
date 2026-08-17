use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult};
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
            ScenarioExecutionCashflow, ScenarioMarketOutcome, ScenarioPayoutState, ScenarioWeight,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{AccountSource, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
    types::{
        Bps, ContentHash, EconomicTierId, EventId, MakerRebateScenarioCreditStatus, MarketId,
        PortfolioPlanId, PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId, Price,
        ReportRouteRunId, SchemaVersion, Shares, SignalCandidateId, TokenId, Usd, UsdHours,
    },
};
use quant_pivot_research::portfolio::{
    AccountSnapshot, CapitalTimeBucketContract, GlobalPortfolioInput,
    SealedPortfolioScenarioArtifact,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

pub struct GlobalFixture {
    pub portfolio_plan_id: PortfolioPlanId,
    pub account: AccountSnapshot,
    pub existing: ExistingPortfolioState,
    pub represented_routes: RepresentedRouteSet,
    pub scenario_model_binding: PortfolioScenarioModelArtifactBinding,
    pub scenario_artifact: SealedPortfolioScenarioArtifact,
    pub policy: PortfolioConfig,
    pub solver: PortfolioSolverDeployConfig,
    pub tiers: Vec<ExecutableEconomicTier>,
    pub top_n: u32,
}

struct ScenarioFixture {
    artifact: SealedPortfolioScenarioArtifact,
    binding: PortfolioScenarioModelArtifactBinding,
}

impl ScenarioFixture {
    fn build(
        decision_at: DateTime<Utc>,
        represented_routes: &RepresentedRouteSet,
        policy: &PortfolioConfig,
        tiers: &[ExecutableEconomicTier],
    ) -> QuantResult<Self> {
        let capital_time_bucket_contract_digest =
            CapitalTimeBucketContract::try_from(policy.tail_risk.capital_time_buckets.as_slice())
                .map_err(|error| QuantError::config(error.to_string()))?
                .content_hash()?;
        let serving_contract_digest = hash("serving-contract")?;
        let calibration_contract_digest = hash("calibration-contract")?;
        let recommendation_contract_digest = hash("recommendation-contract")?;
        let scenario_model_content_hash = hash("scenario-model")?;
        let scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&scenario_model_content_hash);
        let mut artifact = PortfolioScenarioArtifact {
            portfolio_scenario_artifact_id: PortfolioScenarioArtifactId::new(Uuid::from_u128(1)),
            portfolio_scenario_model_artifact_id: scenario_model_artifact_id,
            scenario_model_content_hash,
            schema_version: SchemaVersion::FIRST,
            decision_at,
            visibility: PortfolioScenarioVisibility::PointInTime,
            input_universe_hash: hash("input-universe")?,
            ordered_routes: represented_routes.routes.clone(),
            route_set_digest: represented_routes.digest,
            serving_contract_digest,
            calibration_contract_digest,
            recommendation_contract_digest,
            evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
            capital_time_bucket_contract_digest,
            scenarios: vec![
                scenario(0, PortfolioScenarioKind::PitBootstrap, "pit_bootstrap")?,
                scenario(
                    1,
                    PortfolioScenarioKind::CalibrationUncertainty,
                    "calibration_uncertainty",
                )?,
                scenario(
                    2,
                    PortfolioScenarioKind::StructuralStress,
                    "structural_stress",
                )?,
            ],
            distributions: vec![
                distribution("nominal", true, [6_000, 3_000, 1_000])?,
                distribution("robust", false, [4_000, 3_000, 3_000])?,
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
            content_hash: hash("pending-artifact")?,
        };
        for scenario in &mut artifact.scenarios {
            let payout_state = match scenario.kind {
                PortfolioScenarioKind::PitBootstrap => ScenarioPayoutState::Win,
                PortfolioScenarioKind::CalibrationUncertainty => ScenarioPayoutState::Split,
                PortfolioScenarioKind::StructuralStress => ScenarioPayoutState::Loss,
            };
            let mut outcomes = tiers
                .iter()
                .map(|tier| {
                    Ok(ScenarioMarketOutcome {
                        route: tier.route,
                        market_id: tier.market_id.clone(),
                        token_id: tier.token_id.clone(),
                        outcome_side: tier.outcome_side,
                        payout_state,
                        max_executable_exit_shares: Shares::new(dec!(10_000)),
                        discounted_exit_cash_per_share_usd: Usd::ONE,
                        capital_release_secs: 3_600,
                        source_lineage_hash: tier.lineage_hash,
                        scenario_factor_lineage_hash: scenario.scenario_model_state_hash,
                        outcome_lineage_hash: hash("pending-outcome")?,
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?;
            outcomes.sort_by(|left, right| {
                (
                    left.route,
                    left.market_id.as_str(),
                    left.token_id.as_str(),
                    left.outcome_side.as_str(),
                )
                    .cmp(&(
                        right.route,
                        right.market_id.as_str(),
                        right.token_id.as_str(),
                        right.outcome_side.as_str(),
                    ))
            });
            for outcome in &mut outcomes {
                outcome.outcome_lineage_hash = outcome.recomputed_lineage_hash(
                    scenario_model_content_hash,
                    scenario.scenario_model_state_hash,
                )?;
            }
            scenario.market_outcomes = outcomes;
            scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
        }
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_artifact_id =
            PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
        let binding = PortfolioScenarioModelArtifactBinding {
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
        Ok(Self {
            artifact: SealedPortfolioScenarioArtifact::verify(artifact)?,
            binding,
        })
    }
}

impl GlobalFixture {
    pub fn build() -> QuantResult<Self> {
        let decision_at = Utc
            .timestamp_opt(1_750_000_000, 0)
            .single()
            .ok_or_else(|| QuantError::config("invalid global fixture timestamp"))?;
        let represented_routes = RepresentedRouteSet::from_routes([
            BuyModelRoute::Pooled,
            BuyModelRoute::Crypto,
            BuyModelRoute::Weather,
        ])?;
        let mut policy = PortfolioConfig::default();
        policy.budget.total_budget_usd.value = dec!(404);
        policy.budget.cash_reserve_usd.value = Decimal::ZERO;
        policy.budget.max_open_capital_usd.value = dec!(404);
        policy.exposure_limits.max_single_recommendation_usd.value = dec!(202);
        policy.exposure_limits.max_market_exposure_usd.value = dec!(202);
        policy.exposure_limits.max_event_exposure_usd.value = dec!(404);
        policy.exposure_limits.max_category_exposure_usd.value = dec!(404);
        policy.exposure_limits.max_route_exposure_usd.value = dec!(404);
        policy.exposure_limits.max_open_recommendations = 2;
        policy.tail_risk.max_cvar_usd.value = dec!(400);
        policy.tail_risk.max_scenario_loss_usd.value = dec!(400);
        policy.tail_risk.max_drawdown_usd.value = dec!(400);
        policy.admission.min_nominal_expected_net_usd.value = Decimal::ZERO;
        policy.admission.min_robust_expected_net_usd.value = Decimal::ZERO;
        policy.admission.min_profit_probability_bps = 5_000;
        policy.admission.max_probability_interval_width_bps = 2_000;
        policy.admission.liquidity_buffer_bps = 1_000;

        let (bucket_ends, tiers) = fixture_tiers(&policy)?;
        let scenario_fixture =
            ScenarioFixture::build(decision_at, &represented_routes, &policy, &tiers)?;
        Ok(Self {
            portfolio_plan_id: PortfolioPlanId::new(Uuid::from_u128(200)),
            account: AccountSnapshot::new(
                decision_at,
                AccountSource::Polymarket,
                Usd::new(dec!(404)),
                Usd::new(dec!(404)),
                Usd::new(dec!(404)),
                Usd::ZERO,
                Vec::new(),
            ),
            existing: ExistingPortfolioState {
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
            },
            represented_routes,
            scenario_model_binding: scenario_fixture.binding,
            scenario_artifact: scenario_fixture.artifact,
            policy,
            solver: PortfolioSolverDeployConfig {
                deadline_secs: 10,
                threads: 1,
                max_tiers: 100,
                max_scenarios: 100,
                max_top_n: 2,
            },
            tiers,
            top_n: 2,
        })
    }

    pub fn input(&self) -> GlobalPortfolioInput<'_> {
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
            top_n: self.top_n,
        }
    }
}

#[derive(Clone, Copy)]
struct TierFixture<'a> {
    identity: u128,
    route: BuyModelRoute,
    category: MarketCategory,
    market: &'a str,
    event: &'a str,
    token: &'a str,
    cashflows: [i64; 3],
    nominal: i64,
    robust: i64,
    max_loss: i64,
    profit_bps: i64,
}

fn fixture_tiers(policy: &PortfolioConfig) -> QuantResult<(Vec<u64>, Vec<ExecutableEconomicTier>)> {
    let bucket_ends = policy
        .tail_risk
        .capital_time_buckets
        .iter()
        .map(|bucket| bucket.end_secs)
        .collect::<Vec<_>>();
    let tiers = vec![
        tier(
            TierFixture {
                identity: 101,
                route: BuyModelRoute::Crypto,
                category: MarketCategory::Crypto,
                market: "crypto-market",
                event: "crypto-event",
                token: "crypto-yes",
                cashflows: [100, 20, -20],
                nominal: 64,
                robust: 40,
                max_loss: 20,
                profit_bps: 9_000,
            },
            &bucket_ends,
        )?,
        tier(
            TierFixture {
                identity: 102,
                route: BuyModelRoute::Weather,
                category: MarketCategory::Weather,
                market: "weather-market",
                event: "weather-event",
                token: "weather-yes",
                cashflows: [70, 40, -10],
                nominal: 53,
                robust: 37,
                max_loss: 10,
                profit_bps: 9_000,
            },
            &bucket_ends,
        )?,
        tier(
            TierFixture {
                identity: 103,
                route: BuyModelRoute::Pooled,
                category: MarketCategory::Sports,
                market: "sports-market",
                event: "sports-event",
                token: "sports-yes",
                cashflows: [200, -100, -100],
                nominal: 80,
                robust: 20,
                max_loss: 100,
                profit_bps: 6_000,
            },
            &bucket_ends,
        )?,
    ];
    Ok((bucket_ends, tiers))
}

fn tier(fixture: TierFixture<'_>, bucket_ends: &[u64]) -> QuantResult<ExecutableEconomicTier> {
    let identity = Uuid::from_u128(fixture.identity);
    let filled_shares = Shares::new(dec!(400));
    let immediate_cost =
        ImmediateExecutionCost::new(Usd::new(dec!(200)), Usd::new(dec!(2)), Usd::ZERO)
            .map_err(QuantError::config)?;
    let occupancy_secs = bucket_ends
        .first()
        .copied()
        .ok_or_else(|| QuantError::config("fixture requires at least one capital bucket"))?;
    Ok(ExecutableEconomicTier {
        economic_tier_id: EconomicTierId::new(identity),
        report_route_run_id: ReportRouteRunId::new(identity),
        candidate_id: SignalCandidateId::new(identity),
        tier_ordinal: 1,
        route: fixture.route,
        market_id: MarketId::new(fixture.market),
        event_id: EventId::new(fixture.event),
        category: fixture.category,
        token_id: TokenId::new(fixture.token),
        outcome_side: OutcomeSide::Yes,
        entry_execution: EntryExecutionEconomics::Aggressive(AggressiveEntryEconomics {
            requested_shares: filled_shares,
            filled_shares,
            limit_price: Price::new(dec!(0.5)),
            execution_vwap: Price::new(dec!(0.5)),
            immediate_cost,
            slippage_usd: Usd::new(dec!(1)),
            visible_liquidity_usd: Usd::new(dec!(10_000)),
        }),
        profit_probability_lower_bps: u32::try_from(fixture.profit_bps.saturating_sub(500))
            .map_err(|error| QuantError::config(error.to_string()))?,
        probability_interval_width_bps: 1_000,
        scenario_cashflows: fixture
            .cashflows
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let discounted_net_usd = Usd::new(Decimal::from(*value));
                Ok(ScenarioExecutionCashflow {
                    scenario_index: u32::try_from(index)
                        .map_err(|error| QuantError::config(error.to_string()))?,
                    entry_execution: ScenarioEntryExecution::AggressiveFill,
                    filled_shares,
                    immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
                    discounted_exit_cash_usd: immediate_cost.cash_outlay_usd + discounted_net_usd,
                    maker_rebate_accrual_usd: Usd::ZERO,
                    objective_maker_rebate_usd: Usd::ZERO,
                    maker_rebate_program_date: None,
                    maker_rebate_program_day_baseline_usd: Usd::ZERO,
                    maker_rebate_program_day_total_usd: Usd::ZERO,
                    maker_rebate_credit_status: MakerRebateScenarioCreditStatus::NotApplicable,
                    maker_rebate_expected_by: None,
                    capital_cost_usd: Usd::ZERO,
                    capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                        locked_cash_usd: immediate_cost.cash_outlay_usd,
                        duration_secs: occupancy_secs,
                    }],
                    discounted_net_usd,
                    risk_net_usd: discounted_net_usd,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?,
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
            profit_probability_bps: Bps::new(Decimal::from(fixture.profit_bps)),
            nominal_expected_net_usd: Usd::new(Decimal::from(fixture.nominal)),
            robust_expected_net_usd: Usd::new(Decimal::from(fixture.robust)),
            max_loss_usd: Usd::new(Decimal::from(fixture.max_loss)),
            cvar_contribution_usd: Usd::ZERO,
            capital_occupancy_usd_hours: UsdHours::new(immediate_cost.cash_outlay_usd.inner()),
            marginal_portfolio_value_usd: Usd::ZERO,
        },
        lineage_hash: hash(fixture.market)?,
    })
}

fn scenario(
    scenario_index: u32,
    kind: PortfolioScenarioKind,
    label: &str,
) -> QuantResult<PortfolioScenario> {
    let mut scenario = PortfolioScenario {
        scenario_index,
        kind,
        label: label.to_owned(),
        scenario_model_state_hash: hash(&format!("scenario-model-state-{scenario_index}"))?,
        scenario_state_hash: hash("pending-scenario-state")?,
        market_outcomes: Vec::new(),
    };
    scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
    Ok(scenario)
}

fn distribution(
    distribution_id: &str,
    nominal: bool,
    weights: [u32; 3],
) -> QuantResult<ScenarioDistribution> {
    Ok(ScenarioDistribution {
        distribution_id: distribution_id.to_owned(),
        nominal,
        weights: weights
            .into_iter()
            .enumerate()
            .map(|(index, probability_bps)| {
                Ok(ScenarioWeight {
                    scenario_index: u32::try_from(index)
                        .map_err(|error| QuantError::config(error.to_string()))?,
                    probability_bps,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?,
    })
}

fn zero_cashflows() -> Vec<ScenarioCashflow> {
    (0_u32..3)
        .map(|scenario_index| ScenarioCashflow {
            scenario_index,
            discounted_net_usd: Usd::ZERO,
        })
        .collect()
}

fn hash(label: &str) -> QuantResult<ContentHash> {
    Ok(CanonicalDigest::content_hash_typed(
        "quant-pivot/global-test-fixture",
        1,
        &label,
    )?)
}
