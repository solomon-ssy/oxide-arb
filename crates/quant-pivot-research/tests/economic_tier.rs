mod support;

use chrono::{TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        market::{book::BookLevel, fee::BuilderFeeAttribution},
        order::PolymarketOrderRules,
        quant::{
            EntryExecutionEconomics, ExecutableEconomicTier, ScenarioMarketOutcome,
            ScenarioPayoutState,
        },
    },
    enums::common::TickSize,
    types::{
        Bps, MakerRebateDelayBasis, MakerRebateProgramDayBaseline, MakerRebateValuationEvidence,
        MakerRebateValuationHealth, PortfolioScenarioArtifactId, Price, Shares, Usd,
        trade_policy::{PassiveFillDistribution, PassiveFillState, PassiveFillStateKind},
    },
};
use quant_pivot_research::{
    execution_semantics::{
        PitFeeSchedule, PitMakerRebateEvidence, PitMakerRebateSchedule, PitMarketExecutionEconomics,
    },
    portfolio::{
        EconomicTierFactory, ExecutablePassiveTierSeedFactory, ExecutablePassiveTierSeedInput,
        ExecutableTierSeed, SealedPortfolioScenarioArtifact, TierSeedBuild,
    },
};
use rust_decimal_macros::dec;

use support::GlobalFixture;

#[test]
fn outcomes_form_exact_tier() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let template = fixture.tiers[0].clone();
    assert_eq!(fixture.input().tiers.len(), 3);
    let per_share = [dec!(0.75), dec!(0.55), dec!(0.45)];
    let payout_states = [
        ScenarioPayoutState::Win,
        ScenarioPayoutState::Split,
        ScenarioPayoutState::Loss,
    ];
    let mut artifact = fixture.scenario_artifact.artifact().clone();
    let scenario_model_content_hash = artifact.scenario_model_content_hash;
    for ((scenario, exit_cash), payout_state) in artifact
        .scenarios
        .iter_mut()
        .zip(per_share)
        .zip(payout_states)
    {
        scenario.market_outcomes = vec![ScenarioMarketOutcome {
            route: template.route,
            market_id: template.market_id.clone(),
            token_id: template.token_id.clone(),
            outcome_side: template.outcome_side,
            payout_state,
            max_executable_exit_shares: Shares::new(dec!(500)),
            discounted_exit_cash_per_share_usd: Usd::new(exit_cash),
            capital_release_secs: 3_600,
            source_lineage_hash: template.lineage_hash,
            scenario_factor_lineage_hash: scenario.scenario_model_state_hash,
            outcome_lineage_hash: template.lineage_hash,
        }];
        scenario.market_outcomes[0].outcome_lineage_hash = scenario.market_outcomes[0]
            .recomputed_lineage_hash(
                scenario_model_content_hash,
                scenario.scenario_model_state_hash,
            )?;
        scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
    }
    artifact.content_hash = artifact.recomputed_hash()?;
    artifact.portfolio_scenario_artifact_id =
        PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
    let scenario_artifact = SealedPortfolioScenarioArtifact::verify(artifact)?;

    let tier = EconomicTierFactory::build(seed(&template), &scenario_artifact)?;

    assert_eq!(
        tier.scenario_cashflows[0].discounted_net_usd,
        Usd::new(dec!(98))
    );
    assert_eq!(
        tier.scenario_cashflows[1].discounted_net_usd,
        Usd::new(dec!(18))
    );
    assert_eq!(
        tier.scenario_cashflows[2].discounted_net_usd,
        Usd::new(dec!(-22))
    );
    assert_eq!(tier.economics.nominal_expected_net_usd, Usd::new(dec!(62)));
    assert_eq!(tier.economics.robust_expected_net_usd, Usd::new(dec!(38)));
    assert_eq!(tier.profit_probability_lower_bps, 7_000);
    assert_eq!(tier.probability_interval_width_bps, 2_000);
    Ok(())
}

#[test]
fn missing_leg_fails_closed() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let template = fixture.tiers[0].clone();
    let mut artifact = fixture.scenario_artifact.artifact().clone();
    for scenario in &mut artifact.scenarios {
        scenario.market_outcomes.retain(|outcome| {
            outcome.route != template.route
                || outcome.market_id != template.market_id
                || outcome.token_id != template.token_id
                || outcome.outcome_side != template.outcome_side
        });
        scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
    }
    artifact.content_hash = artifact.recomputed_hash()?;
    artifact.portfolio_scenario_artifact_id =
        PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
    let scenario_artifact = SealedPortfolioScenarioArtifact::verify(artifact)?;

    assert!(
        EconomicTierFactory::build(seed(&template), &scenario_artifact).is_err(),
        "an opaque scenario without the exact market leg must never synthesize cash flow"
    );
    Ok(())
}

#[test]
fn rebate_uses_fill_expectation() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let template = fixture.tiers[0].clone();
    let decision_at = Utc
        .timestamp_opt(1_750_000_000, 0)
        .single()
        .ok_or_else(|| QuantError::config("invalid passive fixture timestamp"))?;
    let bids = [
        BookLevel::from_decimal(Price::new(dec!(0.5)), Shares::new(dec!(1_000)))
            .map_err(|_| QuantError::config("invalid passive fixture bid"))?,
    ];
    let economics = PitMarketExecutionEconomics {
        fee_schedule: PitFeeSchedule {
            schedule_hash: template.lineage_hash,
            effective_at: decision_at,
            available_at: decision_at,
            platform_rate: dec!(0.04),
            exponent: dec!(1),
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        },
        maker_rebate_evidence: PitMakerRebateEvidence::Available {
            schedule: PitMakerRebateSchedule {
                terms_hash: template.lineage_hash,
                available_at: decision_at,
                platform_rate: dec!(0.04),
                exponent: dec!(1),
                taker_only: true,
                rebate_rate: dec!(0.20),
            },
        },
        composite_hash: template.lineage_hash,
    };
    let seed = ExecutablePassiveTierSeedFactory::build(ExecutablePassiveTierSeedInput {
        report_route_run_id: template.report_route_run_id,
        candidate_id: template.candidate_id,
        tier_ordinal: template.tier_ordinal,
        route: template.route,
        market_id: template.market_id,
        event_id: template.event_id,
        category: template.category,
        token_id: template.token_id,
        outcome_side: template.outcome_side,
        bids: &bids,
        execution_economics: &economics,
        decision_at,
        limit_price: Price::new(dec!(0.5)),
        requested_shares: Shares::new(dec!(100.009)),
        cash_budget: Usd::new(dec!(50)),
        good_til_secs: 3_600,
        fill_distribution: PassiveFillDistribution {
            sample_count: 100,
            source_evidence_hash: template.lineage_hash,
            states: vec![
                PassiveFillState {
                    kind: PassiveFillStateKind::NoFill,
                    probability_bps: 5_000,
                    fill_ratio_bps: 0,
                    fill_latency_ms: 0,
                    post_fill_markout_bps: Bps::ZERO,
                },
                PassiveFillState {
                    kind: PassiveFillStateKind::PartialFill,
                    probability_bps: 2_500,
                    fill_ratio_bps: 5_000,
                    fill_latency_ms: 1_000,
                    post_fill_markout_bps: Bps::new(dec!(-100)),
                },
                PassiveFillState {
                    kind: PassiveFillStateKind::FullFill,
                    probability_bps: 2_500,
                    fill_ratio_bps: 10_000,
                    fill_latency_ms: 2_000,
                    post_fill_markout_bps: Bps::new(dec!(-200)),
                },
            ],
        },
        maker_rebate_valuation: MakerRebateValuationEvidence {
            as_of: decision_at,
            health: MakerRebateValuationHealth::Healthy,
            program_day_baselines: vec![MakerRebateProgramDayBaseline {
                program_date: decision_at.date_naive(),
                confirmed_accrual_usd: Usd::new(dec!(0.99)),
            }],
            payout_threshold_usd: Usd::ONE,
            delay_basis: MakerRebateDelayBasis::ObservedP95 {
                lag_from_program_close_secs: 86_400,
                complete_program_days: 30,
            },
            evidence_hash: template.lineage_hash,
        },
        order_rules: PolymarketOrderRules::new(TickSize::Hundredth, Shares::new(dec!(5)))
            .map_err(|error| QuantError::config(error.to_string()))?,
        source_lineage_hash: template.lineage_hash,
    })?;
    let TierSeedBuild::Ready(seed) = seed else {
        return Err(QuantError::config("passive seed unexpectedly unavailable"));
    };
    let seed = *seed;

    let EntryExecutionEconomics::Passive(entry) = seed.entry_execution else {
        return Err(QuantError::config("passive seed changed route"));
    };
    assert_eq!(entry.requested_shares, Shares::new(dec!(100)));
    assert_eq!(entry.hard_reserved_cash_usd, Usd::new(dec!(50)));
    assert_eq!(entry.expected_filled_shares, Shares::new(dec!(37.5)));
    assert_eq!(
        entry.full_fill_maker_rebate_accrual_usd,
        Usd::new(dec!(0.2))
    );
    assert_eq!(
        entry.expected_maker_rebate_accrual_usd,
        Usd::new(dec!(0.075))
    );
    Ok(())
}

fn seed(template: &ExecutableEconomicTier) -> ExecutableTierSeed {
    ExecutableTierSeed {
        report_route_run_id: template.report_route_run_id,
        candidate_id: template.candidate_id,
        tier_ordinal: template.tier_ordinal,
        route: template.route,
        market_id: template.market_id.clone(),
        event_id: template.event_id.clone(),
        category: template.category,
        token_id: template.token_id.clone(),
        outcome_side: template.outcome_side,
        observed_exit_capacity_shares: Shares::new(dec!(500)),
        entry_execution: template.entry_execution.clone(),
        source_lineage_hash: template.lineage_hash,
    }
}
