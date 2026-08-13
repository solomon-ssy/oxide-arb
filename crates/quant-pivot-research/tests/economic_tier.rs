mod support;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::{ExecutableEconomicTier, ScenarioMarketOutcome, ScenarioPayoutState},
    types::{PortfolioScenarioArtifactId, Shares, Usd},
};
use quant_pivot_research::portfolio::{
    EconomicTierFactory, ExecutableTierSeed, SealedPortfolioScenarioArtifact,
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
        Usd::new(dec!(100))
    );
    assert_eq!(
        tier.scenario_cashflows[1].discounted_net_usd,
        Usd::new(dec!(20))
    );
    assert_eq!(
        tier.scenario_cashflows[2].discounted_net_usd,
        Usd::new(dec!(-20))
    );
    assert_eq!(tier.economics.nominal_expected_net_usd, Usd::new(dec!(64)));
    assert_eq!(tier.economics.robust_expected_net_usd, Usd::new(dec!(40)));
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
        shares: template.shares,
        observed_exit_capacity_shares: Shares::new(dec!(500)),
        entry: template.entry.clone(),
        source_lineage_hash: template.lineage_hash,
    }
}
