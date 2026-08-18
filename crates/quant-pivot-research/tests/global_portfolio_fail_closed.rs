mod support;

use chrono::Duration;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::quant::{PortfolioScenarioArtifact, PortfolioScenarioVisibility},
    enums::quant::AccountSource,
    types::{PortfolioScenarioArtifactId, Price, Shares, Usd, VenuePositionSnapshot},
};
use quant_pivot_research::portfolio::{
    AccountSnapshot, GlobalPortfolioPlanner, SealedPortfolioScenarioArtifact,
    TierAdmissionRejectionCode,
};
use rust_decimal_macros::dec;

use support::GlobalFixture;

#[test]
fn artifact_mismatch_fails_closed() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.scenario_model_binding.model_content_hash = fixture.tiers[0].lineage_hash;
    let error = GlobalPortfolioPlanner::solve_and_verify(fixture.input())
        .expect_err("artifact mismatch must fail the complete solve");
    assert!(error.to_string().contains("scenario artifact"));
    Ok(())
}

#[test]
fn pit_binding_precedes_decision() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.scenario_model_binding.bound_at += Duration::seconds(1);

    let error = GlobalPortfolioPlanner::solve_and_verify(fixture.input())
        .expect_err("future point-in-time binding must fail the complete solve");

    assert!(error.to_string().contains("was not visible"));
    Ok(())
}

#[test]
fn purged_visibility_replay_only() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    let mut artifact = fixture.scenario_artifact.artifact().clone();
    artifact.visibility = PortfolioScenarioVisibility::PurgedCrossValidation {
        fit_evidence_hash: fixture.tiers[0].lineage_hash,
        test_groups_hash: fixture.tiers[1].lineage_hash,
    };
    fixture.reseal(artifact)?;

    let error = GlobalPortfolioPlanner::solve_and_verify(fixture.input())
        .expect_err("purged-CV visibility must never enter a live account solve");

    assert!(error.to_string().contains("historical replay account"));
    Ok(())
}

#[test]
fn purged_visibility_accepts_replay() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.account.source = AccountSource::HistoricalReplay;
    fixture.scenario_model_binding.bound_at += Duration::days(30);
    let mut artifact = fixture.scenario_artifact.artifact().clone();
    artifact.visibility = PortfolioScenarioVisibility::PurgedCrossValidation {
        fit_evidence_hash: fixture.tiers[0].lineage_hash,
        test_groups_hash: fixture.tiers[1].lineage_hash,
    };
    fixture.reseal(artifact)?;

    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;

    assert!(result.plan.is_some());
    Ok(())
}

#[test]
fn cap_change_keeps_contract() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.policy.tail_risk.capital_time_buckets[0]
        .max_capital_usd
        .value += dec!(1);

    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;

    assert!(result.plan.is_some());
    Ok(())
}

#[test]
fn boundary_change_fails_closed() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.policy.tail_risk.capital_time_buckets[0].end_secs = 7_200;

    let error = GlobalPortfolioPlanner::solve_and_verify(fixture.input())
        .expect_err("capital-time boundary drift must fail the complete solve");

    assert!(error.to_string().contains("capital-time boundaries"));
    Ok(())
}

#[test]
fn rejections_yield_zero_candidates() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.policy.admission.min_robust_expected_net_usd.value = rust_decimal_macros::dec!(10_000);
    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;
    assert!(result.plan.is_none());
    assert!(result.selected.is_empty());
    assert_eq!(result.rejected.len(), fixture.tiers.len());
    Ok(())
}

#[test]
fn top_n_exceeds_envelope() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    fixture.solver.max_top_n = 1;

    let error = GlobalPortfolioPlanner::solve_and_verify(fixture.input())
        .expect_err("unqualified TopN workload must fail before optimization");

    assert!(error.to_string().contains("selected_recommendations=2"));
    Ok(())
}

#[test]
fn stressed_capacity_rejects_tier() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    set_capacity(&mut fixture, 0, Shares::new(dec!(1)))?;
    let rejected_id = fixture.tiers[0].economic_tier_id;

    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;

    assert!(result.rejected.iter().any(|rejection| {
        rejection.economic_tier_id == rejected_id
            && rejection.code == TierAdmissionRejectionCode::ScenarioExitCapacity
    }));
    Ok(())
}

#[test]
fn existing_position_consumes_capacity() -> QuantResult<()> {
    let mut fixture = GlobalFixture::build()?;
    let tier = fixture.tiers[0].clone();
    set_capacity(&mut fixture, 0, Shares::new(dec!(450)))?;
    let position_value = Usd::new(dec!(50));
    fixture.account = AccountSnapshot::new(
        fixture.account.as_of,
        fixture.account.source,
        fixture.account.venue_net_liquidation_usd,
        fixture.account.capital_base_usd,
        fixture.account.available_usd,
        fixture.account.reserved_usd,
        vec![VenuePositionSnapshot {
            token_id: tier.token_id.clone(),
            market_id: tier.market_id.clone(),
            event_id: Some(tier.event_id.clone()),
            category: tier.category,
            outcome: "Yes".to_owned(),
            size: Shares::new(dec!(100)),
            avg_price: Price::new(dec!(0.5)),
            cur_price: Price::new(dec!(0.5)),
            current_value: position_value,
            redeemable: false,
        }],
    );
    fixture.existing.existing_open_capital_usd = position_value;
    fixture.existing.existing_open_recommendations = 1;

    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;

    assert!(result.rejected.iter().any(|rejection| {
        rejection.economic_tier_id == tier.economic_tier_id
            && rejection.code == TierAdmissionRejectionCode::ScenarioExitCapacity
    }));
    Ok(())
}

fn set_capacity(
    fixture: &mut GlobalFixture,
    tier_index: usize,
    capacity: Shares,
) -> QuantResult<()> {
    let tier = fixture.tiers[tier_index].clone();
    let mut artifact = fixture.scenario_artifact.artifact().clone();
    let scenario_model_content_hash = artifact.scenario_model_content_hash;
    for scenario in &mut artifact.scenarios {
        let outcome = scenario
            .market_outcomes
            .iter_mut()
            .find(|outcome| {
                outcome.route == tier.route
                    && outcome.market_id == tier.market_id
                    && outcome.token_id == tier.token_id
                    && outcome.outcome_side == tier.outcome_side
            })
            .ok_or_else(|| QuantError::config("missing scenario outcome"))?;
        outcome.max_executable_exit_shares = capacity;
        outcome.outcome_lineage_hash = outcome.recomputed_lineage_hash(
            scenario_model_content_hash,
            scenario.scenario_model_state_hash,
        )?;
        scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
    }
    fixture.reseal(artifact)
}

impl GlobalFixture {
    fn reseal(&mut self, mut artifact: PortfolioScenarioArtifact) -> QuantResult<()> {
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_artifact_id =
            PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
        self.scenario_artifact = SealedPortfolioScenarioArtifact::verify(artifact)?;
        Ok(())
    }
}
