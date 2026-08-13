mod support;

use std::collections::HashSet;

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::runtime_config::BuyModelRoute;
use quant_pivot_research::portfolio::GlobalPortfolioPlanner;

use support::GlobalFixture;

#[test]
fn mixed_routes_share_plan() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;
    let routes = result
        .selected
        .iter()
        .map(|selected| selected.tier.route)
        .collect::<HashSet<_>>();
    assert_eq!(
        routes,
        HashSet::from([BuyModelRoute::Crypto, BuyModelRoute::Weather])
    );
    assert_eq!(result.selected.len(), 2);
    let plan = result
        .plan
        .ok_or_else(|| QuantError::config("expected a global plan"))?;
    assert!(plan.solver.optimal);
    assert_eq!(plan.solver.backend, "highs");
    assert!(plan.solver.tie_break_proof_count > 0);
    assert_eq!(
        plan.solver.lexicographic_solve_count,
        4 + plan.objectives.stable_tie_break_stages + plan.solver.tie_break_proof_count
    );
    assert_eq!(
        plan.solver.lexicographic_warm_start_count + 1,
        plan.solver.lexicographic_solve_count
    );
    assert_eq!(plan.solver.lexicographic_model_build_count, 1);
    assert_eq!(plan.solver.marginal_model_build_count, 0);
    assert_eq!(plan.solver.marginal_solve_count, 2);
    assert_eq!(plan.solver.marginal_model_reuse_count, 2);
    Ok(())
}

#[test]
fn exact_milp_matches_bruteforce() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let result = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?;
    let selected = result
        .selected
        .iter()
        .map(|selected| selected.tier.candidate_id)
        .collect::<HashSet<_>>();
    let expected = fixture.tiers[0..2]
        .iter()
        .map(|tier| tier.candidate_id)
        .collect::<HashSet<_>>();
    assert_eq!(selected, expected);
    Ok(())
}

#[test]
fn input_order_preserves_hash() -> QuantResult<()> {
    let fixture = GlobalFixture::build()?;
    let forward = GlobalPortfolioPlanner::solve_and_verify(fixture.input())?
        .plan
        .ok_or_else(|| QuantError::config("expected forward plan"))?;
    let mut reversed = GlobalFixture::build()?;
    reversed.tiers.reverse();
    let reverse = GlobalPortfolioPlanner::solve_and_verify(reversed.input())?
        .plan
        .ok_or_else(|| QuantError::config("expected reverse plan"))?;
    assert_eq!(forward.content_hash, reverse.content_hash);
    assert_eq!(forward.selected_tier_ids, reverse.selected_tier_ids);
    Ok(())
}
