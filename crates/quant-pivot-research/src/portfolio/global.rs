//! Exact preparation, admission, ranking, and Decimal verification for the global MILP.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    time::{Duration, Instant},
};

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::quant::{
        CapitalOccupancyBucket, ExactVerificationEvidence, ExecutableEconomicTier,
        ExistingPortfolioState, GlobalPortfolioPlan, PortfolioConstraintEvidence,
        PortfolioObjectiveEvidence, PortfolioScenarioArtifact, PortfolioScenarioVisibility,
        RepresentedRouteSet, ScenarioCashflow, ScenarioWeight, SolverEvidence,
    },
    enums::quant::{AccountSource, OutcomeSide},
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioConfig, PortfolioScenarioModelArtifactBinding},
    types::{
        ContentHash, EconomicTierId, PortfolioPlanId, PortfolioRejectionReason, Usd, UsdHours,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;

use super::{
    AccountSnapshot, CapitalTimeBucketContract, SealedPortfolioScenarioArtifact,
    solver_boundary::{self, SolvedGlobal, SolvedMarginals},
};

pub(super) const SOLVER_COEFFICIENT_SCALE: i64 = 1_000_000;
const DISTRIBUTION_MASS_BPS: i64 = 10_000;
const MAX_EXACT_F64_INTEGER: u64 = 9_007_199_254_740_991;
const PLAN_DIGEST_DOMAIN: &str = "quant-pivot/global-portfolio-plan";

/// Complete frozen input for the unique production/backtest/replay solve path.
#[derive(Clone, Copy)]
pub struct GlobalPortfolioInput<'a> {
    pub portfolio_plan_id: PortfolioPlanId,
    pub account: &'a AccountSnapshot,
    pub existing: &'a ExistingPortfolioState,
    pub represented_routes: &'a RepresentedRouteSet,
    pub scenario_model_binding: &'a PortfolioScenarioModelArtifactBinding,
    pub scenario_artifact: &'a SealedPortfolioScenarioArtifact,
    pub policy: &'a PortfolioConfig,
    pub solver: &'a PortfolioSolverDeployConfig,
    pub tiers: &'a [ExecutableEconomicTier],
    pub top_n: u32,
}

/// Stable reason why an otherwise valid tier did not enter the MILP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierAdmissionRejectionCode {
    ScenarioExitCapacity,
    NominalExpectedNetFloor,
    RobustExpectedNetFloor,
    ProfitProbabilityFloor,
    ProbabilityIntervalWidth,
    LiquidityBuffer,
    SingleRecommendationExposure,
    ExistingStructuralConflict,
}

impl From<TierAdmissionRejectionCode> for PortfolioRejectionReason {
    fn from(code: TierAdmissionRejectionCode) -> Self {
        match code {
            TierAdmissionRejectionCode::ScenarioExitCapacity => Self::ScenarioExitCapacity,
            TierAdmissionRejectionCode::NominalExpectedNetFloor => Self::NominalExpectedNetFloor,
            TierAdmissionRejectionCode::RobustExpectedNetFloor => Self::RobustExpectedNetFloor,
            TierAdmissionRejectionCode::ProfitProbabilityFloor => Self::ProfitProbabilityFloor,
            TierAdmissionRejectionCode::ProbabilityIntervalWidth => Self::ProbabilityIntervalWidth,
            TierAdmissionRejectionCode::LiquidityBuffer => Self::LiquidityBuffer,
            TierAdmissionRejectionCode::SingleRecommendationExposure => {
                Self::SingleRecommendationExposure
            }
            TierAdmissionRejectionCode::ExistingStructuralConflict => {
                Self::ExistingStructuralConflict
            }
        }
    }
}

/// Funnel evidence for one rejected executable tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierAdmissionRejection {
    pub economic_tier_id: EconomicTierId,
    pub code: TierAdmissionRejectionCode,
}

/// Selected tier with economics updated by the global leave-one-out and `CVaR` solves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedEconomicTier {
    pub tier: ExecutableEconomicTier,
}

struct RankedSelection {
    tiers: Vec<PlannedEconomicTier>,
    marginal_model_build_count: u32,
    marginal_solve_count: u32,
    marginal_model_reuse_count: u32,
}

/// Successful optimizer output. Failure never returns a partial plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalPortfolioResult {
    pub plan: Option<GlobalPortfolioPlan>,
    pub selected: Vec<PlannedEconomicTier>,
    pub rejected: Vec<TierAdmissionRejection>,
}

/// Exact portfolio selection used by PIT/CPCV economic replay.
///
/// Replay consumes the selected executable tiers and their realized cashflows,
/// but it does not publish recommendation ordering or marginal-contribution
/// explanations. Those explanations require one additional leave-one-out MILP
/// per selected candidate and cannot change the already verified global
/// optimum. Keeping this result distinct from [`GlobalPortfolioResult`]
/// prevents validation code from accidentally presenting incomplete economics
/// as a publishable report plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPortfolioSelection {
    pub selected: Vec<ExecutableEconomicTier>,
    pub rejected: Vec<TierAdmissionRejection>,
}

#[derive(Serialize)]
struct PlanPreimage<'a> {
    portfolio_plan_id: PortfolioPlanId,
    selected_tier_ids: &'a [EconomicTierId],
    objectives: &'a PortfolioObjectiveEvidence,
    constraints: &'a PortfolioConstraintEvidence,
    solver: &'a SolverEvidence,
    exact_verification: &'a ExactVerificationEvidence,
}

#[derive(Serialize)]
struct VerificationPreimage<'a> {
    selected: &'a [usize],
    new_notional: i64,
    open_capital: i64,
    maximum_scenario_loss: i64,
    checked: u32,
}

/// Unique global portfolio planner.
pub struct GlobalPortfolioPlanner;

impl GlobalPortfolioPlanner {
    /// Validate artifacts, admit tiers, solve every lexicographic stage, and exact-verify.
    pub fn solve_and_verify(input: GlobalPortfolioInput<'_>) -> QuantResult<GlobalPortfolioResult> {
        match VerifiedGlobalSolve::run(&input, GlobalSolvePurpose::Publishable)? {
            VerifiedGlobalSolve::NoAdmittedTiers { rejected } => Ok(GlobalPortfolioResult {
                plan: None,
                selected: Vec::new(),
                rejected,
            }),
            VerifiedGlobalSolve::Optimal {
                prepared,
                rejected,
                solved,
                verification,
                objectives,
                marginals,
            } => {
                let marginals = marginals.ok_or_else(|| ReportError::PortfolioPostCheck {
                    detail: "publishable portfolio solve omitted marginal evidence".to_owned(),
                })?;
                let selected =
                    rank_selected(&input, &prepared, &solved.selected, &objectives, &marginals)?;
                let plan = build_plan(
                    &input,
                    &prepared,
                    &selected,
                    &verification,
                    &objectives,
                    &solved,
                )?;
                Ok(GlobalPortfolioResult {
                    plan: Some(plan),
                    selected: selected.tiers,
                    rejected,
                })
            }
        }
    }

    /// Solve the identical lexicographic MILP and exact post-check used by a
    /// publishable report, returning only the economic selection needed by a
    /// historical replay.
    ///
    /// Leave-one-out marginal ranking is deliberately absent because it is an
    /// explanatory post-processing step: it cannot alter selection, executed
    /// cashflows, allocation weights, or portfolio returns. Callers cannot
    /// obtain a [`GlobalPortfolioPlan`] from this boundary.
    pub(crate) fn solve_replay_and_verify(
        input: GlobalPortfolioInput<'_>,
    ) -> QuantResult<VerifiedPortfolioSelection> {
        match VerifiedGlobalSolve::run(&input, GlobalSolvePurpose::Replay)? {
            VerifiedGlobalSolve::NoAdmittedTiers { rejected } => Ok(VerifiedPortfolioSelection {
                selected: Vec::new(),
                rejected,
            }),
            VerifiedGlobalSolve::Optimal {
                prepared,
                rejected,
                solved,
                ..
            } => {
                let selected = solved
                    .selected
                    .iter()
                    .map(|index| input.tiers[prepared.tiers[*index].source_index].clone())
                    .collect();
                Ok(VerifiedPortfolioSelection { selected, rejected })
            }
        }
    }
}

enum VerifiedGlobalSolve {
    NoAdmittedTiers {
        rejected: Vec<TierAdmissionRejection>,
    },
    Optimal {
        prepared: Box<PreparedGlobalModel>,
        rejected: Vec<TierAdmissionRejection>,
        solved: SolvedGlobal,
        verification: VerificationSummary,
        objectives: ExactObjectives,
        marginals: Option<SolvedMarginals>,
    },
}

#[derive(Clone, Copy)]
enum GlobalSolvePurpose {
    Publishable,
    Replay,
}

impl VerifiedGlobalSolve {
    fn run(input: &GlobalPortfolioInput<'_>, purpose: GlobalSolvePurpose) -> QuantResult<Self> {
        let started = Instant::now();
        validate_artifact_contract(input)?;
        let (prepared, rejected) = PreparedGlobalModel::new(input)?;
        if prepared.tiers.is_empty() {
            return Ok(Self::NoAdmittedTiers { rejected });
        }

        let deadline = started
            .checked_add(Duration::from_secs(input.solver.deadline_secs))
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: "deadline",
                detail: "solver deadline overflow".to_owned(),
            })?;
        let (solved, marginals) = match purpose {
            GlobalSolvePurpose::Publishable => {
                let (solved, marginals) =
                    solver_boundary::solve_publishable(&prepared, deadline, input.solver)?;
                (solved, Some(marginals))
            }
            GlobalSolvePurpose::Replay => (
                solver_boundary::solve_lexicographic(&prepared, deadline, input.solver)?,
                None,
            ),
        };
        let verification = prepared.verify(&solved.selected)?;
        let objectives = prepared.objectives(&solved.selected)?;
        Ok(Self::Optimal {
            prepared: Box::new(prepared),
            rejected,
            solved,
            verification,
            objectives,
            marginals,
        })
    }
}

fn rank_selected(
    input: &GlobalPortfolioInput<'_>,
    prepared: &PreparedGlobalModel,
    selected_indices: &[usize],
    full: &ExactObjectives,
    marginals: &SolvedMarginals,
) -> QuantResult<RankedSelection> {
    if marginals.selections.len() != selected_indices.len()
        || usize::try_from(marginals.solve_count).ok() != Some(selected_indices.len())
    {
        return Err(ReportError::PortfolioPostCheck {
            detail: "marginal solve evidence differs from the selected portfolio width".to_owned(),
        }
        .into());
    }
    let mut selected = Vec::with_capacity(selected_indices.len());
    for (&index, leave_out_selected) in selected_indices.iter().zip(&marginals.selections) {
        let leave_out_objectives = prepared.objectives(leave_out_selected)?;
        let marginal_numerator = full
            .robust_numerator
            .checked_sub(leave_out_objectives.robust_numerator)
            .ok_or_else(|| ReportError::PortfolioPostCheck {
                detail: "leave-one-out robust optimum exceeded the full optimum".to_owned(),
            })?;
        let without_selected = selected_indices
            .iter()
            .copied()
            .filter(|candidate_index| *candidate_index != index)
            .collect::<Vec<_>>();
        let without_objectives = prepared.objectives(&without_selected)?;
        let cvar_contribution = full
            .cvar_numerator
            .saturating_sub(without_objectives.cvar_numerator);
        let mut tier = input.tiers[prepared.tiers[index].source_index].clone();
        let mut economics = tier.economics;
        economics.marginal_portfolio_value_usd = Usd::new(weighted_micro_to_decimal(
            marginal_numerator,
            DISTRIBUTION_MASS_BPS,
            "marginal portfolio value",
        )?);
        economics.cvar_contribution_usd = Usd::new(weighted_micro_to_decimal(
            cvar_contribution,
            prepared.tail_mass_bps,
            "CVaR contribution",
        )?);
        tier.economics = economics;
        selected.push(PlannedEconomicTier { tier });
    }
    selected.sort_by(|left, right| {
        Reverse(left.tier.economics.marginal_portfolio_value_usd)
            .cmp(&Reverse(right.tier.economics.marginal_portfolio_value_usd))
            .then_with(|| {
                Reverse(left.tier.economics.robust_expected_net_usd)
                    .cmp(&Reverse(right.tier.economics.robust_expected_net_usd))
            })
            .then_with(|| {
                Reverse(left.tier.economics.nominal_expected_net_usd)
                    .cmp(&Reverse(right.tier.economics.nominal_expected_net_usd))
            })
            .then_with(|| stable_tier_key(&left.tier).cmp(&stable_tier_key(&right.tier)))
    });
    Ok(RankedSelection {
        marginal_model_build_count: marginals.model_build_count,
        marginal_solve_count: marginals.solve_count,
        marginal_model_reuse_count: marginals.model_reuse_count,
        tiers: selected,
    })
}

fn build_plan(
    input: &GlobalPortfolioInput<'_>,
    prepared: &PreparedGlobalModel,
    selected: &RankedSelection,
    verification: &VerificationSummary,
    full: &ExactObjectives,
    solved: &SolvedGlobal,
) -> QuantResult<GlobalPortfolioPlan> {
    let expected_warm_starts =
        solved
            .lexicographic_solve_count
            .checked_sub(1)
            .ok_or_else(|| ReportError::PortfolioPostCheck {
                detail: "portfolio optimizer published zero lexicographic solve stages".to_owned(),
            })?;
    if solved.lexicographic_model_build_count != 1
        || solved.lexicographic_warm_start_count != expected_warm_starts
        || selected.marginal_model_build_count != 0
        || selected.marginal_solve_count
            != u32::try_from(selected.tiers.len()).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "marginal_solve_count",
                    detail: error.to_string(),
                }
            })?
        || selected.marginal_model_reuse_count != selected.marginal_solve_count
    {
        return Err(ReportError::PortfolioPostCheck {
            detail: format!(
                "portfolio optimizer evidence violates persistent-model reuse: lexicographic_builds={}, lexicographic_solves={}, lexicographic_warm_starts={}, marginal_builds={}, marginal_solves={}, marginal_reuses={}",
                solved.lexicographic_model_build_count,
                solved.lexicographic_solve_count,
                solved.lexicographic_warm_start_count,
                selected.marginal_model_build_count,
                selected.marginal_solve_count,
                selected.marginal_model_reuse_count,
            ),
        }
        .into());
    }
    let selected_tier_ids = selected
        .tiers
        .iter()
        .map(|planned| planned.tier.economic_tier_id)
        .collect::<Vec<_>>();
    let selected_tier_digest = CanonicalDigest::content_hash_typed(
        "quant-pivot/selected-economic-tiers",
        1,
        &selected_tier_ids,
    )?;
    let recomputed_economics_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/recomputed-recommendation-economics",
        1,
        &selected
            .tiers
            .iter()
            .map(|item| item.tier.economics)
            .collect::<Vec<_>>(),
    )?;
    let objectives = PortfolioObjectiveEvidence {
        robust_expected_net_usd: Usd::new(weighted_micro_to_decimal(
            full.robust_numerator,
            DISTRIBUTION_MASS_BPS,
            "robust objective",
        )?),
        nominal_expected_net_usd: Usd::new(weighted_micro_to_decimal(
            full.nominal_numerator,
            DISTRIBUTION_MASS_BPS,
            "nominal objective",
        )?),
        cvar_usd: Usd::new(weighted_micro_to_decimal(
            full.cvar_numerator,
            prepared.tail_mass_bps,
            "portfolio CVaR",
        )?),
        capital_occupancy_usd_hours: UsdHours::new(micro_to_decimal(full.capital_hours_micro)),
        stable_tie_break_stages: solved.tie_break_stages,
    };
    let constraints = PortfolioConstraintEvidence {
        available_cash_used_usd: Usd::new(micro_to_decimal(verification.new_notional_micro)),
        open_capital_usd: Usd::new(micro_to_decimal(verification.open_capital_micro)),
        selected_recommendation_count: u32::try_from(selected.tiers.len()).map_err(|error| {
            ReportError::NumericOverflow {
                field: "selected_recommendation_count",
                detail: error.to_string(),
            }
        })?,
        maximum_scenario_loss_usd: Usd::new(micro_to_decimal(
            verification.maximum_scenario_loss_micro,
        )),
        checked_constraint_count: verification.checked_constraint_count,
        evidence_hash: verification.evidence_hash,
    };
    let solver_evidence = SolverEvidence {
        backend: "highs".to_owned(),
        lexicographic_model_build_count: solved.lexicographic_model_build_count,
        lexicographic_solve_count: solved.lexicographic_solve_count,
        tie_break_proof_count: solved.tie_break_proof_count,
        lexicographic_warm_start_count: solved.lexicographic_warm_start_count,
        marginal_model_build_count: selected.marginal_model_build_count,
        marginal_solve_count: selected.marginal_solve_count,
        marginal_model_reuse_count: selected.marginal_model_reuse_count,
        configured_deadline_secs: input.solver.deadline_secs,
        deterministic_threads: input.solver.threads,
        coefficient_scale: SOLVER_COEFFICIENT_SCALE,
        bound_scale_exponent: solved.bound_scale_exponent,
        optimal: true,
    };
    let exact_verification = ExactVerificationEvidence {
        passed: true,
        selected_tier_digest,
        recomputed_economics_hash,
    };
    let content_hash = CanonicalDigest::content_hash_typed(
        PLAN_DIGEST_DOMAIN,
        1,
        &PlanPreimage {
            portfolio_plan_id: input.portfolio_plan_id,
            selected_tier_ids: &selected_tier_ids,
            objectives: &objectives,
            constraints: &constraints,
            solver: &solver_evidence,
            exact_verification: &exact_verification,
        },
    )?;
    Ok(GlobalPortfolioPlan {
        portfolio_plan_id: input.portfolio_plan_id,
        selected_tier_ids,
        objectives,
        constraints,
        solver: solver_evidence,
        exact_verification,
        content_hash,
    })
}

fn validate_artifact_contract(input: &GlobalPortfolioInput<'_>) -> QuantResult<()> {
    let binding = input.scenario_model_binding;
    let artifact = input.scenario_artifact;
    let bound_model_hash = binding.model_content_hash;
    let realized_model_hash = artifact.scenario_model_content_hash;
    if binding.portfolio_scenario_model_artifact_id != artifact.portfolio_scenario_model_artifact_id
        || bound_model_hash != realized_model_hash
        || binding.route_set_digest != input.represented_routes.digest
        || binding.route_set_digest != artifact.route_set_digest
        || binding.ordered_routes != input.represented_routes.routes
        || binding.ordered_routes != artifact.ordered_routes
        || binding.serving_contract_digest != artifact.serving_contract_digest
        || binding.calibration_contract_digest != artifact.calibration_contract_digest
        || binding.trade_policy_contract_digest != artifact.trade_policy_contract_digest
        || binding.scenario_model_schema_version != artifact.schema_version
        || binding.capital_time_bucket_contract_digest
            != artifact.capital_time_bucket_contract_digest
    {
        return Err(ReportError::ScenarioArtifact {
            detail:
                "binding, ordered Route set, artifact identity, or compatibility digest mismatch"
                    .to_owned(),
        }
        .into());
    }
    if artifact.decision_at != input.account.as_of {
        return Err(ReportError::ScenarioArtifact {
            detail: "scenario artifact decision time differs from the frozen account".to_owned(),
        }
        .into());
    }
    match artifact.visibility {
        PortfolioScenarioVisibility::PointInTime => {
            if binding.bound_at > input.account.as_of {
                return Err(ReportError::ScenarioArtifact {
                    detail: "point-in-time scenario binding was not visible to the frozen account"
                        .to_owned(),
                }
                .into());
            }
        }
        PortfolioScenarioVisibility::HistoricalReplay {
            governance_frozen_at,
        } => {
            if input.account.source != AccountSource::HistoricalReplay {
                return Err(ReportError::ScenarioArtifact {
                    detail: "historical replay scenarios require a historical replay account"
                        .to_owned(),
                }
                .into());
            }
            if binding.bound_at > governance_frozen_at {
                return Err(ReportError::ScenarioArtifact {
                    detail:
                        "historical replay scenario binding exceeds its frozen governance boundary"
                            .to_owned(),
                }
                .into());
            }
        }
        PortfolioScenarioVisibility::PurgedCrossValidation {
            fit_evidence_hash,
            test_groups_hash,
        } => {
            if input.account.source != AccountSource::HistoricalReplay {
                return Err(ReportError::ScenarioArtifact {
                    detail: "purged cross-validation scenarios require a historical replay account"
                        .to_owned(),
                }
                .into());
            }
            let empty_hash = ContentHash::from_bytes([0_u8; 32]);
            if fit_evidence_hash == empty_hash || test_groups_hash == empty_hash {
                return Err(ReportError::ScenarioArtifact {
                    detail:
                        "purged cross-validation scenario visibility has empty population evidence"
                            .to_owned(),
                }
                .into());
            }
        }
    }
    let bucket_contract =
        CapitalTimeBucketContract::try_from(input.policy.tail_risk.capital_time_buckets.as_slice())
            .map_err(|error| ReportError::ScenarioArtifact {
                detail: format!("ExecutionRiskPolicy capital-time grid is invalid: {error}"),
            })?;
    let bucket_digest = bucket_contract.content_hash()?;
    if bucket_digest != artifact.capital_time_bucket_contract_digest {
        return Err(ReportError::ScenarioArtifact {
            detail:
                "ExecutionRiskPolicy capital-time boundaries do not match the promoted artifact"
                    .to_owned(),
        }
        .into());
    }
    if input.top_n == 0 || input.solver.deadline_secs == 0 || input.solver.threads == 0 {
        return Err(ReportError::PortfolioOptimization {
            stage: "input_contract",
            detail: "top_n, solver time limit, and deterministic thread count must be positive"
                .to_owned(),
        }
        .into());
    }
    let max_tiers =
        usize::try_from(input.solver.max_tiers).map_err(|error| ReportError::NumericOverflow {
            field: "portfolio_solver.max_tiers",
            detail: error.to_string(),
        })?;
    if input.tiers.len() > max_tiers {
        return Err(ReportError::ResourceCapacityExceeded {
            resource: "executable_tiers",
            actual: input.tiers.len(),
            ceiling: max_tiers,
        }
        .into());
    }
    let max_scenarios = usize::try_from(input.solver.max_scenarios).map_err(|error| {
        ReportError::NumericOverflow {
            field: "portfolio_solver.max_scenarios",
            detail: error.to_string(),
        }
    })?;
    if artifact.scenarios.len() > max_scenarios {
        return Err(ReportError::ResourceCapacityExceeded {
            resource: "joint_scenarios",
            actual: artifact.scenarios.len(),
            ceiling: max_scenarios,
        }
        .into());
    }
    let max_top_n =
        usize::try_from(input.solver.max_top_n).map_err(|error| ReportError::NumericOverflow {
            field: "portfolio_solver.max_top_n",
            detail: error.to_string(),
        })?;
    let top_n = usize::try_from(input.top_n).map_err(|error| ReportError::NumericOverflow {
        field: "portfolio_top_n",
        detail: error.to_string(),
    })?;
    if top_n > max_top_n {
        return Err(ReportError::ResourceCapacityExceeded {
            resource: "selected_recommendations",
            actual: top_n,
            ceiling: max_top_n,
        }
        .into());
    }
    Ok(())
}

pub(super) struct PreparedGlobalModel {
    pub tiers: Vec<PreparedTier>,
    pub scenario_count: usize,
    pub distribution_weights: Vec<Vec<i64>>,
    pub nominal_weights: Vec<i64>,
    pub existing_scenario_net_micro: Vec<i64>,
    pub existing_distribution_numerators: Vec<i64>,
    pub existing_nominal_numerator: i64,
    pub existing_capital_hours_micro: i64,
    pub existing_open_capital_micro: i64,
    pub existing_open_recommendations: u32,
    pub current_drawdown_micro: i64,
    pub available_cash_limit_micro: i64,
    pub max_open_capital_micro: i64,
    pub exposure_limits: PreparedExposureLimits,
    pub existing_market_exposure: BTreeMap<String, i64>,
    pub existing_event_exposure: BTreeMap<String, i64>,
    pub existing_category_exposure: BTreeMap<String, i64>,
    pub existing_route_exposure: BTreeMap<String, i64>,
    pub existing_bucket_capital: Vec<i64>,
    pub bucket_caps: Vec<i64>,
    pub tail_mass_bps: i64,
    pub max_cvar_numerator: i64,
    pub max_scenario_loss_micro: i64,
    pub max_drawdown_micro: i64,
    pub top_n: u32,
    pub exclusivity_groups: Vec<Vec<usize>>,
}

pub(super) struct PreparedTier {
    pub source_index: usize,
    pub candidate_key: String,
    pub market_key: String,
    pub event_key: String,
    pub category_key: String,
    pub route_key: String,
    pub stable_key: String,
    pub notional_micro: i64,
    pub scenario_net_micro: Vec<i64>,
    pub distribution_numerators: Vec<i64>,
    pub nominal_numerator: i64,
    pub bucket_capital_micro: Vec<i64>,
    pub capital_hours_micro: i64,
}

pub(super) struct PreparedExposureLimits {
    pub single_micro: i64,
    pub market_micro: i64,
    pub event_micro: i64,
    pub category_micro: i64,
    pub route_micro: i64,
    pub open_recommendations: u32,
}

pub(super) struct ExactObjectives {
    pub robust_numerator: i64,
    pub nominal_numerator: i64,
    pub cvar_numerator: i64,
    pub capital_hours_micro: i64,
}

pub(super) struct VerificationSummary {
    new_notional_micro: i64,
    open_capital_micro: i64,
    maximum_scenario_loss_micro: i64,
    checked_constraint_count: u32,
    evidence_hash: ContentHash,
}

struct PreparedScenarioData {
    distribution_weights: Vec<Vec<i64>>,
    nominal_weights: Vec<i64>,
    existing_scenario_net_micro: Vec<i64>,
    existing_distribution_numerators: Vec<i64>,
    existing_nominal_numerator: i64,
}

struct PreparedBucketData {
    ends: Vec<u64>,
    existing_capital: Vec<i64>,
    existing_capital_hours: i64,
    caps: Vec<i64>,
}

struct PreparedTierData {
    tiers: Vec<PreparedTier>,
    rejected: Vec<TierAdmissionRejection>,
    exclusivity_groups: Vec<Vec<usize>>,
}

struct PreparedRiskData {
    existing_open_capital_micro: i64,
    available_cash_limit_micro: i64,
    max_open_capital_micro: i64,
    exposure_limits: PreparedExposureLimits,
    existing_market_exposure: BTreeMap<String, i64>,
    existing_event_exposure: BTreeMap<String, i64>,
    existing_category_exposure: BTreeMap<String, i64>,
    existing_route_exposure: BTreeMap<String, i64>,
    tail_mass_bps: i64,
    max_cvar_numerator: i64,
    max_scenario_loss_micro: i64,
    max_drawdown_micro: i64,
}

fn prepare_scenario(input: &GlobalPortfolioInput<'_>) -> QuantResult<PreparedScenarioData> {
    let artifact = input.scenario_artifact;
    let nominal = artifact
        .nominal_distribution()
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "nominal distribution is absent or ambiguous".to_owned(),
        })?;
    let distribution_weights = artifact
        .distributions
        .iter()
        .map(|distribution| {
            canonical_weights(distribution.weights.as_slice(), artifact.scenarios.len())
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let nominal_weights = canonical_weights(&nominal.weights, artifact.scenarios.len())?;
    let existing_scenario_net_micro = canonical_cashflows(
        &input.existing.scenario_cashflows,
        artifact.scenarios.len(),
        "existing portfolio scenario cashflows",
    )?;
    let existing_distribution_numerators = distribution_weights
        .iter()
        .map(|weights| {
            weighted_numerator(
                &existing_scenario_net_micro,
                weights,
                "existing robust distribution",
            )
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let existing_nominal_numerator = weighted_numerator(
        &existing_scenario_net_micro,
        &nominal_weights,
        "existing nominal distribution",
    )?;
    Ok(PreparedScenarioData {
        distribution_weights,
        nominal_weights,
        existing_scenario_net_micro,
        existing_distribution_numerators,
        existing_nominal_numerator,
    })
}

fn prepare_buckets(input: &GlobalPortfolioInput<'_>) -> QuantResult<PreparedBucketData> {
    let policy_buckets = &input.policy.tail_risk.capital_time_buckets;
    let policy_contract =
        CapitalTimeBucketContract::try_from(policy_buckets.as_slice()).map_err(|error| {
            ReportError::ScenarioArtifact {
                detail: format!("ExecutionRiskPolicy capital-time grid is invalid: {error}"),
            }
        })?;
    let discount_contract =
        CapitalTimeBucketContract::try_from(input.scenario_artifact.discount_curve.as_slice())
            .map_err(|error| ReportError::ScenarioArtifact {
                detail: format!("scenario discount-curve grid is invalid: {error}"),
            })?;
    if policy_contract != discount_contract {
        return Err(ReportError::ScenarioArtifact {
            detail: "discount curve and risk capital buckets have different boundaries".to_owned(),
        }
        .into());
    }
    let ends = policy_contract.end_secs().to_vec();
    let existing_capital = canonical_occupancy(
        &input.existing.capital_occupancy,
        &ends,
        "existing capital occupancy",
    )?;
    let existing_capital_hours =
        occupancy_hours_micro(&existing_capital, &ends, "existing capital occupancy")?;
    let caps = policy_buckets
        .iter()
        .map(|bucket| decimal_to_micro(bucket.max_capital_usd.value, "capital bucket cap"))
        .collect::<QuantResult<Vec<_>>>()?;
    Ok(PreparedBucketData {
        ends,
        existing_capital,
        existing_capital_hours,
        caps,
    })
}

fn prepare_tiers(
    input: &GlobalPortfolioInput<'_>,
    scenario: &PreparedScenarioData,
    bucket_ends: &[u64],
) -> QuantResult<PreparedTierData> {
    let artifact = input.scenario_artifact;
    let held_structural = held_structural_members(input)?;
    let mut tiers = Vec::new();
    let mut rejected = Vec::new();
    let mut economic_keys = BTreeSet::new();
    for (source_index, tier) in input.tiers.iter().enumerate() {
        validate_tier_contract(
            tier,
            artifact,
            &scenario.distribution_weights,
            &scenario.nominal_weights,
        )?;
        if let Some(code) = admission_rejection(tier, input, &held_structural)? {
            rejected.push(TierAdmissionRejection {
                economic_tier_id: tier.economic_tier_id,
                code,
            });
            continue;
        }
        let scenario_net_micro = canonical_cashflows(
            &tier.scenario_cashflows,
            artifact.scenarios.len(),
            "tier scenario cashflows",
        )?;
        let distribution_numerators = scenario
            .distribution_weights
            .iter()
            .map(|weights| {
                weighted_numerator(&scenario_net_micro, weights, "tier robust distribution")
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let nominal_numerator = weighted_numerator(
            &scenario_net_micro,
            &scenario.nominal_weights,
            "tier nominal distribution",
        )?;
        let bucket_capital_micro = canonical_occupancy(
            &tier.capital_occupancy,
            bucket_ends,
            "tier capital occupancy",
        )?;
        let capital_hours_micro =
            occupancy_hours_micro(&bucket_capital_micro, bucket_ends, "tier capital occupancy")?;
        let stable_key = stable_tier_key(tier);
        if !economic_keys.insert(stable_key.clone()) {
            return Err(ReportError::ContractViolation {
                detail: format!(
                    "portfolio tier catalog repeats economic identity {stable_key}; provenance ids cannot distinguish duplicate economic offers"
                ),
            }
            .into());
        }
        tiers.push(PreparedTier {
            source_index,
            candidate_key: tier.candidate_id.to_string(),
            market_key: tier.market_id.to_string(),
            event_key: tier.event_id.to_string(),
            category_key: tier.category.as_str().to_owned(),
            route_key: route_key(tier.route),
            stable_key,
            notional_micro: usd_to_micro(tier.entry.notional_usd, "tier entry notional")?,
            scenario_net_micro,
            distribution_numerators,
            nominal_numerator,
            bucket_capital_micro,
            capital_hours_micro,
        });
    }
    tiers.sort_by(|left, right| left.stable_key.cmp(&right.stable_key));
    let source_to_prepared = tiers
        .iter()
        .enumerate()
        .map(|(prepared_index, tier)| (tier.source_index, prepared_index))
        .collect::<HashMap<_, _>>();
    let exclusivity_groups = artifact
        .structural_exclusivity
        .iter()
        .filter_map(|group| {
            let indexes = input
                .tiers
                .iter()
                .enumerate()
                .filter(|(_, tier)| {
                    group.members.iter().any(|member| {
                        member.market_id == tier.market_id
                            && member.outcome_side == tier.outcome_side
                    })
                })
                .filter_map(|(source_index, _)| source_to_prepared.get(&source_index).copied())
                .collect::<Vec<_>>();
            (indexes.len() > 1).then_some(indexes)
        })
        .collect();
    Ok(PreparedTierData {
        tiers,
        rejected,
        exclusivity_groups,
    })
}

fn prepare_risk(input: &GlobalPortfolioInput<'_>) -> QuantResult<PreparedRiskData> {
    let budget = &input.policy.budget;
    let limits = &input.policy.exposure_limits;
    let cash_reserve_micro =
        decimal_to_micro(budget.cash_reserve_usd.value, "portfolio cash reserve")?;
    let total_budget_micro =
        decimal_to_micro(budget.total_budget_usd.value, "portfolio total budget")?;
    let account_capital_micro =
        usd_to_micro(input.account.capital_base_usd, "account capital base")?;
    let account_available_micro = usd_to_micro(input.account.available_usd, "available cash")?;
    let cash_after_reserve = account_available_micro
        .checked_sub(cash_reserve_micro)
        .filter(|value| *value >= 0)
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "account_cash",
            detail: "account available cash is below the governed reserve".to_owned(),
        })?;
    let existing_open_capital_micro = usd_to_micro(
        input.existing.existing_open_capital_usd,
        "existing open capital",
    )?;
    let governed_capital_micro = total_budget_micro.min(account_capital_micro);
    let strategy_room = governed_capital_micro
        .checked_sub(existing_open_capital_micro)
        .filter(|value| *value >= 0)
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "account_capital",
            detail: "existing open capital exceeds the governed account capital base".to_owned(),
        })?;
    let available_cash_limit_micro = cash_after_reserve.min(strategy_room);
    let policy_open_capital =
        decimal_to_micro(budget.max_open_capital_usd.value, "maximum open capital")?;
    let max_open_capital_micro = policy_open_capital.min(governed_capital_micro);
    let tail_mass_bps = DISTRIBUTION_MASS_BPS
        .checked_sub(i64::from(input.policy.tail_risk.cvar_confidence_bps))
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "cvar_contract",
            detail: "CVaR confidence exceeds 10000 bps".to_owned(),
        })?;
    let max_cvar_micro =
        decimal_to_micro(input.policy.tail_risk.max_cvar_usd.value, "maximum CVaR")?;
    Ok(PreparedRiskData {
        existing_open_capital_micro,
        available_cash_limit_micro,
        max_open_capital_micro,
        exposure_limits: PreparedExposureLimits {
            single_micro: decimal_to_micro(
                limits.max_single_recommendation_usd.value,
                "single recommendation cap",
            )?,
            market_micro: decimal_to_micro(
                limits.max_market_exposure_usd.value,
                "market exposure cap",
            )?,
            event_micro: decimal_to_micro(
                limits.max_event_exposure_usd.value,
                "event exposure cap",
            )?,
            category_micro: decimal_to_micro(
                limits.max_category_exposure_usd.value,
                "category exposure cap",
            )?,
            route_micro: decimal_to_micro(
                limits.max_route_exposure_usd.value,
                "Route exposure cap",
            )?,
            open_recommendations: limits.max_open_recommendations,
        },
        existing_market_exposure: market_exposure(input)?,
        existing_event_exposure: event_exposure(input)?,
        existing_category_exposure: category_exposure(input)?,
        existing_route_exposure: route_exposure(input)?,
        tail_mass_bps,
        max_cvar_numerator: checked_product(
            max_cvar_micro,
            tail_mass_bps,
            "maximum CVaR scaled numerator",
        )?,
        max_scenario_loss_micro: decimal_to_micro(
            input.policy.tail_risk.max_scenario_loss_usd.value,
            "maximum scenario loss",
        )?,
        max_drawdown_micro: decimal_to_micro(
            input.policy.tail_risk.max_drawdown_usd.value,
            "maximum drawdown",
        )?,
    })
}

fn market_exposure(input: &GlobalPortfolioInput<'_>) -> QuantResult<BTreeMap<String, i64>> {
    input
        .account
        .exposures
        .per_market
        .iter()
        .map(|(key, value)| Ok((key.to_string(), usd_to_micro(*value, "market exposure")?)))
        .collect()
}

fn event_exposure(input: &GlobalPortfolioInput<'_>) -> QuantResult<BTreeMap<String, i64>> {
    input
        .account
        .exposures
        .per_event
        .iter()
        .map(|(key, value)| Ok((key.to_string(), usd_to_micro(*value, "event exposure")?)))
        .collect()
}

fn category_exposure(input: &GlobalPortfolioInput<'_>) -> QuantResult<BTreeMap<String, i64>> {
    input
        .account
        .exposures
        .per_category
        .iter()
        .map(|(key, value)| {
            Ok((
                key.as_str().to_owned(),
                usd_to_micro(*value, "category exposure")?,
            ))
        })
        .collect()
}

fn route_exposure(input: &GlobalPortfolioInput<'_>) -> QuantResult<BTreeMap<String, i64>> {
    let mut exposure = BTreeMap::new();
    for position in &input.account.positions {
        let key = route_key(BuyModelRoute::from(position.category));
        let value = usd_to_micro(position.current_value, "Route exposure")?;
        let entry = exposure.entry(key).or_insert(0_i64);
        *entry = entry
            .checked_add(value)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "existing_route_exposure",
                detail: "micro-USD sum overflow".to_owned(),
            })?;
    }
    Ok(exposure)
}

impl PreparedGlobalModel {
    fn new(input: &GlobalPortfolioInput<'_>) -> QuantResult<(Self, Vec<TierAdmissionRejection>)> {
        let artifact = input.scenario_artifact;
        let scenario = prepare_scenario(input)?;
        let buckets = prepare_buckets(input)?;
        let tier_data = prepare_tiers(input, &scenario, &buckets.ends)?;
        let risk = prepare_risk(input)?;
        let prepared = Self {
            tiers: tier_data.tiers,
            scenario_count: artifact.scenarios.len(),
            distribution_weights: scenario.distribution_weights,
            nominal_weights: scenario.nominal_weights,
            existing_scenario_net_micro: scenario.existing_scenario_net_micro,
            existing_distribution_numerators: scenario.existing_distribution_numerators,
            existing_nominal_numerator: scenario.existing_nominal_numerator,
            existing_capital_hours_micro: buckets.existing_capital_hours,
            existing_open_capital_micro: risk.existing_open_capital_micro,
            existing_open_recommendations: input.existing.existing_open_recommendations,
            current_drawdown_micro: usd_to_micro(
                input.existing.current_drawdown_usd,
                "current drawdown",
            )?,
            available_cash_limit_micro: risk.available_cash_limit_micro,
            max_open_capital_micro: risk.max_open_capital_micro,
            exposure_limits: risk.exposure_limits,
            existing_market_exposure: risk.existing_market_exposure,
            existing_event_exposure: risk.existing_event_exposure,
            existing_category_exposure: risk.existing_category_exposure,
            existing_route_exposure: risk.existing_route_exposure,
            existing_bucket_capital: buckets.existing_capital,
            bucket_caps: buckets.caps,
            tail_mass_bps: risk.tail_mass_bps,
            max_cvar_numerator: risk.max_cvar_numerator,
            max_scenario_loss_micro: risk.max_scenario_loss_micro,
            max_drawdown_micro: risk.max_drawdown_micro,
            top_n: input.top_n,
            exclusivity_groups: tier_data.exclusivity_groups,
        };
        prepared.ensure_solver_range()?;
        Ok((prepared, tier_data.rejected))
    }

    fn ensure_solver_range(&self) -> QuantResult<()> {
        self.maximum_solver_magnitude().map(|_| ())
    }

    pub(super) fn maximum_solver_magnitude(&self) -> QuantResult<i128> {
        let selection_limit =
            usize::try_from(self.top_n).map_err(|error| ReportError::NumericOverflow {
                field: "solver_selection_limit",
                detail: error.to_string(),
            })?;
        self.expression_solver_magnitude(self.scalar_solver_magnitude()?, selection_limit)
    }

    fn scalar_solver_magnitude(&self) -> QuantResult<i128> {
        let mut values = vec![
            self.existing_nominal_numerator,
            self.existing_capital_hours_micro,
            self.existing_open_capital_micro,
            self.current_drawdown_micro,
            self.available_cash_limit_micro,
            self.max_open_capital_micro,
            self.exposure_limits.single_micro,
            self.exposure_limits.market_micro,
            self.exposure_limits.event_micro,
            self.exposure_limits.category_micro,
            self.exposure_limits.route_micro,
            self.tail_mass_bps,
            self.max_cvar_numerator,
            self.max_scenario_loss_micro,
            self.max_drawdown_micro,
        ];
        values.extend(self.distribution_weights.iter().flatten().copied());
        values.extend(self.nominal_weights.iter().copied());
        values.extend(self.existing_scenario_net_micro.iter().copied());
        values.extend(self.existing_distribution_numerators.iter().copied());
        values.extend(self.existing_bucket_capital.iter().copied());
        values.extend(self.bucket_caps.iter().copied());
        values.extend(self.existing_market_exposure.values().copied());
        values.extend(self.existing_event_exposure.values().copied());
        values.extend(self.existing_category_exposure.values().copied());
        values.extend(self.existing_route_exposure.values().copied());
        for tier in &self.tiers {
            values.extend([
                tier.notional_micro,
                tier.nominal_numerator,
                tier.capital_hours_micro,
            ]);
            values.extend(tier.distribution_numerators.iter().copied());
            values.extend(tier.scenario_net_micro.iter().copied());
            values.extend(tier.bucket_capital_micro.iter().copied());
        }
        if values
            .iter()
            .any(|value| value.unsigned_abs() > MAX_EXACT_F64_INTEGER)
        {
            return Err(ReportError::PortfolioOptimization {
                stage: "coefficient_scaling",
                detail: "a scaled integer coefficient exceeds exact f64 integer range".to_owned(),
            }
            .into());
        }
        Ok(values
            .iter()
            .map(|value| i128::from(*value).abs())
            .max()
            .unwrap_or(1))
    }

    fn expression_solver_magnitude(
        &self,
        mut maximum: i128,
        selection_limit: usize,
    ) -> QuantResult<i128> {
        for (index, existing) in self.existing_distribution_numerators.iter().enumerate() {
            maximum = maximum.max(ensure_exact_expression(
                "robust_distribution_expression",
                *existing,
                self.tiers
                    .iter()
                    .map(|tier| tier.distribution_numerators[index]),
                selection_limit,
            )?);
        }
        maximum = maximum.max(ensure_exact_expression(
            "nominal_distribution_expression",
            self.existing_nominal_numerator,
            self.tiers.iter().map(|tier| tier.nominal_numerator),
            selection_limit,
        )?);
        maximum = maximum.max(ensure_exact_expression(
            "capital_hours_expression",
            self.existing_capital_hours_micro,
            self.tiers.iter().map(|tier| tier.capital_hours_micro),
            selection_limit,
        )?);
        let notional_bound = ensure_exact_expression(
            "portfolio_notional_expression",
            0,
            self.tiers.iter().map(|tier| tier.notional_micro),
            selection_limit,
        )?;
        maximum = maximum.max(notional_bound);
        let maximum_existing_exposure = self
            .existing_market_exposure
            .values()
            .chain(self.existing_event_exposure.values())
            .chain(self.existing_category_exposure.values())
            .chain(self.existing_route_exposure.values())
            .map(|value| i128::from(*value).abs())
            .max()
            .unwrap_or(0);
        let grouped_exposure_bound = maximum_existing_exposure
            .checked_add(notional_bound)
            .ok_or_else(solver_range_overflow)?;
        ensure_exact_bound("grouped_exposure_expression", grouped_exposure_bound)?;
        maximum = maximum.max(grouped_exposure_bound);
        for (index, existing) in self.existing_bucket_capital.iter().enumerate() {
            maximum = maximum.max(ensure_exact_expression(
                "capital_bucket_expression",
                *existing,
                self.tiers
                    .iter()
                    .map(|tier| tier.bucket_capital_micro[index]),
                selection_limit,
            )?);
            maximum =
                maximum.max((i128::from(self.bucket_caps[index]) - i128::from(*existing)).abs());
        }
        for (index, existing) in self.existing_scenario_net_micro.iter().enumerate() {
            let scenario_bound = ensure_exact_expression(
                "scenario_cashflow_expression",
                *existing,
                self.tiers.iter().map(|tier| tier.scenario_net_micro[index]),
                selection_limit,
            )?;
            maximum = maximum.max(scenario_bound);
            let auxiliary_bound = i128::from(self.max_scenario_loss_micro)
                .abs()
                .checked_mul(2)
                .and_then(|bound| bound.checked_add(scenario_bound))
                .ok_or_else(solver_range_overflow)?;
            ensure_exact_bound("cvar_excess_constraint", auxiliary_bound)?;
            maximum = maximum.max(auxiliary_bound);
            maximum = maximum
                .max((-i128::from(self.max_scenario_loss_micro) - i128::from(*existing)).abs());
            maximum = maximum.max(
                (i128::from(self.current_drawdown_micro)
                    - i128::from(self.max_drawdown_micro)
                    - i128::from(*existing))
                .abs(),
            );
        }
        let cvar_variable_bound = i128::from(self.max_scenario_loss_micro).abs();
        let cvar_coefficient_mass = i128::from(self.tail_mass_bps)
            .abs()
            .checked_add(
                self.nominal_weights
                    .iter()
                    .map(|weight| i128::from(*weight).abs())
                    .sum::<i128>(),
            )
            .ok_or_else(solver_range_overflow)?;
        let cvar_expression_bound = cvar_variable_bound
            .checked_mul(cvar_coefficient_mass)
            .ok_or_else(solver_range_overflow)?;
        ensure_exact_bound("cvar_expression", cvar_expression_bound)?;
        maximum = maximum.max(cvar_expression_bound);
        maximum = maximum.max(
            (i128::from(self.max_open_capital_micro)
                - i128::from(self.existing_open_capital_micro))
            .abs(),
        );
        let grouped_exposures = [
            (
                &self.existing_market_exposure,
                self.exposure_limits.market_micro,
            ),
            (
                &self.existing_event_exposure,
                self.exposure_limits.event_micro,
            ),
            (
                &self.existing_category_exposure,
                self.exposure_limits.category_micro,
            ),
            (
                &self.existing_route_exposure,
                self.exposure_limits.route_micro,
            ),
        ];
        for (exposures, cap) in grouped_exposures {
            for existing in exposures.values() {
                maximum = maximum.max((i128::from(cap) - i128::from(*existing)).abs());
            }
        }
        maximum = maximum.max(
            (i128::from(self.exposure_limits.open_recommendations)
                - i128::from(self.existing_open_recommendations))
            .abs(),
        );
        let tie_weight_ceiling = self
            .tiers
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_mul(selection_limit))
            .ok_or_else(solver_range_overflow)?;
        maximum = maximum.max(i128::try_from(tie_weight_ceiling).map_err(|error| {
            ReportError::NumericOverflow {
                field: "stable_tie_break_bound",
                detail: error.to_string(),
            }
        })?);
        ensure_exact_bound("solver_model_magnitude", maximum)?;
        Ok(maximum)
    }

    pub(super) fn objectives(&self, selected: &[usize]) -> QuantResult<ExactObjectives> {
        let selected_set = selected.iter().copied().collect::<BTreeSet<_>>();
        if selected_set.len() != selected.len()
            || selected_set.iter().any(|index| *index >= self.tiers.len())
        {
            return Err(ReportError::PortfolioPostCheck {
                detail: "solver returned duplicate or out-of-range tier indexes".to_owned(),
            }
            .into());
        }
        let distribution_totals = self
            .existing_distribution_numerators
            .iter()
            .enumerate()
            .map(|(distribution_index, existing)| {
                selected.iter().try_fold(*existing, |sum, tier_index| {
                    sum.checked_add(
                        self.tiers[*tier_index].distribution_numerators[distribution_index],
                    )
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "robust_objective",
                        detail: "scaled distribution sum overflow".to_owned(),
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let robust_numerator = distribution_totals.into_iter().min().ok_or_else(|| {
            ReportError::PortfolioPostCheck {
                detail: "no robust distribution objective exists".to_owned(),
            }
        })?;
        let nominal_numerator =
            selected
                .iter()
                .try_fold(self.existing_nominal_numerator, |sum, index| {
                    sum.checked_add(self.tiers[*index].nominal_numerator)
                        .ok_or_else(|| ReportError::NumericOverflow {
                            field: "nominal_objective",
                            detail: "scaled nominal sum overflow".to_owned(),
                        })
                })?;
        let scenario_net = self.portfolio_scenario_net(selected)?;
        let cvar_numerator =
            cvar_numerator(&scenario_net, &self.nominal_weights, self.tail_mass_bps)?;
        let capital_hours_micro =
            selected
                .iter()
                .try_fold(self.existing_capital_hours_micro, |sum, index| {
                    sum.checked_add(self.tiers[*index].capital_hours_micro)
                        .ok_or_else(|| ReportError::NumericOverflow {
                            field: "capital_hours_objective",
                            detail: "scaled capital-hours sum overflow".to_owned(),
                        })
                })?;
        Ok(ExactObjectives {
            robust_numerator,
            nominal_numerator,
            cvar_numerator,
            capital_hours_micro,
        })
    }

    fn portfolio_scenario_net(&self, selected: &[usize]) -> QuantResult<Vec<i64>> {
        let mut net = self.existing_scenario_net_micro.clone();
        for index in selected {
            for (scenario_index, value) in self.tiers[*index].scenario_net_micro.iter().enumerate()
            {
                net[scenario_index] = net[scenario_index].checked_add(*value).ok_or_else(|| {
                    ReportError::NumericOverflow {
                        field: "portfolio_scenario_net",
                        detail: "scaled scenario sum overflow".to_owned(),
                    }
                })?;
            }
        }
        Ok(net)
    }

    pub(super) fn verify(&self, selected: &[usize]) -> QuantResult<VerificationSummary> {
        let objectives = self.objectives(selected)?;
        let mut checked = 0_u32;
        let (new_notional, open_capital) = self.verify_exposures(selected, &mut checked)?;
        let maximum_scenario_loss = self.verify_tail(selected, &objectives, &mut checked)?;
        let evidence_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/global-portfolio-exact-verification",
            1,
            &VerificationPreimage {
                selected,
                new_notional,
                open_capital,
                maximum_scenario_loss,
                checked,
            },
        )?;
        Ok(VerificationSummary {
            new_notional_micro: new_notional,
            open_capital_micro: open_capital,
            maximum_scenario_loss_micro: maximum_scenario_loss,
            checked_constraint_count: checked,
            evidence_hash,
        })
    }

    fn verify_exposures(&self, selected: &[usize], checked: &mut u32) -> QuantResult<(i64, i64)> {
        let selected_count =
            u32::try_from(selected.len()).map_err(|error| ReportError::NumericOverflow {
                field: "selected_tier_count",
                detail: error.to_string(),
            })?;
        let total_open = self
            .existing_open_recommendations
            .checked_add(selected_count)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "open_recommendation_count",
                detail: "count overflow".to_owned(),
            })?;
        ensure_constraint(
            selected_count <= self.top_n,
            "selected count exceeds TopN",
            checked,
        )?;
        ensure_constraint(
            total_open <= self.exposure_limits.open_recommendations,
            "open recommendation count exceeds governed cap",
            checked,
        )?;
        let mut candidates = HashSet::new();
        let mut markets = HashSet::new();
        let mut new_notional = 0_i64;
        for index in selected {
            let tier = &self.tiers[*index];
            ensure_constraint(
                candidates.insert(tier.candidate_key.as_str()),
                "more than one tier selected for one candidate",
                checked,
            )?;
            ensure_constraint(
                markets.insert(tier.market_key.as_str()),
                "more than one recommendation selected for one market",
                checked,
            )?;
            ensure_constraint(
                tier.notional_micro <= self.exposure_limits.single_micro,
                "single recommendation exposure cap exceeded",
                checked,
            )?;
            new_notional = new_notional
                .checked_add(tier.notional_micro)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "new_notional",
                    detail: "micro-USD sum overflow".to_owned(),
                })?;
        }
        ensure_constraint(
            new_notional <= self.available_cash_limit_micro,
            "available cash after reserve exceeded",
            checked,
        )?;
        let open_capital = self
            .existing_open_capital_micro
            .checked_add(new_notional)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "open_capital",
                detail: "micro-USD sum overflow".to_owned(),
            })?;
        ensure_constraint(
            open_capital <= self.max_open_capital_micro,
            "maximum open capital exceeded",
            checked,
        )?;
        verify_grouped_exposure(
            selected,
            &self.tiers,
            &self.existing_market_exposure,
            self.exposure_limits.market_micro,
            |tier| &tier.market_key,
            "market exposure",
            checked,
        )?;
        verify_grouped_exposure(
            selected,
            &self.tiers,
            &self.existing_event_exposure,
            self.exposure_limits.event_micro,
            |tier| &tier.event_key,
            "event exposure",
            checked,
        )?;
        verify_grouped_exposure(
            selected,
            &self.tiers,
            &self.existing_category_exposure,
            self.exposure_limits.category_micro,
            |tier| &tier.category_key,
            "category exposure",
            checked,
        )?;
        verify_grouped_exposure(
            selected,
            &self.tiers,
            &self.existing_route_exposure,
            self.exposure_limits.route_micro,
            |tier| &tier.route_key,
            "Route exposure",
            checked,
        )?;
        for group in &self.exclusivity_groups {
            let count = group
                .iter()
                .filter(|index| selected.contains(index))
                .count();
            ensure_constraint(
                count <= 1,
                "structural exclusivity group selected more than one member",
                checked,
            )?;
        }
        for bucket_index in 0..self.bucket_caps.len() {
            let total = selected.iter().try_fold(
                self.existing_bucket_capital[bucket_index],
                |sum, tier_index| {
                    sum.checked_add(self.tiers[*tier_index].bucket_capital_micro[bucket_index])
                        .ok_or_else(|| ReportError::NumericOverflow {
                            field: "capital_bucket",
                            detail: "micro-USD sum overflow".to_owned(),
                        })
                },
            )?;
            ensure_constraint(
                total <= self.bucket_caps[bucket_index],
                "capital time-bucket cap exceeded",
                checked,
            )?;
        }
        Ok((new_notional, open_capital))
    }

    fn verify_tail(
        &self,
        selected: &[usize],
        objectives: &ExactObjectives,
        checked: &mut u32,
    ) -> QuantResult<i64> {
        let scenario_net = self.portfolio_scenario_net(selected)?;
        let mut maximum_scenario_loss = 0_i64;
        for net in scenario_net {
            let loss = net.saturating_neg().max(0);
            maximum_scenario_loss = maximum_scenario_loss.max(loss);
            ensure_constraint(
                loss <= self.max_scenario_loss_micro,
                "maximum scenario loss exceeded",
                checked,
            )?;
            ensure_constraint(
                self.current_drawdown_micro.saturating_add(loss) <= self.max_drawdown_micro,
                "drawdown cap exceeded in a scenario",
                checked,
            )?;
        }
        ensure_constraint(
            objectives.cvar_numerator <= self.max_cvar_numerator,
            "CVaR cap exceeded",
            checked,
        )?;
        Ok(maximum_scenario_loss)
    }
}

fn validate_tier_contract(
    tier: &ExecutableEconomicTier,
    artifact: &PortfolioScenarioArtifact,
    distribution_weights: &[Vec<i64>],
    nominal_weights: &[i64],
) -> QuantResult<()> {
    if !artifact.ordered_routes.contains(&tier.route) {
        return Err(ReportError::ScenarioArtifact {
            detail: format!(
                "tier {} belongs to an unrepresented Route",
                tier.economic_tier_id
            ),
        }
        .into());
    }
    if tier.tier_ordinal == 0
        || !tier.shares.is_positive()
        || !tier.entry.notional_usd.is_positive()
    {
        return Err(ReportError::ContractViolation {
            detail: format!(
                "tier {} has non-positive ordinal, shares, or notional",
                tier.economic_tier_id
            ),
        }
        .into());
    }
    let cashflows = canonical_cashflows(
        &tier.scenario_cashflows,
        artifact.scenarios.len(),
        "tier economics verification",
    )?;
    let expected = distribution_weights
        .iter()
        .map(|weights| {
            weighted_numerator(
                &cashflows,
                weights,
                "tier economics distribution verification",
            )
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let nominal_index = artifact
        .distributions
        .iter()
        .position(|distribution| distribution.nominal)
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "nominal distribution is absent".to_owned(),
        })?;
    let robust = expected
        .iter()
        .copied()
        .min()
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "robust distribution set is empty".to_owned(),
        })?;
    let nominal = expected[nominal_index];
    let profit_bps = cashflows
        .iter()
        .zip(nominal_weights)
        .filter(|(cashflow, _)| **cashflow > 0)
        .map(|(_, weight)| *weight)
        .sum::<i64>();
    let max_loss_micro = cashflows
        .iter()
        .map(|cashflow| cashflow.saturating_neg().max(0))
        .max()
        .unwrap_or_default();
    let bucket_ends = artifact
        .discount_curve
        .iter()
        .map(|point| point.end_secs)
        .collect::<Vec<_>>();
    let occupancy = canonical_occupancy(
        &tier.capital_occupancy,
        &bucket_ends,
        "tier economics verification",
    )?;
    let capital_hours =
        occupancy_hours_micro(&occupancy, &bucket_ends, "tier economics verification")?;
    let comparisons = [
        (
            tier.economics.nominal_expected_net_usd.inner(),
            weighted_micro_to_decimal(nominal, DISTRIBUTION_MASS_BPS, "nominal economics")?,
            "nominal expected net USD",
        ),
        (
            tier.economics.robust_expected_net_usd.inner(),
            weighted_micro_to_decimal(robust, DISTRIBUTION_MASS_BPS, "robust economics")?,
            "robust expected net USD",
        ),
        (
            tier.economics.max_loss_usd.inner(),
            micro_to_decimal(max_loss_micro),
            "maximum loss USD",
        ),
        (
            tier.economics.capital_occupancy_usd_hours.inner(),
            micro_to_decimal(capital_hours),
            "capital occupancy USD-hours",
        ),
    ];
    for (stored, recomputed, field) in comparisons {
        if stored.normalize() != recomputed.normalize() {
            return Err(ReportError::PortfolioPostCheck {
                detail: format!(
                    "tier {} {field} differs from exact scenario recomputation",
                    tier.economic_tier_id
                ),
            }
            .into());
        }
    }
    if tier.economics.profit_probability_bps.inner() != Decimal::from(profit_bps) {
        return Err(ReportError::PortfolioPostCheck {
            detail: format!(
                "tier {} profit probability differs from the nominal scenario distribution",
                tier.economic_tier_id
            ),
        }
        .into());
    }
    Ok(())
}

fn admission_rejection(
    tier: &ExecutableEconomicTier,
    input: &GlobalPortfolioInput<'_>,
    held_structural: &BTreeMap<String, (String, OutcomeSide)>,
) -> QuantResult<Option<TierAdmissionRejectionCode>> {
    let policy = input.policy;
    let artifact = input.scenario_artifact;
    if exit_capacity_rejection(tier, input)? {
        return Ok(Some(TierAdmissionRejectionCode::ScenarioExitCapacity));
    }
    let admission = &policy.admission;
    if tier.economics.nominal_expected_net_usd.inner()
        < admission.min_nominal_expected_net_usd.value
    {
        return Ok(Some(TierAdmissionRejectionCode::NominalExpectedNetFloor));
    }
    if tier.economics.robust_expected_net_usd.inner() < admission.min_robust_expected_net_usd.value
    {
        return Ok(Some(TierAdmissionRejectionCode::RobustExpectedNetFloor));
    }
    if tier.profit_probability_lower_bps < admission.min_profit_probability_bps {
        return Ok(Some(TierAdmissionRejectionCode::ProfitProbabilityFloor));
    }
    if tier.probability_interval_width_bps > admission.max_probability_interval_width_bps {
        return Ok(Some(TierAdmissionRejectionCode::ProbabilityIntervalWidth));
    }
    if tier.entry.notional_usd.inner() > policy.exposure_limits.max_single_recommendation_usd.value
    {
        return Ok(Some(
            TierAdmissionRejectionCode::SingleRecommendationExposure,
        ));
    }
    let allowed_bps = 10_000_u32
        .checked_sub(admission.liquidity_buffer_bps)
        .ok_or_else(|| ReportError::ContractViolation {
            detail: "liquidity buffer exceeds 10000 bps".to_owned(),
        })?;
    if tier.entry.notional_usd.inner() * Decimal::from(10_000_u32)
        > tier.entry.visible_liquidity_usd.inner() * Decimal::from(allowed_bps)
    {
        return Ok(Some(TierAdmissionRejectionCode::LiquidityBuffer));
    }
    for group in &artifact.structural_exclusivity {
        let is_member = group.members.iter().any(|member| {
            member.market_id == tier.market_id && member.outcome_side == tier.outcome_side
        });
        if is_member
            && let Some((held_market, held_side)) = held_structural.get(&group.group_id)
            && (held_market != tier.market_id.as_str() || *held_side != tier.outcome_side)
        {
            return Ok(Some(TierAdmissionRejectionCode::ExistingStructuralConflict));
        }
    }
    Ok(None)
}

fn exit_capacity_rejection(
    tier: &ExecutableEconomicTier,
    input: &GlobalPortfolioInput<'_>,
) -> QuantResult<bool> {
    let existing_shares = input
        .account
        .positions
        .iter()
        .filter(|position| {
            position.market_id == tier.market_id
                && position.token_id == tier.token_id
                && BuyModelRoute::from(position.category) == tier.route
        })
        .try_fold(Decimal::ZERO, |total, position| {
            total
                .checked_add(position.size.inner())
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "scenario_exit_capacity.existing_shares",
                    detail: "existing position shares overflowed Decimal".to_owned(),
                })
        })?;
    let required_shares = existing_shares
        .checked_add(tier.shares.inner())
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_exit_capacity.required_shares",
            detail: "existing plus proposed shares overflowed Decimal".to_owned(),
        })?;
    for scenario in &input.scenario_artifact.scenarios {
        let mut matches = scenario.market_outcomes.iter().filter(|outcome| {
            outcome.route == tier.route
                && outcome.market_id == tier.market_id
                && outcome.token_id == tier.token_id
                && outcome.outcome_side == tier.outcome_side
        });
        let outcome = matches
            .next()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: format!(
                    "scenario {} has no exit-capacity outcome for tier {}",
                    scenario.scenario_index, tier.economic_tier_id
                ),
            })?;
        if matches.next().is_some() {
            return Err(ReportError::ScenarioArtifact {
                detail: format!(
                    "scenario {} repeats exit-capacity outcome for tier {}",
                    scenario.scenario_index, tier.economic_tier_id
                ),
            }
            .into());
        }
        if required_shares > outcome.max_executable_exit_shares.inner() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn held_structural_members(
    input: &GlobalPortfolioInput<'_>,
) -> QuantResult<BTreeMap<String, (String, OutcomeSide)>> {
    let mut held = BTreeMap::new();
    for position in &input.account.positions {
        if !position.current_value.is_positive() {
            continue;
        }
        let Some(side) = parse_outcome_side(&position.outcome) else {
            continue;
        };
        for group in &input.scenario_artifact.structural_exclusivity {
            if group
                .members
                .iter()
                .any(|member| member.market_id == position.market_id && member.outcome_side == side)
            {
                let member = (position.market_id.to_string(), side);
                if let Some(existing) = held.insert(group.group_id.clone(), member.clone())
                    && existing != member
                {
                    return Err(ReportError::ScenarioArtifact {
                        detail: format!(
                            "existing positions occupy multiple members of structural group {}",
                            group.group_id
                        ),
                    }
                    .into());
                }
            }
        }
    }
    Ok(held)
}

fn parse_outcome_side(outcome: &str) -> Option<OutcomeSide> {
    match outcome.trim().to_ascii_lowercase().as_str() {
        "yes" => Some(OutcomeSide::Yes),
        "no" => Some(OutcomeSide::No),
        _ => None,
    }
}

fn canonical_weights(weights: &[ScenarioWeight], scenario_count: usize) -> QuantResult<Vec<i64>> {
    let mut canonical = vec![None; scenario_count];
    for weight in weights {
        let index = usize::try_from(weight.scenario_index).map_err(|error| {
            ReportError::ScenarioArtifact {
                detail: format!("scenario index conversion failed: {error}"),
            }
        })?;
        let slot = canonical
            .get_mut(index)
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: format!("scenario index {} is out of range", weight.scenario_index),
            })?;
        if slot.replace(i64::from(weight.probability_bps)).is_some() {
            return Err(ReportError::ScenarioArtifact {
                detail: format!("scenario index {} is duplicated", weight.scenario_index),
            }
            .into());
        }
    }
    canonical
        .into_iter()
        .enumerate()
        .map(|(index, weight)| {
            weight.ok_or_else(|| {
                ReportError::ScenarioArtifact {
                    detail: format!("scenario index {index} is absent"),
                }
                .into()
            })
        })
        .collect()
}

fn canonical_cashflows(
    cashflows: &[ScenarioCashflow],
    scenario_count: usize,
    field: &'static str,
) -> QuantResult<Vec<i64>> {
    let mut canonical = vec![None; scenario_count];
    for cashflow in cashflows {
        let index = usize::try_from(cashflow.scenario_index).map_err(|error| {
            ReportError::ContractViolation {
                detail: format!("{field} scenario index conversion failed: {error}"),
            }
        })?;
        let slot = canonical
            .get_mut(index)
            .ok_or_else(|| ReportError::ContractViolation {
                detail: format!(
                    "{field} scenario index {} is out of range",
                    cashflow.scenario_index
                ),
            })?;
        if slot
            .replace(usd_to_micro(cashflow.discounted_net_usd, field)?)
            .is_some()
        {
            return Err(ReportError::ContractViolation {
                detail: format!(
                    "{field} scenario index {} is duplicated",
                    cashflow.scenario_index
                ),
            }
            .into());
        }
    }
    canonical
        .into_iter()
        .enumerate()
        .map(|(index, cashflow)| {
            cashflow.ok_or_else(|| {
                ReportError::ContractViolation {
                    detail: format!("{field} scenario index {index} is absent"),
                }
                .into()
            })
        })
        .collect()
}

fn canonical_occupancy(
    occupancy: &[CapitalOccupancyBucket],
    expected_ends: &[u64],
    field: &'static str,
) -> QuantResult<Vec<i64>> {
    if occupancy.len() != expected_ends.len()
        || occupancy
            .iter()
            .zip(expected_ends)
            .any(|(bucket, expected)| bucket.end_secs != *expected)
    {
        return Err(ReportError::ContractViolation {
            detail: format!("{field} does not exactly cover artifact time buckets"),
        }
        .into());
    }
    occupancy
        .iter()
        .map(|bucket| usd_to_micro(bucket.locked_usd, field))
        .collect()
}

fn occupancy_hours_micro(values: &[i64], ends: &[u64], field: &'static str) -> QuantResult<i64> {
    let mut prior = 0_u64;
    let mut numerator = 0_i128;
    for (value, end) in values.iter().zip(ends) {
        let duration = end
            .checked_sub(prior)
            .ok_or_else(|| ReportError::ContractViolation {
                detail: format!("{field} bucket boundaries are not increasing"),
            })?;
        prior = *end;
        numerator = numerator
            .checked_add(i128::from(*value) * i128::from(duration))
            .ok_or_else(|| ReportError::NumericOverflow {
                field,
                detail: "USD-seconds multiplication overflow".to_owned(),
            })?;
    }
    let hours = numerator / 3_600;
    i64::try_from(hours).map_err(|error| {
        ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        }
        .into()
    })
}

fn weighted_numerator(values: &[i64], weights: &[i64], field: &'static str) -> QuantResult<i64> {
    let numerator = values
        .iter()
        .zip(weights)
        .try_fold(0_i128, |sum, (value, weight)| {
            sum.checked_add(i128::from(*value) * i128::from(*weight))
                .ok_or_else(|| ReportError::NumericOverflow {
                    field,
                    detail: "weighted micro-USD sum overflow".to_owned(),
                })
        })?;
    i64::try_from(numerator).map_err(|error| {
        ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        }
        .into()
    })
}

fn cvar_numerator(values: &[i64], weights: &[i64], tail_mass_bps: i64) -> QuantResult<i64> {
    if tail_mass_bps <= 0 {
        return Err(ReportError::PortfolioOptimization {
            stage: "cvar_contract",
            detail: "CVaR tail probability must be positive".to_owned(),
        }
        .into());
    }
    let mut losses = values
        .iter()
        .zip(weights)
        .enumerate()
        .map(|(index, (net, weight))| (net.saturating_neg().max(0), *weight, index))
        .collect::<Vec<_>>();
    losses.sort_by(|left, right| right.0.cmp(&left.0).then(left.2.cmp(&right.2)));
    let mut remaining = tail_mass_bps;
    let mut numerator = 0_i128;
    for (loss, weight, _) in losses {
        if remaining == 0 {
            break;
        }
        let used = remaining.min(weight);
        numerator = numerator
            .checked_add(i128::from(loss) * i128::from(used))
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "cvar_numerator",
                detail: "weighted tail loss overflow".to_owned(),
            })?;
        remaining -= used;
    }
    if remaining != 0 {
        return Err(ReportError::ScenarioArtifact {
            detail: "nominal distribution cannot fill the configured CVaR tail".to_owned(),
        }
        .into());
    }
    i64::try_from(numerator).map_err(|error| {
        ReportError::NumericOverflow {
            field: "cvar_numerator",
            detail: error.to_string(),
        }
        .into()
    })
}

fn checked_product(left: i64, right: i64, field: &'static str) -> QuantResult<i64> {
    left.checked_mul(right).ok_or_else(|| {
        ReportError::NumericOverflow {
            field,
            detail: "scaled integer multiplication overflow".to_owned(),
        }
        .into()
    })
}

fn decimal_to_micro(value: Decimal, field: &'static str) -> QuantResult<i64> {
    let scaled = value * Decimal::from(SOLVER_COEFFICIENT_SCALE);
    if scaled.fract() != Decimal::ZERO {
        return Err(ReportError::PortfolioOptimization {
            stage: "coefficient_scaling",
            detail: format!("{field} has precision finer than one micro-unit: {value}"),
        }
        .into());
    }
    scaled.to_i64().ok_or_else(|| {
        ReportError::NumericOverflow {
            field,
            detail: format!("{value} is outside the scaled i64 range"),
        }
        .into()
    })
}

fn usd_to_micro(value: Usd, field: &'static str) -> QuantResult<i64> {
    decimal_to_micro(value.inner(), field)
}

fn micro_to_decimal(value: i64) -> Decimal {
    (Decimal::from(value) / Decimal::from(SOLVER_COEFFICIENT_SCALE)).normalize()
}

fn weighted_micro_to_decimal(
    numerator: i64,
    denominator: i64,
    field: &'static str,
) -> QuantResult<Decimal> {
    if denominator <= 0 {
        return Err(ReportError::PortfolioPostCheck {
            detail: format!("{field} denominator must be positive"),
        }
        .into());
    }
    Ok((Decimal::from(numerator)
        / Decimal::from(denominator)
        / Decimal::from(SOLVER_COEFFICIENT_SCALE))
    .normalize())
}

fn stable_tier_key(tier: &ExecutableEconomicTier) -> String {
    format!(
        "{}:{}:{}:{}:{}:{:010}",
        route_key(tier.route),
        tier.event_id,
        tier.market_id,
        tier.token_id,
        tier.outcome_side.as_str(),
        tier.tier_ordinal
    )
}

fn route_key(route: BuyModelRoute) -> String {
    match route {
        BuyModelRoute::Pooled => "pooled",
        BuyModelRoute::Crypto => "crypto",
        BuyModelRoute::Weather => "weather",
    }
    .to_owned()
}

fn ensure_constraint(condition: bool, detail: &'static str, checked: &mut u32) -> QuantResult<()> {
    *checked = checked
        .checked_add(1)
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "checked_constraint_count",
            detail: "count overflow".to_owned(),
        })?;
    if condition {
        Ok(())
    } else {
        Err(ReportError::PortfolioPostCheck {
            detail: detail.to_owned(),
        }
        .into())
    }
}

fn ensure_exact_expression(
    field: &'static str,
    constant: i64,
    coefficients: impl IntoIterator<Item = i64>,
    selection_limit: usize,
) -> QuantResult<i128> {
    let mut selected = BinaryHeap::<Reverse<i128>>::with_capacity(selection_limit);
    for coefficient in coefficients {
        let coefficient = i128::from(coefficient).abs();
        if selected.len() < selection_limit {
            selected.push(Reverse(coefficient));
        } else if let Some(Reverse(smallest)) = selected.peek().copied()
            && coefficient > smallest
        {
            selected.pop();
            selected.push(Reverse(coefficient));
        }
    }
    let bound = selected.into_iter().try_fold(
        i128::from(constant).abs(),
        |bound, Reverse(coefficient)| {
            bound
                .checked_add(coefficient)
                .ok_or_else(solver_range_overflow)
        },
    )?;
    ensure_exact_bound(field, bound)?;
    Ok(bound)
}

fn ensure_exact_bound(field: &'static str, bound: i128) -> QuantResult<()> {
    if bound > i128::from(MAX_EXACT_F64_INTEGER) {
        return Err(ReportError::PortfolioOptimization {
            stage: "coefficient_scaling",
            detail: format!(
                "{field} can reach {bound}, outside the exactly representable f64 integer range"
            ),
        }
        .into());
    }
    Ok(())
}

fn solver_range_overflow() -> ReportError {
    ReportError::PortfolioOptimization {
        stage: "coefficient_scaling",
        detail: "solver expression range proof overflowed i128".to_owned(),
    }
}

fn verify_grouped_exposure<'a>(
    selected: &[usize],
    tiers: &'a [PreparedTier],
    existing: &BTreeMap<String, i64>,
    cap: i64,
    key: impl Fn(&'a PreparedTier) -> &'a String,
    label: &'static str,
    checked: &mut u32,
) -> QuantResult<()> {
    let mut totals = existing.clone();
    for index in selected {
        let tier = &tiers[*index];
        let entry = totals.entry(key(tier).clone()).or_default();
        *entry =
            entry
                .checked_add(tier.notional_micro)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: label,
                    detail: "micro-USD exposure sum overflow".to_owned(),
                })?;
    }
    for total in totals.values() {
        ensure_constraint(*total <= cap, label, checked)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_EXACT_F64_INTEGER, cvar_numerator, ensure_exact_expression, occupancy_hours_micro,
    };

    #[test]
    fn cvar_uses_boundary_fraction() {
        let cvar =
            cvar_numerator(&[-100, -50, 10], &[400, 400, 9_200], 500).expect("CVaR numerator");
        assert_eq!(cvar, 45_000);
    }

    #[test]
    fn occupancy_integrates_disjoint_buckets() {
        let hours = occupancy_hours_micro(&[100, 50], &[3_600, 10_800], "test").expect("hours");
        assert_eq!(hours, 200);
    }

    #[test]
    fn solver_expression_is_exact() {
        let half = i64::try_from(MAX_EXACT_F64_INTEGER / 2).expect("exact f64 half range");
        let error = ensure_exact_expression("aggregate_test", 0, [half + 1, half + 1], 2)
            .expect_err("aggregate range beyond 2^53 must fail closed");
        assert!(error.to_string().contains("aggregate_test"));
    }
}
