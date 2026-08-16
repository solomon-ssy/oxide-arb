//! Exact-verified output of the unique global portfolio solver.

use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::types::{ContentHash, EconomicTierId, PortfolioPlanId, Usd, UsdHours};

/// Discounted net cash flow of the already-open portfolio in one promoted scenario.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct ScenarioCashflow {
    pub scenario_index: u32,
    pub discounted_net_usd: Usd,
}

/// Capital already locked by existing positions through one governed time bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapitalOccupancyBucket {
    pub end_secs: u64,
    pub locked_usd: Usd,
}

/// Frozen existing-position economics evaluated under the promoted joint scenarios.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExistingPortfolioState {
    pub existing_open_capital_usd: Usd,
    pub existing_open_recommendations: u32,
    pub current_drawdown_usd: Usd,
    pub scenario_cashflows: Vec<ScenarioCashflow>,
    pub capital_occupancy: Vec<CapitalOccupancyBucket>,
}

/// Exact Decimal values fixed between lexicographic solve stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioObjectiveEvidence {
    pub robust_expected_net_usd: Usd,
    pub nominal_expected_net_usd: Usd,
    pub cvar_usd: Usd,
    pub capital_occupancy_usd_hours: UsdHours,
    pub stable_tie_break_stages: u32,
}

/// Aggregate hard-constraint values recomputed outside the solver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioConstraintEvidence {
    pub available_cash_used_usd: Usd,
    pub open_capital_usd: Usd,
    pub selected_recommendation_count: u32,
    pub maximum_scenario_loss_usd: Usd,
    pub checked_constraint_count: u32,
    pub evidence_hash: ContentHash,
}

/// Stable `HiGHS` evidence. A plan exists only when every stage is proven optimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SolverEvidence {
    pub backend: String,
    /// Number of immutable matrices uploaded for the complete publishable solve.
    /// This must be one; leave-one-out ranking reuses the same matrix.
    pub lexicographic_model_build_count: u32,
    pub lexicographic_solve_count: u32,
    /// Optimal Hamming-distance solves proving that the stable identity locks
    /// admit exactly one binary tier selection.
    pub tie_break_proof_count: u32,
    /// Number of later lexicographic stages seeded from the prior optimal solution.
    pub lexicographic_warm_start_count: u32,
    /// Additional matrices uploaded for leave-one-out solves. This must be zero.
    pub marginal_model_build_count: u32,
    pub marginal_solve_count: u32,
    /// Leave-one-out solves that reuse the lexicographic matrix.
    pub marginal_model_reuse_count: u32,
    pub configured_deadline_secs: u64,
    pub deterministic_threads: u32,
    pub coefficient_scale: i64,
    /// Exact power-of-two `HiGHS` user-bound scaling applied to this model.
    pub bound_scale_exponent: i32,
    pub optimal: bool,
}

/// Exact post-solve verification evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExactVerificationEvidence {
    pub passed: bool,
    pub selected_tier_digest: ContentHash,
    pub recomputed_economics_hash: ContentHash,
}

/// Unique global portfolio plan persisted and referenced by every recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct GlobalPortfolioPlan {
    pub portfolio_plan_id: PortfolioPlanId,
    pub selected_tier_ids: Vec<EconomicTierId>,
    pub objectives: PortfolioObjectiveEvidence,
    pub constraints: PortfolioConstraintEvidence,
    pub solver: SolverEvidence,
    pub exact_verification: ExactVerificationEvidence,
    pub content_hash: ContentHash,
}

/// Durable terminal portfolio decision. Zero candidates is explicit evidence;
/// solver failure never produces this value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum PortfolioDecisionResult {
    ZeroCandidates {
        rejected_tier_count: u32,
        evidence_hash: ContentHash,
    },
    Optimized {
        plan: Box<GlobalPortfolioPlan>,
    },
}
