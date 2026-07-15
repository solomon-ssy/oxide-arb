//! Portfolio plane: the governed "how much to buy" closed loop.
//!
//! Pure compute (no repo / no core / no live `BookStore`): the
//! [`PortfolioPlanner`] consumes scored candidates, the real
//! [`AccountSnapshot`] capital base, and the governed budget / constraints /
//! sizing / optimizer config, then produces per-recommendation sizing + risk
//! envelopes and a persistable plan row. Capital allocation is a single `good_lp`
//! LP/MILP code path ([`LinearProgrammingPortfolioAllocator`]) shared with the
//! backtest plane — there is no greedy allocator.

pub mod account;
pub mod allocator;
pub mod correlation;
pub mod lp;
pub mod optimizer;
pub mod planner;
pub mod sizing;

pub use account::AccountSnapshot;
pub use allocator::{
    Allocation, AllocationInput, AllocationOutput, CandidateMeta, PortfolioAllocator,
};
pub use correlation::{
    CorrelationConstraint, CorrelationEstimator, CorrelationGroups, CorrelationInput,
    CorrelationMarket, HistoricalCorrelationEstimator, ProxyCorrelationEstimator,
};
pub use lp::LinearProgrammingPortfolioAllocator;
#[cfg(debug_assertions)]
pub use lp::debug_test_hooks;
pub use optimizer::{OptimizerConfig, OptimizerOutcome, backtest_optimizer, optimizer_from_config};
pub use planner::{
    DefaultPortfolioPlanner, PlanCandidate, PlannedRecommendation, PortfolioPlanInput,
    PortfolioPlanOutput, PortfolioPlanner, RejectedCandidate,
};
pub use sizing::{
    DrawdownState, ExecutableSizingTier, KellySizingModel, SizingInput, SizingModel, SizingOutcome,
    SizingSuggestion, sizing_model_from_config,
};
