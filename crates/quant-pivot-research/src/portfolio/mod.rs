//! Portfolio plane: the governed "how much to buy" closed loop.
//!
//! Pure compute (no repo / no core / no live `BookStore`): the
//! [`PortfolioPlanner`] consumes scored candidates, the real
//! [`AccountSnapshot`] capital base, and the governed budget / constraints /
//! sizing config, then produces per-recommendation sizing + risk envelopes and
//! a persistable plan row. The greedy [`PortfolioAllocator`] is shared with the
//! backtest plane.

pub mod account;
pub mod allocator;
pub mod planner;
pub mod sizing;

pub use account::AccountSnapshot;
pub use allocator::{
    Allocation, AllocationInput, AllocationOutput, CandidateMeta, GreedyPortfolioAllocator,
    PortfolioAllocator,
};
pub use planner::{
    DefaultPortfolioPlanner, PlanCandidate, PlannedRecommendation, PortfolioPlanInput,
    PortfolioPlanOutput, PortfolioPlanner, RejectedCandidate,
};
pub use sizing::{
    DrawdownState, KellySizingModel, SizingInput, SizingModel, SizingOutcome, SizingSuggestion,
    sizing_model_from_config,
};
