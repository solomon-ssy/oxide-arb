//! Optimizer selection, parsed configuration, and solve provenance.
//!
//! [`optimizer_from_config`] builds the single production allocator
//! ([`LinearProgrammingPortfolioAllocator`]) from the governed
//! [`PortfolioOptimizerConfig`]; [`backtest_optimizer`] pins the deterministic
//! relaxation mode + pure-Rust `microlp` backend so the backtest report hash is
//! reproducible and build-independent. [`OptimizerOutcome`] records exactly
//! which solve path produced an allocation (observable end-to-end via the plan's
//! `optimizer_meta_json`).

use std::sync::Arc;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::{
        CorrelationSource, OptimizerSolverStatus, PortfolioSolveMode, PortfolioSolverKind,
    },
    runtime_config::PortfolioOptimizerConfig,
};
use rust_decimal::Decimal;

use crate::portfolio::{allocator::PortfolioAllocator, lp::LinearProgrammingPortfolioAllocator};

/// Parsed, validated optimizer configuration (money/weight as [`Decimal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerConfig {
    /// The requested solver backend (may be downgraded if its feature is absent).
    pub solver: PortfolioSolverKind,
    /// `true` ⇒ exact binary-inclusion MILP; `false` ⇒ continuous relaxation.
    pub integer_inclusion: bool,
    /// `λ ≥ 0`: expected-return tilt in the per-dollar objective weight.
    pub lambda: Decimal,
}

impl OptimizerConfig {
    /// Parse the wire config (decimal `objective_return_weight` → `λ`).
    ///
    /// # Errors
    /// Returns a configuration error when `objective_return_weight` is malformed
    /// (runtime-config validation rejects this upstream, so this is a hard guard).
    pub fn from_wire(config: &PortfolioOptimizerConfig) -> QuantResult<Self> {
        let lambda = config.objective_return_weight.value.max(Decimal::ZERO);
        Ok(Self {
            solver: config.solver,
            integer_inclusion: config.integer_inclusion,
            lambda,
        })
    }

    /// The deterministic backtest configuration: continuous relaxation on the
    /// pure-Rust `microlp` backend, no expected-return tilt, no wall-clock bound.
    #[must_use]
    pub const fn backtest() -> Self {
        Self {
            solver: PortfolioSolverKind::Microlp,
            integer_inclusion: false,
            lambda: Decimal::ZERO,
        }
    }

    /// The solver actually used after honoring compile-time feature availability
    /// (`HiGHS` downgrades to microlp when `lp-solver-highs` is not built).
    #[must_use]
    pub const fn effective_solver(self) -> PortfolioSolverKind {
        match self.solver {
            PortfolioSolverKind::Highs if cfg!(feature = "lp-solver-highs") => {
                PortfolioSolverKind::Highs
            }
            PortfolioSolverKind::Highs | PortfolioSolverKind::Microlp => {
                PortfolioSolverKind::Microlp
            }
        }
    }
}

/// Solve provenance for one allocation — mirrors the persisted
/// [`PortfolioOptimizerMeta`](quant_pivot_models::types::PortfolioOptimizerMeta).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerOutcome {
    /// The solver backend actually used.
    pub solver: PortfolioSolverKind,
    /// The solve mode that produced the allocation.
    pub solve_mode: PortfolioSolveMode,
    /// Terminal solver status.
    pub status: OptimizerSolverStatus,
    /// Whether the MILP path failed and the relaxation produced the plan.
    pub fell_back_to_relaxation: bool,
    /// Achieved objective value (`Σ wᵢ·uᵢ`), when a solve produced one.
    pub objective_value: Option<Decimal>,
    /// Wall-clock solve duration in milliseconds.
    pub elapsed_ms: u64,
    /// Provenance of the correlation clusters applied to the correlation cap.
    pub correlation_source: CorrelationSource,
    /// Human-readable conflicting constraints when the model was infeasible.
    pub constraint_conflicts: Vec<String>,
}

impl OptimizerOutcome {
    /// The neutral outcome for a trivially-empty allocation (nothing to solve).
    #[must_use]
    pub const fn empty(solver: PortfolioSolverKind, solve_mode: PortfolioSolveMode) -> Self {
        Self {
            solver,
            solve_mode,
            status: OptimizerSolverStatus::Optimal,
            fell_back_to_relaxation: false,
            objective_value: Some(Decimal::ZERO),
            elapsed_ms: 0,
            correlation_source: CorrelationSource::Disabled,
            constraint_conflicts: Vec::new(),
        }
    }
}

/// Build the production allocator from the governed optimizer config.
///
/// # Errors
/// Propagates [`OptimizerConfig::from_wire`] parse failures.
pub fn optimizer_from_config(
    config: &PortfolioOptimizerConfig,
) -> QuantResult<Arc<dyn PortfolioAllocator>> {
    let parsed = OptimizerConfig::from_wire(config)?;
    Ok(Arc::new(LinearProgrammingPortfolioAllocator::new(parsed)))
}

/// Build the deterministic backtest allocator (pinned microlp + relaxation).
#[must_use]
pub fn backtest_optimizer() -> Arc<dyn PortfolioAllocator> {
    Arc::new(LinearProgrammingPortfolioAllocator::new(
        OptimizerConfig::backtest(),
    ))
}
