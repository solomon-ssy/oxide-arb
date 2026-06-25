//! `argmin`-backed weight refinement (Phase 3.6, `optimize` feature).
//!
//! The deterministic coordinate search in [`crate::model::trainer`] is the base
//! optimizer. When the `optimize` feature is enabled, this module runs a
//! gradient-free Nelder–Mead refinement seeded from the coordinate-search
//! solution. The unconstrained parameter vector is mapped onto the weight
//! **simplex** by a softmax inside the cost function, so feasibility is exact and
//! no penalty/projection hack is needed. The refinement is accepted by the
//! caller only when it strictly improves the Decimal training objective, so the
//! `f64` optimizer never weakens the model.
//!
//! Determinism: a fixed initial simplex + fixed iteration budget make the search
//! reproducible on a given platform; the accept/reject decision is made in
//! `Decimal` by the trainer.

use argmin::{
    core::{CostFunction, Executor, State},
    solver::neldermead::NelderMead,
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    model::trainer::{SampleRow, penalized_objective},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Maximum Nelder–Mead iterations.
const MAX_ITERS: u64 = 400;

/// Perturbation applied to each seed coordinate to build the initial simplex.
const SIMPLEX_DELTA: f64 = 0.75;

/// Numerical floor so `ln(weight)` is finite for the softmax seed.
const LN_FLOOR: f64 = 1e-6;

/// Refine `grid_weights` with Nelder–Mead. Returns the refined simplex weights
/// (Decimal, rounded + renormalized), or `None` when the solver fails or the
/// problem is degenerate (`< 2` weights).
#[must_use]
pub(crate) fn refine_weights(
    grid_weights: &[Decimal],
    rows: &[SampleRow],
    l2: Decimal,
) -> Option<Vec<Decimal>> {
    let n = grid_weights.len();
    if n < 2 || rows.is_empty() {
        return None;
    }

    let problem = WeightObjective {
        rows: rows.to_vec(),
        l2,
    };

    // Seed unconstrained params via inverse-softmax (log) of the grid weights.
    let seed: Vec<f64> = grid_weights
        .iter()
        .map(|w| decimal_to_f64(*w).max(LN_FLOOR).ln())
        .collect();

    // Deterministic initial simplex: the seed plus one perturbation per axis.
    let mut simplex = Vec::with_capacity(n + 1);
    simplex.push(seed.clone());
    for axis in 0..n {
        let mut vertex = seed.clone();
        vertex[axis] += SIMPLEX_DELTA;
        simplex.push(vertex);
    }

    let solver = NelderMead::new(simplex);
    let result = Executor::new(problem, solver)
        .configure(|state| state.max_iters(MAX_ITERS))
        .run()
        .ok()?;

    let best = result.state().get_best_param()?.clone();
    Some(softmax_to_simplex_decimal(&best))
}

/// The Nelder–Mead cost: negative penalized objective over the softmax-projected
/// weights (minimization).
struct WeightObjective {
    rows: Vec<SampleRow>,
    l2: Decimal,
}

impl CostFunction for WeightObjective {
    type Param = Vec<f64>;
    type Output = f64;

    fn cost(&self, param: &Self::Param) -> Result<f64, argmin::core::Error> {
        let weights = softmax_to_simplex_decimal(param);
        let objective = penalized_objective(&weights, &self.rows, self.l2);
        Ok(-decimal_to_f64(objective))
    }
}

/// Softmax an unconstrained `f64` vector onto the weight simplex, then quantize
/// to `Decimal` at the research scale and renormalize (so the sum is exactly 1).
fn softmax_to_simplex_decimal(param: &[f64]) -> Vec<Decimal> {
    let max = param.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let exps: Vec<f64> = param.iter().map(|x| (x - max).exp()).collect();
    let sum: f64 = exps.iter().sum();
    let raw: Vec<Decimal> = exps
        .iter()
        .map(|e| {
            let w = if sum > 0.0 { e / sum } else { 0.0 };
            Decimal::from_f64(w)
                .unwrap_or(Decimal::ZERO)
                .round_dp(RESEARCH_DECIMAL_SCALE)
        })
        .collect();
    let total: Decimal = raw.iter().map(|w| (*w).max(Decimal::ZERO)).sum();
    if total.is_zero() {
        let uniform = Decimal::ONE / Decimal::from(raw.len() as u64);
        return vec![uniform; raw.len()];
    }
    raw.iter()
        .map(|w| (*w).max(Decimal::ZERO) / total)
        .collect()
}

/// Convert a `Decimal` to `f64` (optimizer boundary only).
fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}
