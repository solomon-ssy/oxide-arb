//! `argmin`-backed weight refinement behind the `optimize` feature.
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
use argmin_math::Error;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    model::objective::{CrossSectionGroup, ObjectiveEvaluator},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Maximum Nelder–Mead iterations.
const MAX_ITERS: u64 = 400;

/// Perturbation applied to each seed coordinate to build the initial simplex.
const SIMPLEX_DELTA: f64 = 0.75;

/// Numerical floor so `ln(weight)` is finite for the softmax seed.
const LN_FLOOR: f64 = 1e-6;

/// Refine `grid_weights` with Nelder–Mead. Returns the refined simplex weights
/// (Decimal, rounded + renormalized), or `None` when the problem is degenerate
/// (`< 2` weights). Solver and numeric-boundary failures are returned explicitly.
pub(crate) fn refine_weights(
    grid_weights: &[Decimal],
    groups: &[CrossSectionGroup],
    evaluator: &ObjectiveEvaluator,
) -> Result<Option<Vec<Decimal>>, Error> {
    let n = grid_weights.len();
    if n < 2 || groups.is_empty() {
        return Ok(None);
    }

    let problem = WeightObjective {
        groups: groups.to_vec(),
        evaluator: evaluator.clone(),
    };

    // Seed unconstrained params via inverse-softmax (log) of the grid weights.
    let mut seed = Vec::with_capacity(n);
    for weight in grid_weights {
        seed.push(decimal_to_f64(*weight)?.max(LN_FLOOR).ln());
    }

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
        .run()?;

    let best = result
        .state()
        .get_best_param()
        .ok_or_else(|| Error::msg("optimizer returned no best parameter"))?;
    Ok(Some(softmax_to_simplex_decimal(best)?))
}

/// The Nelder–Mead cost: negative LTR objective over the softmax-projected
/// weights (minimization).
struct WeightObjective {
    groups: Vec<CrossSectionGroup>,
    evaluator: ObjectiveEvaluator,
}

impl CostFunction for WeightObjective {
    type Param = Vec<f64>;
    type Output = f64;

    fn cost(&self, param: &Self::Param) -> Result<f64, Error> {
        let weights = softmax_to_simplex_decimal(param)?;
        let objective = self
            .evaluator
            .evaluate(&weights, &self.groups)
            .map_err(|error| Error::msg(error.to_string()))?
            .objective_value();
        Ok(-decimal_to_f64(objective)?)
    }
}

/// Softmax an unconstrained `f64` vector onto the weight simplex, then quantize
/// to `Decimal` at the research scale and renormalize (so the sum is exactly 1).
fn softmax_to_simplex_decimal(param: &[f64]) -> Result<Vec<Decimal>, Error> {
    if param.is_empty() || param.iter().any(|value| !value.is_finite()) {
        return Err(Error::msg(
            "optimizer softmax requires a non-empty finite parameter vector",
        ));
    }
    let max = param.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let exps: Vec<f64> = param.iter().map(|x| (x - max).exp()).collect();
    let sum: f64 = exps.iter().sum();
    if !sum.is_finite() || sum <= 0.0 {
        return Err(Error::msg(format!(
            "optimizer softmax normalization is invalid: {sum}"
        )));
    }
    let raw: Vec<Decimal> = exps
        .iter()
        .map(|e| -> Result<Decimal, Error> {
            let w = e / sum;
            if !w.is_finite() {
                return Err(Error::msg(format!(
                    "optimizer softmax emitted non-finite weight {w}"
                )));
            }
            Decimal::from_f64(w)
                .map(|value| value.round_dp(RESEARCH_DECIMAL_SCALE))
                .ok_or_else(|| Error::msg(format!("optimizer weight {w} does not fit Decimal")))
        })
        .collect::<Result<_, _>>()?;
    let total: Decimal = raw.iter().map(|w| (*w).max(Decimal::ZERO)).sum();
    if total.is_zero() {
        return Err(Error::msg(
            "optimizer softmax weights quantized to an all-zero simplex",
        ));
    }
    Ok(raw
        .iter()
        .map(|w| (*w).max(Decimal::ZERO) / total)
        .collect())
}

/// Convert a `Decimal` to `f64` (optimizer boundary only). Fail-closed: never
/// silently substitute `0.0` for a non-representable value.
fn decimal_to_f64(value: Decimal) -> Result<f64, Error> {
    value.to_f64().ok_or_else(|| {
        Error::msg(format!(
            "optimize boundary could not convert Decimal {value} to f64"
        ))
    })
}
