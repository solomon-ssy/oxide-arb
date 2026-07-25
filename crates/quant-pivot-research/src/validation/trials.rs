//! Governed hyperparameter trial grid.
//!
//! CSCV/PBO (`validation::pbo`) and the Deflated Sharpe Ratio's
//! multiple-testing correction (`validation::dsr`) both need `N` genuinely
//! distinct, independently governed strategy configurations — not a single
//! trained artifact's local optimizer trajectory (`coordinate_search`'s
//! hill-climbing moves are data-dependent and not comparable across the
//! different train/test partitions the CSCV procedure resamples). This
//! module deterministically expands a small, config-bounded Cartesian grid
//! over the frozen `research.training` objective (Buy-side `WeightedFactor`)
//! or the classical-ML hyperparameters (`ml-classical`) into a fixed list of
//! [`Trial`]s; each trial gets one full-window train + backtest by the
//! caller (`quant-pivot-core`'s CPCV/trial orchestration), never a resample.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::RankLossKind, types::model_training::TrainingObjectiveSpec,
};
use rust_decimal::Decimal;
#[cfg(feature = "ml-classical")]
use rust_decimal::prelude::ToPrimitive;

#[cfg(feature = "ml-classical")]
use crate::model::classical::{ClassicalParams, ForestParams, LinearParams};

/// One governed, independently trainable configuration in the trial grid.
#[derive(Debug, Clone)]
pub struct Trial {
    /// Stable index into the grid (`0..grid.len`), deterministic across runs
    /// for the same [`TrialGridSpec`].
    pub trial_id: u32,
    /// Human-readable summary of what varies from the base config (audit /
    /// UI display — e.g. `"lambda_tail=x0.5,rank_loss=pairwise_ranknet"`).
    pub label: String,
    /// The full training-objective override for a `WeightedFactor` trial.
    pub weighted_factor_objective: Option<TrainingObjectiveSpec>,
    /// The full classical-ML hyperparameter override for a classical trial.
    #[cfg(feature = "ml-classical")]
    pub classical_params: Option<ClassicalParams>,
}

/// Buy-side `WeightedFactor` trial grid: a Cartesian sweep of lambda
/// multipliers (applied to the governed `lambda_tail`/`lambda_turnover`/
/// `lambda_l2`) crossed with the available rank-loss kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedFactorTrialGrid {
    /// Multipliers applied to the base `lambda_tail`/`lambda_turnover`/`lambda_l2`.
    pub lambda_multipliers: Vec<Decimal>,
    /// Rank-loss variants to cross with each lambda multiplier.
    pub rank_loss_kinds: Vec<RankLossKind>,
    /// Hard cap on the number of trials the grid may expand to.
    pub max_trials: u32,
}

/// Classical-ML trial grid (`ml-classical`): a Cartesian sweep of multipliers
/// applied to the base `ForestParams.n_trees` and `LinearParams.alpha`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicalTrialGrid {
    /// Multipliers applied to the base `ForestParams.n_trees`.
    pub forest_n_trees_multipliers: Vec<Decimal>,
    /// Multipliers applied to the base `LinearParams.alpha`.
    pub linear_alpha_multipliers: Vec<Decimal>,
    /// Hard cap on the number of trials the grid may expand to.
    pub max_trials: u32,
}

/// The governed trial-grid definition for one model family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrialGridSpec {
    /// Buy-side weighted-factor LTR grid (lambda multipliers × rank-loss kind).
    WeightedFactor(WeightedFactorTrialGrid),
    /// Classical-ML grid (forest/linear hyperparameter multipliers).
    Classical(ClassicalTrialGrid),
}

impl TrialGridSpec {
    /// Expand the grid into a deterministic, ordered list of [`Trial`]s.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] when the expanded
    /// grid would exceed `max_trials` (a config-time sizing error — the
    /// operator must narrow the grid, never have it silently truncated) or
    /// the grid is empty.
    pub fn generate(&self, base_objective: &TrainingObjectiveSpec) -> QuantResult<Vec<Trial>> {
        match self {
            Self::WeightedFactor(grid) => generate_weighted_factor(grid, base_objective),
            Self::Classical(grid) => (grid).generate_classical(),
        }
    }
}

fn generate_weighted_factor(
    grid: &WeightedFactorTrialGrid,
    base: &TrainingObjectiveSpec,
) -> QuantResult<Vec<Trial>> {
    let expanded = grid
        .lambda_multipliers
        .len()
        .checked_mul(grid.rank_loss_kinds.len())
        .ok_or_else(|| methodology("weighted-factor trial grid size overflowed usize"))?;
    validate_grid_size(expanded, grid.max_trials)?;

    let mut trials = Vec::with_capacity(expanded);
    for &multiplier in &grid.lambda_multipliers {
        for &rank_loss in &grid.rank_loss_kinds {
            let trial_id = u32::try_from(trials.len()).map_err(|error| {
                methodology(format!("weighted-factor trial id exceeds u32: {error}"))
            })?;
            let objective = TrainingObjectiveSpec {
                rank_loss,
                lambda_tail: base.lambda_tail * multiplier,
                lambda_turnover: base.lambda_turnover * multiplier,
                lambda_l2: base.lambda_l2 * multiplier,
                ..*base
            };
            trials.push(Trial {
                trial_id,
                label: format!("lambda_x{multiplier},rank_loss={}", rank_loss.as_str()),
                weighted_factor_objective: Some(objective),
                #[cfg(feature = "ml-classical")]
                classical_params: None,
            });
        }
    }
    Ok(trials)
}

#[cfg(feature = "ml-classical")]
impl ClassicalTrialGrid {
    fn generate_classical(&self) -> QuantResult<Vec<Trial>> {
        // Sum, not Cartesian product: forest and linear params apply to disjoint
        // ClassicalKind families. Crossing them inflated DSR N with inert dimensions.
        let expanded = self
            .forest_n_trees_multipliers
            .len()
            .checked_add(self.linear_alpha_multipliers.len())
            .ok_or_else(|| methodology("classical trial grid size overflowed usize"))?;
        validate_grid_size(expanded, self.max_trials)?;

        let base = ClassicalParams {
            forest: ForestParams::default(),
            linear: LinearParams::default(),
        };
        let mut trials = Vec::with_capacity(expanded);
        for &forest_multiplier in &self.forest_n_trees_multipliers {
            if forest_multiplier <= Decimal::ZERO {
                return Err(methodology(format!(
                    "forest n_trees multiplier must be positive, got {forest_multiplier}"
                )));
            }
            let trial_id = u32::try_from(trials.len())
                .map_err(|error| methodology(format!("classical trial id exceeds u32: {error}")))?;
            let base_n_trees = u64::try_from(base.forest.n_trees).map_err(|error| {
                methodology(format!("base forest n_trees does not fit u64: {error}"))
            })?;
            let scaled_n_trees = Decimal::from(base_n_trees)
                .checked_mul(forest_multiplier)
                .ok_or_else(|| methodology("forest n_trees scaling overflowed Decimal"))?
                .round();
            let n_trees = scaled_n_trees.to_usize().ok_or_else(|| {
                methodology(format!(
                    "scaled forest n_trees {scaled_n_trees} does not fit usize"
                ))
            })?;
            if n_trees == 0 {
                return Err(methodology(
                    "scaled forest n_trees must be positive".to_owned(),
                ));
            }
            trials.push(Trial {
                trial_id,
                label: format!("forest_x{forest_multiplier}"),
                weighted_factor_objective: None,
                classical_params: Some(ClassicalParams {
                    forest: ForestParams {
                        n_trees,
                        ..base.forest
                    },
                    linear: base.linear,
                }),
            });
        }
        for &linear_multiplier in &self.linear_alpha_multipliers {
            if linear_multiplier <= Decimal::ZERO {
                return Err(methodology(format!(
                    "linear alpha multiplier must be positive, got {linear_multiplier}"
                )));
            }
            let trial_id = u32::try_from(trials.len())
                .map_err(|error| methodology(format!("classical trial id exceeds u32: {error}")))?;
            let linear_scale = linear_multiplier.to_f64().ok_or_else(|| {
                methodology(format!(
                    "linear alpha multiplier {linear_multiplier} cannot be represented as f64"
                ))
            })?;
            let alpha = base.linear.alpha * linear_scale;
            if !alpha.is_finite() {
                return Err(methodology("scaled linear alpha is non-finite".to_owned()));
            }
            trials.push(Trial {
                trial_id,
                label: format!("linear_x{linear_multiplier}"),
                weighted_factor_objective: None,
                classical_params: Some(ClassicalParams {
                    forest: base.forest,
                    linear: LinearParams {
                        alpha,
                        ..base.linear
                    },
                }),
            });
        }
        Ok(trials)
    }
}

impl ClassicalTrialGrid {
    #[cfg(not(feature = "ml-classical"))]
    fn generate_classical(&self) -> QuantResult<Vec<Trial>> {
        Err(ResearchError::ValidationMethodology {
            detail: format!(
                "classical trial grid requires the `ml-classical` feature \
                 (forest_multipliers={}, linear_multipliers={}, max_trials={})",
                self.forest_n_trees_multipliers.len(),
                self.linear_alpha_multipliers.len(),
                self.max_trials
            ),
        }
        .into())
    }
}

fn validate_grid_size(expanded: usize, max_trials: u32) -> QuantResult<()> {
    if expanded == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: "trial grid expanded to zero trials".to_owned(),
        }
        .into());
    }
    let max_trials = usize::try_from(max_trials).map_err(|error| {
        methodology(format!(
            "max_trials cannot be represented as usize: {error}"
        ))
    })?;
    if expanded > max_trials {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "trial grid expands to {expanded} trials, exceeding max_trials={max_trials}"
            ),
        }
        .into());
    }
    Ok(())
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        runtime_config::RankLossKind, types::model_training::TrainingObjectiveSpec,
    };
    use rust_decimal_macros::dec;

    #[cfg(feature = "ml-classical")]
    use super::ClassicalTrialGrid;
    use super::{Trial, TrialGridSpec, WeightedFactorTrialGrid};

    #[test]
    fn weighted_factor_expands_product() {
        let grid = TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
            lambda_multipliers: vec![dec!(0.5), dec!(1), dec!(2)],
            rank_loss_kinds: vec![
                RankLossKind::RankIcWeightedRanknet,
                RankLossKind::PairwiseRanknet,
            ],
            max_trials: 32,
        });
        let trials = grid
            .generate(&TrainingObjectiveSpec::default())
            .expect("grid");
        assert_eq!(
            trials.len(),
            6,
            "3 multipliers x 2 rank-loss kinds = 6 trials"
        );
        let ids: Vec<u32> = trials.iter().map(|t: &Trial| t.trial_id).collect();
        assert_eq!(
            ids,
            (0..6).collect::<Vec<_>>(),
            "trial ids must be deterministic and ordered"
        );
    }

    #[test]
    fn weighted_factor_grid_base() {
        let base = TrainingObjectiveSpec {
            lambda_tail: dec!(0.5),
            lambda_turnover: dec!(0.2),
            lambda_l2: dec!(0.01),
            ..TrainingObjectiveSpec::default()
        };
        let grid = TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
            lambda_multipliers: vec![dec!(2)],
            rank_loss_kinds: vec![RankLossKind::RankIcWeightedRanknet],
            max_trials: 8,
        });
        let trials = grid.generate(&base).expect("grid");
        let objective = trials[0]
            .weighted_factor_objective
            .as_ref()
            .expect("objective");
        assert_eq!(objective.lambda_tail, dec!(1.0));
        assert_eq!(objective.lambda_turnover, dec!(0.4));
        assert_eq!(objective.lambda_l2, dec!(0.02));
    }

    #[test]
    fn grid_rejected_not_truncated() {
        let grid = TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
            lambda_multipliers: vec![dec!(0.5), dec!(1), dec!(2)],
            rank_loss_kinds: vec![
                RankLossKind::RankIcWeightedRanknet,
                RankLossKind::PairwiseRanknet,
            ],
            max_trials: 4, // grid expands to 6 > 4
        });
        assert!(grid.generate(&TrainingObjectiveSpec::default()).is_err());
    }

    #[cfg(feature = "ml-classical")]
    #[test]
    fn classical_grid_not_cartesian() {
        let grid = TrialGridSpec::Classical(ClassicalTrialGrid {
            forest_n_trees_multipliers: vec![dec!(0.5), dec!(1), dec!(2)],
            linear_alpha_multipliers: vec![dec!(0.5), dec!(1)],
            max_trials: 32,
        });
        let trials = grid
            .generate(&TrainingObjectiveSpec::default())
            .expect("classical grid");
        assert_eq!(
            trials.len(),
            5,
            "3 forest + 2 linear multipliers (not 3×2=6)"
        );
    }
}
