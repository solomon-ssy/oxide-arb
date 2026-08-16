//! The only floating-point boundary in portfolio construction: integer-scaled economics to `HiGHS`.

use std::{collections::BTreeMap, time::Instant};

use highs::{Col, HighsModelStatus, Model, Row, RowProblem, Sense, Solution};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{config::PortfolioSolverDeployConfig, hashing::CanonicalDigest};
use rust_decimal::prelude::ToPrimitive;

use super::global::{PreparedGlobalModel, PreparedTier};

const BINARY_TOLERANCE: f64 = 1e-8;
// HiGHS 1.14 defines 1e6/1e-4 as its large-bound and objective-range targets.
// Power-of-two user scaling preserves the exact integer model at the f64 boundary.
const HIGHS_LARGE_BOUND_TARGET: u128 = 1_000_000;
const HIGHS_LARGE_OBJECTIVE_TARGET: f64 = 1e6;
const HIGHS_SMALL_MATRIX_VALUE: f64 = 1e-9;
const HIGHS_SMALL_OBJECTIVE_TARGET: f64 = 1e-4;
const MAX_EXACT_F64_INTEGER: i64 = 9_007_199_254_740_991;
const MAX_SAFE_BOUND_SCALE_SHIFT: u32 = 29;
const OPTIMAL_GAP_TOLERANCE: f64 = 1e-12;

pub(super) struct SolvedGlobal {
    pub selected: Vec<usize>,
    pub lexicographic_solve_count: u32,
    pub tie_break_stages: u32,
    pub tie_break_proof_count: u32,
    pub lexicographic_model_build_count: u32,
    pub lexicographic_warm_start_count: u32,
    pub bound_scale_exponent: i32,
}

pub(super) struct SolvedMarginals {
    pub selections: Vec<Vec<usize>>,
    pub model_build_count: u32,
    pub solve_count: u32,
    pub model_reuse_count: u32,
}

#[derive(Debug, Clone, Copy)]
enum SolveStage {
    Robust,
    Nominal,
    Cvar,
    Capital,
    Tie { pass: u32 },
    TieUniqueness,
}

#[derive(Debug, Clone, Copy)]
struct TieLock {
    pass: u32,
    optimum: i64,
}

#[derive(Default)]
struct ObjectiveLocks {
    robust: Option<i64>,
    nominal: Option<i64>,
    cvar: Option<i64>,
    capital: Option<i64>,
    ties: Vec<TieLock>,
}

#[derive(Clone)]
struct LinearForm {
    constant: i64,
    terms: Vec<(Col, i64)>,
}

impl LinearForm {
    const fn new(constant: i64, terms: Vec<(Col, i64)>) -> Self {
        Self { constant, terms }
    }

    fn factors(&self) -> QuantResult<Vec<(Col, f64)>> {
        self.terms
            .iter()
            .map(|(column, coefficient)| Ok((*column, exact_f64(*coefficient)?)))
            .collect()
    }

    fn shifted_bound(&self, bound: i64, field: &'static str) -> QuantResult<f64> {
        let shifted =
            bound
                .checked_sub(self.constant)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field,
                    detail: "constraint bound minus expression constant overflowed i64".to_owned(),
                })?;
        exact_f64(shifted)
    }
}

struct SolverColumns {
    tiers: Vec<Col>,
    eta: Col,
    excess: Vec<Col>,
    robust_floor: Col,
    all: Vec<Col>,
}

struct ModelForms {
    distributions: Vec<LinearForm>,
    nominal: LinearForm,
    scenarios: Vec<LinearForm>,
    cvar: LinearForm,
    capital: LinearForm,
}

impl ModelForms {
    fn new(prepared: &PreparedGlobalModel, columns: &SolverColumns) -> Self {
        let distributions = prepared
            .existing_distribution_numerators
            .iter()
            .enumerate()
            .map(|(distribution_index, existing)| {
                LinearForm::new(
                    *existing,
                    columns
                        .tiers
                        .iter()
                        .zip(&prepared.tiers)
                        .map(|(column, tier)| {
                            (*column, tier.distribution_numerators[distribution_index])
                        })
                        .collect(),
                )
            })
            .collect();
        let nominal = LinearForm::new(
            prepared.existing_nominal_numerator,
            columns
                .tiers
                .iter()
                .zip(&prepared.tiers)
                .map(|(column, tier)| (*column, tier.nominal_numerator))
                .collect(),
        );
        let scenarios = (0..prepared.scenario_count)
            .map(|scenario_index| {
                LinearForm::new(
                    prepared.existing_scenario_net_micro[scenario_index],
                    columns
                        .tiers
                        .iter()
                        .zip(&prepared.tiers)
                        .map(|(column, tier)| {
                            (*column, tier.scenario_risk_net_micro[scenario_index])
                        })
                        .collect(),
                )
            })
            .collect();
        let mut cvar_terms = Vec::with_capacity(columns.excess.len().saturating_add(1));
        cvar_terms.push((columns.eta, prepared.tail_mass_bps));
        cvar_terms.extend(
            columns
                .excess
                .iter()
                .zip(&prepared.nominal_weights)
                .map(|(column, weight)| (*column, *weight)),
        );
        let cvar = LinearForm::new(0, cvar_terms);
        let capital = LinearForm::new(
            prepared.existing_capital_hours_micro,
            columns
                .tiers
                .iter()
                .zip(&prepared.tiers)
                .map(|(column, tier)| (*column, tier.capital_hours_micro))
                .collect(),
        );
        Self {
            distributions,
            nominal,
            scenarios,
            cvar,
            capital,
        }
    }
}

struct ConstraintBuilder<'a> {
    problem: &'a mut RowProblem,
    prepared: &'a PreparedGlobalModel,
    columns: &'a SolverColumns,
    forms: &'a ModelForms,
}

impl ConstraintBuilder<'_> {
    fn add_all(&mut self) -> QuantResult<()> {
        self.add_count()?;
        self.add_capital()?;
        self.add_exposure()?;
        self.add_exclusivity()?;
        self.add_buckets()?;
        self.add_tail()?;
        self.add_robust_floor()
    }

    fn add_count(&mut self) -> QuantResult<()> {
        let count = LinearForm::new(
            0,
            self.columns
                .tiers
                .iter()
                .map(|column| (*column, 1_i64))
                .collect(),
        );
        self.add_upper(&count, i64::from(self.prepared.top_n), "top_n")?;
        let open_room = i64::from(self.prepared.exposure_limits.open_recommendations)
            .checked_sub(i64::from(self.prepared.existing_open_recommendations))
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: "hard_constraints",
                detail: "existing open recommendations exceed the governed maximum".to_owned(),
            })?;
        self.add_upper(&count, open_room, "open_recommendations")
    }

    fn add_capital(&mut self) -> QuantResult<()> {
        let notional = LinearForm::new(
            0,
            self.columns
                .tiers
                .iter()
                .zip(&self.prepared.tiers)
                .map(|(column, tier)| (*column, tier.notional_micro))
                .collect(),
        );
        self.add_upper(
            &notional,
            self.prepared.available_cash_limit_micro,
            "available_cash",
        )?;
        let open_capital_room = self
            .prepared
            .max_open_capital_micro
            .checked_sub(self.prepared.existing_open_capital_micro)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "open_capital_constraint",
                detail: "maximum open capital minus existing capital overflowed i64".to_owned(),
            })?;
        self.add_upper(&notional, open_capital_room, "open_capital")
    }

    fn add_exposure(&mut self) -> QuantResult<()> {
        self.add_unit_groups(|tier| tier.candidate_key.as_str())?;
        self.add_unit_groups(|tier| tier.market_key.as_str())?;
        self.add_group_caps(
            &self.prepared.existing_market_exposure,
            self.prepared.exposure_limits.market_micro,
            |tier| tier.market_key.as_str(),
        )?;
        self.add_group_caps(
            &self.prepared.existing_event_exposure,
            self.prepared.exposure_limits.event_micro,
            |tier| tier.event_key.as_str(),
        )?;
        self.add_group_caps(
            &self.prepared.existing_category_exposure,
            self.prepared.exposure_limits.category_micro,
            |tier| tier.category_key.as_str(),
        )?;
        self.add_group_caps(
            &self.prepared.existing_route_exposure,
            self.prepared.exposure_limits.route_micro,
            |tier| tier.route_key.as_str(),
        )?;
        for (column, tier) in self.columns.tiers.iter().zip(&self.prepared.tiers) {
            let single = LinearForm::new(0, vec![(*column, tier.notional_micro)]);
            self.add_upper(
                &single,
                self.prepared.exposure_limits.single_micro,
                "single_recommendation_exposure",
            )?;
        }
        Ok(())
    }

    fn add_unit_groups(&mut self, key: impl Fn(&PreparedTier) -> &str) -> QuantResult<()> {
        let mut groups = BTreeMap::<String, LinearForm>::new();
        for (tier, column) in self.prepared.tiers.iter().zip(&self.columns.tiers) {
            groups
                .entry(key(tier).to_owned())
                .or_insert_with(|| LinearForm::new(0, Vec::new()))
                .terms
                .push((*column, 1));
        }
        for form in groups.into_values() {
            self.add_upper(&form, 1, "tier_one_hot")?;
        }
        Ok(())
    }

    fn add_group_caps(
        &mut self,
        existing: &BTreeMap<String, i64>,
        cap: i64,
        key: impl Fn(&PreparedTier) -> &str,
    ) -> QuantResult<()> {
        let mut groups = existing
            .iter()
            .map(|(group, value)| (group.clone(), LinearForm::new(*value, Vec::new())))
            .collect::<BTreeMap<_, _>>();
        for (tier, column) in self.prepared.tiers.iter().zip(&self.columns.tiers) {
            groups
                .entry(key(tier).to_owned())
                .or_insert_with(|| LinearForm::new(0, Vec::new()))
                .terms
                .push((*column, tier.notional_micro));
        }
        for form in groups.into_values() {
            self.add_upper(&form, cap, "grouped_exposure")?;
        }
        Ok(())
    }

    fn add_exclusivity(&mut self) -> QuantResult<()> {
        for group in &self.prepared.exclusivity_groups {
            let form = LinearForm::new(
                0,
                group
                    .iter()
                    .map(|index| (self.columns.tiers[*index], 1_i64))
                    .collect(),
            );
            self.add_upper(&form, 1, "structural_exclusivity")?;
        }
        Ok(())
    }

    fn add_buckets(&mut self) -> QuantResult<()> {
        for (bucket_index, cap) in self.prepared.bucket_caps.iter().enumerate() {
            let form = LinearForm::new(
                self.prepared.existing_bucket_capital[bucket_index],
                self.columns
                    .tiers
                    .iter()
                    .zip(&self.prepared.tiers)
                    .map(|(column, tier)| (*column, tier.bucket_capital_micro[bucket_index]))
                    .collect(),
            );
            self.add_upper(&form, *cap, "capital_time_bucket")?;
        }
        Ok(())
    }

    fn add_tail(&mut self) -> QuantResult<()> {
        let drawdown_floor = self
            .prepared
            .current_drawdown_micro
            .checked_sub(self.prepared.max_drawdown_micro)
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: "hard_constraints",
                detail: "drawdown scenario floor overflow".to_owned(),
            })?;
        for ((scenario, excess), _scenario_index) in self
            .forms
            .scenarios
            .iter()
            .zip(&self.columns.excess)
            .zip(0..self.prepared.scenario_count)
        {
            self.add_lower(
                scenario,
                self.prepared.max_scenario_loss_micro.saturating_neg(),
                "maximum_scenario_loss",
            )?;
            self.add_lower(scenario, drawdown_floor, "drawdown")?;
            let mut epigraph = scenario.clone();
            epigraph.terms.push((self.columns.eta, 1));
            epigraph.terms.push((*excess, 1));
            self.add_lower(&epigraph, 0, "cvar_epigraph")?;
        }
        self.add_upper(
            &self.forms.cvar,
            self.prepared.max_cvar_numerator,
            "maximum_cvar",
        )
    }

    fn add_robust_floor(&mut self) -> QuantResult<()> {
        for distribution in &self.forms.distributions {
            let mut floor = distribution.clone();
            floor.terms.push((self.columns.robust_floor, -1));
            self.add_lower(&floor, 0, "robust_floor")?;
        }
        Ok(())
    }

    fn add_lower(&mut self, form: &LinearForm, bound: i64, field: &'static str) -> QuantResult<()> {
        let lower = form.shifted_bound(bound, field)?;
        self.problem.add_row(lower.., form.factors()?);
        Ok(())
    }

    fn add_upper(&mut self, form: &LinearForm, bound: i64, field: &'static str) -> QuantResult<()> {
        let upper = form.shifted_bound(bound, field)?;
        self.problem.add_row(..=upper, form.factors()?);
        Ok(())
    }
}

struct PersistentModel<'a> {
    prepared: &'a PreparedGlobalModel,
    model: Option<Model>,
    columns: SolverColumns,
    forms: ModelForms,
    objective_lock_relaxations: Vec<Col>,
    last_solution: Option<Vec<f64>>,
    warm_start_count: u32,
    bound_scale_exponent: i32,
}

impl<'a> PersistentModel<'a> {
    fn new(
        prepared: &'a PreparedGlobalModel,
        deploy: &PortfolioSolverDeployConfig,
    ) -> QuantResult<Self> {
        let derived_bound_scale = bound_scale_exponent(prepared.maximum_solver_magnitude()?)?;
        let mut problem = RowProblem::new();
        let tiers = prepared
            .tiers
            .iter()
            .map(|_| problem.add_integer_column(0.0, 0_i32..=1_i32))
            .collect::<Vec<_>>();
        let max_loss_bound = exact_f64(prepared.max_scenario_loss_micro)?;
        let eta = problem.add_column(0.0, 0.0..=max_loss_bound);
        let excess = (0..prepared.scenario_count)
            .map(|_| problem.add_column(0.0, 0.0..=max_loss_bound))
            .collect::<Vec<_>>();
        let robust_floor = problem.add_column(0.0, f64::NEG_INFINITY..);
        let mut all = tiers.clone();
        all.push(eta);
        all.extend(excess.iter().copied());
        all.push(robust_floor);
        let columns = SolverColumns {
            tiers,
            eta,
            excess,
            robust_floor,
            all,
        };
        let forms = ModelForms::new(prepared, &columns);
        ConstraintBuilder {
            problem: &mut problem,
            prepared,
            columns: &columns,
            forms: &forms,
        }
        .add_all()?;
        let mut model = problem.try_optimise(Sense::Maximise).map_err(|error| {
            ReportError::PortfolioOptimization {
                stage: "model_build",
                detail: format!("HiGHS rejected the canonical model: {error:?}"),
            }
        })?;
        Self::configure(&mut model, deploy, derived_bound_scale)?;
        Ok(Self {
            prepared,
            model: Some(model),
            columns,
            forms,
            objective_lock_relaxations: Vec::new(),
            last_solution: None,
            warm_start_count: 0,
            bound_scale_exponent: derived_bound_scale,
        })
    }

    fn configure(
        model: &mut Model,
        deploy: &PortfolioSolverDeployConfig,
        bound_scale_exponent: i32,
    ) -> QuantResult<()> {
        let threads =
            i32::try_from(deploy.threads).map_err(|error| ReportError::PortfolioOptimization {
                stage: "model_build",
                detail: format!("HiGHS thread count is outside i32: {error}"),
            })?;
        let options = [
            model.try_set_option("threads", threads),
            model.try_set_option("parallel", "off"),
            model.try_set_option("random_seed", 0_i32),
            model.try_set_option("mip_rel_gap", 0.0_f64),
            model.try_set_option("mip_abs_gap", 0.0_f64),
            model.try_set_option("mip_feasibility_tolerance", 1e-9_f64),
            model.try_set_option("small_matrix_value", HIGHS_SMALL_MATRIX_VALUE),
            model.try_set_option("user_bound_scale", bound_scale_exponent),
            model.try_set_option("output_flag", false),
            model.try_set_option("log_to_console", false),
        ];
        if options.iter().any(Result::is_err) {
            return Err(ReportError::PortfolioOptimization {
                stage: "model_build",
                detail: "HiGHS rejected one or more deterministic solver options".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn solve(
        &mut self,
        stage: SolveStage,
        deadline: Instant,
        locks: &ObjectiveLocks,
    ) -> QuantResult<Vec<usize>> {
        self.set_objective(stage)?;
        self.solve_current(stage, deadline, locks)
    }

    fn prove_tie_unique(
        &mut self,
        incumbent: &[usize],
        deadline: Instant,
        locks: &ObjectiveLocks,
    ) -> QuantResult<bool> {
        self.set_uniqueness_objective(incumbent)?;
        let alternative = self.solve_current(SolveStage::TieUniqueness, deadline, locks)?;
        Ok(alternative == incumbent)
    }

    fn solve_current(
        &mut self,
        stage: SolveStage,
        deadline: Instant,
        locks: &ObjectiveLocks,
    ) -> QuantResult<Vec<usize>> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: "the end-to-end lexicographic solver deadline was exhausted".to_owned(),
            }
            .into());
        }
        let model = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: "the persistent HiGHS model is unavailable".to_owned(),
            })?;
        model
            .try_set_option("time_limit", remaining.as_secs_f64())
            .map_err(|error| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: format!("HiGHS rejected the remaining deadline: {error:?}"),
            })?;
        if let Some(values) = &self.last_solution {
            model
                .try_set_solution(Some(values), None, None, None)
                .map_err(|error| ReportError::PortfolioOptimization {
                    stage: stage.name(),
                    detail: format!("HiGHS rejected the exact previous-stage MIP start: {error:?}"),
                })?;
            self.warm_start_count = next_stage(self.warm_start_count)?;
        }
        let model = self
            .model
            .take()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: "the persistent HiGHS model was consumed before solve".to_owned(),
            })?;
        let solved = model
            .try_solve()
            .map_err(|error| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: format!("HiGHS execution failed: {error:?}"),
            })?;
        let outcome = (|| -> QuantResult<_> {
            let status = solved.status();
            if status != HighsModelStatus::Optimal {
                let baseline = match self.prepared.verify(&[]) {
                    Ok(_) => {
                        "exact empty-selection baseline satisfies every hard constraint".to_owned()
                    }
                    Err(error) => format!(
                        "exact empty-selection baseline is outside the governed envelope: {error}"
                    ),
                };
                return Err(ReportError::PortfolioOptimization {
                    stage: stage.name(),
                    detail: format!("HiGHS returned non-optimal status {status:?}; {baseline}"),
                }
                .into());
            }
            let mip_gap = solved.mip_gap();
            if !mip_gap.is_finite() || mip_gap.abs() > OPTIMAL_GAP_TOLERANCE {
                return Err(ReportError::PortfolioOptimization {
                    stage: stage.name(),
                    detail: format!("HiGHS optimal status carried invalid MIP gap {mip_gap}"),
                }
                .into());
            }
            let solution = solved.get_solution();
            if solution.columns().iter().any(|value| !value.is_finite()) {
                return Err(ReportError::PortfolioPostCheck {
                    detail: format!("{} returned a non-finite solver variable", stage.name()),
                }
                .into());
            }
            let selected = extract_binary(&solution, &self.columns.tiers, stage.name())?;
            let _ = self.prepared.verify(&selected)?;
            verify_locks(self.prepared, &selected, stage, locks)?;
            Ok((selected, solution.columns().to_vec()))
        })();
        self.model = Some(solved.into());
        let (selected, values) = outcome?;
        self.last_solution = Some(values);
        Ok(selected)
    }

    fn set_candidate_upper(&mut self, candidate_key: &str, upper: i32) -> QuantResult<()> {
        if !(0..=1).contains(&upper) {
            return Err(ReportError::PortfolioOptimization {
                stage: "marginal_ranking",
                detail: format!("candidate upper bound {upper} is not binary"),
            }
            .into());
        }
        let model = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: "marginal_ranking",
                detail: "the persistent marginal model is unavailable".to_owned(),
            })?;
        let mut matched = false;
        for (column, tier) in self.columns.tiers.iter().zip(&self.prepared.tiers) {
            if tier.candidate_key == candidate_key {
                model.change_column_bounds(*column, 0_i32..=upper);
                matched = true;
            }
        }
        if !matched {
            return Err(ReportError::PortfolioPostCheck {
                detail: "marginal ranking candidate is absent from the prepared tier catalog"
                    .to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn unlock_objectives(&mut self) -> QuantResult<()> {
        let model = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: "marginal_ranking",
                detail: "the persistent HiGHS model is unavailable for objective unlock".to_owned(),
            })?;
        for column in &self.objective_lock_relaxations {
            model.change_column_bounds(*column, 0.0..);
        }
        self.last_solution = None;
        Ok(())
    }

    fn set_objective(&mut self, stage: SolveStage) -> QuantResult<()> {
        let objective_scale_exponent = self.objective_scale(stage)?;
        let model = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: "the persistent HiGHS model is unavailable for objective mutation"
                    .to_owned(),
            })?;
        model
            .try_set_option("user_objective_scale", objective_scale_exponent)
            .map_err(|error| ReportError::PortfolioOptimization {
                stage: stage.name(),
                detail: format!("HiGHS rejected the derived objective scale: {error:?}"),
            })?;
        for column in &self.columns.all {
            model.change_column_cost(*column, 0.0);
        }
        match stage {
            SolveStage::Robust => model.change_column_cost(self.columns.robust_floor, 1.0),
            SolveStage::Nominal => {
                for (column, tier) in self.columns.tiers.iter().zip(&self.prepared.tiers) {
                    model.change_column_cost(*column, exact_f64(tier.nominal_numerator)?);
                }
            }
            SolveStage::Cvar => {
                model
                    .change_column_cost(self.columns.eta, -exact_f64(self.prepared.tail_mass_bps)?);
                for (column, weight) in self
                    .columns
                    .excess
                    .iter()
                    .zip(&self.prepared.nominal_weights)
                {
                    model.change_column_cost(*column, -exact_f64(*weight)?);
                }
            }
            SolveStage::Capital => {
                for (column, tier) in self.columns.tiers.iter().zip(&self.prepared.tiers) {
                    model.change_column_cost(*column, -exact_f64(tier.capital_hours_micro)?);
                }
            }
            SolveStage::Tie { pass } => {
                for (index, column) in self.columns.tiers.iter().enumerate() {
                    model.change_column_cost(
                        *column,
                        exact_f64(tie_weight(self.prepared, index, pass)?)?,
                    );
                }
            }
            SolveStage::TieUniqueness => {
                return Err(ReportError::PortfolioOptimization {
                    stage: stage.name(),
                    detail: "tie uniqueness requires an incumbent selection".to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn objective_scale(&self, stage: SolveStage) -> QuantResult<i32> {
        let bound_scale = 2_f64.powi(self.bound_scale_exponent);
        let maximum = match stage {
            SolveStage::Robust => 1.0,
            SolveStage::Nominal => self.prepared.tiers.iter().try_fold(
                0.0_f64,
                |maximum, tier| -> QuantResult<f64> {
                    Ok(maximum.max(exact_f64(tier.nominal_numerator)?.abs() * bound_scale))
                },
            )?,
            SolveStage::Cvar => self
                .prepared
                .nominal_weights
                .iter()
                .copied()
                .chain([self.prepared.tail_mass_bps])
                .try_fold(0.0_f64, |maximum, coefficient| -> QuantResult<f64> {
                    Ok(maximum.max(exact_f64(coefficient)?.abs()))
                })?,
            SolveStage::Capital => self.prepared.tiers.iter().try_fold(
                0.0_f64,
                |maximum, tier| -> QuantResult<f64> {
                    Ok(maximum.max(exact_f64(tier.capital_hours_micro)?.abs() * bound_scale))
                },
            )?,
            SolveStage::Tie { pass } => self.prepared.tiers.iter().enumerate().try_fold(
                0.0_f64,
                |maximum, (index, _)| -> QuantResult<f64> {
                    Ok(maximum
                        .max(exact_f64(tie_weight(self.prepared, index, pass)?)? * bound_scale))
                },
            )?,
            SolveStage::TieUniqueness => bound_scale,
        };
        objective_scale_exponent(maximum)
    }

    fn set_uniqueness_objective(&mut self, incumbent: &[usize]) -> QuantResult<()> {
        let objective_scale_exponent = self.objective_scale(SolveStage::TieUniqueness)?;
        let model = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage: SolveStage::TieUniqueness.name(),
                detail: "the persistent HiGHS model is unavailable for uniqueness proof".to_owned(),
            })?;
        model
            .try_set_option("user_objective_scale", objective_scale_exponent)
            .map_err(|error| ReportError::PortfolioOptimization {
                stage: SolveStage::TieUniqueness.name(),
                detail: format!("HiGHS rejected the derived objective scale: {error:?}"),
            })?;
        for column in &self.columns.all {
            model.change_column_cost(*column, 0.0);
        }
        for (index, column) in self.columns.tiers.iter().enumerate() {
            let coefficient = if incumbent.binary_search(&index).is_ok() {
                -1.0
            } else {
                1.0
            };
            model.change_column_cost(*column, coefficient);
        }
        Ok(())
    }

    fn lock_robust(&mut self, optimum: i64) -> QuantResult<()> {
        let distributions = self.forms.distributions.clone();
        for form in distributions {
            self.add_lower_lock(&form, optimum, "robust_expected_net")?;
        }
        Ok(())
    }

    fn lock_nominal(&mut self, optimum: i64) -> QuantResult<()> {
        let form = self.forms.nominal.clone();
        self.add_lower_lock(&form, optimum, "nominal_expected_net")
    }

    fn lock_cvar(&mut self, optimum: i64) -> QuantResult<()> {
        let form = self.forms.cvar.clone();
        self.add_upper_lock(&form, optimum, "cvar")
    }

    fn lock_capital(&mut self, optimum: i64) -> QuantResult<()> {
        let form = self.forms.capital.clone();
        self.add_upper_lock(&form, optimum, "capital_occupancy")
    }

    fn lock_tie(&mut self, lock: TieLock) -> QuantResult<()> {
        let form = tie_form(self.prepared, &self.columns.tiers, lock.pass)?;
        self.add_lower_lock(&form, lock.optimum, "stable_tie_break")
    }

    fn add_lower_lock(
        &mut self,
        form: &LinearForm,
        bound: i64,
        stage: &'static str,
    ) -> QuantResult<()> {
        let lower = form.shifted_bound(bound, stage)?;
        let row = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage,
                detail: "the persistent HiGHS model is unavailable for an objective lock"
                    .to_owned(),
            })?
            .try_add_row(lower.., form.factors()?)
            .map_err(|error| ReportError::PortfolioOptimization {
                stage,
                detail: format!("HiGHS rejected the exact lower objective lock: {error:?}"),
            })?;
        self.add_lock_relaxation(row, 1.0, stage)
    }

    fn add_upper_lock(
        &mut self,
        form: &LinearForm,
        bound: i64,
        stage: &'static str,
    ) -> QuantResult<()> {
        let upper = form.shifted_bound(bound, stage)?;
        let row = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage,
                detail: "the persistent HiGHS model is unavailable for an objective lock"
                    .to_owned(),
            })?
            .try_add_row(..=upper, form.factors()?)
            .map_err(|error| ReportError::PortfolioOptimization {
                stage,
                detail: format!("HiGHS rejected the exact upper objective lock: {error:?}"),
            })?;
        self.add_lock_relaxation(row, -1.0, stage)
    }

    fn add_lock_relaxation(
        &mut self,
        row: Row,
        coefficient: f64,
        stage: &'static str,
    ) -> QuantResult<()> {
        let column = self
            .model
            .as_mut()
            .ok_or_else(|| ReportError::PortfolioOptimization {
                stage,
                detail: "the persistent HiGHS model is unavailable for a lock relaxation"
                    .to_owned(),
            })?
            .try_add_column(0.0, 0.0..=0.0, [(row, coefficient)])
            .map_err(|error| ReportError::PortfolioOptimization {
                stage,
                detail: format!("HiGHS rejected the objective-lock relaxation: {error:?}"),
            })?;
        self.columns.all.push(column);
        self.objective_lock_relaxations.push(column);
        if let Some(solution) = &mut self.last_solution {
            solution.push(0.0);
        }
        Ok(())
    }
}

fn solve_lexicographic_with(
    solver: &mut PersistentModel<'_>,
    prepared: &PreparedGlobalModel,
    deadline: Instant,
) -> QuantResult<SolvedGlobal> {
    let mut locks = ObjectiveLocks::default();
    let mut stage_count = 0_u32;

    let robust_selected = solver.solve(SolveStage::Robust, deadline, &locks)?;
    stage_count = next_stage(stage_count)?;
    let robust = prepared.objectives(&robust_selected)?.robust_numerator;
    locks.robust = Some(robust);
    solver.lock_robust(robust)?;

    let nominal_selected = solver.solve(SolveStage::Nominal, deadline, &locks)?;
    stage_count = next_stage(stage_count)?;
    let nominal = prepared.objectives(&nominal_selected)?.nominal_numerator;
    locks.nominal = Some(nominal);
    solver.lock_nominal(nominal)?;

    let cvar_selected = solver.solve(SolveStage::Cvar, deadline, &locks)?;
    stage_count = next_stage(stage_count)?;
    let cvar = prepared.objectives(&cvar_selected)?.cvar_numerator;
    locks.cvar = Some(cvar);
    solver.lock_cvar(cvar)?;

    let capital_selected = solver.solve(SolveStage::Capital, deadline, &locks)?;
    stage_count = next_stage(stage_count)?;
    let capital = prepared.objectives(&capital_selected)?.capital_hours_micro;
    locks.capital = Some(capital);
    solver.lock_capital(capital)?;

    let mut selected = capital_selected;
    let mut tie_break_stages = 0_u32;
    let mut tie_break_proof_count = 0_u32;
    let mut unique = false;
    for pass in 0..prepared.tiers.len() {
        let pass = u32::try_from(pass).map_err(|error| ReportError::NumericOverflow {
            field: "stable_tie_break_pass",
            detail: error.to_string(),
        })?;
        selected = solver.solve(SolveStage::Tie { pass }, deadline, &locks)?;
        stage_count = next_stage(stage_count)?;
        tie_break_stages = next_stage(tie_break_stages)?;
        let lock = TieLock {
            pass,
            optimum: tie_value(prepared, &selected, pass)?,
        };
        locks.ties.push(lock);
        solver.lock_tie(lock)?;
        unique = solver.prove_tie_unique(&selected, deadline, &locks)?;
        stage_count = next_stage(stage_count)?;
        tie_break_proof_count = next_stage(tie_break_proof_count)?;
        if unique {
            break;
        }
    }
    if !unique {
        return Err(ReportError::PortfolioOptimization {
            stage: "stable_tie_break_proof",
            detail: "canonical identity weights did not isolate one exact selection".to_owned(),
        }
        .into());
    }

    Ok(SolvedGlobal {
        selected,
        lexicographic_solve_count: stage_count,
        tie_break_stages,
        tie_break_proof_count,
        lexicographic_model_build_count: 1,
        lexicographic_warm_start_count: solver.warm_start_count,
        bound_scale_exponent: solver.bound_scale_exponent,
    })
}

pub(super) fn solve_lexicographic(
    prepared: &PreparedGlobalModel,
    deadline: Instant,
    deploy: &PortfolioSolverDeployConfig,
) -> QuantResult<SolvedGlobal> {
    let mut solver = PersistentModel::new(prepared, deploy)?;
    solve_lexicographic_with(&mut solver, prepared, deadline)
}

pub(super) fn solve_publishable(
    prepared: &PreparedGlobalModel,
    deadline: Instant,
    deploy: &PortfolioSolverDeployConfig,
) -> QuantResult<(SolvedGlobal, SolvedMarginals)> {
    let mut solver = PersistentModel::new(prepared, deploy)?;
    let lexicographic = solve_lexicographic_with(&mut solver, prepared, deadline)?;
    solver.unlock_objectives()?;
    let marginals = solve_marginals_with(&mut solver, prepared, deadline, &lexicographic.selected)?;
    Ok((lexicographic, marginals))
}

fn solve_marginals_with(
    solver: &mut PersistentModel<'_>,
    prepared: &PreparedGlobalModel,
    deadline: Instant,
    selected: &[usize],
) -> QuantResult<SolvedMarginals> {
    solver.set_objective(SolveStage::Robust)?;
    let locks = ObjectiveLocks::default();
    let mut selections = Vec::with_capacity(selected.len());
    for index in selected {
        let candidate_key = prepared
            .tiers
            .get(*index)
            .ok_or_else(|| ReportError::PortfolioPostCheck {
                detail: "selected tier is absent from the marginal model".to_owned(),
            })?
            .candidate_key
            .as_str();
        solver.set_candidate_upper(candidate_key, 0)?;
        solver.last_solution = None;
        let outcome = solver.solve_current(SolveStage::Robust, deadline, &locks);
        if solver.model.is_some() {
            solver.set_candidate_upper(candidate_key, 1)?;
        }
        let selection = outcome?;
        if selection
            .iter()
            .any(|selected| prepared.tiers[*selected].candidate_key == candidate_key)
        {
            return Err(ReportError::PortfolioPostCheck {
                detail: "marginal solve selected the explicitly excluded candidate".to_owned(),
            }
            .into());
        }
        selections.push(selection);
    }
    let solve_count =
        u32::try_from(selections.len()).map_err(|error| ReportError::NumericOverflow {
            field: "marginal_solve_count",
            detail: error.to_string(),
        })?;
    Ok(SolvedMarginals {
        solve_count,
        selections,
        model_build_count: 0,
        model_reuse_count: solve_count,
    })
}

fn tie_form(
    prepared: &PreparedGlobalModel,
    variables: &[Col],
    pass: u32,
) -> QuantResult<LinearForm> {
    if variables.len() != prepared.tiers.len() || variables.is_empty() {
        return Err(ReportError::PortfolioOptimization {
            stage: "stable_tie_break",
            detail: "stable tie-break variables differ from the prepared tier catalog".to_owned(),
        }
        .into());
    }
    Ok(LinearForm::new(
        0,
        variables
            .iter()
            .enumerate()
            .map(|(index, variable)| Ok((*variable, tie_weight(prepared, index, pass)?)))
            .collect::<QuantResult<Vec<_>>>()?,
    ))
}

fn tie_value(prepared: &PreparedGlobalModel, selected: &[usize], pass: u32) -> QuantResult<i64> {
    selected.iter().try_fold(0_i64, |sum, index| {
        if *index >= prepared.tiers.len() {
            return Err(ReportError::PortfolioPostCheck {
                detail: "stable tie-break selection is outside the prepared tier catalog"
                    .to_owned(),
            }
            .into());
        }
        sum.checked_add(tie_weight(prepared, *index, pass)?)
            .ok_or_else(|| {
                ReportError::NumericOverflow {
                    field: "stable_tie_break",
                    detail: "tie-break objective overflow".to_owned(),
                }
                .into()
            })
    })
}

fn tie_weight(prepared: &PreparedGlobalModel, index: usize, pass: u32) -> QuantResult<i64> {
    let tier = prepared
        .tiers
        .get(index)
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "stable_tie_break",
            detail: "stable tie-break index is outside the prepared tier catalog".to_owned(),
        })?;
    let selection_limit = usize::try_from(prepared.top_n)
        .map_err(|error| ReportError::NumericOverflow {
            field: "stable_tie_break_selection_limit",
            detail: error.to_string(),
        })?
        .min(prepared.tiers.len());
    if selection_limit == 0 {
        return Err(ReportError::PortfolioOptimization {
            stage: "stable_tie_break",
            detail: "stable tie-break requires a positive TopN limit".to_owned(),
        }
        .into());
    }
    let weight_ceiling =
        prepared
            .tiers
            .len()
            .checked_mul(2)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "stable_tie_break_weight_ceiling",
                detail: "twice the prepared tier count overflowed usize".to_owned(),
            })?;
    let maximum_sum = weight_ceiling.checked_mul(selection_limit).ok_or_else(|| {
        ReportError::NumericOverflow {
            field: "stable_tie_break_maximum_sum",
            detail: "tie-break weight ceiling multiplied by TopN overflowed usize".to_owned(),
        }
    })?;
    let maximum_sum = i64::try_from(maximum_sum).map_err(|error| ReportError::NumericOverflow {
        field: "stable_tie_break_maximum_sum",
        detail: error.to_string(),
    })?;
    if maximum_sum > MAX_EXACT_F64_INTEGER {
        return Err(ReportError::PortfolioOptimization {
            stage: "stable_tie_break",
            detail: "stable identity objective can exceed the exact f64 integer range".to_owned(),
        }
        .into());
    }
    let weight_ceiling =
        u64::try_from(weight_ceiling).map_err(|error| ReportError::NumericOverflow {
            field: "stable_tie_break_weight_ceiling",
            detail: error.to_string(),
        })?;
    let digest = CanonicalDigest::content_hash_typed(
        "quant-pivot/stable-portfolio-tie-break",
        1,
        &(pass, tier.stable_key.as_str()),
    )?;
    let bytes = digest.as_bytes();
    let raw = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let weight = raw % weight_ceiling + 1;
    i64::try_from(weight).map_err(|error| {
        ReportError::NumericOverflow {
            field: "stable_tie_break_weight",
            detail: error.to_string(),
        }
        .into()
    })
}

fn extract_binary(
    solution: &Solution,
    variables: &[Col],
    stage: &'static str,
) -> QuantResult<Vec<usize>> {
    variables
        .iter()
        .enumerate()
        .filter_map(|(index, variable)| {
            let value = solution[*variable];
            if !value.is_finite() {
                return Some(Err(ReportError::PortfolioPostCheck {
                    detail: format!("{stage} returned a non-finite binary variable"),
                }
                .into()));
            }
            if value.abs() <= BINARY_TOLERANCE {
                None
            } else if (value - 1.0).abs() <= BINARY_TOLERANCE {
                Some(Ok(index))
            } else {
                Some(Err(ReportError::PortfolioPostCheck {
                    detail: format!("{stage} returned non-integral tier value {value}"),
                }
                .into()))
            }
        })
        .collect()
}

fn verify_locks(
    prepared: &PreparedGlobalModel,
    selected: &[usize],
    stage: SolveStage,
    locks: &ObjectiveLocks,
) -> QuantResult<()> {
    let objectives = prepared.objectives(selected)?;
    let comparisons = [
        (locks.robust, objectives.robust_numerator, "robust"),
        (locks.nominal, objectives.nominal_numerator, "nominal"),
        (locks.cvar, objectives.cvar_numerator, "CVaR"),
        (locks.capital, objectives.capital_hours_micro, "capital"),
    ];
    for (locked, actual, label) in comparisons {
        if let Some(locked) = locked
            && locked != actual
        {
            let delta = i128::from(actual) - i128::from(locked);
            return Err(ReportError::PortfolioPostCheck {
                detail: format!(
                    "{} exact {label} objective differs from its lexicographic lock: locked={locked}, actual={actual}, delta={delta}, selected_tiers={}",
                    stage.name(),
                    selected.len(),
                ),
            }
            .into());
        }
    }
    for tie in &locks.ties {
        if tie_value(prepared, selected, tie.pass)? != tie.optimum {
            return Err(ReportError::PortfolioPostCheck {
                detail: format!(
                    "{} exact stable tie-break objective differs from its pass {} lock",
                    stage.name(),
                    tie.pass,
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn bound_scale_exponent(maximum: i128) -> QuantResult<i32> {
    let maximum = u128::try_from(maximum).map_err(|error| ReportError::PortfolioOptimization {
        stage: "coefficient_scaling",
        detail: format!("solver magnitude must be non-negative: {error}"),
    })?;
    if maximum <= HIGHS_LARGE_BOUND_TARGET {
        return Ok(0);
    }
    let ratio = maximum
        .checked_add(HIGHS_LARGE_BOUND_TARGET - 1)
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "coefficient_scaling",
            detail: "solver magnitude ratio overflowed u128".to_owned(),
        })?
        / HIGHS_LARGE_BOUND_TARGET;
    let shift = u128::BITS - (ratio - 1).leading_zeros();
    if shift > MAX_SAFE_BOUND_SCALE_SHIFT {
        return Err(ReportError::PortfolioOptimization {
            stage: "coefficient_scaling",
            detail: format!(
                "solver magnitude {maximum} requires user_bound_scale=-{shift}, which would scale unit integer constraints to or below HiGHS small_matrix_value={HIGHS_SMALL_MATRIX_VALUE}; maximum safe shift is {MAX_SAFE_BOUND_SCALE_SHIFT}"
            ),
        }
        .into());
    }
    let shift = i32::try_from(shift).map_err(|error| ReportError::PortfolioOptimization {
        stage: "coefficient_scaling",
        detail: format!("bound scale shift is outside i32: {error}"),
    })?;
    Ok(-shift)
}

fn objective_scale_exponent(maximum: f64) -> QuantResult<i32> {
    if maximum == 0.0 {
        return Ok(0);
    }
    if !maximum.is_finite() || maximum < 0.0 {
        return Err(ReportError::PortfolioOptimization {
            stage: "objective_scaling",
            detail: format!("objective magnitude must be finite and non-negative, got {maximum}"),
        }
        .into());
    }
    let exponent = if maximum > HIGHS_LARGE_OBJECTIVE_TARGET {
        (HIGHS_LARGE_OBJECTIVE_TARGET / maximum).log2().floor()
    } else if maximum < HIGHS_SMALL_OBJECTIVE_TARGET {
        (HIGHS_SMALL_OBJECTIVE_TARGET / maximum).log2().ceil()
    } else {
        0.0
    };
    exponent.to_i32().ok_or_else(|| {
        ReportError::PortfolioOptimization {
            stage: "objective_scaling",
            detail: format!("objective scale exponent is outside i32: {exponent}"),
        }
        .into()
    })
}

fn exact_f64(value: i64) -> QuantResult<f64> {
    let converted = value
        .to_f64()
        .ok_or_else(|| ReportError::PortfolioOptimization {
            stage: "solver_boundary",
            detail: format!("integer coefficient {value} cannot be represented as f64"),
        })?;
    if converted.to_i64() != Some(value) {
        return Err(ReportError::PortfolioOptimization {
            stage: "solver_boundary",
            detail: format!("integer coefficient {value} is not exactly representable as f64"),
        }
        .into());
    }
    Ok(converted)
}

fn next_stage(current: u32) -> QuantResult<u32> {
    current.checked_add(1).ok_or_else(|| {
        ReportError::NumericOverflow {
            field: "portfolio_solve_stage_count",
            detail: "solve stage count overflow".to_owned(),
        }
        .into()
    })
}

impl SolveStage {
    const fn name(self) -> &'static str {
        match self {
            Self::Robust => "robust_expected_net",
            Self::Nominal => "nominal_expected_net",
            Self::Cvar => "cvar",
            Self::Capital => "capital_occupancy",
            Self::Tie { .. } => "stable_tie_break",
            Self::TieUniqueness => "stable_tie_break_proof",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };

    use quant_pivot_models::config::PortfolioSolverDeployConfig;

    use super::{
        HIGHS_LARGE_BOUND_TARGET, HIGHS_SMALL_MATRIX_VALUE, MAX_SAFE_BOUND_SCALE_SHIFT,
        bound_scale_exponent, objective_scale_exponent, solve_lexicographic, tie_weight,
    };
    use crate::portfolio::global::{PreparedExposureLimits, PreparedGlobalModel, PreparedTier};

    #[test]
    fn bound_scale_is_safe() {
        assert_eq!(
            bound_scale_exponent(1_000_000).expect("target magnitude needs no scaling"),
            0
        );
        assert_eq!(
            bound_scale_exponent(3_260_434_713_864).expect("production-scale magnitude"),
            -22
        );
        let safe = HIGHS_LARGE_BOUND_TARGET * (1_u128 << MAX_SAFE_BOUND_SCALE_SHIFT);
        assert_eq!(
            bound_scale_exponent(i128::try_from(safe).expect("safe bound fits i128"))
                .expect("last unit-preserving scale"),
            -29
        );
        let error = bound_scale_exponent(i128::try_from(safe + 1).expect("unsafe bound fits i128"))
            .expect_err("a scale that erases unit constraints must fail closed");
        assert!(error.to_string().contains("small_matrix_value"));
        assert!(2_f64.powi(-29) > HIGHS_SMALL_MATRIX_VALUE);
        assert!(2_f64.powi(-30) <= HIGHS_SMALL_MATRIX_VALUE);
    }

    #[test]
    fn objective_scale_conditions_costs() {
        assert_eq!(objective_scale_exponent(0.0).expect("zero objective"), 0);
        assert_eq!(objective_scale_exponent(1.0).expect("unit objective"), 0);
        assert_eq!(
            objective_scale_exponent(2_f64.powi(-22)).expect("scaled binary objective"),
            9
        );
        assert_eq!(
            objective_scale_exponent(2_000_000.0).expect("large objective"),
            -1
        );
    }

    #[test]
    fn scaled_lexicographic_order_exact() {
        let mut prepared = symmetric_model(3);
        prepared.distribution_weights = vec![prepared.nominal_weights.clone(); 2];
        prepared.existing_distribution_numerators = vec![0; 2];
        prepared.tiers[0].distribution_numerators = vec![3_255_460_869_290, 3_255_460_869_290];
        prepared.tiers[0].nominal_numerator = 4_000_000_000_000;
        prepared.tiers[1].distribution_numerators = vec![3_260_434_713_864, 3_260_434_713_864];
        prepared.tiers[1].nominal_numerator = 3_000_000_000_000;
        prepared.tiers[2].distribution_numerators = vec![3_260_434_713_864, 3_250_000_000_000];
        prepared.tiers[2].nominal_numerator = 4_500_000_000_000;
        let deploy = PortfolioSolverDeployConfig {
            deadline_secs: 10,
            threads: 1,
            max_tiers: 3,
            max_scenarios: 3,
            max_top_n: 1,
        };

        let solved =
            solve_lexicographic(&prepared, Instant::now() + Duration::from_secs(10), &deploy)
                .expect("scaled model preserves the exact robust-first ordering");

        assert_eq!(solved.selected, vec![1]);
        assert_eq!(solved.bound_scale_exponent, -23);
        assert_eq!(
            prepared
                .objectives(&solved.selected)
                .expect("exact scaled objectives")
                .robust_numerator,
            3_260_434_713_864
        );
    }

    #[test]
    fn dense_cvar_lock_feasible() {
        let nominal_weights = vec![25_i64; 400];
        let scenario_net_micro = (0..400)
            .map(|index| {
                if index < 20 {
                    -100_000_001_i64
                } else {
                    10_000_003_i64
                }
            })
            .collect::<Vec<_>>();
        let nominal_numerator = scenario_net_micro
            .iter()
            .zip(&nominal_weights)
            .map(|(value, weight)| value * weight)
            .sum::<i64>();
        let mut prepared = PreparedGlobalModel {
            tiers: vec![PreparedTier {
                source_index: 0,
                candidate_key: "candidate".to_owned(),
                market_key: "market".to_owned(),
                event_key: "event".to_owned(),
                category_key: "category".to_owned(),
                route_key: "route".to_owned(),
                stable_key: "stable".to_owned(),
                notional_micro: 1_000_000,
                scenario_risk_net_micro: scenario_net_micro.clone(),
                scenario_net_micro,
                distribution_numerators: vec![nominal_numerator],
                nominal_numerator,
                bucket_capital_micro: vec![1_000_000],
                capital_hours_micro: 1_000_000,
            }],
            scenario_count: 400,
            distribution_weights: vec![nominal_weights.clone()],
            nominal_weights,
            existing_scenario_net_micro: vec![0; 400],
            existing_distribution_numerators: vec![0],
            existing_nominal_numerator: 0,
            existing_capital_hours_micro: 0,
            existing_open_capital_micro: 0,
            existing_open_recommendations: 0,
            current_drawdown_micro: 0,
            available_cash_limit_micro: 1_000_000_000,
            max_open_capital_micro: 1_000_000_000,
            exposure_limits: PreparedExposureLimits {
                single_micro: 1_000_000_000,
                market_micro: 1_000_000_000,
                event_micro: 1_000_000_000,
                category_micro: 1_000_000_000,
                route_micro: 1_000_000_000,
                open_recommendations: 1,
            },
            existing_market_exposure: BTreeMap::new(),
            existing_event_exposure: BTreeMap::new(),
            existing_category_exposure: BTreeMap::new(),
            existing_route_exposure: BTreeMap::new(),
            existing_bucket_capital: vec![0],
            bucket_caps: vec![1_000_000_000],
            tail_mass_bps: 500,
            max_cvar_numerator: 100_000_000_000,
            max_scenario_loss_micro: 200_000_000,
            max_drawdown_micro: 200_000_000,
            top_n: 1,
            exclusivity_groups: Vec::new(),
        };
        let deploy = PortfolioSolverDeployConfig {
            deadline_secs: 10,
            threads: 1,
            max_tiers: 100,
            max_scenarios: 1_000,
            max_top_n: 1,
        };

        let solved =
            solve_lexicographic(&prepared, Instant::now() + Duration::from_secs(10), &deploy)
                .expect("dense 400-scenario CVaR remains feasible through later objectives");

        assert_eq!(solved.selected, vec![0]);
        assert_eq!(solved.lexicographic_model_build_count, 1);
        assert_eq!(solved.lexicographic_solve_count, 6);
        assert_eq!(solved.tie_break_stages, 1);
        assert_eq!(solved.tie_break_proof_count, 1);
        assert_eq!(
            solved.lexicographic_warm_start_count,
            solved.lexicographic_solve_count - 1
        );
        assert_eq!(
            prepared
                .objectives(&solved.selected)
                .expect("exact objectives")
                .cvar_numerator,
            50_000_000_500
        );

        prepared.existing_bucket_capital[0] = 1_000_000_001;
        let Err(error) =
            solve_lexicographic(&prepared, Instant::now() + Duration::from_secs(10), &deploy)
        else {
            panic!("existing capital above the bucket cap must be infeasible");
        };
        assert!(
            error.to_string().contains(
                "exact empty-selection baseline is outside the governed envelope: global portfolio exact verification failed: capital time-bucket cap exceeded"
            ),
            "unexpected infeasibility diagnostic: {error}"
        );
    }

    #[test]
    fn multipass_tie_proves_unique() {
        let mut prepared = symmetric_model(8);
        let mut maximum_keys = Vec::new();
        let mut lower_keys = Vec::new();
        for ordinal in 0..10_000 {
            let key = format!("stable-{ordinal}");
            prepared.tiers[0].stable_key.clone_from(&key);
            let pass_zero = tie_weight(&prepared, 0, 0).expect("pass-zero weight");
            let pass_one = tie_weight(&prepared, 0, 1).expect("pass-one weight");
            if maximum_keys.len() < 2
                && pass_zero == 16
                && maximum_keys
                    .first()
                    .is_none_or(|(_, existing_pass_one)| *existing_pass_one != pass_one)
            {
                maximum_keys.push((key, pass_one));
            } else if pass_zero < 16 && lower_keys.len() < 6 {
                lower_keys.push(key);
            }
            if maximum_keys.len() == 2 && lower_keys.len() == 6 {
                break;
            }
        }
        assert_eq!(maximum_keys.len(), 2, "fixture needs a pass-zero collision");
        assert_eq!(lower_keys.len(), 6, "fixture needs six lower-weight keys");
        let keys = maximum_keys
            .into_iter()
            .map(|(key, _)| key)
            .chain(lower_keys)
            .collect::<Vec<_>>();
        for (tier, key) in prepared.tiers.iter_mut().zip(keys) {
            tier.stable_key = key;
        }
        let deploy = PortfolioSolverDeployConfig {
            deadline_secs: 10,
            threads: 1,
            max_tiers: 8,
            max_scenarios: 3,
            max_top_n: 1,
        };

        let solved =
            solve_lexicographic(&prepared, Instant::now() + Duration::from_secs(10), &deploy)
                .expect("second deterministic pass isolates the exact selection");

        assert_eq!(solved.selected.len(), 1);
        assert_eq!(solved.tie_break_stages, 2);
        assert_eq!(solved.tie_break_proof_count, 2);
        assert_eq!(solved.lexicographic_solve_count, 8);
        assert_eq!(solved.lexicographic_warm_start_count, 7);
    }

    fn symmetric_model(tier_count: usize) -> PreparedGlobalModel {
        let nominal_weights = vec![3_334_i64, 3_333, 3_333];
        let scenario_net_micro = vec![1_000_000_i64; 3];
        let nominal_numerator = 10_000_000_000_i64;
        PreparedGlobalModel {
            tiers: (0..tier_count)
                .map(|index| PreparedTier {
                    source_index: index,
                    candidate_key: format!("candidate-{index}"),
                    market_key: format!("market-{index}"),
                    event_key: format!("event-{index}"),
                    category_key: format!("category-{index}"),
                    route_key: format!("route-{index}"),
                    stable_key: format!("placeholder-{index}"),
                    notional_micro: 1_000_000,
                    scenario_net_micro: scenario_net_micro.clone(),
                    scenario_risk_net_micro: scenario_net_micro.clone(),
                    distribution_numerators: vec![nominal_numerator],
                    nominal_numerator,
                    bucket_capital_micro: vec![1_000_000],
                    capital_hours_micro: 1_000_000,
                })
                .collect(),
            scenario_count: 3,
            distribution_weights: vec![nominal_weights.clone()],
            nominal_weights,
            existing_scenario_net_micro: vec![0; 3],
            existing_distribution_numerators: vec![0],
            existing_nominal_numerator: 0,
            existing_capital_hours_micro: 0,
            existing_open_capital_micro: 0,
            existing_open_recommendations: 0,
            current_drawdown_micro: 0,
            available_cash_limit_micro: 1_000_000_000,
            max_open_capital_micro: 1_000_000_000,
            exposure_limits: PreparedExposureLimits {
                single_micro: 1_000_000_000,
                market_micro: 1_000_000_000,
                event_micro: 1_000_000_000,
                category_micro: 1_000_000_000,
                route_micro: 1_000_000_000,
                open_recommendations: 1,
            },
            existing_market_exposure: BTreeMap::new(),
            existing_event_exposure: BTreeMap::new(),
            existing_category_exposure: BTreeMap::new(),
            existing_route_exposure: BTreeMap::new(),
            existing_bucket_capital: vec![0],
            bucket_caps: vec![1_000_000_000],
            tail_mass_bps: 500,
            max_cvar_numerator: 100_000_000_000,
            max_scenario_loss_micro: 200_000_000,
            max_drawdown_micro: 200_000_000,
            top_n: 1,
            exclusivity_groups: Vec::new(),
        }
    }
}
