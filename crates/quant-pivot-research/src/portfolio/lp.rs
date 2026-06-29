//! The single portfolio allocator: a `good_lp` LP/MILP global optimizer.
//!
//! Objective (per-dollar conviction-weighted funded exposure):
//!
//! ```text
//! maximize  Σ_i  w_i · u_i           w_i = scoreᵢ · (1 + λ · ret_normᵢ)
//! ```
//!
//! subject to the budget / available-cash / single / liquidity / per-market /
//! per-event / per-category / correlated-cluster caps and the binary `TopN`
//! inclusion cardinality `Σ x_i ≤ top_n` (so budget and exposure are only ever
//! consumed by names that are actually published — the core upgrade over the
//! former greedy fill).
//!
//! Failure ladder (a report is *always* produced):
//!
//! 1. **MILP** (binary inclusion) on the configured backend (microlp / `HiGHS`).
//! 2. on any failure (solver error / panic / infeasible) → **continuous LP
//!    relaxation** on pure-Rust microlp + deterministic `TopN`/min recovery.
//! 3. on relaxation failure → **empty allocation** (all-zero) → empty report.
//!
//! Determinism: variables are created in a canonical order, weights carry an
//! infinitesimal lexicographic tie-break, the solution is snapped to the money
//! scale, and any solver panic is caught — so the same `(candidates, account,
//! config)` yields the same plan. `f64` is confined to this module's solver
//! boundary; every emitted size is a rounded [`Usd`].

use std::collections::BTreeMap;
use std::time::Instant;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::{
        BindingConstraint, CorrelationSource, OptimizerSolverStatus, PortfolioSolveMode,
        PortfolioSolverKind,
    },
    types::{MarketId, Usd},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    backtest::PortfolioCaps,
    portfolio::{
        allocator::{
            Allocation, AllocationInput, AllocationOutput, BudgetRoom, CandidateMeta,
            CeilingInputs, ExposureLedger, PortfolioAllocator, bucket_cap, decide_ceiling,
        },
        optimizer::{OptimizerConfig, OptimizerOutcome},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Lexicographic tie-break magnitude applied to objective weights so the solver
/// resolves equal-utility candidates toward the canonical order deterministically.
const TIE_BREAK_EPSILON: f64 = 1e-9;

/// The `good_lp` LP/MILP portfolio allocator (the only allocator).
#[derive(Debug, Clone, Copy)]
pub struct LinearProgrammingPortfolioAllocator {
    config: OptimizerConfig,
}

impl LinearProgrammingPortfolioAllocator {
    /// Construct the allocator from parsed optimizer configuration.
    #[must_use]
    pub const fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }
}

impl PortfolioAllocator for LinearProgrammingPortfolioAllocator {
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput> {
        let start = Instant::now();
        let model = PreparedModel::build(input, self.config);
        let solver = self.config.effective_solver();
        let correlation_source = input
            .correlation
            .map_or(CorrelationSource::Disabled, |c| c.source);

        if model.candidates.is_empty() {
            return Ok(AllocationOutput {
                allocations: Vec::new(),
                outcome: OptimizerOutcome::empty(solver, model.primary_mode()),
            });
        }

        let solve_result = model.solve(solver);
        let allocations = model.assemble(&solve_result.values);
        let objective_value = model.objective_value(&allocations);
        let outcome = OptimizerOutcome {
            solver,
            solve_mode: solve_result.mode,
            status: solve_result.status,
            fell_back_to_relaxation: solve_result.fell_back,
            objective_value: Some(objective_value),
            elapsed_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            correlation_source,
            constraint_conflicts: solve_result.conflicts,
        };
        Ok(AllocationOutput {
            allocations,
            outcome,
        })
    }
}

/// One candidate projected into solver space (canonical order).
struct LpCandidate {
    meta_index: usize,
    market_key: String,
    event_key: String,
    cluster: Option<usize>,
    /// Per-dollar objective weight `wᵢ` (decimal, for the reported objective).
    weight: Decimal,
    /// Solver weight with the lexicographic tie-break applied.
    weight_f64: f64,
    /// Per-candidate upper bound `min(desired, max_single, liquidity_room)`.
    ub: Decimal,
}

/// A bucket sum constraint: `Σ_{i∈indices} u_i ≤ rhs` (rhs = cap − held).
struct BucketConstraint {
    indices: Vec<usize>,
    rhs: Decimal,
}

/// The fully-prepared deterministic model (caps resolved, weights computed).
struct PreparedModel<'a> {
    input: &'a AllocationInput<'a>,
    caps: &'a PortfolioCaps,
    budget: BudgetRoom,
    /// Canonical-ordered solver candidates.
    candidates: Vec<LpCandidate>,
    /// Per-cluster seeded (held) exposure from the account snapshot.
    cluster_initial: BTreeMap<usize, Decimal>,
    /// All bucket / cluster sum constraints with finite caps.
    buckets: Vec<BucketConstraint>,
    min_rec: Decimal,
    top_n: usize,
    integer_inclusion: bool,
}

/// The outcome of the solve ladder.
struct Solved {
    /// Final per-candidate allocated USD (decimal, rounded, min/`top_n` applied).
    values: Vec<Decimal>,
    mode: PortfolioSolveMode,
    status: OptimizerSolverStatus,
    fell_back: bool,
    conflicts: Vec<String>,
}

impl<'a> PreparedModel<'a> {
    const fn primary_mode(&self) -> PortfolioSolveMode {
        if self.integer_inclusion {
            PortfolioSolveMode::MilpExact
        } else {
            PortfolioSolveMode::ContinuousRelaxation
        }
    }

    /// Project the allocation input into deterministic solver space.
    fn build(input: &'a AllocationInput<'a>, config: OptimizerConfig) -> Self {
        let caps = input.caps;
        let budget = BudgetRoom::resolve(caps.total_budget_usd, input.available_usd);
        let min_rec = caps.min_recommendation_usd.max(Decimal::ZERO);

        // Cluster membership: market id → cluster index, plus seeded held.
        let mut cluster_of: BTreeMap<String, usize> = BTreeMap::new();
        let mut cluster_initial: BTreeMap<usize, Decimal> = BTreeMap::new();
        if let Some(correlation) = input.correlation {
            for (cluster_idx, members) in correlation.clusters.iter().enumerate() {
                let mut held = Decimal::ZERO;
                for market in members {
                    cluster_of.insert(market.as_str().to_owned(), cluster_idx);
                    held += input
                        .initial_exposures
                        .per_market
                        .get(market)
                        .map_or(Decimal::ZERO, |usd| usd.inner());
                }
                cluster_initial.insert(cluster_idx, held);
            }
        }

        // Expected-return min-max normalization (only when λ tilts the weight).
        let (ret_min, ret_max) = expected_return_bounds(&input.candidates);
        let lambda = config.lambda;

        let mut order: Vec<usize> = (0..input.candidates.len()).collect();
        order.sort_by(|&a, &b| canonical_order(&input.candidates[a], &input.candidates[b]));

        let n = order.len();
        let mut candidates = Vec::with_capacity(n);
        for (rank, &meta_index) in order.iter().enumerate() {
            let meta = &input.candidates[meta_index];
            let score = meta.candidate.composite_score.inner() * meta.candidate.confidence.inner();
            let ret_norm = normalize(meta.candidate.expected_return_bps, ret_min, ret_max);
            let weight = (score * (Decimal::ONE + lambda * ret_norm)).max(Decimal::ZERO);
            let tie = TIE_BREAK_EPSILON.mul_add(count_to_f64(n - rank), 1.0);
            let weight_f64 = (decimal_to_f64(weight) * tie).max(0.0);
            let ub = candidate_upper_bound(meta, caps);
            let market_key = meta.candidate.market_id.as_str().to_owned();
            let event_key = meta
                .event_id
                .as_ref()
                .map_or_else(|| market_key.clone(), |id| id.as_str().to_owned());
            let cluster = cluster_of.get(&market_key).copied();
            candidates.push(LpCandidate {
                meta_index,
                market_key,
                event_key,
                cluster,
                weight,
                weight_f64,
                ub,
            });
        }

        let buckets = build_buckets(&candidates, caps, input, &cluster_initial);

        Self {
            input,
            caps,
            budget,
            candidates,
            cluster_initial,
            buckets,
            min_rec,
            top_n: input.top_n,
            integer_inclusion: config.integer_inclusion,
        }
    }

    /// Run the solve ladder (MILP → relaxation → empty) and recover final sizes.
    fn solve(&self, solver: PortfolioSolverKind) -> Solved {
        let ctx = self.solver_context();

        if self.integer_inclusion {
            let milp_result = try_milp(&ctx, solver);
            match milp_result {
                Ok(raw) => {
                    return Solved {
                        values: self.recover(&raw),
                        mode: PortfolioSolveMode::MilpExact,
                        status: OptimizerSolverStatus::Optimal,
                        fell_back: false,
                        conflicts: Vec::new(),
                    };
                }
                Err(failure) => {
                    let conflicts = failure.conflicts();
                    // Fall back to the pure-Rust continuous relaxation.
                    if let Ok(raw) = try_relaxation(&ctx) {
                        return Solved {
                            values: self.recover(&raw),
                            mode: PortfolioSolveMode::ContinuousRelaxation,
                            status: OptimizerSolverStatus::FellBackRelaxation,
                            fell_back: true,
                            conflicts,
                        };
                    }
                    return self.empty_solved(true, conflicts);
                }
            }
        }

        match try_relaxation(&ctx) {
            Ok(raw) => Solved {
                values: self.recover(&raw),
                mode: PortfolioSolveMode::ContinuousRelaxation,
                status: OptimizerSolverStatus::Optimal,
                fell_back: false,
                conflicts: Vec::new(),
            },
            Err(failure) => self.empty_solved(false, failure.conflicts()),
        }
    }

    fn empty_solved(&self, fell_back: bool, conflicts: Vec<String>) -> Solved {
        Solved {
            values: vec![Decimal::ZERO; self.candidates.len()],
            mode: PortfolioSolveMode::ContinuousRelaxation,
            status: OptimizerSolverStatus::SolverUnavailable,
            fell_back,
            conflicts,
        }
    }

    fn solver_context(&self) -> SolveContext {
        SolveContext {
            n: self.candidates.len(),
            weight: self.candidates.iter().map(|c| c.weight_f64).collect(),
            ub: self
                .candidates
                .iter()
                .map(|c| decimal_to_f64(c.ub))
                .collect(),
            min_rec: decimal_to_f64(self.min_rec),
            budget: decimal_to_f64(self.budget.effective.max(Decimal::ZERO)),
            top_n: self.top_n,
            buckets: self
                .buckets
                .iter()
                .map(|b| (b.indices.clone(), decimal_to_f64(b.rhs.max(Decimal::ZERO))))
                .collect(),
        }
    }

    /// Snap raw solver values to the money scale, enforce min-ticket, and apply
    /// `TopN` selection (by conviction-weighted contribution, canonical tie-break).
    /// Dropping a candidate only frees shared cap room, so the result stays
    /// feasible without a second solve.
    fn recover(&self, raw: &[f64]) -> Vec<Decimal> {
        let mut snapped: Vec<Decimal> = self
            .candidates
            .iter()
            .zip(raw)
            .map(|(candidate, &value)| {
                f64_to_decimal(value)
                    .clamp(Decimal::ZERO, candidate.ub)
                    .round_dp(RESEARCH_DECIMAL_SCALE)
            })
            .collect();

        // Drop sub-minimum tickets.
        for (candidate_idx, value) in snapped.iter_mut().enumerate() {
            let _ = candidate_idx;
            if *value < self.min_rec {
                *value = Decimal::ZERO;
            }
        }

        // TopN selection: keep the highest conviction-weighted contributions.
        // `candidates` is already in canonical order, so the index order is the
        // deterministic tie-break.
        let mut funded: Vec<usize> = (0..self.candidates.len())
            .filter(|&i| snapped[i] > Decimal::ZERO)
            .collect();
        funded.sort_by(|&a, &b| {
            let ca = self.candidates[a].weight * snapped[a];
            let cb = self.candidates[b].weight * snapped[b];
            cb.cmp(&ca).then(a.cmp(&b))
        });
        for &candidate_idx in funded.iter().skip(self.top_n) {
            snapped[candidate_idx] = Decimal::ZERO;
        }
        snapped
    }

    /// Build one [`Allocation`] per original candidate with binding attribution.
    fn assemble(&self, values: &[Decimal]) -> Vec<Allocation> {
        // Fold funded allocations into the exposure ledger (initial + round).
        let mut ledger = ExposureLedger::seed(self.input.initial_exposures);
        for (candidate, &allocated) in self.candidates.iter().zip(values) {
            if allocated > Decimal::ZERO {
                let category = self.input.candidates[candidate.meta_index].category;
                ledger.add(
                    &candidate.market_key,
                    &candidate.event_key,
                    category,
                    candidate.cluster,
                    allocated,
                );
            }
        }

        let correlated_cap = self
            .input
            .correlation
            .map_or(Decimal::ZERO, |c| c.cap_usd.inner());

        let mut allocations = Vec::with_capacity(self.candidates.len());
        for (candidate, &allocated) in self.candidates.iter().zip(values) {
            let meta = &self.input.candidates[candidate.meta_index];
            let market_total = ledger.market_held(&candidate.market_key);
            let event_total = ledger.event_held(&candidate.event_key);
            let category_total = ledger.category_held(meta.category);
            let cluster_total = candidate.cluster.map(|cluster| {
                self.cluster_initial
                    .get(&cluster)
                    .copied()
                    .unwrap_or(Decimal::ZERO)
                    + ledger.cluster_held(cluster)
            });

            // Attribute the binding from the perspective of "how much more could
            // this candidate have received with everyone else fixed".
            let others = |total: Decimal| (total - allocated).max(Decimal::ZERO);
            let decision = decide_ceiling(&CeilingInputs {
                meta,
                caps: self.caps,
                budget: &self.budget,
                spent_total: others(ledger.total),
                market_held: others(market_total),
                event_held: others(event_total),
                category_held: others(category_total),
                cluster_held: cluster_total.map(others),
                correlated_cap,
            });

            let binding = if allocated > Decimal::ZERO {
                // Funded: bound by whichever cap equals the achieved size, else
                // it took its full desired size (None → planner may upgrade to
                // KellyCap from the sizing provenance).
                if allocated >= meta.desired_usd.inner() {
                    BindingConstraint::None
                } else {
                    decision.binding
                }
            } else if decision.alloc_pre >= self.min_rec {
                // Room existed but the global TopN selection did not fund it.
                BindingConstraint::None
            } else {
                // No feasible room: name the exhausted cap.
                decision.binding
            };

            allocations.push(Allocation {
                signal_candidate_id: meta.candidate.signal_candidate_id.clone(),
                market_id: meta.candidate.market_id.clone(),
                allocated_usd: Usd::new(allocated),
                binding_constraint: binding,
                market_exposure_after_usd: Usd::new(market_total),
                event_exposure_after_usd: Usd::new(event_total),
                category_exposure_after_usd: Usd::new(category_total),
                liquidity_feasible: decision.liquidity_feasible,
            });
        }
        allocations
    }

    fn objective_value(&self, allocations: &[Allocation]) -> Decimal {
        self.candidates
            .iter()
            .zip(allocations)
            .map(|(candidate, allocation)| candidate.weight * allocation.allocated_usd.inner())
            .sum()
    }
}

/// Build all finite-cap bucket sum constraints (market / event / category /
/// cluster). Unconfigured (`<= 0`) caps are unlimited and produce no constraint.
fn build_buckets(
    candidates: &[LpCandidate],
    caps: &PortfolioCaps,
    input: &AllocationInput<'_>,
    cluster_initial: &BTreeMap<usize, Decimal>,
) -> Vec<BucketConstraint> {
    let mut buckets = Vec::new();
    grouped_constraint(
        candidates,
        caps.max_market_exposure_usd,
        |candidate| candidate.market_key.clone(),
        |candidate| {
            input
                .initial_exposures
                .per_market
                .get(&MarketId::new(candidate.market_key.as_str()))
                .map_or(Decimal::ZERO, |usd| usd.inner())
        },
        &mut buckets,
    );
    grouped_constraint(
        candidates,
        caps.max_event_exposure_usd,
        |candidate| candidate.event_key.clone(),
        |candidate| {
            input
                .initial_exposures
                .per_event
                .iter()
                .find(|(id, _)| id.as_str() == candidate.event_key)
                .map_or(Decimal::ZERO, |(_, usd)| usd.inner())
        },
        &mut buckets,
    );

    // Category constraint keyed by the meta category.
    grouped_constraint(
        candidates,
        caps.max_category_exposure_usd,
        |candidate| {
            input.candidates[candidate.meta_index]
                .category
                .as_str()
                .to_owned()
        },
        |candidate| {
            input
                .initial_exposures
                .per_category
                .get(&input.candidates[candidate.meta_index].category)
                .map_or(Decimal::ZERO, |usd| usd.inner())
        },
        &mut buckets,
    );

    // Correlated-cluster caps.
    if let Some(correlation) = input.correlation {
        let cap = correlation.cap_usd.inner();
        if cap > Decimal::ZERO {
            let mut by_cluster: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (idx, candidate) in candidates.iter().enumerate() {
                if let Some(cluster) = candidate.cluster {
                    by_cluster.entry(cluster).or_default().push(idx);
                }
            }
            for (cluster, indices) in by_cluster {
                let held = cluster_initial
                    .get(&cluster)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                buckets.push(BucketConstraint {
                    indices,
                    rhs: (cap - held).max(Decimal::ZERO),
                });
            }
        }
    }
    buckets
}

/// Group candidates by a key and emit one `Σ u ≤ cap − held` constraint per
/// group when the cap is finite.
fn grouped_constraint(
    candidates: &[LpCandidate],
    cap_raw: Decimal,
    key_of: impl Fn(&LpCandidate) -> String,
    held_of: impl Fn(&LpCandidate) -> Decimal,
    out: &mut Vec<BucketConstraint>,
) {
    let cap = bucket_cap(cap_raw);
    if cap == Decimal::MAX {
        return;
    }
    let mut groups: BTreeMap<String, (Vec<usize>, Decimal)> = BTreeMap::new();
    for (idx, candidate) in candidates.iter().enumerate() {
        let key = key_of(candidate);
        let held = held_of(candidate);
        let entry = groups.entry(key).or_insert((Vec::new(), held));
        entry.0.push(idx);
    }
    for (_, (indices, held)) in groups {
        out.push(BucketConstraint {
            indices,
            rhs: (cap - held).max(Decimal::ZERO),
        });
    }
}

/// Per-candidate upper bound: the Kelly-desired size, capped by the single-rec
/// and liquidity-usage limits.
fn candidate_upper_bound(meta: &CandidateMeta<'_>, caps: &PortfolioCaps) -> Decimal {
    let desired = meta.desired_usd.inner().max(Decimal::ZERO);
    let mut ub = desired.min(bucket_cap(caps.max_single_recommendation_usd));
    if let Some(liquidity) = meta.liquidity_usd {
        let room = (liquidity.inner() * caps.liquidity_usage_cap_pct.max(Decimal::ZERO))
            .max(Decimal::ZERO);
        ub = ub.min(room);
    }
    ub.max(Decimal::ZERO)
}

/// Canonical ordering: risk-adjusted score desc, then market id, then token id.
fn canonical_order(a: &CandidateMeta<'_>, b: &CandidateMeta<'_>) -> std::cmp::Ordering {
    let ra = a.candidate.composite_score.inner() * a.candidate.confidence.inner();
    let rb = b.candidate.composite_score.inner() * b.candidate.confidence.inner();
    rb.cmp(&ra)
        .then_with(|| {
            a.candidate
                .market_id
                .as_str()
                .cmp(b.candidate.market_id.as_str())
        })
        .then_with(|| {
            a.candidate
                .token_id
                .as_str()
                .cmp(b.candidate.token_id.as_str())
        })
}

/// Cross-section expected-return bounds for min-max normalization.
fn expected_return_bounds(candidates: &[CandidateMeta<'_>]) -> (Decimal, Decimal) {
    let mut min = Decimal::MAX;
    let mut max = Decimal::MIN;
    for meta in candidates {
        let value = meta.candidate.expected_return_bps;
        min = min.min(value);
        max = max.max(value);
    }
    if candidates.is_empty() {
        (Decimal::ZERO, Decimal::ZERO)
    } else {
        (min, max)
    }
}

/// Min-max normalize a value into `[0, 1]`; a degenerate range yields `0`.
fn normalize(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if max <= min {
        Decimal::ZERO
    } else {
        ((value - min) / (max - min)).clamp(Decimal::ZERO, Decimal::ONE)
    }
}

fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}

/// Convert a (small) count to `f64` without a lossy `as` cast.
fn count_to_f64(count: usize) -> f64 {
    u32::try_from(count).map_or(f64::MAX, f64::from)
}

fn f64_to_decimal(value: f64) -> Decimal {
    if !value.is_finite() {
        return Decimal::ZERO;
    }
    Decimal::from_f64(value).unwrap_or(Decimal::ZERO)
}

/// Solver-space view (pure f64) handed to the `good_lp` model builder.
struct SolveContext {
    n: usize,
    weight: Vec<f64>,
    ub: Vec<f64>,
    min_rec: f64,
    budget: f64,
    top_n: usize,
    buckets: Vec<(Vec<usize>, f64)>,
}

/// A solve failure, carrying any human-readable conflicting-constraint detail.
enum SolveFail {
    Panicked,
    Infeasible,
    Other(String),
}

impl SolveFail {
    fn conflicts(&self) -> Vec<String> {
        match self {
            Self::Panicked => vec!["solver panicked".to_owned()],
            Self::Infeasible => vec!["model infeasible under configured caps".to_owned()],
            Self::Other(detail) => vec![detail.clone()],
        }
    }
}

#[cfg(all(feature = "lp-solver", debug_assertions))]
pub mod debug_test_hooks {
    //! Debug-only overrides for the MILP / relaxation solve ladder (integration tests).

    use std::cell::RefCell;

    /// How the MILP dispatch behaves when a test installs an override.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub enum MilpBehavior {
        /// Run the real solver.
        #[default]
        Normal,
        /// Return `Infeasible` without calling the solver.
        FailInfeasible,
        /// Panic inside `catch_unwind` (exercises the panic → fallback path).
        Panic,
    }

    /// How the relaxation dispatch behaves when a test installs an override.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub enum RelaxBehavior {
        /// Run the real solver.
        #[default]
        Normal,
        /// Return `Infeasible` without calling the solver.
        FailInfeasible,
        /// Panic inside `catch_unwind`.
        Panic,
    }

    thread_local! {
        static MILP: RefCell<MilpBehavior> = const { RefCell::new(MilpBehavior::Normal) };
        static RELAX: RefCell<RelaxBehavior> = const { RefCell::new(RelaxBehavior::Normal) };
    }

    /// Install a MILP-path override (cleared by [`reset`] or [`Guard`] drop).
    pub fn set_milp(behavior: MilpBehavior) {
        MILP.with(|slot| *slot.borrow_mut() = behavior);
    }

    /// Install a relaxation-path override (cleared by [`reset`] or [`Guard`] drop).
    pub fn set_relax(behavior: RelaxBehavior) {
        RELAX.with(|slot| *slot.borrow_mut() = behavior);
    }

    /// Clear all overrides.
    pub fn reset() {
        MILP.with(|slot| *slot.borrow_mut() = MilpBehavior::Normal);
        RELAX.with(|slot| *slot.borrow_mut() = RelaxBehavior::Normal);
    }

    /// RAII guard that resets overrides when dropped.
    pub struct Guard;

    impl Default for Guard {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Guard {
        /// Construct a guard that resets hook state on drop.
        #[must_use]
        pub const fn new() -> Self {
            Self
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            reset();
        }
    }

    /// Current MILP override (defaults to [`MilpBehavior::Normal`]).
    pub fn current_milp() -> MilpBehavior {
        MILP.with(|slot| *slot.borrow())
    }

    /// Current relaxation override (defaults to [`RelaxBehavior::Normal`]).
    pub fn current_relax() -> RelaxBehavior {
        RELAX.with(|slot| *slot.borrow())
    }
}

#[cfg(feature = "lp-solver")]
fn try_milp(ctx: &SolveContext, solver: PortfolioSolverKind) -> Result<Vec<f64>, SolveFail> {
    #[cfg(debug_assertions)]
    match debug_test_hooks::current_milp() {
        debug_test_hooks::MilpBehavior::Normal => {}
        debug_test_hooks::MilpBehavior::FailInfeasible => return Err(SolveFail::Infeasible),
        debug_test_hooks::MilpBehavior::Panic => {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                panic!("debug_test_hooks: forced MILP panic");
            }));
            return Err(SolveFail::Panicked);
        }
    }
    dispatch_solve(ctx, true, solver)
}

#[cfg(not(feature = "lp-solver"))]
fn try_milp(_ctx: &SolveContext, _solver: PortfolioSolverKind) -> Result<Vec<f64>, SolveFail> {
    Err(SolveFail::Other("lp-solver feature not built".to_owned()))
}

#[cfg(feature = "lp-solver")]
fn try_relaxation(ctx: &SolveContext) -> Result<Vec<f64>, SolveFail> {
    #[cfg(debug_assertions)]
    match debug_test_hooks::current_relax() {
        debug_test_hooks::RelaxBehavior::Normal => {}
        debug_test_hooks::RelaxBehavior::FailInfeasible => return Err(SolveFail::Infeasible),
        debug_test_hooks::RelaxBehavior::Panic => {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                panic!("debug_test_hooks: forced relaxation panic");
            }));
            return Err(SolveFail::Panicked);
        }
    }
    dispatch_solve(ctx, false, PortfolioSolverKind::Microlp)
}

#[cfg(not(feature = "lp-solver"))]
fn try_relaxation(_ctx: &SolveContext) -> Result<Vec<f64>, SolveFail> {
    Err(SolveFail::Other("lp-solver feature not built".to_owned()))
}

#[cfg(feature = "lp-solver")]
fn dispatch_solve(
    ctx: &SolveContext,
    binary: bool,
    solver: PortfolioSolverKind,
) -> Result<Vec<f64>, SolveFail> {
    match solver {
        #[cfg(feature = "lp-solver-highs")]
        PortfolioSolverKind::Highs => build_and_solve(ctx, binary, good_lp::highs),
        _ => build_and_solve(ctx, binary, good_lp::microlp),
    }
}

#[cfg(not(feature = "lp-solver"))]
fn dispatch_solve(
    _ctx: &SolveContext,
    _binary: bool,
    _solver: PortfolioSolverKind,
) -> Result<Vec<f64>, SolveFail> {
    Err(SolveFail::Other("lp-solver feature not built".to_owned()))
}

#[cfg(feature = "lp-solver")]
fn build_and_solve<F, M>(ctx: &SolveContext, binary: bool, solver: F) -> Result<Vec<f64>, SolveFail>
where
    F: FnMut(good_lp::variable::UnsolvedProblem) -> M,
    M: good_lp::SolverModel,
{
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        solve_inner(ctx, binary, solver)
    }));
    outcome.unwrap_or(Err(SolveFail::Panicked))
}

#[cfg(feature = "lp-solver")]
fn solve_inner<F, M>(ctx: &SolveContext, binary: bool, solver: F) -> Result<Vec<f64>, SolveFail>
where
    F: FnMut(good_lp::variable::UnsolvedProblem) -> M,
    M: good_lp::SolverModel,
{
    use good_lp::{Expression, ProblemVariables, Solution, Variable, constraint, variable};

    let mut vars = ProblemVariables::new();
    let u: Vec<Variable> = (0..ctx.n)
        .map(|i| vars.add(variable().min(0.0).max(ctx.ub[i])))
        .collect();
    let x: Vec<Variable> = if binary {
        (0..ctx.n).map(|_| vars.add(variable().binary())).collect()
    } else {
        Vec::new()
    };

    let objective = u
        .iter()
        .zip(&ctx.weight)
        .fold(Expression::from(0.0), |acc, (&v, &w)| acc + v * w);

    let mut constraints: Vec<good_lp::constraint::Constraint> = Vec::new();
    // Total budget / available cash.
    let total = u.iter().fold(Expression::from(0.0), |acc, &v| acc + v);
    constraints.push(constraint!(total <= ctx.budget));
    // Bucket / cluster sum caps.
    for (indices, rhs) in &ctx.buckets {
        let sum = indices
            .iter()
            .fold(Expression::from(0.0), |acc, &i| acc + u[i]);
        constraints.push(constraint!(sum <= *rhs));
    }
    // Binary inclusion linkage + TopN cardinality (MILP only).
    if binary {
        for i in 0..ctx.n {
            constraints.push(constraint!(u[i] <= ctx.ub[i] * x[i]));
            constraints.push(constraint!(u[i] >= ctx.min_rec * x[i]));
        }
        let cardinality = x.iter().fold(Expression::from(0.0), |acc, &v| acc + v);
        let top_n = count_to_f64(ctx.top_n.min(ctx.n));
        constraints.push(constraint!(cardinality <= top_n));
    }

    let mut model = vars.maximise(objective).using(solver);
    for c in constraints {
        model = model.with(c);
    }
    let solution = model.solve().map_err(|error| {
        let detail = error.to_string();
        if detail.to_lowercase().contains("infeasible") {
            SolveFail::Infeasible
        } else {
            SolveFail::Other(detail)
        }
    })?;
    Ok(u.iter().map(|&v| solution.value(v)).collect())
}
