//! Leakage-aware validation & backtest-overfitting control (Phase 11.5).
//!
//! This module owns the **financial-ML validation methodology** layer that sits
//! above [`crate::training`] (dataset/label construction) and [`crate::backtest`]
//! (the deterministic point-in-time replay engine): label-horizon-aware
//! purge/embargo splitting, Combinatorial Purged Cross-Validation (CPCV) with
//! full φ-path reconstruction, a governed hyperparameter trial grid, and the
//! Deflated/Probabilistic Sharpe Ratio + Probability of Backtest Overfitting
//! statistics that turn a single fragile backtest number into an auditable
//! distribution with a significance test attached.
//!
//! Every type here is deliberately **agnostic to what an atomic split unit
//! represents** ([`TimelineGroup`] is just a time interval): the Buy-side
//! (`WeightedFactor` / classical ML) wiring in [`crate::backtest`] groups by
//! same-`as_of` cross-section (mirroring the [`crate::model::trainer`] LTR query
//! groups); Phase 11.5.1 groups by lot instead, with zero changes to this
//! module. See `docs/plans/quant-pivot/phase-11/11.5-leakage-aware-validation-and-overfitting.md`.
//!
//! Authoritative references (cited again at each algorithm's definition site):
//! López de Prado, *Advances in Financial Machine Learning* (2018) Ch.7/11/12;
//! Bailey & López de Prado, *The Sharpe Ratio Efficient Frontier* (2012);
//! Bailey & López de Prado, *The Deflated Sharpe Ratio* (2014); Bailey, Borwein,
//! López de Prado & Zhu, *The Probability of Backtest Overfitting* (2014/2017).

mod combinatorics;
pub mod cpcv;
pub mod dsr;
pub mod pbo;
pub mod purge;
pub mod trials;

pub use cpcv::{
    BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
    DefaultCombinatorialPurgedBacktester, FoldModelSource, FoldRuntime, GroupEvaluation,
    GroupRowFilter, PolicyFoldRuntime, RankObservation, ReplayEngine,
};
pub use dsr::{DsrInput, DsrReport, deflated_sharpe_ratio, min_track_record_length};
pub use pbo::{PboInput, TrialPerformanceMatrix, probability_of_backtest_overfitting};
pub use purge::{DefaultPurgedSplitter, PurgeConfig, PurgedSplit, PurgedSplitter, TimelineGroup};
pub use trials::{ClassicalTrialGrid, Trial, TrialGridSpec, WeightedFactorTrialGrid};
