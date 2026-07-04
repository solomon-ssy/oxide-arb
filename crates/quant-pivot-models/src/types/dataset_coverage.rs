//! JSONB content contracts for training-dataset build coverage accounting.

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{jsonb_active, types::SelectionExclusionSummary};

/// Decimal scale for coverage ratio helpers (matches research-plane precision).
const DATASET_COVERAGE_DECIMAL_SCALE: u32 = 12;

/// Row counts from an optional training-matrix probe at build time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct MatrixCoverageProbe {
    /// Rows that would enter the dense matrix.
    pub accepted_rows: u64,
    /// Rows rejected (missing label, critical feature, or non-finite cell).
    pub rejected_rows: u64,
    /// Supervised label used for the probe.
    pub label_name: String,
    /// Horizon of the probed label column.
    pub label_horizon_secs: u64,
    /// Number of numeric feature columns in the probe spec.
    pub feature_columns: u64,
}

/// Per-sample coverage accounting for a built dataset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct DatasetCoverage {
    /// Sample instants the plan produced.
    pub planned_samples: u64,
    /// Examples actually materialized (passed feature build + leakage).
    pub built_examples: u64,
    /// Distinct markets represented.
    pub markets: u64,
    /// Labels resolved to a value.
    pub labels_available: u64,
    /// Labels deferred as not-yet-mature.
    pub labels_not_mature: u64,
    /// Labels that can never be produced.
    pub labels_unavailable: u64,
    /// Samples dropped because feature inputs were insufficient.
    pub samples_dropped_insufficient: u64,
    /// Live attribution rows considered for dataset materialization.
    #[serde(default)]
    pub live_attribution_candidates: u64,
    /// Live attribution rows dropped because frozen recommendation evidence was incomplete.
    #[serde(default)]
    pub live_attribution_dropped_missing_evidence: u64,
    /// Book snapshot rows skipped due to malformed JSON or invalid level pairs.
    pub book_decode_failures: u64,
    /// `ExitDecision` lot-timeline candidates considered.
    #[serde(default)]
    pub exit_decision_candidates: u64,
    /// `ExitDecision` examples materialized.
    #[serde(default)]
    pub exit_decision_built: u64,
    /// `ExitDecision` rows simulated from full L2 books.
    #[serde(default)]
    pub exit_fill_l2_rows: u64,
    /// `ExitDecision` rows simulated from microstructure fallback.
    #[serde(default)]
    pub exit_fill_fallback_rows: u64,
    /// Markets evaluated by the point-in-time selection funnel across all `as_of`.
    #[serde(default)]
    pub pit_selection_candidates: u64,
    /// Markets that passed the funnel and entered the spine.
    #[serde(default)]
    pub pit_selection_included: u64,
    /// Aggregate point-in-time selection exclusions (by reason bucket).
    #[serde(default)]
    pub pit_selection_excluded: SelectionExclusionSummary,
    /// Optional training-matrix probe (diagnostic only; does not gate build).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix_probe: Option<MatrixCoverageProbe>,
}

impl DatasetCoverage {
    /// Fraction of label slots that resolved to a usable value, in `[0, 1]`.
    #[must_use]
    pub fn label_coverage(&self) -> Decimal {
        let total = self.labels_available + self.labels_not_mature + self.labels_unavailable;
        if total == 0 {
            return Decimal::ZERO;
        }
        (Decimal::from(self.labels_available) / Decimal::from(total))
            .round_dp(DATASET_COVERAGE_DECIMAL_SCALE)
    }

    /// Fraction of planned samples that materialized into examples, in `[0, 1]`.
    #[must_use]
    pub fn feature_build_coverage(&self) -> Decimal {
        if self.planned_samples == 0 {
            return Decimal::ZERO;
        }
        (Decimal::from(self.built_examples) / Decimal::from(self.planned_samples))
            .round_dp(DATASET_COVERAGE_DECIMAL_SCALE)
    }

    /// Fraction of `ExitDecision` rows simulated from full L2 books, in `[0, 1]`.
    #[must_use]
    pub fn exit_l2_fidelity_ratio(&self) -> Decimal {
        let total = self.exit_fill_l2_rows + self.exit_fill_fallback_rows;
        if total == 0 {
            return Decimal::ZERO;
        }
        (Decimal::from(self.exit_fill_l2_rows) / Decimal::from(total))
            .round_dp(DATASET_COVERAGE_DECIMAL_SCALE)
    }

    /// Fraction of `ExitDecision` rows using microstructure fallback, in `[0, 1]`.
    #[must_use]
    pub fn exit_fallback_ratio(&self) -> Decimal {
        let total = self.exit_fill_l2_rows + self.exit_fill_fallback_rows;
        if total == 0 {
            return Decimal::ZERO;
        }
        (Decimal::from(self.exit_fill_fallback_rows) / Decimal::from(total))
            .round_dp(DATASET_COVERAGE_DECIMAL_SCALE)
    }
}

/// Forward label horizons (seconds) persisted on a training-dataset ledger row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct TrainingHorizonsSecs(pub Vec<u64>);

jsonb_active!(DatasetCoverage, MatrixCoverageProbe, TrainingHorizonsSecs);
