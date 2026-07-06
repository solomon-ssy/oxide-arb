//! Normalization outcome types: the audited [`NormalizedFactor`] result, the
//! raw factor column a normalizer fits/applies over, and the clamp audit.

use quant_pivot_models::{
    enums::factor::{FactorIndeterminateReason, NormalizationSource},
    types::Probability,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::factors::value::FactorName;

/// An audited normalization clamp: the out-of-domain value and the bound it was
/// clamped to. Clamping is **never silent** — every clamp is recorded here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationClampAudit {
    /// The normalization method whose domain was exceeded.
    pub method: String,
    /// The value (raw or intermediate, e.g. a z-score) that fell out of domain.
    pub raw: Decimal,
    /// The bound the value was clamped to before mapping into `[0, 1]`.
    pub clamped_to: Decimal,
}

/// The normalization outcome for one market's factor value.
///
/// This is the single source of truth for a factor's normalized magnitude. A
/// missing input or a degenerate cross-section is modeled **explicitly** —
/// there is no silent neutral `0.5`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum NormalizedFactor {
    /// A normalized `[0, 1]` score with provenance and any recorded clamp.
    Scored {
        /// The normalized score in `[0, 1]`.
        score: Probability,
        /// How the score was derived (cross-section / per-market / history).
        source: NormalizationSource,
        /// Recorded clamp, when the value fell outside the normalization domain.
        clamp: Option<NormalizationClampAudit>,
    },
    /// The factor's raw input was unavailable for this market.
    MissingInput,
    /// The factor does not apply to this market's structure (e.g. a neg-risk
    /// full-leg factor on a binary market). Structurally absent — distinct from a
    /// missing input and from an indeterminate cross-section; never a fake `0.5`.
    NotApplicable,
    /// The cross-section was too small or carried no dispersion. The factor
    /// contributes nothing and the reason is recorded (never a fake `0.5`).
    Indeterminate {
        /// Why the factor could not be normalized.
        reason: FactorIndeterminateReason,
    },
}

impl NormalizedFactor {
    /// A cross-section-scored value with no clamp (convenience for fixtures and
    /// callers that already hold a `[0, 1]` score).
    #[must_use]
    pub const fn cross_section(score: Probability) -> Self {
        Self::Scored {
            score,
            source: NormalizationSource::CrossSection,
            clamp: None,
        }
    }
}

/// A factor's raw values across one same-`as_of` cross-section, index-aligned
/// with the batch of feature vectors. A `None` entry means the factor's input
/// was missing for that market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFactorColumn {
    /// The factor this column describes.
    pub factor: FactorName,
    /// Raw values, index-aligned with the batch (`None` = missing input).
    pub values: Vec<Option<Decimal>>,
}

impl RawFactorColumn {
    /// The present (non-missing) raw values, in batch order.
    #[must_use]
    pub fn present(&self) -> Vec<Decimal> {
        self.values.iter().filter_map(|value| *value).collect()
    }

    /// The count of present (non-missing) raw values.
    #[must_use]
    pub fn present_count(&self) -> usize {
        self.values.iter().filter(|value| value.is_some()).count()
    }
}
