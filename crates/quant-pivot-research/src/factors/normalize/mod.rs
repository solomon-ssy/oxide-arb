//! Factor normalization: the cross-sectional fit/apply contract, its concrete
//! methods (winsorized z-score, rank, semantic min/max), fitted statistics, and
//! the audited normalization outcome.
//!
//! Two invariants set this apart from a naive scaler:
//!
//! - **No hardcoded constants.** Every distributional parameter (`winsor_p`,
//!   `clamp_sigma`, min/max bounds) comes from runtime config via
//!   [`resolve_normalizer`]; the code holds only the method choice.
//! - **No silent neutral.** A missing input is [`NormalizedFactor::MissingInput`]
//!   and a degenerate / too-small cross-section is
//!   [`NormalizedFactor::Indeterminate`] with a recorded reason — never a fake
//!   `0.5`.

mod cross_section;
mod outcome;
mod stats;

pub(in crate::factors) use cross_section::indeterminate_present;
pub use cross_section::{
    CrossSectionalNormalizer, MinMaxNormalizer, RankNormalizer, WinsorizedZScoreNormalizer,
    resolve_normalizer,
};
pub use outcome::{NormalizationClampAudit, NormalizedFactor, RawFactorColumn};
pub use stats::NormalizationStats;
