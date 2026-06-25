//! Research governance plane: the **pure** computation behind the offline
//! governance closure (3.7).
//!
//! Publish / rollback / dataset-promotion *orchestration* (which needs the
//! repository + audit layers) lives in `quant-pivot-core`; this module owns only
//! the deterministic, IO-free pieces: the signal-layer [`shadow`] comparison.
//! The quality gate proper lives in [`crate::gates`].

pub mod shadow;

pub use shadow::{
    OutcomeDelta, RankDelta, ScoreDelta, ShadowComparison, compute_shadow_comparison,
};
