//! Research governance plane: the **pure** computation behind the offline
//! governance closure.
//!
//! Publish / rollback / dataset-promotion *orchestration* (which needs the
//! repository + audit layers) lives in `quant-pivot-core`; this module owns only
//! the deterministic, IO-free pieces: the signal-layer [`shadow`] comparison.
//! The quality gate proper lives in [`crate::gates`].

pub mod shadow;
