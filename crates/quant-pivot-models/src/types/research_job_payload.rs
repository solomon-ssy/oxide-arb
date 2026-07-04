//! JSONB content contracts for the durable research-job ledger.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{enums::quant::ResearchJobErrorCode, jsonb_active};

/// Live progress snapshot persisted to `progress_json` and pushed over WS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ResearchJobProgress {
    /// Human-readable current phase (e.g. `prefetch`, `cross_section`, `finalize`).
    pub phase: String,
    /// Units of work completed so far.
    pub processed: u64,
    /// Total units of work, when known ahead of time.
    pub total: Option<u64>,
}

impl ResearchJobProgress {
    /// Build a progress snapshot for a phase with a known total.
    #[must_use]
    pub fn with_total(phase: impl Into<String>, processed: u64, total: u64) -> Self {
        Self {
            phase: phase.into(),
            processed,
            total: Some(total),
        }
    }

    /// Build a progress snapshot for a phase with an unknown total.
    #[must_use]
    pub fn indeterminate(phase: impl Into<String>, processed: u64) -> Self {
        Self {
            phase: phase.into(),
            processed,
            total: None,
        }
    }

    /// Completion fraction in `[0, 1]` when a positive total is known.
    #[must_use]
    pub fn pct(&self) -> Option<f64> {
        match self.total {
            Some(total) if total > 0 =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some((self.processed.min(total) as f64) / (total as f64))
            }
            _ => None,
        }
    }
}

/// Structured failure payload persisted to `error_json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ResearchJobError {
    /// Stable machine code (see [`ResearchJobErrorCode`]).
    pub code: ResearchJobErrorCode,
    /// Human-readable detail for operators.
    pub message: String,
}

impl ResearchJobError {
    /// Build a structured job error.
    #[must_use]
    pub fn new(code: ResearchJobErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

jsonb_active!(ResearchJobProgress, ResearchJobError);
