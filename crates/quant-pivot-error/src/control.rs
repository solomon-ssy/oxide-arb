//! Runtime control plane errors (mode switch, config apply, book subscriptions).

use std::fmt::{Display, Formatter, Result as FmtResult};

use thiserror::Error;

use crate::QuantError;

/// Stage at which a committed runtime generation failed to converge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeApplyStage {
    Prepare,
    Publish,
    GenerationMismatch,
}

impl Display for RuntimeApplyStage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(match self {
            Self::Prepare => "prepare",
            Self::Publish => "publish",
            Self::GenerationMismatch => "generation_mismatch",
        })
    }
}

/// Failures from governed runtime control operations.
#[derive(Debug, Error)]
pub enum ControlError {
    #[error("precondition failed: {0}")]
    Precondition(String),

    #[error("control operation failed: {0}")]
    Engine(String),

    #[error(
        "committed runtime generation {desired_generation} failed during {stage}; applied \
         generation remains {applied_generation}: {detail}"
    )]
    CommittedGenerationApply {
        desired_generation: i64,
        applied_generation: i64,
        stage: RuntimeApplyStage,
        detail: String,
    },
}

impl From<QuantError> for ControlError {
    fn from(error: QuantError) -> Self {
        Self::Engine(error.to_string())
    }
}
