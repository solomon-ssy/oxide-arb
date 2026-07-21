//! Inference degradation policy.
//!
//! A pure mapping from a runtime failure to the action the online round must
//! take. Keeping the table here makes the money-safety contract auditable in one
//! place; the core `ModelRunner` owns the side effects (alerting, run finalize,
//! shadow isolation) the action describes.

use quant_pivot_models::enums::quant::ModelRunErrorCode;

/// Where in the inference round a failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceStage {
    /// Loading the active model runtime (artifact read / hash / schema).
    ActiveLoad,
    /// Running active inference after a successful load.
    ActiveInference,
    /// Loading the shadow model runtime.
    ShadowLoad,
    /// Running shadow inference after a successful load.
    ShadowInference,
}

impl InferenceStage {
    /// Whether the stage belongs to the shadow path (failure is isolated).
    #[must_use]
    pub const fn is_shadow(self) -> bool {
        matches!(self, Self::ShadowLoad | Self::ShadowInference)
    }

    /// Map an active-path failure at this stage to a persisted [`ModelRunErrorCode`].
    #[must_use]
    pub const fn active_error_code(self) -> ModelRunErrorCode {
        match self {
            Self::ActiveLoad => ModelRunErrorCode::ArtifactLoadFailed,
            Self::ActiveInference | Self::ShadowLoad | Self::ShadowInference => {
                ModelRunErrorCode::ActiveInferenceFailed
            }
        }
    }

    /// Map a shadow-path failure at this stage to a persisted [`ModelRunErrorCode`].
    #[must_use]
    pub const fn shadow_error_code(self) -> ModelRunErrorCode {
        match self {
            Self::ShadowLoad => ModelRunErrorCode::ArtifactLoadFailed,
            Self::ShadowInference | Self::ActiveLoad | Self::ActiveInference => {
                ModelRunErrorCode::ShadowInferenceFailed
            }
        }
    }
}

/// The action the online round takes for a degradation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradeAction {
    /// Fail the run and raise a critical alert (active path is unrecoverable).
    FailRunCritical,
    /// Keep the active result; record the shadow failure without failing the run.
    KeepActiveRecordShadow,
}

/// The governed action for a failure at `stage`.
///
/// Active-path failures fail the run with a critical alert (an empty report is
/// never silently fabricated); shadow-path failures are isolated so a broken
/// candidate model can never take down live reporting.
#[must_use]
pub const fn degrade_action(stage: InferenceStage) -> DegradeAction {
    if stage.is_shadow() {
        DegradeAction::KeepActiveRecordShadow
    } else {
        DegradeAction::FailRunCritical
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::enums::quant::ModelRunErrorCode;

    use super::{DegradeAction, InferenceStage, degrade_action};

    #[test]
    fn active_failures_are_critical_shadow_failures_isolated() {
        assert_eq!(
            degrade_action(InferenceStage::ActiveLoad),
            DegradeAction::FailRunCritical
        );
        assert_eq!(
            degrade_action(InferenceStage::ActiveInference),
            DegradeAction::FailRunCritical
        );
        assert_eq!(
            degrade_action(InferenceStage::ShadowLoad),
            DegradeAction::KeepActiveRecordShadow
        );
        assert_eq!(
            degrade_action(InferenceStage::ShadowInference),
            DegradeAction::KeepActiveRecordShadow
        );
    }

    #[test]
    fn active_error_codes_map_active_stages() {
        assert_eq!(
            InferenceStage::ActiveLoad.active_error_code(),
            ModelRunErrorCode::ArtifactLoadFailed
        );
        assert_eq!(
            InferenceStage::ActiveInference.active_error_code(),
            ModelRunErrorCode::ActiveInferenceFailed
        );
    }

    #[test]
    fn shadow_error_codes_map_shadow_stages() {
        assert_eq!(
            InferenceStage::ShadowLoad.shadow_error_code(),
            ModelRunErrorCode::ArtifactLoadFailed
        );
        assert_eq!(
            InferenceStage::ShadowInference.shadow_error_code(),
            ModelRunErrorCode::ShadowInferenceFailed
        );
    }
}
