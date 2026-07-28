//! Exhaustive closed-DAG dispatch across the concrete feedback-stage adapters.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::quant::{FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobInfo},
    enums::quant::FeedbackStage,
};
use quant_pivot_repository::traits::FeedbackCycleLeaseGuard;

use crate::service::{
    feedback_comparison_stage::FeedbackComparisonStageAdapter,
    feedback_coordinator::{FeedbackStagePort, FeedbackStageSuccess},
    feedback_decision_stage::FeedbackDecisionStageAdapter,
    feedback_learning_stage::FeedbackLearningStageAdapter,
    feedback_shadow_stage::FeedbackShadowStageAdapter,
    feedback_signal_stage::FeedbackSignalStageAdapter,
};

/// Concrete adapter graph for [`FeedbackStageDispatcher`].
pub struct FeedbackStageDispatcherDeps {
    pub signals: Arc<FeedbackSignalStageAdapter>,
    pub learning: Arc<FeedbackLearningStageAdapter>,
    pub comparison: Arc<FeedbackComparisonStageAdapter>,
    pub shadow: Arc<FeedbackShadowStageAdapter>,
    pub decision: Arc<FeedbackDecisionStageAdapter>,
}

/// Single exhaustive dispatcher for the closed feedback DAG.
pub struct FeedbackStageDispatcher {
    signals: Arc<FeedbackSignalStageAdapter>,
    learning: Arc<FeedbackLearningStageAdapter>,
    comparison: Arc<FeedbackComparisonStageAdapter>,
    shadow: Arc<FeedbackShadowStageAdapter>,
    decision: Arc<FeedbackDecisionStageAdapter>,
}

impl FeedbackStageDispatcher {
    #[must_use]
    pub fn new(deps: FeedbackStageDispatcherDeps) -> Self {
        Self {
            signals: deps.signals,
            learning: deps.learning,
            comparison: deps.comparison,
            shadow: deps.shadow,
            decision: deps.decision,
        }
    }

    fn invalid(stage: FeedbackStage) -> FeedbackError {
        FeedbackError::InvalidCoordinatorState {
            detail: format!("feedback dispatcher cannot execute stage {stage}"),
        }
    }
}

#[async_trait]
impl FeedbackStagePort for FeedbackStageDispatcher {
    async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        match identity.feedback_stage() {
            FeedbackStage::Trigger => Err(Self::invalid(FeedbackStage::Trigger).into()),
            FeedbackStage::Coverage => self.signals.prepare_coverage(cycle, identity),
            FeedbackStage::Drift => self.signals.prepare_drift(cycle, identity).await,
            FeedbackStage::DatasetSeal
            | FeedbackStage::Training
            | FeedbackStage::Calibration
            | FeedbackStage::Cpcv => self.learning.prepare(cycle, identity).await,
            FeedbackStage::Comparison => {
                self.comparison
                    .prepare_comparison(cycle, lease, identity)
                    .await
            }
            FeedbackStage::ShadowReplay => self.shadow.prepare_shadow(cycle, lease, identity).await,
            FeedbackStage::Decision => self.decision.prepare_decision(cycle, lease, identity).await,
        }
    }

    async fn succeeded(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let stage = job
            .feedback_stage
            .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                detail: format!("feedback job {} has no stage identity", job.job_id),
            })?;
        match stage {
            FeedbackStage::Trigger => Err(Self::invalid(stage).into()),
            FeedbackStage::Coverage => self.signals.succeeded_coverage(cycle, job).await,
            FeedbackStage::Drift => self.signals.succeeded_drift(cycle, job).await,
            FeedbackStage::DatasetSeal => self.learning.succeeded_dataset_seal(cycle, job).await,
            FeedbackStage::Training => self.learning.succeeded_training(cycle, job).await,
            FeedbackStage::Calibration => self.learning.succeeded_calibration(cycle, job).await,
            FeedbackStage::Cpcv => self.learning.succeeded_cpcv(cycle, job).await,
            FeedbackStage::Comparison => self.comparison.succeeded_comparison(cycle, job).await,
            FeedbackStage::ShadowReplay => self.shadow.succeeded_shadow(cycle, job).await,
            FeedbackStage::Decision => self.decision.succeeded_decision(cycle, job).await,
        }
    }
}
