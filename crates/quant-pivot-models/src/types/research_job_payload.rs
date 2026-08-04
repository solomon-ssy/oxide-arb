//! JSONB content contracts for the durable research-job ledger.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        api::{
            BacktestJobParams, BuildTrainingDatasetRequest, CpcvBacktestJobParams,
            FeatureParityJobParams, FeedbackCoverageJobParams, FeedbackDriftJobParams,
            ModelTrainJobParams, TradePolicyFitJobParams, TradePolicyValidationJobParams,
        },
        ports::{
            BiasTableFitJobParams, CandidateRecipePlanJobParams, FeedbackAttributionJobParams,
            FeedbackCalibrationJobParams, FeedbackComparisonJobParams, FeedbackCpcvJobParams,
            FeedbackDatasetSealJobParams, FeedbackDecisionJobParams, FeedbackShadowJobParams,
            FeedbackTrainingJobParams, FeedbackTruthFreezeJobParams, FeedbackValidationJobParams,
            ModelCalibrationFitJobParams, ShadowBindingJobParams,
        },
    },
    enums::quant::{ResearchJobErrorCode, ResearchJobKind},
};

/// Frozen, replayable request carried by one durable research job.
///
/// The tagged variant is redundant with the relational `kind` column on
/// purpose: the boot schema constrains both discriminators to agree, making a
/// corrupt or incorrectly dispatched row impossible to consume silently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum ResearchJobParams {
    DatasetBuild(BuildTrainingDatasetRequest),
    ModelTrain(ModelTrainJobParams),
    Backtest(BacktestJobParams),
    BiasTableFit(BiasTableFitJobParams),
    ModelCalibrationFit(ModelCalibrationFitJobParams),
    CpcvBacktest(CpcvBacktestJobParams),
    FeatureParity(FeatureParityJobParams),
    FeedbackTruthFreeze(FeedbackTruthFreezeJobParams),
    FeedbackCoverage(FeedbackCoverageJobParams),
    FeedbackAttribution(FeedbackAttributionJobParams),
    FeedbackDrift(FeedbackDriftJobParams),
    FeedbackRecipePlan(Box<CandidateRecipePlanJobParams>),
    FeedbackDatasetSeal(FeedbackDatasetSealJobParams),
    FeedbackTraining(FeedbackTrainingJobParams),
    FeedbackCalibration(FeedbackCalibrationJobParams),
    FeedbackCpcv(FeedbackCpcvJobParams),
    FeedbackValidation(FeedbackValidationJobParams),
    FeedbackComparison(Box<FeedbackComparisonJobParams>),
    FeedbackShadowBind(Box<ShadowBindingJobParams>),
    FeedbackShadow(Box<FeedbackShadowJobParams>),
    FeedbackDecision(Box<FeedbackDecisionJobParams>),
    TradePolicyFit(TradePolicyFitJobParams),
    TradePolicyValidation(TradePolicyValidationJobParams),
}

impl ResearchJobParams {
    /// Relational discriminator corresponding to this typed payload.
    #[must_use]
    pub const fn kind(&self) -> ResearchJobKind {
        match self {
            Self::DatasetBuild(_) => ResearchJobKind::DatasetBuild,
            Self::ModelTrain(_) => ResearchJobKind::ModelTrain,
            Self::Backtest(_) => ResearchJobKind::Backtest,
            Self::BiasTableFit(_) => ResearchJobKind::BiasTableFit,
            Self::ModelCalibrationFit(_) => ResearchJobKind::ModelCalibrationFit,
            Self::CpcvBacktest(_) => ResearchJobKind::CpcvBacktest,
            Self::FeatureParity(_) => ResearchJobKind::FeatureParity,
            Self::FeedbackTruthFreeze(_) => ResearchJobKind::FeedbackTruthFreeze,
            Self::FeedbackCoverage(_) => ResearchJobKind::FeedbackCoverage,
            Self::FeedbackAttribution(_) => ResearchJobKind::FeedbackAttribution,
            Self::FeedbackDrift(_) => ResearchJobKind::FeedbackDrift,
            Self::FeedbackRecipePlan(_) => ResearchJobKind::FeedbackRecipePlan,
            Self::FeedbackDatasetSeal(_) => ResearchJobKind::FeedbackDatasetSeal,
            Self::FeedbackTraining(_) => ResearchJobKind::FeedbackTraining,
            Self::FeedbackCalibration(_) => ResearchJobKind::FeedbackCalibration,
            Self::FeedbackCpcv(_) => ResearchJobKind::FeedbackCpcv,
            Self::FeedbackValidation(_) => ResearchJobKind::FeedbackValidation,
            Self::FeedbackComparison(_) => ResearchJobKind::FeedbackComparison,
            Self::FeedbackShadowBind(_) => ResearchJobKind::FeedbackShadowBind,
            Self::FeedbackShadow(_) => ResearchJobKind::FeedbackShadow,
            Self::FeedbackDecision(_) => ResearchJobKind::FeedbackDecision,
            Self::TradePolicyFit(_) => ResearchJobKind::TradePolicyFit,
            Self::TradePolicyValidation(_) => ResearchJobKind::TradePolicyValidation,
        }
    }
}

/// Live progress snapshot persisted to `progress_json` and pushed over WS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
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
            Some(total) if total > 0 => {
                let processed = self.processed.min(total);
                let processed = u32::try_from(processed).ok().map(f64::from)?;
                let total = u32::try_from(total).ok().map(f64::from)?;
                Some(processed / total)
            }
            _ => None,
        }
    }
}

/// Structured failure payload persisted to `error_json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod tests {
    use super::ResearchJobParams;
    use crate::{
        domain::api::TradePolicyValidationJobParams,
        enums::quant::ResearchJobKind,
        types::{TradePolicyArtifactId, TradePolicyValidationRunId, UserId},
    };

    impl ResearchJobParams {
        fn test_fixture() -> Self {
            Self::TradePolicyValidation(TradePolicyValidationJobParams {
                validation_run_id: TradePolicyValidationRunId::from_v7(),
                artifact_id: TradePolicyArtifactId::from_v7(),
                actor_id: UserId::from_v7(),
                reason: "typed round trip".to_owned(),
            })
        }
    }

    #[test]
    fn tagged_params_reject_unknown() {
        let params = ResearchJobParams::test_fixture();
        assert_eq!(params.kind(), ResearchJobKind::TradePolicyValidation);
        let encoded = serde_json::to_value(&params).expect("serialize typed params");
        let decoded: ResearchJobParams =
            serde_json::from_value(encoded.clone()).expect("deserialize typed params");
        assert_eq!(decoded, params);

        let mut wrong_kind = encoded.clone();
        wrong_kind["kind"] = serde_json::json!("backtest");
        assert!(serde_json::from_value::<ResearchJobParams>(wrong_kind).is_err());

        let mut unknown = encoded;
        unknown["params"]["unversioned_extension"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ResearchJobParams>(unknown).is_err());
    }
}
