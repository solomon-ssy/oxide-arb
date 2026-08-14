//! Quant-pivot repository traits.

mod account_snapshot;
mod attribution_artifact;
mod backtest_path_set;
mod backtest_report;
mod basis_alert;
mod calibration_artifact;
mod capital_allocation;
mod comparison_report;
mod dataset;
mod domain_projection;
mod domain_source_cursor;
mod domain_source_expectation;
mod entry_condition;
mod equity_snapshot;
mod execution_account;
mod execution_attempt_outcome;
mod execution_order;
mod execution_submission;
mod fact;
mod fact_read;
mod factor;
mod feature;
mod feature_parity;
mod feedback_cohort;
mod feedback_cycle;
mod feedback_recipe;
mod feedback_scheduler;
mod governance_audit;
mod market_linkage;
mod model;
mod model_candidate_manifest;
mod model_registry;
mod model_route_bootstrap;
mod model_route_promotion;
mod model_route_shadow_binding;
mod order_intent;
mod portfolio_plan;
mod position;
mod promotion_permit;
mod recommendation;
mod recommendation_execution_rollup;
mod recommendation_report;
mod recommendation_resolution_outcome;
mod reconciliation;
mod report_run;
mod research_job;
mod research_readiness;
mod reserved_capital;
mod resolution_observation;
mod runtime_activity;
mod selection;
pub mod settlement_governance;
pub mod settlement_redeem;
mod shadow_comparison;
mod source_slice;
mod trade_policy;
mod trade_tape_block_cursor;

pub use account_snapshot::AccountSnapshotRepository;
pub use attribution_artifact::{AttributionArtifactRepository, AttributionArtifactWriteOutcome};
pub use backtest_path_set::{BacktestPathSetRepository, CpcvPathSetCommit};
pub use backtest_report::BacktestReportRepository;
pub use basis_alert::BasisAlertRepository;
pub use calibration_artifact::CalibrationArtifactRepository;
pub use capital_allocation::CapitalAllocationRepository;
pub use comparison_report::ModelComparisonReportRepository;
pub use dataset::TrainingDatasetRepository;
pub use domain_projection::DomainProjectionRepository;
pub use domain_source_cursor::DomainSourceCursorRepository;
pub use domain_source_expectation::DomainSourceExpectationRepository;
pub use entry_condition::EntryConditionRepository;
pub use equity_snapshot::EquitySnapshotRepository;
pub use execution_account::ExecutionAccountRepository;
pub use execution_attempt_outcome::ExecutionAttemptOutcomeRepository;
pub use execution_order::ExecutionOrderRepository;
pub use execution_submission::ExecutionSubmissionRepository;
pub use fact::{FactWriter, QuantFactRepository};
pub use fact_read::QuantFactReadRepository;
pub use factor::FactorRepository;
pub use feature::FeatureRepository;
pub use feature_parity::{
    EnqueueFrozenFeatureParityOutcome, FeatureParityEventRepository, FeatureParityLatchActor,
    FeatureParityRepository, ServingEvidenceRepository,
};
pub use feedback_cohort::FeedbackCohortRepository;
pub use feedback_cycle::{
    DriftReportWriteOutcome, FeedbackCoordinatorFaultWriteOutcome, FeedbackCoordinatorQuarantine,
    FeedbackCycleCasOutcome, FeedbackCycleClaim, FeedbackCycleClaimMode, FeedbackCycleGeneration,
    FeedbackCycleLeaseGuard, FeedbackCycleRepository, FeedbackCycleWriteOutcome,
    FeedbackEvaluationWriteOutcome, FeedbackOutboxRepository, FeedbackStageWriteOutcome,
    FeedbackTriggerCommit, FeedbackTriggerWriteOutcome,
};
pub use feedback_recipe::{FeedbackRecipeTemplateRepository, FeedbackRecipeTemplateWriteOutcome};
pub use feedback_scheduler::FeedbackSchedulerRepository;
pub use governance_audit::ModelGovernanceAuditRepository;
pub use market_linkage::MarketLinkageRepository;
pub use model::ModelRunRepository;
pub use model_candidate_manifest::{
    ModelCandidateManifestRepository, ModelCandidateManifestWriteOutcome,
};
pub use model_registry::ModelRegistryRepository;
pub use model_route_bootstrap::{
    ModelRouteBootstrapCommit, ModelRouteBootstrapOutcome, ModelRouteBootstrapRepository,
};
pub use model_route_promotion::{
    ModelRoutePromotionCommit, ModelRoutePromotionOutcome, ModelRoutePromotionRepository,
};
pub use model_route_shadow_binding::{
    ModelRouteShadowBindingRepository, ShadowBindingCancelCommit, ShadowBindingCancelOutcome,
    ShadowBindingCommit, ShadowBindingCommitOutcome, ShadowBindingRejectCommit,
    ShadowBindingRejectOutcome,
};
pub use order_intent::OrderIntentRepository;
pub use portfolio_plan::PortfolioPlanRepository;
pub use position::PositionRepository;
pub use promotion_permit::{
    PromotionPermitIssueOutcome, PromotionPermitPage, PromotionPermitRepository,
    PromotionPermitRevokeOutcome,
};
pub use recommendation::RecommendationRepository;
pub use recommendation_execution_rollup::RecommendationExecutionRollupRepository;
pub use recommendation_report::RecommendationReportRepository;
pub use recommendation_resolution_outcome::RecommendationResolutionOutcomeRepository;
pub use reconciliation::ReconciliationRepository;
pub use report_run::ReportRunRepository;
pub use research_job::{
    KindRunningCount, ReclaimOutcome, ResearchJobEnqueueOutcome, ResearchJobRepository,
    ResearchJobRetryOutcome,
};
pub use research_readiness::{ResearchReadinessEvidenceRepository, ShadowLatencyObservation};
pub use reserved_capital::ReservedCapitalRepository;
pub use resolution_observation::ResolutionObservationRepository;
pub use runtime_activity::RuntimeActivityRepository;
pub use selection::MarketSelectionRepository;
pub use shadow_comparison::{ShadowComparisonRepository, ShadowComparisonWriteOutcome};
pub use source_slice::SourceSliceRepository;
pub use trade_policy::TradePolicyRepository;
pub use trade_tape_block_cursor::TradeTapeBlockCursorRepository;
