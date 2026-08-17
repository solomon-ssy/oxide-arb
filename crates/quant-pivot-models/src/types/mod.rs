pub mod account;
pub mod backtest;
pub mod book_snapshot_ref;
pub mod calibration;
pub mod catalog;
pub mod clob_market_info;
pub mod config_governance;
pub mod content;
pub mod data_plane;
pub mod dataset_coverage;
pub(crate) mod decimal_string_option;
pub mod domain;
pub mod domain_capability;
pub mod domain_classification;
pub mod entry_condition;
pub mod execution_payload;
pub mod factor;
pub mod feature;
pub mod ids;
pub mod maker_rebate;
pub mod market_context;
pub mod micro;
pub mod model_input;
pub mod model_lineage;
pub mod model_metrics;
pub mod model_quality;
pub mod model_serving;
pub mod model_spec;
pub mod model_training;
pub mod money;
pub mod outcome_binding;
pub mod persistence_document;
pub mod recommendation_identity;
pub mod reconciliation_payload;
pub mod report_data_quality;
pub mod report_fact_bundle;
pub mod report_funnel;
pub mod report_payload;
pub mod research_job_payload;
pub mod research_profile;
pub mod research_readiness;
pub mod selection;
pub mod semantic;
pub mod settlement_payload;
pub mod shadow;
pub mod source_slice;
pub mod stable_name;
pub mod trade_policy;
pub mod trade_policy_evidence;
pub mod training;
pub mod venue_fill;

pub use account::{AccountPositions, ExposureBreakdown, PositionSnapshot};
pub use book_snapshot_ref::{BookSnapshotRef, BookSnapshotRefParseError, BookSnapshotSource};
pub use catalog::CatalogMarketIds;
pub use clob_market_info::{
    ClobFeeDetails, ClobMarketInfoVersion, ClobTokenDescriptor, ClobTokenSet,
};
pub use config_governance::{
    ConfigGovernanceTextError, DeploymentEnvironment, PolicyBundleGeneration,
    PolicyBundleGenerationError, PolicyIdempotencyKey, PolicyPreflightToken,
    SCHEMA_MUTATION_ADVISORY_LOCK_KEY,
};
pub use content::{ArtifactUri, ContentHash, ContentHashText, SchemaVersion};
pub use data_plane::{PartitionBatchId, PartitionId, TokenKey};
pub use dataset_coverage::{
    DatasetCoverage, DatasetFeatureStateCounts, MatrixCoverageProbe, TrainingHorizonsSecs,
};
pub use domain::{
    BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainVocabularyError, HkoStation,
    IcaoStation, ResolverVersion, TemperatureBand, TemperatureCelsius, TemperatureUnit,
    WeatherContractFinalizationPolicy, WeatherTemperatureStatistic,
    WeatherTemperatureStatisticEnum, WeatherTemperatureStatisticIter,
    WeatherTemperatureStatisticParseError, WeatherTemperatureStatisticVariant,
    WeatherTemperatureStatisticVariantIter,
};
pub use domain_capability::{
    CapabilityEligibility, CapabilitySourceBinding, DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION,
    DomainCapabilityReasonCode, DomainCapabilityRegistryArtifact, DomainContractCapability,
    DomainContractFamily, DomainMeasurementUnit, DomainTimezonePolicy, SourceCredentialPolicy,
    WeatherVariable,
};
pub use entry_condition::{
    ClockAnchor, ClockCondition, ConditionLeafEvidence, ConditionNodeEvaluation, ConditionTruth,
    ConditionUnavailableReason, ConfirmationPolicy, CryptoEnteredFoldState, CryptoPriceInput,
    CryptoPriceReportInput, CryptoSubjectPredicateEntered, ENTRY_CONDITION_EVALUATOR_VERSION,
    ENTRY_CONDITION_INPUT_CHANNEL, ENTRY_CONDITION_MAX_CANDIDATES, ENTRY_CONDITION_MAX_DEPTH,
    ENTRY_CONDITION_MAX_GROUP_CHILDREN, ENTRY_CONDITION_MAX_NODES,
    ENTRY_CONDITION_MIN_GROUP_CHILDREN, ENTRY_CONDITION_SCHEMA_VERSION, EntryConditionArtifactV1,
    EntryConditionBinding, EntryConditionFactorBinding, EntryConditionFoldState,
    EntryConditionInputSet, EntryConditionNode, EntryConditionPlan, EntryConditionSourceBinding,
    EntryConditionV1, EntryConditionValidationError, ExecutablePriceInput, FactorCondition,
    FactorMeasure, FactorSnapshotInput, MarketEventCondition, PriceCondition,
    WeatherDailyTemperatureCrossedTerminalBound, WeatherDailyTemperatureEnteredBand,
    WeatherDailyTemperatureInput, WeatherObservationDayClosedOutsideBand,
};
pub use execution_payload::{
    EntryMakerRebateTerms, EntryOrderSpec, ExitPolicySpec, ExitReinferenceObservation,
    ExitReinferenceVerdictKind, NextScaleOutProjection, OrderAmount, PendingScaleOut,
    PreparedFeeSchedule, PreparedVenueOrder, ScaleOutState, VenueOrderAmount,
};
pub use feature::{
    CatalogDecisionRef, DecisionCaptureEvidence, DecisionSnapshotEvidence, DomainFeatureSlice,
    EvidenceSourceRef, FeatureCell, FeatureCellState, FeatureParityDetail,
    FeatureParityDetailSource, FeatureSourceRefs, FeatureStaleness, FeatureValue,
    FeatureVectorPayload, FinalizedExecutionEvidence, NullReason, SelectionMemberEvidence,
    SelectorParityEvidence,
};
pub use ids::{
    AccountSnapshotId, AttributionArtifactId, AuditEventId, BacktestPathSetId, BacktestReportId,
    BasisAlertId, CalibrationArtifactId, CalibrationArtifactPublicationId,
    CandidateRecipePlanArtifactId, CapitalAllocationId, CatalogEventChangeId, CatalogEventObjectId,
    CatalogMarketChangeId, CatalogMarketObjectId, CatalogSyncBatchId, CatalogSyncRejectionId,
    ClobMarketInfoVersionId, CorrelationId, DecisionPolicySnapshotId, DiagnosticCode,
    DomainEventId, DomainInstrumentKey, DomainSourceExpectationId, DomainSourceId, DriftReportId,
    EconomicTierId, EntryConditionArtifactId, EntryConditionAuditId,
    EntryConditionEvaluationOutboxId, EntryConditionInstanceId, EquitySnapshotId, EventId,
    ExecutionAccountId, ExecutionFeeMeasurementId, ExecutionFillId, ExecutionOrderId,
    ExecutionTradeRefId, ExecutionTransactionRefId, FactorDefinitionId, FactorValueId,
    FeatureParityCandidateId, FeatureParityEventId, FeatureParityRunId, FeatureParityStateId,
    FeatureParitySubjectId, FeatureVectorId, FeedbackAttributionManifestId,
    FeedbackComparisonArtifactId, FeedbackCoordinatorFaultId, FeedbackCoverageArtifactId,
    FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackDriftArtifactId, FeedbackEvaluationUseId,
    FeedbackLearningStageArtifactId, FeedbackRecipeTemplateId, FeedbackShadowArtifactId,
    FeedbackStageEventId, FeedbackTriggerEventId, FeedbackTruthFreezeArtifactId,
    FeedbackValidationArtifactId, FreshBootRunEventId, FreshBootRunId, HistoryFitSealId,
    HistoryServingHeadSealId, MarketId, MarketLinkageId, MarketSelectionId, MenuId,
    ModelCandidateManifestId, ModelComparisonReportId, ModelGovernanceAuditId, ModelRunId,
    ModelSpecId, ModelVersionId, OperationAction, OperationLogId, OrderId, OrderIntentId,
    PolicyActivationId, PolicyApprovalId, PolicyRevisionId, PortfolioPlanId,
    PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId, PositionId,
    PreproductionResetNonce, ProfileArtifactId, PromotionPermitId, RecommendationId,
    RecommendationReportId, ReconciliationId, ReportDataQualitySnapshotId, ReportRouteRunId,
    ReportRunId, ReportScheduleGapId, ReportScheduleId, ResearchJobId, ResearchProfileId,
    ResearchReadinessEvidenceId, ResolutionObservationId, ResolutionRemediationId, RoleCode,
    RoleId, RuntimeControlTransitionId, SettlementAuthorizationId, SettlementChainSubmissionId,
    SettlementExternalCursorId, SettlementGovernedActionId, SettlementInventoryLotId,
    SettlementRedeemId, SettlementRedeemLotId, ShadowBindingArtifactId, ShadowComparisonId,
    SignalCandidateId, SourceSliceId, TokenId, TradePolicyArtifactId, TradePolicyGovernanceAuditId,
    TradePolicyTrialAttemptId, TradePolicyValidationRunId, TrainingDatasetId, TrainingExampleId,
    UserId, VenueIncentiveEventId, VenueTradeId, WorkerId,
};
pub use maker_rebate::{
    MakerRebateDelayBasis, MakerRebateObjectiveStatus, MakerRebateObjectiveZeroReason,
    MakerRebateProgramDayBaseline, MakerRebateScenarioCreditStatus, MakerRebateValuationEvidence,
    MakerRebateValuationHealth,
};
pub use market_context::MarketContext;
pub use micro::{
    MICRO_SCALE, MicroBps, MicroConversionError, MicroPct, MicroPrice, MicroProb, MicroScore,
    MicroShares, MicroUsd,
};
pub use model_input::{
    ModelInputContract, ModelInputRequiredness, ModelInputSpec, ModelTrainingContract,
    ModelTrainingTarget,
};
pub use money::{Bps, PayoutRatio, PayoutRatioError, Price, Probability, Shares, Usd, UsdHours};
pub use outcome_binding::{OutcomeTokenBinding, OutcomeTokenBindingError};
pub use persistence_document::{
    ExternalJsonDocument, OperationDetailDocument, OperationDetailError,
};
pub use recommendation_identity::RecommendationIdentity;
pub use reconciliation_payload::{ReconciliationEvidence, ReconciliationEvidenceChain};
pub use report_data_quality::{ReportDataQualityTokens, TokenDataQualityRecord};
pub use report_fact_bundle::{
    REPORT_FACT_BUNDLE_FORMAT_VERSION, ReportFactBundleV2, ReportFactNotificationRecommendationV2,
    ReportFactNotificationV2, ReportFactTableCommitment,
};
pub use report_funnel::{
    MissingFeatureDiagnostic, ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage,
};
pub use report_payload::{
    BootstrapExitGuidance, ConfidenceSummary, DataQualitySummary, EligibilitySummary,
    EntryOrderPolicy, EntryPlan, EvidenceRefs, EvidenceRefsInput, ExecutionEligibility, ExitPlan,
    FactorBreakdownEntry, OpportunisticExitPolicy, PortfolioRejectionReason,
    RecommendationExitPlan, RecommendationFactorBreakdown, RecommendationPolicyProvenance,
    RecommendationTradePlan, RejectionReasonCount, ReportSummary, RiskEnvelope,
    RiskEnvelopeHashInput, ScaleOutTarget, SizingPlan, ThesisInvalidationPolicy,
    TradePolicyCohortProvenance, TrailingStopPolicy,
};
pub use research_job_payload::{ResearchJobError, ResearchJobParams, ResearchJobProgress};
pub use research_profile::{
    CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, CRYPTO_PRICE_15M_HORIZON_SECS,
    CRYPTO_PRICE_15M_PROFILE_ID, POOLED_1H_CONTROL_PROFILE_ID, POOLED_1H_HORIZON_SECS,
    POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID, ResearchAvailabilityPolicy, ResearchCohortContract,
    ResearchDecisionTrigger, ResearchEvaluationTrack, ResearchEvaluationTrackEnum,
    ResearchEvaluationTrackIter, ResearchEvaluationTrackParseError, ResearchEvaluationTrackVariant,
    ResearchEvaluationTrackVariantIter, ResearchFeatureContract, ResearchFeedbackPolicy,
    ResearchInformationRegime, ResearchLabelContract, ResearchPolicyFitter,
    ResearchProfileArtifact, ResearchProfileArtifactId, ResearchProfileArtifactIdParseError,
    ResearchProfileDataSource, ResearchProfileRef, ResearchProfileSpec, ServingAuthority,
    WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID, WEATHER_FORECAST_24H_HORIZON_SECS,
    WEATHER_FORECAST_24H_PROFILE_ID, builtin_research_profiles, minimum_raw_retention_days,
};
pub use research_readiness::{
    HistoryCoverage, RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, ResearchReadinessEvidencePayload,
    ResearchReadinessSource, ResearchReadinessSourceParseError, ResearchSourceBinding,
    ResearchSourceFilter, ResearchSourceRegistry, ResearchSourceStorageKind,
    ResearchSourceStorageKindParseError, ResearchSourceTimeEncoding,
    ResearchSourceTimeEncodingParseError, RetentionRunwayEvidenceV1, RetentionSourceObservationV1,
    SHADOW_LATENCY_PROFILE_FORMAT_VERSION, ShadowLatencyProfileV1, research_source_registry,
};
pub use selection::{SelectionExclusionSummary, SelectorHashEvidence};
pub use semantic::{
    ArtifactVersion, AttestationKeyId, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash,
    EvmConditionId, EvmTransactionHash, EvmUint256, ReaderContractVersion, RelayerTransactionId,
    ReportTriggerKey, SchemaContractVersion, SemanticTextError, SettlementActionIdempotencyKey,
    SettlementEvidenceVersion, TradePolicyCandidateId,
};
pub use source_slice::{
    SOURCE_SLICE_MANIFEST_FORMAT_VERSION, SourceSliceCatalogProof, SourceSliceInvalidSession,
    SourceSliceManifest, SourceSliceManifestRef, SourceSliceObjectKind, SourceSliceObjectRef,
    SourceSlicePitCutoff, SourceSliceSessionInvalidationReason,
};
pub use trade_policy::{
    EntryConditionTemplate, EntryConditionTemplateV1, EntryOrderTemplate, ExecutablePriceBasis,
    ExitExecutionTemplate, MarketEventTemplate, PassiveFillDistribution, PassiveFillState,
    PassiveFillStateKind, PassivePlacement, ResidualSharePolicy, ScaleOutTemplate,
    TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION,
    TRADE_POLICY_MAX_CANDIDATES, TradePolicyArtifact, TradePolicyArtifactPayload,
    TradePolicyCandidateSpec, TradePolicyCohort, TradePolicyCohortDimension, TradePolicyCohortKey,
    TradePolicyEntryRoute, TradePolicyEvidenceBundleManifest, TradePolicyEvidenceBundleRef,
    TradePolicyEvidenceGap, TradePolicyEvidenceObjectKind, TradePolicyEvidenceObjectRef,
    TradePolicyExecutionEvidence, TradePolicyExitTemplate, TradePolicyFitContract,
    TradePolicyParameterSource, TradePolicyPitCutoffEvidence, TradePolicyPublicationBlocker,
    TradePolicyQualityGate, TradePolicyShrinkDimension, TradePolicyTrialMetrics,
    TradePolicyValidationEvidence, TrailingStopTemplate, VerticalActivationTarget,
    VerticalGateEvidence, VerticalGateKind, canonicalize_policy_candidates,
};
pub use trade_policy_evidence::{
    POLICY_EVIDENCE_OBJECT_FORMAT_VERSION, StructuralVolatilityOosEvidence,
    StructuralVolatilityOosFoldRow, TradePolicyCandidateTrialRow, TradePolicyCohortTrialRow,
    TradePolicyCoverageGapRow, TradePolicyCpcvPathRow, TradePolicyEvidenceFillOutcome,
    TradePolicyEvidenceLiquidityRole, TradePolicyFillEvidenceRow, TradePolicyLatencyScenario,
    TradePolicyObservationCapability, TradePolicyObservationEligibilityRow, TradePolicyReplayGap,
    TradePolicyStatisticalSummaryRow,
};
pub use training::{
    CapabilityRegistryHashes, CohortCensorCount, CohortExclusionCount,
    DATASET_ARTIFACT_FORMAT_VERSION, DATASET_COHORT_MANIFEST_FORMAT_VERSION,
    DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
    DatasetCohortManifest, DatasetManifest, DatasetManifestContractError, DatasetSourceLineage,
    MODEL_LEARNING_COHORT_FORMAT_VERSION, MODEL_SCORE_COHORT_FORMAT_VERSION,
    ModelLearningCohortArtifact, ModelLearningCohortRow, ModelScoreCohortArtifact,
    ModelScoreCohortRow, NewModelLearningCohortRow, NewModelScoreCohortRow, TrainingSampleSource,
    TrainingSampleSources, TrainingSampleSourcesError,
};
pub use venue_fill::{
    FeeMeasurement, FeeMeasurementPriority, MatchMakerRebateEvidence, MatchRebateUnavailableReason,
    VenueFillObservation,
};
