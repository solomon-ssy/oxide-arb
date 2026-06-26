//! Curated quant domain prelude.
//!
//! This module keeps quant-facing call sites explicit without re-exporting
//! `SeaORM` entity `Model` types through the domain layer.

pub use super::{
    AccountSnapshotInfo, ApproveOrderIntent, BacktestReportInfo, CapitalAllocationInfo,
    ExecutionOrderInfo, FactorDefinitionInfo, FactorValueInfo, FactorValueModel, FeatureVectorInfo,
    FeatureVectorModel, MarketSelectionInfo, MarketSelectionMemberInfo, MarketSelectionModel,
    ModelComparisonReportInfo, ModelRunInfo, ModelSpecInfo, ModelVersionInfo, NewAccountSnapshot,
    NewBacktestReport, NewCapitalAllocation, NewExecutionOrder, NewFactorDefinition,
    NewFactorValue, NewFeatureVector, NewMarketSelection, NewMarketSelectionMember,
    NewModelComparisonReport, NewModelRun, NewModelSpec, NewModelVersion, NewOrderIntent,
    NewPortfolioPlan, NewPosition, NewRecommendation, NewRecommendationAttribution,
    NewRecommendationReport, NewReconciliation, NewReportTransaction, NewTrainingDataset,
    OrderIntentInfo, PortfolioPlanInfo, PositionInfo, QuantModelRunModel,
    RecommendationAttributionInfo, RecommendationInfo, RecommendationReportInfo,
    ReconciliationInfo, TrainingDatasetInfo,
};
