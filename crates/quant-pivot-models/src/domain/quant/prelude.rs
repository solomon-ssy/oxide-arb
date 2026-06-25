//! Curated quant domain prelude.
//!
//! This module keeps quant-facing call sites explicit without re-exporting
//! `SeaORM` entity `Model` types through the domain layer.

pub use super::{
    ApproveOrderIntent, BacktestReportInfo, ExecutionOrderInfo, ExecutionOrderModel,
    FactorDefinitionInfo, FactorValueInfo, FactorValueModel, FeatureVectorInfo, FeatureVectorModel,
    MarketSelectionInfo, MarketSelectionMemberInfo, MarketSelectionModel,
    ModelComparisonReportInfo, ModelRunInfo, ModelSpecInfo, ModelVersionInfo, NewBacktestReport,
    NewExecutionOrder, NewFactorDefinition, NewFactorValue, NewFeatureVector, NewMarketSelection,
    NewMarketSelectionMember, NewModelComparisonReport, NewModelRun, NewModelSpec, NewModelVersion,
    NewOrderIntent, NewPortfolioPlan, NewRecommendation, NewRecommendationAttribution,
    NewRecommendationReport, NewTrainingDataset, OrderIntentInfo, PortfolioPlanInfo,
    PortfolioPlanModel, QuantModelRunModel, RecommendationAttributionInfo,
    RecommendationAttributionModel, RecommendationInfo, RecommendationReportInfo,
    RecommendationReportModel, TrainingDatasetInfo,
};
