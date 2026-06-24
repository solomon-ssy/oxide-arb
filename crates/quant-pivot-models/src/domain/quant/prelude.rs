//! Curated quant domain prelude.
//!
//! This module keeps quant-facing call sites explicit without re-exporting
//! `SeaORM` entity `Model` types through the domain layer.

pub use super::{
    ApproveOrderIntent, ExecutionOrderInfo, ExecutionOrderModel, FactorDefinitionInfo,
    FactorValueInfo, FactorValueModel, FeatureVectorInfo, FeatureVectorModel, MarketSelectionInfo,
    MarketSelectionMemberInfo, MarketSelectionModel, ModelRunInfo, ModelSpecInfo, ModelVersionInfo,
    NewExecutionOrder, NewFactorDefinition, NewFactorValue, NewFeatureVector, NewMarketSelection,
    NewMarketSelectionMember, NewModelRun, NewModelSpec, NewModelVersion, NewOrderIntent,
    NewPortfolioPlan, NewRecommendation, NewRecommendationAttribution, NewRecommendationReport,
    NewTrainingDataset, OrderIntentInfo, PortfolioPlanInfo, PortfolioPlanModel, QuantModelRunModel,
    RecommendationAttributionInfo, RecommendationAttributionModel, RecommendationInfo,
    RecommendationReportInfo, RecommendationReportModel, TrainingDatasetInfo,
};
