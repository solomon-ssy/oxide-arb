//! Curated quant domain prelude.
//!
//! This module keeps quant-facing call sites explicit without re-exporting
//! `SeaORM` entity `Model` types through the domain layer.

pub use super::{
    ApproveOrderIntent, ExecutionOrderInfo, ExecutionOrderModel, FactorDefinitionInfo,
    FactorValueInfo, FactorValueModel, FeatureVectorInfo, FeatureVectorModel, ModelRunInfo,
    ModelSpecInfo, ModelVersionInfo, NewExecutionOrder, NewFactorDefinition, NewFactorValue,
    NewFeatureVector, NewModelRun, NewModelSpec, NewModelVersion, NewOrderIntent, NewPortfolioPlan,
    NewRecommendation, NewRecommendationAttribution, NewRecommendationReport, NewUniverseMember,
    NewUniverseSnapshot, OrderIntentInfo, PortfolioPlanInfo, PortfolioPlanModel,
    QuantModelRunModel, RecommendationAttributionInfo, RecommendationAttributionModel,
    RecommendationInfo, RecommendationReportInfo, RecommendationReportModel, UniverseMemberInfo,
    UniverseSnapshotInfo, UniverseSnapshotModel,
};
