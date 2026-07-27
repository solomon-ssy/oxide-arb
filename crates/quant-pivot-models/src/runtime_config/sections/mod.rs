//! Runtime-config section structs grouped by document area.

mod config;

pub use config::{
    AutoExecutionConfig, CryptoCrossCheckConfig, CryptoDomainConfig, DataQualityConfig,
    DomainConfig, EntryConditionWorkerConfig, ExecutionConfig, FactorCrossSectionConfig,
    FactorHeadConfig, FactorNormalizationConfig, FactorOrthogonalizeConfig, FactorsConfig,
    FavoriteLongshotConfig, FeaturesConfig, KellySafetyConfig, MAX_REPORT_TOP_N,
    ModelCalibrationConfig, ModelConfig, MomentumFeaturesConfig, NegRiskStructuralConfig,
    ParticipantConcentrationConfig, PerFactorNormalization, PolicyValidationConfig,
    PortfolioBudget, PortfolioConfig, PortfolioConstraints, QualityGateConfig,
    ReportScheduleConfig, ReportsConfig, ResearchConfig, ResearchTrainingConfig,
    ResearchValidationConfig, ResearchValidationCpcvConfig, ResearchValidationGatesConfig,
    ResearchValidationPboConfig, ResearchValidationPurgeConfig, ResearchValidationTrialsConfig,
    ReversalAfterShockConfig, SelectionConfig, SellQualityGateConfig, SellScorerConfig,
    SemiAutoConfig, StructuralFactorsConfig, StructuralFeaturesConfig, TrainingConfig,
    WeatherDomainConfig,
};
