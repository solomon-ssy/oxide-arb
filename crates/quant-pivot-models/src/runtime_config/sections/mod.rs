//! Runtime-config section structs grouped by document area.

mod config;

pub use config::{
    BuyRouteBinding, CapitalTimeBucketLimit, CryptoCrossCheckConfig, CryptoDomainConfig,
    DataQualityConfig, DomainConfig, EntryConditionWorkerConfig, FactorCrossSectionConfig,
    FactorHeadConfig, FactorNormalizationConfig, FactorOrthogonalizeConfig, FactorsConfig,
    FavoriteLongshotConfig, FeaturesConfig, MAX_REPORT_TOP_N, ModelBinding, ModelBindingSource,
    ModelCalibrationConfig, ModelConfig, MomentumFeaturesConfig, NegRiskStructuralConfig,
    ParticipantConcentrationConfig, PerFactorNormalization, PolicyAutomaticLimits,
    PolicyValidationConfig, PortfolioAdmission, PortfolioBudget, PortfolioConfig,
    PortfolioExposureLimits, PortfolioScenarioModelArtifactBinding, PortfolioTailRisk,
    QualityGateConfig, ReportScheduleConfig, ReportsConfig, ResearchConfig, ResearchTrainingConfig,
    ResearchValidationConfig, ResearchValidationCpcvConfig, ResearchValidationGatesConfig,
    ResearchValidationPboConfig, ResearchValidationPurgeConfig, ResearchValidationTrialsConfig,
    ReversalAfterShockConfig, SelectionConfig, SellQualityGateConfig, SellScorerConfig,
    StructuralFactorsConfig, StructuralFeaturesConfig, TrainingConfig, WeatherDomainConfig,
};
