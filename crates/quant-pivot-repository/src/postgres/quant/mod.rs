//! Quant-pivot Postgres repository implementations.

mod backtest_report;
mod comparison_report;
mod dataset;
mod factor;
mod feature;
mod governance_audit;
mod market_selection;
mod model_registry;
mod model_run;
mod order_intent;
mod recommendation_report;
mod shadow_comparison;

pub use backtest_report::PgBacktestReportRepository;
pub use comparison_report::PgModelComparisonReportRepository;
pub use dataset::PgTrainingDatasetRepository;
pub use factor::PgFactorRepository;
pub use feature::PgFeatureRepository;
pub use governance_audit::PgModelGovernanceAuditRepository;
pub use market_selection::PgMarketSelectionRepository;
pub use model_registry::PgModelRegistryRepository;
pub use model_run::PgModelRunRepository;
pub use order_intent::PgOrderIntentRepository;
pub use recommendation_report::PgRecommendationReportRepository;
pub use shadow_comparison::PgShadowComparisonRepository;
