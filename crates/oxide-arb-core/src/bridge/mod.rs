pub mod balance_querier;
pub mod calibration_source;
pub mod fee_estimator;
pub mod potential_loss_store;
pub mod risk_audit_sink;
pub mod risk_metrics;
pub mod risk_persistence;
pub mod trading_gate;

pub use fee_estimator::CoreFeeEstimator;
pub use oxide_arb_algorithm::pipeline::OpportunityPipeline;

/// Production opportunity pipeline wired with the core fee estimator.
pub type CoreOpportunityPipeline = OpportunityPipeline<CoreFeeEstimator>;
