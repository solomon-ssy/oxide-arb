pub mod calibration_source;
pub mod execution_mode;
pub mod fee_estimator;
pub mod market_data;
pub mod metrics_scrape;
pub mod potential_loss_store;
pub mod risk_audit_sink;
pub mod risk_metrics;
pub mod risk_persistence;
pub mod trading_gate;

use crate::bridge::fee_estimator::CoreFeeEstimator;
use oxide_arb_algorithm::pipeline::OpportunityPipeline;

/// Production opportunity pipeline wired with the core fee estimator.
pub type CoreOpportunityPipeline = OpportunityPipeline<CoreFeeEstimator>;
