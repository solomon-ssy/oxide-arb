//! Core port implementations — HTTP/admin adapters over domain traits.

pub mod account_read;
pub mod backtest;
pub mod cpcv_backtest;
pub mod execution_read;
pub mod execution_recovery;
pub mod market_data;
pub mod metrics_scrape;
pub mod model_training;
pub mod quant_report;
pub mod reconciliation;
pub mod research_catalog;
pub mod research_job;
pub mod settlement_control;
pub mod structural_monitor;
pub mod training_dataset;
