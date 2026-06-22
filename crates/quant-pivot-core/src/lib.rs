//! `oxide-arb-core` — System hub for the Endgame trading engine.
//!
//! Owns the application lifecycle (`AppContext`), data pipeline, detection
//! layer, execution engine, DI bridge adapters, and orchestration services.
//!
//! - [`bridge`] — trait adapters (`impl RiskMetrics`, `impl FeeEstimator`, …)
//! - [`service`] — periodic orchestration (`GammaService`, cache refresh, …)

pub mod app;
pub mod bridge;
pub mod control;
pub mod detection;
pub mod execution;
pub mod exposure;
pub mod infra;
pub mod observability;
pub mod pipeline;
pub mod post_trade;
pub mod runtime_config;
pub mod service;
pub mod trade_integrity;
