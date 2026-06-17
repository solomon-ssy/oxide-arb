//! End-to-end web integration tests (auth + authorization) against real Postgres
//! and Redis via testcontainers.
//!
//! Run with `cargo test -p oxide-arb-web --test web -- --ignored`.
//! Docker testcontainers may flake under parallel load; prefer `cargo test-docker`
//! (serial) or pass `--test-threads=1` when running locally.

#[path = "common/auth_helpers.rs"]
mod auth_helpers;
#[path = "common/client.rs"]
mod client;
#[path = "common/control_factor_fixture.rs"]
mod control_factor_fixture;
#[path = "common/harness.rs"]
mod harness;
#[path = "common/headers.rs"]
mod headers;
#[path = "common/pg.rs"]
mod pg;
#[path = "common/redis.rs"]
mod redis;
#[path = "common/repos.rs"]
mod repos;

#[path = "web/auth.rs"]
mod auth;
#[path = "web/authz.rs"]
mod authz;
#[path = "web/business.rs"]
mod business;
#[path = "web/governance.rs"]
mod governance;
#[path = "web/metrics.rs"]
mod metrics;
#[path = "web/operation_log.rs"]
mod operation_log;
#[path = "web/opportunities.rs"]
mod opportunities;
#[path = "web/readiness.rs"]
mod readiness;
#[path = "web/replay_governance.rs"]
mod replay_governance;
#[path = "web/risk_governance.rs"]
mod risk_governance;
#[path = "web/runtime_config.rs"]
mod runtime_config;
#[path = "web/trade_reconcile_governance.rs"]
mod trade_reconcile_governance;
#[path = "web/ws.rs"]
mod ws;
