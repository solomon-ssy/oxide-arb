//! End-to-end web integration tests (auth + authorization) against real Postgres
//! and Redis via testcontainers.
//!
//! Run with `cargo test -p oxide-arb-web --test web -- --ignored`.
//! Docker testcontainers may flake under parallel load; use `--test-threads=1` in CI if needed.

#[path = "common/auth_helpers.rs"]
mod auth_helpers;
#[path = "common/client.rs"]
mod client;
#[path = "common/control_factor_fixture.rs"]
mod control_factor_fixture;
#[path = "common/harness.rs"]
mod harness;
#[path = "common/pg.rs"]
mod pg;
#[path = "common/redis.rs"]
mod redis;

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
#[path = "web/readiness.rs"]
mod readiness;
#[path = "web/runtime_config.rs"]
mod runtime_config;
#[path = "web/ws.rs"]
mod ws;
