//! End-to-end web integration tests (auth + authorization) against real Postgres
//! and Redis via testcontainers.
//!
//! Run with `cargo test -p oxide-arb-web --test web -- --ignored`.

#[path = "common/auth_helpers.rs"]
mod auth_helpers;
#[path = "common/client.rs"]
mod client;
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
#[path = "web/governance.rs"]
mod governance;
#[path = "web/operation_log.rs"]
mod operation_log;
#[path = "web/runtime_config.rs"]
mod runtime_config;
