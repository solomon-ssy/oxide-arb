//! End-to-end web integration tests (auth + authorization) against real Postgres
//! and Redis via testcontainers.

#[path = "common/auth_helpers.rs"]
mod auth_helpers;
#[path = "common/client.rs"]
mod client;
#[path = "common/core_report_port.rs"]
mod core_report_port;
#[path = "common/harness.rs"]
mod harness;
#[path = "common/order_intent_port.rs"]
mod order_intent_port;
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
#[path = "web/metrics.rs"]
mod metrics;
#[path = "web/operation_log.rs"]
mod operation_log;
#[path = "web/phase0.rs"]
mod phase0;
#[path = "web/quant_intents.rs"]
mod quant_intents;
#[path = "web/quant_reports.rs"]
mod quant_reports;
#[path = "web/readiness.rs"]
mod readiness;
#[path = "web/ws.rs"]
mod ws;
