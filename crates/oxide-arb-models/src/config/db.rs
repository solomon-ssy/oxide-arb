//! Database configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub postgres: PostgresConfig,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PostgresConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_max_conns")]
    pub max_connections: u32,
    #[serde(default = "default_min_conns")]
    pub min_connections: u32,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_acquire_timeout")]
    pub acquire_timeout_secs: u64,
    #[serde(default = "default_max_lifetime")]
    pub max_lifetime_secs: u64,

    /// `statement_timeout` prevents runaway queries. Applied per-connection via
    /// libpq startup `options` parameter so every pool connection inherits it.
    /// Unit: milliseconds. 0 = disabled.
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,

    /// `idle_in_transaction_session_timeout` kills idle transactions that hold
    /// locks. Unit: milliseconds. 0 = disabled.
    #[serde(default = "default_idle_in_transaction_timeout_ms")]
    pub idle_in_transaction_timeout_ms: u64,

    /// `lock_timeout` kills statements that wait too long for a row/table lock.
    /// Unit: milliseconds. 0 = disabled.
    #[serde(default = "default_lock_timeout_ms")]
    pub lock_timeout_ms: u64,

    /// Per-operation sort/hash memory. Passed as-is to `PostgreSQL`.
    #[serde(default = "default_work_mem")]
    pub work_mem: String,

    /// Whether to run a post-connect self-check that verifies session-level GUC
    /// parameters actually took effect. Catches `PgBouncer` stripping startup
    /// `options`. On mismatch: abort startup with a descriptive error.
    #[serde(default = "default_verify_session_params")]
    pub verify_session_params: bool,

    /// Statement cache capacity per-connection (sqlx `PreparedStatementCache`).
    #[serde(default = "default_statement_cache_capacity")]
    pub statement_cache_capacity: u32,

    /// Application name reported to `pg_stat_activity`.
    #[serde(default = "default_application_name")]
    pub application_name: String,
}

impl PostgresConfig {
    /// Build the connection URL.
    pub fn to_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode=prefer",
            self.user, self.password, self.host, self.port, self.database,
        )
    }

    /// Build `-c key=value` pairs for libpq startup `options` GUC injection.
    /// Only non-zero / non-empty values are included.
    pub fn startup_guc_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::with_capacity(4);
        if self.statement_timeout_ms > 0 {
            pairs.push(("statement_timeout", self.statement_timeout_ms.to_string()));
        }
        if self.idle_in_transaction_timeout_ms > 0 {
            pairs.push((
                "idle_in_transaction_session_timeout",
                self.idle_in_transaction_timeout_ms.to_string(),
            ));
        }
        if self.lock_timeout_ms > 0 {
            pairs.push(("lock_timeout", self.lock_timeout_ms.to_string()));
        }
        let wm = self.work_mem.trim();
        if !wm.is_empty() {
            pairs.push(("work_mem", wm.to_owned()));
        }
        pairs
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            user: default_user(),
            password: String::new(),
            database: default_database(),
            schema: default_schema(),
            max_connections: default_max_conns(),
            min_connections: default_min_conns(),
            connect_timeout_secs: default_connect_timeout(),
            idle_timeout_secs: default_idle_timeout(),
            acquire_timeout_secs: default_acquire_timeout(),
            max_lifetime_secs: default_max_lifetime(),
            statement_timeout_ms: default_statement_timeout_ms(),
            idle_in_transaction_timeout_ms: default_idle_in_transaction_timeout_ms(),
            lock_timeout_ms: default_lock_timeout_ms(),
            work_mem: default_work_mem(),
            verify_session_params: default_verify_session_params(),
            statement_cache_capacity: default_statement_cache_capacity(),
            application_name: default_application_name(),
        }
    }
}

fn default_host() -> String {
    "localhost".into()
}
const fn default_port() -> u16 {
    5432
}
fn default_user() -> String {
    "oxide".into()
}
fn default_database() -> String {
    "oxide_arb".into()
}
fn default_schema() -> String {
    "public".into()
}
const fn default_max_conns() -> u32 {
    10
}
const fn default_min_conns() -> u32 {
    2
}
const fn default_connect_timeout() -> u64 {
    10
}
const fn default_idle_timeout() -> u64 {
    300
}
const fn default_acquire_timeout() -> u64 {
    10
}
const fn default_max_lifetime() -> u64 {
    1800
}
const fn default_statement_timeout_ms() -> u64 {
    30_000
}
const fn default_idle_in_transaction_timeout_ms() -> u64 {
    60_000
}
const fn default_lock_timeout_ms() -> u64 {
    5_000
}
fn default_work_mem() -> String {
    "16MB".into()
}
const fn default_verify_session_params() -> bool {
    true
}
const fn default_statement_cache_capacity() -> u32 {
    256
}
fn default_application_name() -> String {
    "oxide-arb".into()
}
