//! Database configuration (`[db]`, deploy): Postgres OLTP + `ClickHouse` OLAP.

use serde::Deserialize;

/// Postgres + `ClickHouse` connections.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Postgres (system of record: trades, positions, governance, RBAC).
    pub postgres: PostgresConfig,
    /// `ClickHouse` (analytics timeseries: ticks, books, detections, evidence).
    pub clickhouse: ClickHouseConfig,
}

/// Postgres connection + pool + per-session GUC parameters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresConfig {
    /// Server host. Default: `localhost`.
    pub host: String,
    /// Server port. Default: `5432`.
    pub port: u16,
    /// Role name. Default: `oxide`.
    pub user: String,
    /// Role password. Set via `QUANT_PIVOT__DB__POSTGRES__PASSWORD` in
    /// production — never in the TOML. Default: empty.
    pub password: String,
    /// Database name. Default: `oxide_arb`.
    pub database: String,
    /// Schema search path. Default: `public`.
    pub schema: String,
    /// Pool upper bound. Size for worst-case concurrent repository access.
    /// Default: `10`.
    pub max_connections: u32,
    /// Pool warm floor. Default: `2`.
    pub min_connections: u32,
    /// TCP connect timeout (seconds). Default: `10`.
    pub connect_timeout_secs: u64,
    /// Idle connection reap timeout (seconds). Default: `300`.
    pub idle_timeout_secs: u64,
    /// Pool acquire timeout (seconds); exceeding indicates pool exhaustion.
    /// Default: `10`.
    pub acquire_timeout_secs: u64,
    /// Max connection lifetime (seconds) before recycling. Default: `1800`.
    pub max_lifetime_secs: u64,

    /// `statement_timeout` prevents runaway queries. Applied per-connection via
    /// libpq startup `options` so every pool connection inherits it.
    /// Unit: milliseconds, `0` = disabled. Default: `30000`.
    pub statement_timeout_ms: u64,

    /// `idle_in_transaction_session_timeout` kills idle transactions that hold
    /// locks. Unit: milliseconds, `0` = disabled. Default: `60000`.
    pub idle_in_transaction_timeout_ms: u64,

    /// `lock_timeout` kills statements that wait too long for a row/table
    /// lock. Unit: milliseconds, `0` = disabled. Default: `5000`.
    pub lock_timeout_ms: u64,

    /// Per-operation sort/hash memory, passed verbatim to `PostgreSQL`.
    /// Default: `16MB`.
    pub work_mem: String,

    /// Post-connect self-check that session GUC parameters actually took
    /// effect (catches `PgBouncer` stripping startup `options`). On mismatch:
    /// abort startup with a descriptive error. Default: `true`.
    pub verify_session_params: bool,

    /// Prepared-statement cache capacity per connection. Default: `256`.
    pub statement_cache_capacity: u32,

    /// Application name reported to `pg_stat_activity`. Default: `quant-pivot`.
    pub application_name: String,
}

impl PostgresConfig {
    /// Build the connection URL for the configured application database.
    pub fn to_url(&self) -> String {
        self.to_url_with_database(&self.database)
    }

    /// Build a connection URL targeting a specific database name on the same
    /// server (e.g. the `postgres` maintenance catalog).
    pub fn to_url_with_database(&self, database: &str) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode=prefer",
            self.user, self.password, self.host, self.port, database,
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
    "quant-pivot".into()
}

/// `ClickHouse` connection + batched-write tuning.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// HTTP endpoint. Default: `http://localhost:8123`.
    pub url: String,
    /// Database name. Default: `oxide_arb`.
    pub database: String,
    /// User name. Default: `default`.
    pub user: String,
    /// Password. Set via `QUANT_PIVOT__DB__CLICKHOUSE__PASSWORD` in production —
    /// never in the TOML. Default: empty.
    pub password: String,
    /// Max age (seconds) of a partial batch before it is flushed. Lower =
    /// fresher analytics, more insert requests. Default: `5`.
    pub flush_interval_secs: u64,
    /// Rows per insert batch; a full batch flushes immediately. `ClickHouse`
    /// favors large batches — sized for the L2 tick feed (~3K rows/s peaks).
    /// Default: `5000`.
    pub batch_size: usize,
    /// Maximum concurrent insert operations (semaphore). Prevents overwhelming
    /// the server under high tick ingestion rates. Default: `8`.
    pub max_concurrent_inserts: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_ch_url(),
            database: default_ch_database(),
            user: default_ch_user(),
            password: String::new(),
            flush_interval_secs: default_ch_flush_interval(),
            batch_size: default_ch_batch_size(),
            max_concurrent_inserts: default_ch_max_concurrent_inserts(),
        }
    }
}

fn default_ch_url() -> String {
    "http://localhost:8123".into()
}
fn default_ch_database() -> String {
    "oxide_arb".into()
}
fn default_ch_user() -> String {
    "default".into()
}
const fn default_ch_flush_interval() -> u64 {
    5
}
const fn default_ch_batch_size() -> usize {
    5000
}
const fn default_ch_max_concurrent_inserts() -> usize {
    8
}
