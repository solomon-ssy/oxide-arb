//! Database configuration (`[db]`, deploy): Postgres OLTP + `ClickHouse` OLAP.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::{ParseError, Url};

use super::secret::SecretText;

/// Postgres + `ClickHouse` connections.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Postgres (system of record: trades, positions, governance, RBAC).
    pub postgres: PostgresConfig,
    /// `ClickHouse` (analytics timeseries: ticks, books, detections, evidence).
    pub clickhouse: ClickHouseConfig,
}

/// Postgres connection + pool + per-session GUC parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// Server host. Default: `localhost`.
    pub host: String,
    /// `PostgreSQL` TCP port used by every transactional repository pool. Default: `5432`.
    pub port: u16,
    /// Database role used by both runtime and explicit schema commands.
    /// Default: `quant_pivot`.
    pub user: String,
    /// Zeroizing `PostgreSQL` authentication secret; safe projections expose only configured state.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub password: SecretText,
    /// Database name. Default: `quant_pivot`.
    pub database: String,
    /// Schema search path. Default: `public`.
    pub schema: String,
    /// Pool upper bound. Size for worst-case concurrent repository access.
    /// Default: `10`.
    pub max_connections: u32,
    /// Minimum number of `PostgreSQL` connections kept warm by the process pool. Default: `2`.
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
    pub fn try_connection_url(&self) -> Result<String, ParseError> {
        self.try_database_url(&self.database)
    }

    /// Build a connection URL targeting a specific database name on the same
    /// server (e.g. the `postgres` maintenance catalog).
    pub fn try_database_url(&self, database: &str) -> Result<String, ParseError> {
        let mut url = Url::parse("postgres://localhost/")?;
        url.set_host(Some(self.host.as_str()))?;
        url.set_port(Some(self.port))
            .map_err(|()| ParseError::InvalidPort)?;
        let escaped_user = escape_url_percent(&self.user);
        if url.set_username(&escaped_user).is_err() {
            return Err(ParseError::InvalidDomainCharacter);
        }
        if !self.password.is_empty() {
            let escaped_password = escape_url_percent(self.password.expose_secret());
            url.set_password(Some(&escaped_password))
                .map_err(|()| ParseError::InvalidDomainCharacter)?;
        }
        url.set_path(&format!("/{database}"));
        url.query_pairs_mut().append_pair("sslmode", "prefer");
        Ok(url.into())
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

/// The `url` setters intentionally preserve percent-escape sequences. Escape a
/// literal percent first so credentials such as `%2F` retain their exact bytes.
fn escape_url_percent(value: &str) -> String {
    value.replace('%', "%25")
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            user: default_user(),
            password: SecretText::default(),
            database: default_database(),
            schema: default_schema(),
            max_connections: default_max_conns(),
            min_connections: default_min_conns(),
            connect_timeout_secs: default_connect_timeout(),
            idle_timeout_secs: default_idle_timeout(),
            acquire_timeout_secs: default_acquire_timeout(),
            max_lifetime_secs: default_max_lifetime(),
            statement_timeout_ms: default_statement_timeout_ms(),
            idle_in_transaction_timeout_ms: default_idle_timeout_ms(),
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
    "quant_pivot".into()
}
fn default_database() -> String {
    "quant_pivot".into()
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
const fn default_idle_timeout_ms() -> u64 {
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

/// Ownership boundary for `ClickHouse` server-level resource governance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseResourceGovernance {
    /// quant-pivot owns the server and requires the exact repository-managed
    /// background-pool and system-log contract at startup.
    #[default]
    SelfManaged,
    /// A managed provider owns server settings/system tables. Application-side
    /// admission and query thread limits still apply; external capacity and
    /// retention evidence is a separate promotion gate.
    ProviderManaged,
}

pub const CLICKHOUSE_INSERT_MAX_ATTEMPTS: u32 = 3;
pub const CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS: u64 = 100;
pub const CLICKHOUSE_INSERT_RETRY_BACKOFF_TOTAL_MS: u64 = 300;
pub const CLICKHOUSE_CANONICAL_PUBLICATION_TIMEOUT_MS: u64 = 2_000;
pub const CLICKHOUSE_CRITICAL_ATTEMPT_MAX_MS: u64 = 1_900;
pub const CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS: u64 = 12_000;
pub const CLICKHOUSE_DURABLE_ADMISSION_TIMEOUT_MS: u64 = 250;
pub const CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS: u64 = 500;
pub const CLICKHOUSE_DURABLE_SHUTDOWN_STAGE_SECS: u64 = 20;
pub const CLICKHOUSE_DERIVED_FACT_FLUSH_MS: u64 = 1_000;
pub const CLICKHOUSE_FLUSH_INTERVAL_MAX_SECS: u64 = 5;
const CLICKHOUSE_BULK_RETRY_MAX_MS: u64 = CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS
    - CLICKHOUSE_DERIVED_FACT_FLUSH_MS
    - CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS;
pub const CLICKHOUSE_BULK_ACK_MAX_MS: u64 = CLICKHOUSE_FLUSH_INTERVAL_MAX_SECS * 1_000
    + CLICKHOUSE_BULK_RETRY_MAX_MS
    + CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS;

const _: () = assert!(
    CLICKHOUSE_INSERT_RETRY_BACKOFF_TOTAL_MS
        == CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS
            * ((1_u64 << (CLICKHOUSE_INSERT_MAX_ATTEMPTS - 1)) - 1)
);
const _: () =
    assert!(CLICKHOUSE_CRITICAL_ATTEMPT_MAX_MS < CLICKHOUSE_CANONICAL_PUBLICATION_TIMEOUT_MS);
const _: () = assert!(
    CLICKHOUSE_DERIVED_FACT_FLUSH_MS + CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS
        < CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS
);
const _: () = assert!(
    CLICKHOUSE_DERIVED_FACT_FLUSH_MS
        + CLICKHOUSE_BULK_RETRY_MAX_MS
        + CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS
        == CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS
);
const _: () = assert!(CLICKHOUSE_DURABLE_SHUTDOWN_STAGE_SECS * 1_000 > CLICKHOUSE_BULK_ACK_MAX_MS);

/// Send, response, and whole-attempt budgets for one insert lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseInsertIoConfig {
    /// Maximum time to flush one encoded chunk to the socket.
    #[schemars(range(min = 1, max = 3_899))]
    pub send_timeout_ms: u64,
    /// Maximum time to receive the server's durable insert response.
    #[schemars(range(min = 1, max = 3_899))]
    pub end_timeout_ms: u64,
    /// Total metadata + all chunks + response deadline for one attempt.
    #[schemars(range(min = 1, max = 3_899))]
    pub attempt_timeout_ms: u64,
}

impl ClickHouseInsertIoConfig {
    pub(super) const fn budgets_fit(self) -> bool {
        self.send_timeout_ms > 0
            && self.end_timeout_ms > 0
            && self.attempt_timeout_ms > 0
            && match self.send_timeout_ms.checked_add(self.end_timeout_ms) {
                Some(io_budget) => io_budget <= self.attempt_timeout_ms,
                None => false,
            }
    }

    #[must_use]
    pub fn retry_window_ms(self) -> Option<u64> {
        self.attempt_timeout_ms
            .checked_mul(u64::from(CLICKHOUSE_INSERT_MAX_ATTEMPTS))
            .and_then(|attempts| attempts.checked_add(CLICKHOUSE_INSERT_RETRY_BACKOFF_TOTAL_MS))
    }
}

/// Complete `ClickHouse` network deadline contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseIoConfig {
    /// Total admission + response deadline for one runtime analytical query.
    #[schemars(range(min = 1, max = 300_000))]
    pub query_timeout_ms: u64,
    /// Total deadline for each bootstrap, verify, or maintenance query.
    #[schemars(range(min = 1, max = 600_000))]
    pub maintenance_timeout_ms: u64,
    /// Canonical ledger insert budgets.
    pub critical_insert: ClickHouseInsertIoConfig,
    /// Bulk/telemetry insert budgets.
    pub bulk_insert: ClickHouseInsertIoConfig,
}

impl Default for ClickHouseIoConfig {
    fn default() -> Self {
        Self {
            query_timeout_ms: 30_000,
            maintenance_timeout_ms: 120_000,
            critical_insert: ClickHouseInsertIoConfig {
                send_timeout_ms: 300,
                end_timeout_ms: 1_200,
                attempt_timeout_ms: 1_800,
            },
            bulk_insert: ClickHouseInsertIoConfig {
                send_timeout_ms: 750,
                end_timeout_ms: 1_750,
                attempt_timeout_ms: 3_000,
            },
        }
    }
}

/// `ClickHouse` connection + batched-write tuning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// Ownership boundary for server-level resource governance. Default:
    /// `self_managed`.
    pub resource_governance: ClickHouseResourceGovernance,
    /// Typed total-query, maintenance, and per-insert-lane I/O budgets.
    pub io: ClickHouseIoConfig,
    /// Stable deployment identity included in signed research evidence.
    pub deployment_id: String,
    /// `ClickHouse` Cloud cluster/service identity, or an explicit local identity.
    pub cluster_id: String,
    /// HTTP endpoint. Default: `http://localhost:8123`.
    pub url: String,
    /// Database name. Default: `quant_pivot`.
    pub database: String,
    /// `ClickHouse` service identity authenticated by ingestion and analytical query clients. Default: `default`.
    pub user: String,
    /// Zeroizing `ClickHouse` authentication secret; safe projections expose only configured state.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub password: SecretText,
    /// Max age (seconds) of a partial batch before it is flushed. Lower =
    /// fresher analytics, more insert requests. Default: `5`.
    #[schemars(range(min = 1, max = 5))]
    pub flush_interval_secs: u64,
    /// Rows per insert batch; a full batch flushes immediately. `ClickHouse`
    /// favors large batches — sized for the L2 tick feed (~3K rows/s peaks).
    /// Default: `5000`.
    #[schemars(range(min = 1, max = 5_000))]
    pub batch_size: usize,
    /// Maximum concurrent insert operations. One permit is reserved for the
    /// canonical L2 ledger; the remainder serve bulk/telemetry writes. Must be
    /// at least two. Default: `8`.
    #[schemars(range(min = 2))]
    pub max_concurrent_inserts: usize,
    /// Process-wide byte budget retained by variable-size bulk write queues.
    /// Default: 64 MiB.
    #[schemars(range(min = 1_048_576, max = 1_073_741_824))]
    pub max_inflight_write_bytes: usize,
    /// Maximum analytical reads admitted concurrently by the process-wide
    /// `ClickHousePool`. Every admitted read is also capped by
    /// `max_threads_per_query`; together these bounds reserve server capacity
    /// for the canonical ledger writer. Default: `4`.
    #[schemars(range(min = 1, max = 32))]
    pub max_concurrent_reads: usize,
    /// Maximum `ClickHouse` worker threads available to any single query or
    /// insert. This prevents concurrent research reads from exhausting the
    /// server thread pool needed by canonical ledger writes. Default: `2`.
    #[schemars(range(min = 1, max = 64))]
    pub max_threads_per_query: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            resource_governance: ClickHouseResourceGovernance::default(),
            io: ClickHouseIoConfig::default(),
            deployment_id: "local-development".to_owned(),
            cluster_id: "local-clickhouse".to_owned(),
            url: default_ch_url(),
            database: default_ch_database(),
            user: default_ch_user(),
            password: SecretText::default(),
            flush_interval_secs: default_ch_flush_interval(),
            batch_size: default_ch_batch_size(),
            max_concurrent_inserts: default_ch_insert_limit(),
            max_inflight_write_bytes: default_ch_write_bytes(),
            max_concurrent_reads: default_ch_read_limit(),
            max_threads_per_query: default_ch_query_threads(),
        }
    }
}

impl ClickHouseConfig {
    /// Maximum wait from application batch admission through the complete
    /// bounded bulk-insert retry window.
    #[must_use]
    pub fn bulk_ack_window_ms(&self) -> Option<u64> {
        let flush_window = self.flush_interval_secs.checked_mul(1_000)?;
        flush_window
            .checked_add(self.io.bulk_insert.retry_window_ms()?)?
            .checked_add(CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS)
    }

    /// Maximum wait for a one-second derived-fact batch to receive its
    /// durable bulk-insert acknowledgement.
    #[must_use]
    pub fn derived_ack_window_ms(&self) -> Option<u64> {
        CLICKHOUSE_DERIVED_FACT_FLUSH_MS
            .checked_add(self.io.bulk_insert.retry_window_ms()?)?
            .checked_add(CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS)
    }
}

fn default_ch_url() -> String {
    "http://localhost:8123".into()
}
fn default_ch_database() -> String {
    "quant_pivot".into()
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
const fn default_ch_insert_limit() -> usize {
    8
}
const fn default_ch_write_bytes() -> usize {
    64 * 1_024 * 1_024
}
const fn default_ch_read_limit() -> usize {
    4
}
const fn default_ch_query_threads() -> usize {
    2
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::PostgresConfig;

    #[test]
    fn postgres_url_encodes_characters() {
        let config = PostgresConfig {
            password: "@:/?#%".into(),
            ..PostgresConfig::default()
        };
        let connection = config.try_connection_url().expect("valid URL");
        assert!(!connection.contains("@:/?#%"));
        let parsed = Url::parse(&connection).expect("parse generated URL");
        assert_eq!(parsed.host_str(), Some("localhost"));
        assert_eq!(parsed.password(), Some("%40%3A%2F%3F%23%25"));
        assert_eq!(parsed.path(), "/quant_pivot");
    }

    #[test]
    fn database_debug_redacts_password() {
        let config = PostgresConfig {
            password: "database-secret".into(),
            ..PostgresConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("database-secret"));
        assert!(debug.contains("redacted"));
    }
}
