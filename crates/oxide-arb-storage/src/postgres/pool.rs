//! Production-grade `PostgreSQL` pool with GUC injection and session verification.
//!
//! Key design decisions:
//! - Session-level GUCs (`statement_timeout`, `lock_timeout`, etc.) are injected
//!   via libpq startup `options` parameter, ensuring every pool connection inherits
//!   them regardless of how the pool recycles connections.
//! - `test_before_acquire(true)` ensures broken connections are discarded before
//!   handing them to application code.
//! - Post-connect verification optionally confirms that GUCs actually took effect,
//!   catching silent stripping by connection poolers like `PgBouncer` in transaction mode.

use super::ensure;
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::PostgresConfig;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
};
use std::time::Duration;
use tracing::{debug, error, info};

pub struct PostgresPool {
    db: DatabaseConnection,
    config: PostgresConfig,
}

impl PostgresPool {
    pub async fn connect(config: &PostgresConfig) -> Result<Self, StorageError> {
        ensure::ensure_database(config).await?;

        let url = config.to_url();
        let mut opts = ConnectOptions::new(&url);

        opts.max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .sqlx_logging(false)
            .test_before_acquire(true)
            .set_schema_search_path(&config.schema);

        // Inject session-level GUCs and pool tuning via sqlx PostgreSQL options.
        let guc_pairs = config.startup_guc_pairs();
        let app_name = config.application_name.clone();
        let stmt_cache_cap = config.statement_cache_capacity;

        opts.map_sqlx_postgres_opts(move |po| {
            let po = po.application_name(&app_name).statement_cache_capacity(
                ToPrimitive::to_usize(&stmt_cache_cap).unwrap_or(usize::MAX),
            );

            // GUC injection via `-c key=value` in libpq startup options.
            // This ensures each connection in the pool inherits these settings
            // at the protocol level, not via post-connect SET commands.
            let guc_opts: Vec<(&str, &str)> =
                guc_pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();

            if guc_opts.is_empty() {
                po
            } else {
                po.options(guc_opts)
            }
        });

        let db = Database::connect(opts)
            .await
            .map_err(|e| StorageError::Connection(format!("PostgreSQL connection failed: {e}")))?;

        info!(
            host = %config.host,
            port = config.port,
            database = %config.database,
            schema = %config.schema,
            max_conns = config.max_connections,
            statement_timeout_ms = config.statement_timeout_ms,
            lock_timeout_ms = config.lock_timeout_ms,
            "PostgreSQL pool connected"
        );

        let pool = Self {
            db,
            config: config.clone(),
        };

        // Post-connect verification: confirm GUCs took effect.
        if config.verify_session_params {
            pool.verify_session_params().await?;
        }

        Ok(pool)
    }

    /// Get a reference to the underlying `SeaORM` connection.
    pub const fn connection(&self) -> &DatabaseConnection {
        &self.db
    }

    /// Verify the connection is alive.
    pub async fn health_check(&self) -> Result<(), StorageError> {
        self.db
            .execute(Statement::from_string(DbBackend::Postgres, "SELECT 1"))
            .await
            .map_err(|e| {
                StorageError::Connection(format!("PostgreSQL health check failed: {e}"))
            })?;
        Ok(())
    }

    /// Gracefully close the connection pool.
    pub async fn close(self) {
        if let Err(e) = self.db.close().await {
            error!("Error closing PostgreSQL pool: {e}");
        }
    }

    /// Verify that session-level GUC parameters actually took effect.
    ///
    /// This catches `PgBouncer` (transaction/statement mode) silently stripping
    /// the startup `options` parameter. On mismatch the application aborts
    /// startup rather than running with unprotected connections.
    async fn verify_session_params(&self) -> Result<(), StorageError> {
        let mut mismatches: Vec<String> = Vec::new();

        if self.config.statement_timeout_ms > 0 {
            let actual = self.show_guc("statement_timeout").await?;
            let expected_ms = self.config.statement_timeout_ms;
            if !duration_matches_ms(&actual, expected_ms) {
                mismatches.push(format!(
                    "statement_timeout: expected ~{expected_ms}ms, got '{actual}'"
                ));
            }
        }

        if self.config.lock_timeout_ms > 0 {
            let actual = self.show_guc("lock_timeout").await?;
            let expected_ms = self.config.lock_timeout_ms;
            if !duration_matches_ms(&actual, expected_ms) {
                mismatches.push(format!(
                    "lock_timeout: expected ~{expected_ms}ms, got '{actual}'"
                ));
            }
        }

        if self.config.idle_in_transaction_timeout_ms > 0 {
            let actual = self.show_guc("idle_in_transaction_session_timeout").await?;
            let expected_ms = self.config.idle_in_transaction_timeout_ms;
            if !duration_matches_ms(&actual, expected_ms) {
                mismatches.push(format!(
                    "idle_in_transaction_session_timeout: expected ~{expected_ms}ms, got '{actual}'"
                ));
            }
        }

        if !mismatches.is_empty() {
            let msg = format!(
                "PostgreSQL session parameter verification failed. Mismatches: [{}]. \
                 This almost always means a connection pooler (e.g. PgBouncer in \
                 transaction/statement mode) is stripping the startup `options` parameter. \
                 Either switch to session mode, use pgbouncer_fdw, or disable \
                 `verify_session_params` in config (NOT recommended for production).",
                mismatches.join("; ")
            );
            return Err(StorageError::Connection(msg));
        }

        debug!("PostgreSQL session parameter verification passed");
        Ok(())
    }

    /// Execute `SHOW <param>` and return the result as a string.
    async fn show_guc(&self, param: &str) -> Result<String, StorageError> {
        let sql = format!("SHOW {param}");
        let result = self
            .db
            .query_one(Statement::from_string(DbBackend::Postgres, sql))
            .await
            .map_err(|e| StorageError::Connection(format!("Failed to SHOW {param}: {e}")))?;

        match result {
            Some(row) => {
                let val: String = row.try_get_by_index(0).map_err(|e| {
                    StorageError::Connection(format!("Failed to read SHOW {param} result: {e}"))
                })?;
                Ok(val)
            }
            None => Ok(String::new()),
        }
    }
}

/// Parse a `PostgreSQL` duration string (e.g. "30s", "5000ms", "1min") and check
/// if it approximately matches the expected milliseconds. `PostgreSQL` SHOW output
/// format varies by magnitude: "30s", "5s", "1min", "500ms", etc.
fn duration_matches_ms(pg_value: &str, expected_ms: u64) -> bool {
    parse_pg_duration_ms(pg_value).is_some_and(|actual_ms| actual_ms.abs_diff(expected_ms) <= 1000)
}

/// Parse `PostgreSQL`'s SHOW output for interval-like GUCs into milliseconds.
/// `PostgreSQL` returns these in various formats depending on magnitude:
/// - "0" (disabled)
/// - "500ms"
/// - "5s"
/// - "1min"
/// - "30000" (raw ms for timeout parameters when set via startup options)
fn parse_pg_duration_ms(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed == "0" {
        return Some(0);
    }

    if let Some(ms_str) = trimmed.strip_suffix("ms") {
        return ms_str.trim().parse::<u64>().ok();
    }
    if let Some(s_str) = trimmed.strip_suffix("min") {
        return s_str.trim().parse::<u64>().ok().map(|m| m * 60_000);
    }
    if let Some(s_str) = trimmed.strip_suffix('s') {
        return s_str.trim().parse::<u64>().ok().map(|s| s * 1000);
    }

    // Raw number — PostgreSQL timeout GUCs set via startup options are stored
    // internally as ms and SHOW returns the plain number.
    trimmed.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_duration_zero() {
        assert_eq!(parse_pg_duration_ms("0"), Some(0));
    }

    #[test]
    fn parse_pg_duration_ms_suffix() {
        assert_eq!(parse_pg_duration_ms("500ms"), Some(500));
    }

    #[test]
    fn parse_pg_duration_s_suffix() {
        assert_eq!(parse_pg_duration_ms("30s"), Some(30_000));
    }

    #[test]
    fn parse_pg_duration_min_suffix() {
        assert_eq!(parse_pg_duration_ms("1min"), Some(60_000));
    }

    #[test]
    fn parse_pg_duration_raw_number() {
        assert_eq!(parse_pg_duration_ms("30000"), Some(30_000));
    }

    #[test]
    fn duration_matches_exact() {
        assert!(duration_matches_ms("30s", 30_000));
    }

    #[test]
    fn duration_matches_raw() {
        assert!(duration_matches_ms("30000", 30_000));
    }

    #[test]
    fn duration_mismatch() {
        assert!(!duration_matches_ms("5s", 30_000));
    }

    #[test]
    fn guc_pairs_builds_correctly() {
        let config = PostgresConfig {
            statement_timeout_ms: 30_000,
            lock_timeout_ms: 5_000,
            idle_in_transaction_timeout_ms: 0, // disabled
            work_mem: "32MB".into(),
            ..PostgresConfig::default()
        };
        let pairs = config.startup_guc_pairs();
        assert_eq!(pairs.len(), 3); // statement_timeout, lock_timeout, work_mem (idle disabled)
        assert_eq!(pairs[0], ("statement_timeout", "30000".to_string()));
        assert_eq!(pairs[1], ("lock_timeout", "5000".to_string()));
        assert_eq!(pairs[2], ("work_mem", "32MB".to_string()));
    }

    #[test]
    fn guc_pairs_empty_when_all_disabled() {
        let config = PostgresConfig {
            statement_timeout_ms: 0,
            lock_timeout_ms: 0,
            idle_in_transaction_timeout_ms: 0,
            work_mem: String::new(),
            ..PostgresConfig::default()
        };
        assert!(config.startup_guc_pairs().is_empty());
    }
}
