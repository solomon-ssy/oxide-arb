//! Bootstrap the application database on first connect.
//!
//! Connects to the fixed maintenance catalog (`postgres`), checks
//! `pg_database`, and issues `CREATE DATABASE` when the configured target is
//! missing. Idempotent and safe under concurrent first-start races.

use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::PostgresConfig;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, DbErr, Statement, Value};
use std::time::Duration;
use tracing::info;

/// Maintenance catalog used for `CREATE DATABASE` bootstrap.
const MAINTENANCE_DATABASE: &str = "postgres";

/// Ensure the configured application database exists, creating it when absent.
pub async fn ensure_database(config: &PostgresConfig) -> Result<(), StorageError> {
    validate_pg_identifier(&config.database, "database")?;
    validate_pg_identifier(&config.user, "user")?;

    let mut opts = ConnectOptions::new(config.to_url_with_database(MAINTENANCE_DATABASE));
    opts.max_connections(1)
        .min_connections(1)
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .sqlx_logging(cfg!(debug_assertions));

    let db = Database::connect(opts).await.map_err(|e| {
        StorageError::Connection(format!(
            "PostgreSQL maintenance connection failed ({MAINTENANCE_DATABASE}): {e}"
        ))
    })?;

    let exists = db
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT 1 FROM pg_database WHERE datname = $1",
            [Value::from(config.database.clone())],
        ))
        .await?;

    if exists.is_some() {
        info!(database = %config.database, "PostgreSQL database already exists");
        close_quietly(db).await;
        return Ok(());
    }

    let create_sql = format!(
        "CREATE DATABASE {} OWNER {}",
        quote_ident(&config.database),
        quote_ident(&config.user),
    );

    match db
        .execute(Statement::from_string(DbBackend::Postgres, create_sql))
        .await
    {
        Ok(_) => {
            info!(database = %config.database, owner = %config.user, "PostgreSQL database created");
        }
        Err(e) if duplicate_database_error(&e) => {
            info!(
                database = %config.database,
                "PostgreSQL database already exists (concurrent bootstrap)"
            );
        }
        Err(e) => {
            return Err(StorageError::Connection(format!(
                "Failed to CREATE DATABASE {}: {e}",
                config.database
            )));
        }
    }

    close_quietly(db).await;
    Ok(())
}

async fn close_quietly(db: sea_orm::DatabaseConnection) {
    if let Err(e) = db.close().await {
        tracing::debug!("Maintenance connection close: {e}");
    }
}

/// Validate a `PostgreSQL` unquoted identifier before embedding in DDL.
fn validate_pg_identifier(ident: &str, field: &str) -> Result<(), StorageError> {
    if ident.is_empty() || ident.len() > 63 {
        return Err(StorageError::Connection(format!(
            "Invalid PostgreSQL {field} name `{ident}`: must be 1–63 characters"
        )));
    }

    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return Err(StorageError::Connection(format!(
            "Invalid PostgreSQL {field} name: empty"
        )));
    };

    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(StorageError::Connection(format!(
            "Invalid PostgreSQL {field} name `{ident}`: must start with a letter or underscore"
        )));
    }

    if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(StorageError::Connection(format!(
            "Invalid PostgreSQL {field} name `{ident}`: only ASCII letters, digits, and underscores are allowed"
        )));
    }

    Ok(())
}

/// Double-quote an identifier for safe inclusion in DDL.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn duplicate_database_error(err: &DbErr) -> bool {
    let DbErr::Exec(sea_orm::RuntimeErr::SqlxError(sqlx_err)) = err else {
        return false;
    };
    sqlx_err
        .as_database_error()
        .and_then(sea_orm::sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "42P04")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_default_database_name() {
        assert!(validate_pg_identifier("oxide_arb", "database").is_ok());
    }

    #[test]
    fn validate_rejects_empty_name() {
        assert!(validate_pg_identifier("", "database").is_err());
    }

    #[test]
    fn validate_rejects_invalid_first_char() {
        assert!(validate_pg_identifier("1bad", "database").is_err());
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident(r#"foo"bar"#), r#""foo""bar""#);
    }
}
