//! Bootstrap the application database on first connect.
//!
//! Connects to the fixed maintenance catalog (`default`), checks
//! `system.databases`, and issues `CREATE DATABASE` when the configured target
//! is missing. Idempotent and safe under concurrent first-start races.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::ClickHouseConfig;
use tracing::info;

/// Maintenance catalog used for `CREATE DATABASE` bootstrap.
const MAINTENANCE_DATABASE: &str = "default";

/// Ensure the configured application database exists, creating it when absent.
pub async fn ensure_database(config: &ClickHouseConfig) -> Result<(), StorageError> {
    if config.database == MAINTENANCE_DATABASE {
        return Ok(());
    }

    validate_ch_identifier(&config.database, "database")?;

    let client = clickhouse::Client::default()
        .with_url(&config.url)
        .with_database(MAINTENANCE_DATABASE)
        .with_user(&config.user)
        .with_password(&config.password);

    let exists: u64 = client
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind(&config.database)
        .fetch_one()
        .await
        .map_err(|e| {
            StorageError::Connection(format!(
                "ClickHouse maintenance connection failed ({MAINTENANCE_DATABASE}): {e}"
            ))
        })?;

    if exists > 0 {
        info!(database = %config.database, "ClickHouse database already exists");
        return Ok(());
    }

    let create_sql = format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_ident(&config.database),
    );

    client.query(&create_sql).execute().await.map_err(|e| {
        StorageError::Connection(format!(
            "Failed to CREATE DATABASE {}: {e}",
            config.database
        ))
    })?;

    info!(database = %config.database, "ClickHouse database created");
    Ok(())
}

/// Validate a `ClickHouse` unquoted identifier before embedding in DDL.
fn validate_ch_identifier(ident: &str, field: &str) -> Result<(), StorageError> {
    if ident.is_empty() || ident.len() > 255 {
        return Err(StorageError::Connection(format!(
            "Invalid ClickHouse {field} name `{ident}`: must be 1–255 characters"
        )));
    }

    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return Err(StorageError::Connection(format!(
            "Invalid ClickHouse {field} name: empty"
        )));
    };

    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(StorageError::Connection(format!(
            "Invalid ClickHouse {field} name `{ident}`: must start with a letter or underscore"
        )));
    }

    if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(StorageError::Connection(format!(
            "Invalid ClickHouse {field} name `{ident}`: only ASCII letters, digits, and underscores are allowed"
        )));
    }

    Ok(())
}

/// Backtick-quote an identifier for safe inclusion in DDL.
fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_default_database_name() {
        assert!(validate_ch_identifier("oxide_arb", "database").is_ok());
    }

    #[test]
    fn validate_rejects_empty_name() {
        assert!(validate_ch_identifier("", "database").is_err());
    }

    #[test]
    fn validate_rejects_invalid_first_char() {
        assert!(validate_ch_identifier("1bad", "database").is_err());
    }

    #[test]
    fn quote_ident_escapes_backticks() {
        assert_eq!(quote_ident("foo`bar"), "`foo``bar`");
    }
}
