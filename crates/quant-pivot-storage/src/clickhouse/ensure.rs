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
const PREPRODUCTION_DATABASE: &str = "quant_pivot";

/// Ensure the configured application database exists, creating it when absent.
pub(super) async fn ensure_database(config: &ClickHouseConfig) -> Result<(), StorageError> {
    if config.database == MAINTENANCE_DATABASE {
        return Ok(());
    }

    validate_ch_identifier(&config.database, "database")?;

    let client = clickhouse::Client::default()
        .with_url(&config.url)
        .with_database(MAINTENANCE_DATABASE)
        .with_user(&config.user)
        .with_password(config.password.expose_secret());

    if database_exists(config).await? {
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

pub(super) async fn database_exists(config: &ClickHouseConfig) -> Result<bool, StorageError> {
    if config.database == MAINTENANCE_DATABASE {
        return Ok(true);
    }
    validate_ch_identifier(&config.database, "database")?;
    let client = clickhouse::Client::default()
        .with_url(&config.url)
        .with_database(MAINTENANCE_DATABASE)
        .with_user(&config.user)
        .with_password(config.password.expose_secret());
    client
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind(&config.database)
        .fetch_one::<u64>()
        .await
        .map(|count| count == 1)
        .map_err(|error| {
            StorageError::Connection(format!(
                "ClickHouse maintenance connection failed ({MAINTENANCE_DATABASE}): {error}"
            ))
        })
}

pub async fn database_object_count(config: &ClickHouseConfig) -> Result<u64, StorageError> {
    validate_preproduction_target(config)?;
    let client = maintenance_client(config);
    client
        .query("SELECT count() FROM system.tables WHERE database = ?")
        .bind(&config.database)
        .fetch_one::<u64>()
        .await
        .map_err(Into::into)
}

pub async fn reset_preproduction_database(config: &ClickHouseConfig) -> Result<(), StorageError> {
    validate_preproduction_target(config)?;
    let client = maintenance_client(config);
    client
        .query("DROP DATABASE IF EXISTS `quant_pivot` SYNC")
        .execute()
        .await
        .map_err(|error| {
            StorageError::Migration(format!("drop ClickHouse preproduction database: {error}"))
        })?;
    client
        .query("CREATE DATABASE `quant_pivot`")
        .execute()
        .await
        .map_err(|error| {
            StorageError::Migration(format!("create ClickHouse preproduction database: {error}"))
        })?;
    Ok(())
}

fn validate_preproduction_target(config: &ClickHouseConfig) -> Result<(), StorageError> {
    if config.database != PREPRODUCTION_DATABASE {
        return Err(StorageError::Migration(format!(
            "preproduction reset only permits ClickHouse database `{PREPRODUCTION_DATABASE}`; configured `{}`",
            config.database
        )));
    }
    validate_ch_identifier(&config.database, "database")
}

fn maintenance_client(config: &ClickHouseConfig) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(&config.url)
        .with_database(MAINTENANCE_DATABASE)
        .with_user(&config.user)
        .with_password(config.password.expose_secret())
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
        assert!(validate_ch_identifier("quant_pivot", "database").is_ok());
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
