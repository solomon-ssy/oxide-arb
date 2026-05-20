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
}

impl PostgresConfig {
    /// Build the connection URL.
    pub fn to_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode=prefer",
            self.user, self.password, self.host, self.port, self.database,
        )
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
