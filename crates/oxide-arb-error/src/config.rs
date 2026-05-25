//! Configuration loading and validation errors.

use crate::config_validation::ConfigValidationReport;
use thiserror::Error;

/// Errors from configuration loading (TOML/env) and semantic validation.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[cfg(feature = "config")]
    #[error("Configuration load failed: {0}")]
    Load(#[from] config::ConfigError),

    #[cfg(not(feature = "config"))]
    #[error("Configuration load failed: {0}")]
    Load(String),

    #[error("Configuration validation failed: {0}")]
    Validation(#[from] ConfigValidationReport),

    #[error("Missing required field: {field} in section [{section}]")]
    MissingField { section: String, field: String },

    #[error("Invalid value for {field}: {reason}")]
    InvalidValue { field: String, reason: String },
}
