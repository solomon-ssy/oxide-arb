//! Configuration loading and validation errors.

use std::{io::Error as IoError, path::PathBuf};

use thiserror::Error;

use crate::config_validation::ConfigValidationReport;

/// Errors from configuration loading (TOML/env) and semantic validation.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration file I/O failed for {path}: {source}")]
    FileIo {
        path: PathBuf,
        #[source]
        source: IoError,
    },

    #[error("configuration file rejected at {path}: {reason}")]
    UnsafeFile { path: PathBuf, reason: String },

    #[error("configuration TOML is invalid at {path}: {reason}")]
    Parse { path: PathBuf, reason: String },

    #[error("configuration file contains an unreplaced placeholder at {path}")]
    Placeholder { path: PathBuf },

    #[error("configuration environment mismatch: expected {expected}, found {actual}")]
    EnvironmentMismatch { expected: String, actual: String },

    #[error("Configuration validation failed: {0}")]
    Validation(#[from] ConfigValidationReport),

    #[error("Missing required field: {field} in section [{section}]")]
    MissingField { section: String, field: String },

    #[error("Invalid value for {field}: {reason}")]
    InvalidValue { field: String, reason: String },
}
