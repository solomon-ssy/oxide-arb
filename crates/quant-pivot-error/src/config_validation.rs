//! Strongly-typed startup configuration validation diagnostics.

use std::fmt::Display;

use rust_decimal::Decimal;
use thiserror::Error;

/// Validation report produced by startup config checks.
#[derive(Debug, Default, Clone, Error)]
#[error("{}", .errors.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
pub struct ConfigValidationReport {
    pub errors: Vec<ConfigValidationError>,
    pub warnings: Vec<ConfigWarning>,
}

impl ConfigValidationReport {
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty()
    }

    #[must_use]
    pub fn single_error(error: ConfigValidationError) -> Self {
        Self {
            errors: vec![error],
            warnings: Vec::new(),
        }
    }
}

/// Fatal configuration error — system cannot operate correctly.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ConfigValidationError {
    #[error("{field_low} ({value_low}) must be < {field_high} ({value_high})")]
    InfeasibleRange {
        field_low: &'static str,
        value_low: Decimal,
        field_high: &'static str,
        value_high: Decimal,
    },
    #[error("kelly_fraction must be in (0, 1], got {0}")]
    InvalidKellyFraction(Decimal),
    #[error("[{mode}] missing required credentials: {}", .missing.join(", "))]
    MissingCredentials {
        mode: String,
        missing: Vec<&'static str>,
    },
    #[error("{field}: {detail}")]
    InvalidValue { field: &'static str, detail: String },
}

impl ConfigValidationError {
    #[must_use]
    pub fn invalid_value(field: &'static str, detail: impl Into<String>) -> Self {
        Self::InvalidValue {
            field,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn invalid_decimal(field: &'static str, raw: &str, source: impl Display) -> Self {
        Self::invalid_value(field, format!("`{raw}` is not a valid decimal: {source}"))
    }
}

/// Non-fatal configuration concern.
#[derive(Debug, Clone, Error)]
pub enum ConfigWarning {
    #[error("kelly_fraction={0} is aggressive (>0.5); consider reducing")]
    LargeKellyFraction(Decimal),
    #[error("web.jwt Ed25519 signer/keyring configuration is incomplete")]
    JwtKeyringUnconfigured,
}
