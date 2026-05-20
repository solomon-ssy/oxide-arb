//! Startup config validation: detects infeasible ranges, contradictions, and
//! cross-section inconsistencies that would cause silent failure at runtime.
//!
//! Validation is split into **mode-agnostic** and **mode-aware** halves.
//! The mode-agnostic half is called during [`super::Settings::new`]
//! (see [`validate_settings_common`]); errors there abort startup
//! regardless of which subcommand the operator invoked. The mode-aware
//! half is called from the CLI runner once the final [`ExecutionMode`]
//! has been determined (see [`validate_settings_mode`]).
//!
//! # Mode → severity matrix
//!
//! | Polymarket creds   | `DryRun` | `Paper` | `Live` |
//! |--------------------|----------|---------|--------|
//! | all populated      | pass     | pass    | pass   |
//! | all empty          | pass     | warn    | fatal  |
//! | partial (1-3 set)  | warn     | fatal   | fatal  |

use super::Inner;
use crate::enums::common::ExecutionMode;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::fmt;

/// Validation report produced by [`validate_settings_common`] or
/// [`validate_settings_mode`].
#[derive(Debug, Default)]
pub struct ConfigValidationReport {
    pub errors: Vec<ConfigValidationError>,
    pub warnings: Vec<ConfigWarning>,
}

impl ConfigValidationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty()
    }
}

/// Fatal configuration error — system cannot operate correctly.
#[derive(Debug)]
pub enum ConfigValidationError {
    InfeasibleRange {
        field_low: &'static str,
        value_low: Decimal,
        field_high: &'static str,
        value_high: Decimal,
    },
    InvalidKellyFraction(Decimal),
    InvertedEndgameThresholds {
        high: Decimal,
        low: Decimal,
    },
    MissingCredentials {
        mode: ExecutionMode,
        missing: Vec<&'static str>,
    },
    PartialCredentials {
        mode: ExecutionMode,
        present: Vec<&'static str>,
        missing: Vec<&'static str>,
    },
    InvalidValue {
        field: &'static str,
        detail: String,
    },
}

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InfeasibleRange {
                field_low,
                value_low,
                field_high,
                value_high,
            } => write!(
                f,
                "{field_low} ({value_low}) must be < {field_high} ({value_high})"
            ),
            Self::InvalidKellyFraction(v) => {
                write!(f, "kelly_fraction must be in (0, 1], got {v}")
            }
            Self::InvertedEndgameThresholds { high, low } => write!(
                f,
                "endgame high_threshold ({high}) must be > low_threshold ({low})"
            ),
            Self::MissingCredentials { mode, missing } => write!(
                f,
                "[{mode}] missing required credentials: {}",
                missing.join(", ")
            ),
            Self::PartialCredentials {
                mode,
                present,
                missing,
            } => write!(
                f,
                "[{mode}] partial credentials (have: {}, missing: {})",
                present.join(", "),
                missing.join(", ")
            ),
            Self::InvalidValue { field, detail } => write!(f, "{field}: {detail}"),
        }
    }
}

/// Non-fatal configuration concern.
#[derive(Debug)]
pub enum ConfigWarning {
    LargeKellyFraction(Decimal),
    PartialCredentialsDryRun {
        present: Vec<&'static str>,
        missing: Vec<&'static str>,
    },
    NoCredentialsPaper,
}

impl fmt::Display for ConfigWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LargeKellyFraction(v) => {
                write!(
                    f,
                    "kelly_fraction={v} is aggressive (>0.5); consider reducing"
                )
            }
            Self::PartialCredentialsDryRun { present, missing } => write!(
                f,
                "DryRun with partial credentials (have: {}, missing: {})",
                present.join(", "),
                missing.join(", ")
            ),
            Self::NoCredentialsPaper => {
                write!(
                    f,
                    "Paper mode without credentials; user-trade stream disabled"
                )
            }
        }
    }
}

/// Mode-agnostic validation: checks mathematical invariants that must hold
/// regardless of execution mode.
pub fn validate_settings_common(inner: &Inner) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    let kelly = inner.sizing.kelly_fraction;
    if kelly <= Decimal::ZERO || kelly > Decimal::ONE {
        report
            .errors
            .push(ConfigValidationError::InvalidKellyFraction(kelly));
    } else if kelly > dec!(0.5) {
        report
            .warnings
            .push(ConfigWarning::LargeKellyFraction(kelly));
    }

    let high = inner.detection.endgame.high_threshold;
    let low = inner.detection.endgame.low_threshold;
    if high <= low {
        report
            .errors
            .push(ConfigValidationError::InvertedEndgameThresholds { high, low });
    }

    if inner.risk.max_single_bet_usd > Decimal::ZERO
        && inner.sizing.max_single_trade_usd > Decimal::ZERO
        && inner.sizing.min_trade_usd > inner.sizing.max_single_trade_usd
    {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "sizing.min_trade_usd",
            value_low: inner.sizing.min_trade_usd,
            field_high: "sizing.max_single_trade_usd",
            value_high: inner.sizing.max_single_trade_usd,
        });
    }

    if inner.detection.min_profit_threshold_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.min_profit_threshold_usd",
            detail: "must be > 0".into(),
        });
    }

    if inner.risk.min_depth_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.min_depth_usd",
            detail: "must be > 0".into(),
        });
    }

    if inner.risk.max_depth_usage_pct <= Decimal::ZERO || inner.risk.max_depth_usage_pct > dec!(100)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_depth_usage_pct",
            detail: "must be in (0, 100]".into(),
        });
    }

    if inner.risk.min_depth_usd > inner.sizing.max_single_trade_usd
        && inner.sizing.max_single_trade_usd > Decimal::ZERO
    {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.min_depth_usd",
            value_low: inner.risk.min_depth_usd,
            field_high: "sizing.max_single_trade_usd",
            value_high: inner.sizing.max_single_trade_usd,
        });
    }

    if inner.polymarket.fees.exponent <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.exponent",
            detail: "must be > 0".into(),
        });
    }

    for (category, rate) in &inner.polymarket.fees.category_rates {
        if *rate < Decimal::ZERO || *rate > dec!(1) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.fees.category_rates",
                detail: format!("{category} rate {rate} out of range [0, 1]"),
            });
        }
    }

    if inner.polymarket.fees.unknown_category_rate < Decimal::ZERO
        || inner.polymarket.fees.unknown_category_rate > dec!(1)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.unknown_category_rate",
            detail: "must be in [0, 1]".into(),
        });
    }

    report
}

/// Mode-aware validation: enforces credential policies based on the
/// execution mode that will actually run.
pub fn validate_settings_mode(inner: &Inner, mode: ExecutionMode) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    let keys = &inner.keys;
    let mut present = Vec::new();
    let mut missing = Vec::new();

    if keys.polymarket_api_key.is_some() {
        present.push("api_key");
    } else {
        missing.push("api_key");
    }
    if keys.polymarket_api_secret.is_some() {
        present.push("api_secret");
    } else {
        missing.push("api_secret");
    }
    if keys.polymarket_passphrase.is_some() {
        present.push("passphrase");
    } else {
        missing.push("passphrase");
    }
    if keys.private_key.is_some() {
        present.push("private_key");
    } else {
        missing.push("private_key");
    }

    let all_present = missing.is_empty();
    let all_missing = present.is_empty();

    match mode {
        ExecutionMode::DryRun => {
            if !all_present && !all_missing {
                report
                    .warnings
                    .push(ConfigWarning::PartialCredentialsDryRun {
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
        ExecutionMode::Paper => {
            if all_missing {
                report.warnings.push(ConfigWarning::NoCredentialsPaper);
            } else if !all_present {
                report
                    .errors
                    .push(ConfigValidationError::PartialCredentials {
                        mode,
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
        ExecutionMode::Live => {
            if all_missing {
                report
                    .errors
                    .push(ConfigValidationError::MissingCredentials {
                        mode,
                        missing: missing.clone(),
                    });
            } else if !all_present {
                report
                    .errors
                    .push(ConfigValidationError::PartialCredentials {
                        mode,
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_validation_passes_for_defaults() {
        let inner = Inner::default();
        let report = validate_settings_common(&inner);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn inverted_kelly_fraction_is_fatal() {
        let mut inner = Inner::default();
        inner.sizing.kelly_fraction = dec!(1.5);
        let report = validate_settings_common(&inner);
        assert!(report.has_errors());
    }

    #[test]
    fn inverted_thresholds_is_fatal() {
        let mut inner = Inner::default();
        inner.detection.endgame.high_threshold = dec!(0.03);
        inner.detection.endgame.low_threshold = dec!(0.95);
        let report = validate_settings_common(&inner);
        assert!(report.has_errors());
    }

    #[test]
    fn live_mode_requires_all_credentials() {
        let inner = Inner::default();
        let report = validate_settings_mode(&inner, ExecutionMode::Live);
        assert!(report.has_errors());
    }

    #[test]
    fn dry_run_permits_empty_credentials() {
        let inner = Inner::default();
        let report = validate_settings_mode(&inner, ExecutionMode::DryRun);
        assert!(!report.has_errors());
    }
}
