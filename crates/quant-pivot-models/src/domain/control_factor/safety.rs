//! Payload safety helpers for control-factor contracts.

use quant_pivot_error::control::PayloadSafetyError;
use rust_decimal::Decimal;

pub(super) fn ensure_multiplier(
    value: Decimal,
    field: &'static str,
) -> Result<(), PayloadSafetyError> {
    if value < Decimal::ZERO || value > Decimal::ONE {
        return Err(PayloadSafetyError::MultiplierOutOfRange { field });
    }
    Ok(())
}

pub(super) fn ensure_non_negative(
    value: Decimal,
    field: &'static str,
) -> Result<(), PayloadSafetyError> {
    if value < Decimal::ZERO {
        return Err(PayloadSafetyError::NegativeAddon { field });
    }
    Ok(())
}
