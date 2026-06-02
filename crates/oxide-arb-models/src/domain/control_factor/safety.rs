//! Payload safety helpers for control-factor contracts.

use oxide_arb_error::control::PayloadSafetyError;
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

pub(super) const fn ensure_block_monotonic(
    field: &'static str,
    previous: bool,
    next: bool,
    has_manual_approval: bool,
) -> Result<(), PayloadSafetyError> {
    if previous && !next && !has_manual_approval {
        return Err(PayloadSafetyError::BlockFlagRelaxed { field });
    }
    Ok(())
}
