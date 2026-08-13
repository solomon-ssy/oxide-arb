//! Research-plane computational contracts.
//!
//! Values here are **not** operator tunables — they define cross-platform
//! determinism for scoring, normalization, and artifact hashing. Changing them
//! invalidates golden tests and content-addressed artifacts.

use rust_decimal::{Decimal, RoundingStrategy};

/// Fixed decimal scale for all research-plane intermediate arithmetic.
///
/// Append-only contract: changing this invalidates artifact hashes and golden
/// tests.
pub const RESEARCH_DECIMAL_SCALE: u32 = 12;

/// Fixed-math scale of Polymarket CLOB making/taking amounts.
///
/// The wire contract represents both amounts as integers with six decimal
/// places. Economic tiers, execution replay, and settlement therefore cross
/// the venue boundary through this one quantizer.
pub const VENUE_AMOUNT_SCALE: u32 = 6;

/// Quantize one venue amount without crossing through binary floating point.
#[must_use]
pub fn quantize_venue_amount(value: Decimal) -> Decimal {
    value.round_dp_with_strategy(VENUE_AMOUNT_SCALE, RoundingStrategy::MidpointAwayFromZero)
}
