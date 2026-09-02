//! Canonical Polymarket order-grid and fixed-math rules.
//!
//! These rules are protocol facts, not operator tunables. Research, serving,
//! admission, and the SDK adapter must consume this single implementation so
//! the economic order and the signed order cannot diverge.

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::common::{Side, TickSize},
    types::{Bps, Price, Shares, Usd, VenueOrderAmount},
};

const WIRE_SCALE: u32 = 6;
const AMOUNT_ROUNDING_GUARD_DIGITS: u32 = 4;

/// Canonical high-level and fixed-math amounts for one Polymarket order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalOrderAmounts {
    /// Canonical high-level amount passed to the SDK builder.
    pub venue_amount: VenueOrderAmount,
    /// Exact share leg encoded by the signed order.
    pub requested_shares: Shares,
    /// Exact collateral leg encoded by the signed order.
    pub principal_usd: Usd,
    /// Human-scale maker amount; multiplying by `10^6` yields the wire integer.
    #[serde(with = "rust_decimal::serde::str")]
    pub maker_amount: Decimal,
    /// Human-scale taker amount; multiplying by `10^6` yields the wire integer.
    #[serde(with = "rust_decimal::serde::str")]
    pub taker_amount: Decimal,
}

/// Immutable order constraints from one point-in-time CLOB market-info fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolymarketOrderRules {
    /// Minimum valid price increment from the PIT CLOB observation.
    pub tick_size: TickSize,
    /// Minimum signed share leg accepted by the venue.
    pub minimum_order_size: Shares,
}

impl PolymarketOrderRules {
    /// Canonical user-supplied share scale for every supported tick.
    pub const SHARE_SCALE: u32 = 2;

    /// Build rules from a typed CLOB tick and a strictly positive share minimum.
    pub fn new(
        tick_size: TickSize,
        minimum_order_size: Shares,
    ) -> Result<Self, VenueOrderRuleError> {
        if !minimum_order_size.is_positive() {
            return Err(VenueOrderRuleError::InvalidMinimum {
                value: minimum_order_size,
            });
        }
        Ok(Self {
            tick_size,
            minimum_order_size,
        })
    }

    /// Validate an untrusted decimal tick before constructing the rule set.
    pub fn try_new(
        tick_size: Decimal,
        minimum_order_size: Shares,
    ) -> Result<Self, VenueOrderRuleError> {
        let tick_size = TickSize::try_from(tick_size)
            .map_err(|_| VenueOrderRuleError::UnsupportedTick { value: tick_size })?;
        Self::new(tick_size, minimum_order_size)
    }

    /// Maximum price scale for this tick.
    #[must_use]
    pub const fn price_scale(self) -> u32 {
        match self.tick_size {
            TickSize::Tenth => 1,
            TickSize::Hundredth => 2,
            TickSize::HalfCent | TickSize::Thousandth => 3,
            TickSize::QuarterCent | TickSize::TenThousandth => 4,
        }
    }

    /// Maximum derived counter-amount scale for this tick.
    #[must_use]
    pub const fn amount_scale(self) -> u32 {
        match self.tick_size {
            TickSize::Tenth => 3,
            TickSize::Hundredth => 4,
            TickSize::HalfCent | TickSize::Thousandth => 5,
            TickSize::QuarterCent | TickSize::TenThousandth => 6,
        }
    }

    /// Greatest valid BUY limit no higher than the governed slippage cap.
    pub fn aggressive_buy_limit(
        self,
        best_ask: Price,
        max_slippage_bps: Bps,
    ) -> Result<Price, VenueOrderRuleError> {
        self.validate_price(best_ask)?;
        if max_slippage_bps.is_negative() {
            return Err(VenueOrderRuleError::NegativeSlippage {
                value: max_slippage_bps,
            });
        }
        let tick = self.tick_size.as_decimal();
        let multiplier = Decimal::ONE
            .checked_add(max_slippage_bps.to_fraction())
            .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                operation: "buy_limit_multiplier",
            })?;
        let raw = best_ask.inner().checked_mul(multiplier).ok_or(
            VenueOrderRuleError::ArithmeticOverflow {
                operation: "buy_limit_product",
            },
        )?;
        let upper = Decimal::ONE - tick;
        let capped = raw.min(upper);
        let units = capped
            .checked_div(tick)
            .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                operation: "buy_limit_ticks",
            })?;
        let aligned =
            units
                .floor()
                .checked_mul(tick)
                .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                    operation: "buy_limit_alignment",
                })?;
        if aligned < best_ask.inner() {
            return Err(VenueOrderRuleError::NoMarketableBuyLimit {
                best_ask,
                raw_cap: Price::new(capped),
                tick_size: self.tick_size,
            });
        }
        let limit = Price::new(aligned);
        self.validate_price(limit)?;
        Ok(limit)
    }

    /// Least tick-aligned SELL limit that is no lower than the governed hard
    /// minimum. A ceiling beyond the venue's upper bound is rejected.
    pub fn sell_limit_at_least(self, minimum: Price) -> Result<Price, VenueOrderRuleError> {
        let tick = self.tick_size.as_decimal();
        let units =
            minimum
                .inner()
                .checked_div(tick)
                .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                    operation: "sell_limit_ticks",
                })?;
        let aligned =
            units
                .ceil()
                .checked_mul(tick)
                .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                    operation: "sell_limit_alignment",
                })?;
        let limit = Price::new(aligned);
        self.validate_price(limit)?;
        Ok(limit)
    }

    /// Canonicalize an order amount and derive the exact signed amount legs.
    pub fn canonical_order(
        self,
        side: Side,
        amount: VenueOrderAmount,
        price: Price,
    ) -> Result<CanonicalOrderAmounts, VenueOrderRuleError> {
        self.validate_price(price)?;
        let (venue_amount, requested_shares, principal_usd) = match (side, amount) {
            (Side::Buy, VenueOrderAmount::PrincipalUsd(principal)) => {
                let principal = Self::canonical_principal(principal)?;
                let shares = principal.inner().checked_div(price.inner()).ok_or(
                    VenueOrderRuleError::ArithmeticOverflow {
                        operation: "market_buy_shares",
                    },
                )?;
                let requested_shares = Shares::new(self.canonical_counter_amount(shares)?);
                (
                    VenueOrderAmount::PrincipalUsd(principal),
                    requested_shares,
                    principal,
                )
            }
            (_, VenueOrderAmount::Shares(shares)) => {
                let shares = Self::canonical_shares(shares)?;
                let principal = shares.inner().checked_mul(price.inner()).ok_or(
                    VenueOrderRuleError::ArithmeticOverflow {
                        operation: "share_order_principal",
                    },
                )?;
                (
                    VenueOrderAmount::Shares(shares),
                    shares,
                    Usd::new(self.canonical_counter_amount(principal)?),
                )
            }
            (Side::Sell, VenueOrderAmount::PrincipalUsd(_)) => {
                return Err(VenueOrderRuleError::InvalidAmountUnit { side: Side::Sell });
            }
        };
        if requested_shares < self.minimum_order_size {
            return Err(VenueOrderRuleError::OrderBelowMinimum {
                requested: requested_shares,
                minimum: self.minimum_order_size,
            });
        }
        let (maker_amount, taker_amount) = match side {
            Side::Buy => (principal_usd.inner(), requested_shares.inner()),
            Side::Sell => (requested_shares.inner(), principal_usd.inner()),
        };
        Ok(CanonicalOrderAmounts {
            venue_amount,
            requested_shares,
            principal_usd,
            maker_amount: wire_amount(maker_amount),
            taker_amount: wire_amount(taker_amount),
        })
    }

    /// Require the caller's amount to already equal the canonical SDK input.
    pub fn validate_order(
        self,
        side: Side,
        amount: VenueOrderAmount,
        price: Price,
    ) -> Result<CanonicalOrderAmounts, VenueOrderRuleError> {
        let canonical = self.canonical_order(side, amount, price)?;
        let (value, expected) = match (amount, canonical.venue_amount) {
            (VenueOrderAmount::PrincipalUsd(value), VenueOrderAmount::PrincipalUsd(expected)) => {
                (value.inner(), expected.inner())
            }
            (VenueOrderAmount::Shares(value), VenueOrderAmount::Shares(expected)) => {
                (value.inner(), expected.inner())
            }
            _ => return Err(VenueOrderRuleError::InvalidAmountUnit { side }),
        };
        if value != expected {
            return Err(VenueOrderRuleError::NonCanonicalPrecision {
                field: "venue_amount",
                value,
                expected,
            });
        }
        Ok(canonical)
    }

    /// Floor a positive share input to the venue's two-decimal lot scale.
    pub fn canonical_shares(shares: Shares) -> Result<Shares, VenueOrderRuleError> {
        if !shares.is_positive() {
            return Err(VenueOrderRuleError::NonPositiveAmount {
                field: "shares",
                value: shares.inner(),
            });
        }
        let canonical = floor_scale(shares.inner(), Self::SHARE_SCALE);
        if canonical.is_zero() {
            return Err(VenueOrderRuleError::CanonicalAmountZero {
                field: "shares",
                value: shares.inner(),
            });
        }
        Ok(Shares::new(canonical))
    }

    fn canonical_principal(principal: Usd) -> Result<Usd, VenueOrderRuleError> {
        if !principal.is_positive() {
            return Err(VenueOrderRuleError::NonPositiveAmount {
                field: "principal_usd",
                value: principal.inner(),
            });
        }
        let canonical = floor_scale(principal.inner(), Self::SHARE_SCALE);
        if canonical.is_zero() {
            return Err(VenueOrderRuleError::CanonicalAmountZero {
                field: "principal_usd",
                value: principal.inner(),
            });
        }
        Ok(Usd::new(canonical))
    }

    fn canonical_counter_amount(self, value: Decimal) -> Result<Decimal, VenueOrderRuleError> {
        if value <= Decimal::ZERO {
            return Err(VenueOrderRuleError::NonPositiveAmount {
                field: "counter_amount",
                value,
            });
        }
        let normalized = value.normalize();
        if normalized.scale() <= self.amount_scale() {
            return Ok(normalized);
        }
        let guarded = value.round_dp_with_strategy(
            self.amount_scale() + AMOUNT_ROUNDING_GUARD_DIGITS,
            RoundingStrategy::ToPositiveInfinity,
        );
        Ok(floor_scale(guarded, self.amount_scale()).normalize())
    }

    fn validate_price(self, price: Price) -> Result<(), VenueOrderRuleError> {
        let value = price.inner();
        let tick = self.tick_size.as_decimal();
        if value < tick || value > Decimal::ONE - tick {
            return Err(VenueOrderRuleError::PriceOutsideBounds {
                price,
                tick_size: self.tick_size,
            });
        }
        let units = value
            .checked_div(tick)
            .ok_or(VenueOrderRuleError::ArithmeticOverflow {
                operation: "price_ticks",
            })?;
        if !units.fract().is_zero() || value.normalize().scale() > self.price_scale() {
            return Err(VenueOrderRuleError::PriceNotAligned {
                price,
                tick_size: self.tick_size,
            });
        }
        Ok(())
    }
}

fn floor_scale(value: Decimal, scale: u32) -> Decimal {
    value.round_dp_with_strategy(scale, RoundingStrategy::ToZero)
}

fn wire_amount(value: Decimal) -> Decimal {
    floor_scale(value, WIRE_SCALE).normalize()
}

/// A canonical Polymarket order rule was violated before signing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VenueOrderRuleError {
    #[error("unsupported Polymarket tick size `{value}`")]
    UnsupportedTick { value: Decimal },
    #[error("minimum order size must be positive, got `{value}`")]
    InvalidMinimum { value: Shares },
    #[error("price `{price}` is outside [{tick_size}, 1-{tick_size}]")]
    PriceOutsideBounds { price: Price, tick_size: TickSize },
    #[error("price `{price}` is not aligned to tick `{tick_size}`")]
    PriceNotAligned { price: Price, tick_size: TickSize },
    #[error("negative aggressive BUY slippage `{value}` is invalid")]
    NegativeSlippage { value: Bps },
    #[error(
        "no marketable BUY limit: best ask `{best_ask}`, raw cap `{raw_cap}`, tick `{tick_size}`"
    )]
    NoMarketableBuyLimit {
        best_ask: Price,
        raw_cap: Price,
        tick_size: TickSize,
    },
    #[error("{field} must be positive, got `{value}`")]
    NonPositiveAmount { field: &'static str, value: Decimal },
    #[error("canonical {field} is zero after flooring `{value}`")]
    CanonicalAmountZero { field: &'static str, value: Decimal },
    #[error("{field} `{value}` is not canonical; expected `{expected}`")]
    NonCanonicalPrecision {
        field: &'static str,
        value: Decimal,
        expected: Decimal,
    },
    #[error("order requests `{requested}` shares below venue minimum `{minimum}`")]
    OrderBelowMinimum { requested: Shares, minimum: Shares },
    #[error("{side} orders cannot use this venue amount unit")]
    InvalidAmountUnit { side: Side },
    #[error("decimal arithmetic overflow during {operation}")]
    ArithmeticOverflow { operation: &'static str },
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{PolymarketOrderRules, VenueOrderRuleError};
    use crate::{
        enums::common::{Side, TickSize},
        types::{Bps, Price, Shares, Usd, VenueOrderAmount},
    };

    impl PolymarketOrderRules {
        fn test_for(tick_size: TickSize) -> Self {
            Self::new(tick_size, Shares::new(dec!(5))).expect("valid rules")
        }
    }

    #[test]
    fn tick_scales_match_protocol() {
        for (tick, price, amount) in [
            (TickSize::Tenth, 1, 3),
            (TickSize::Hundredth, 2, 4),
            (TickSize::HalfCent, 3, 5),
            (TickSize::QuarterCent, 4, 6),
            (TickSize::Thousandth, 3, 5),
            (TickSize::TenThousandth, 4, 6),
        ] {
            let rules = PolymarketOrderRules::test_for(tick);
            assert_eq!(rules.price_scale(), price);
            assert_eq!(PolymarketOrderRules::SHARE_SCALE, 2);
            assert_eq!(rules.amount_scale(), amount);
        }
    }

    #[test]
    fn misaligned_prices_reject() {
        for (tick, price) in [
            (TickSize::Tenth, dec!(0.55)),
            (TickSize::Hundredth, dec!(0.505)),
            (TickSize::HalfCent, dec!(0.501)),
            (TickSize::QuarterCent, dec!(0.5001)),
            (TickSize::Thousandth, dec!(0.5005)),
            (TickSize::TenThousandth, dec!(0.50005)),
        ] {
            let error = PolymarketOrderRules::test_for(tick)
                .validate_price(Price::new(price))
                .expect_err("misaligned price must fail closed");
            assert_eq!(
                error,
                VenueOrderRuleError::PriceNotAligned {
                    price: Price::new(price),
                    tick_size: tick,
                }
            );
        }
    }

    #[test]
    fn aggressive_buy_stays_governed() {
        for tick in [
            TickSize::Tenth,
            TickSize::Hundredth,
            TickSize::HalfCent,
            TickSize::QuarterCent,
            TickSize::Thousandth,
            TickSize::TenThousandth,
        ] {
            let rules = PolymarketOrderRules::test_for(tick);
            let best = Price::new(dec!(0.5));
            let limit = rules
                .aggressive_buy_limit(best, Bps::new(dec!(57)))
                .expect("aligned limit");
            let raw = best.inner() * (Decimal::ONE + dec!(57) / dec!(10000));
            assert!(limit >= best);
            assert!(limit.inner() <= raw);
            assert!(limit.inner() <= Decimal::ONE - tick.as_decimal());
            assert!((limit.inner() / tick.as_decimal()).fract().is_zero());
        }
    }

    #[test]
    fn sell_limit_never_decreases() {
        let rules = PolymarketOrderRules::test_for(TickSize::Hundredth);
        let limit = rules
            .sell_limit_at_least(Price::new(dec!(0.501)))
            .expect("aligned SELL hard minimum");
        assert_eq!(limit, Price::new(dec!(0.51)));
        assert!(limit >= Price::new(dec!(0.501)));
    }

    #[test]
    fn sell_limit_rejects_ceiling() {
        let error = PolymarketOrderRules::test_for(TickSize::Hundredth)
            .sell_limit_at_least(Price::new(dec!(0.999)))
            .expect_err("SELL ceiling beyond the venue upper bound must defer");
        assert!(matches!(
            error,
            VenueOrderRuleError::PriceOutsideBounds { .. }
        ));
    }

    #[test]
    fn order_amounts_are_canonical() {
        let rules = PolymarketOrderRules::test_for(TickSize::Hundredth);
        let buy = rules
            .canonical_order(
                Side::Buy,
                VenueOrderAmount::PrincipalUsd(Usd::new(dec!(10.129))),
                Price::new(dec!(0.52)),
            )
            .expect("canonical market buy");
        assert_eq!(
            buy.venue_amount,
            VenueOrderAmount::PrincipalUsd(Usd::new(dec!(10.12)))
        );
        assert_eq!(buy.principal_usd, Usd::new(dec!(10.12)));
        assert_eq!(buy.requested_shares, Shares::new(dec!(19.4615)));
        assert_eq!(buy.maker_amount, dec!(10.12));
        assert_eq!(buy.taker_amount, dec!(19.4615));

        let sell = rules
            .canonical_order(
                Side::Sell,
                VenueOrderAmount::Shares(Shares::new(dec!(9.999))),
                Price::new(dec!(0.52)),
            )
            .expect("canonical sell");
        assert_eq!(
            sell.venue_amount,
            VenueOrderAmount::Shares(Shares::new(dec!(9.99)))
        );
        assert_eq!(sell.principal_usd, Usd::new(dec!(5.1948)));
        assert_eq!(sell.maker_amount, dec!(9.99));
        assert_eq!(sell.taker_amount, dec!(5.1948));
    }

    #[test]
    fn minimum_applies_after_flooring() {
        let rules = PolymarketOrderRules::test_for(TickSize::Hundredth);
        let error = rules
            .canonical_order(
                Side::Buy,
                VenueOrderAmount::Shares(Shares::new(dec!(4.999))),
                Price::new(dec!(0.50)),
            )
            .expect_err("floored order is below venue minimum");
        assert_eq!(
            error,
            VenueOrderRuleError::OrderBelowMinimum {
                requested: Shares::new(dec!(4.99)),
                minimum: Shares::new(dec!(5)),
            }
        );
        assert!(
            rules
                .validate_order(
                    Side::Buy,
                    VenueOrderAmount::Shares(Shares::new(dec!(5))),
                    Price::new(dec!(0.50)),
                )
                .is_ok()
        );
    }

    #[test]
    fn validation_rejects_hidden_rounding() {
        let error = PolymarketOrderRules::test_for(TickSize::Hundredth)
            .validate_order(
                Side::Sell,
                VenueOrderAmount::Shares(Shares::new(dec!(5.001))),
                Price::new(dec!(0.50)),
            )
            .expect_err("adapter must not silently floor");
        assert!(matches!(
            error,
            VenueOrderRuleError::NonCanonicalPrecision {
                field: "venue_amount",
                ..
            }
        ));
    }

    proptest! {
        #[test]
        fn share_floor_never_increases(whole in 1_i64..10_000, tail in 0_i64..1_000_000) {
            let value = Decimal::new(whole * 1_000_000 + tail, 6);
            let canonical = PolymarketOrderRules::canonical_shares(Shares::new(value))
                .expect("positive canonical shares");
            prop_assert!(canonical.inner() <= value);
            prop_assert!(canonical.inner().normalize().scale() <= 2);
        }
    }
}
