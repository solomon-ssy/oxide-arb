//! Immutable pre-trade snapshot of internal risk engine state.
//!
//! Published via `ArcSwap` so `pre_trade_check_core` reads all subsystems in a
//! single atomic load instead of six separate `RwLock` acquisitions.

use crate::{
    context::{CircuitBreakerGate, ManualHaltGate},
    sizing::DrawdownGuard,
    types::DrawdownAction,
};
use oxide_arb_models::{
    enums::risk::{BlacklistReason, BlacklistScope},
    types::{MarketId, TokenId, Usd},
};
use rust_decimal::Decimal;

/// Fixed 512-bit bloom filter (k=3). Used for blacklist fast-path negatives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BloomFilter512 {
    bits: [u64; 8],
}

impl BloomFilter512 {
    const M: u32 = 512;
    const K: u32 = 3;

    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..Self::K {
            let bit = Self::hash(key, i) % Self::M;
            self.set_bit(bit);
        }
    }

    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..Self::K {
            let bit = Self::hash(key, i) % Self::M;
            if !self.test_bit(bit) {
                return false;
            }
        }
        true
    }

    #[inline]
    const fn set_bit(&mut self, bit: u32) {
        let word = (bit / 64) as usize;
        let offset = bit % 64;
        self.bits[word] |= 1_u64 << offset;
    }

    #[inline]
    const fn test_bit(&self, bit: u32) -> bool {
        let word = (bit / 64) as usize;
        let offset = bit % 64;
        (self.bits[word] & (1_u64 << offset)) != 0
    }

    #[inline]
    fn hash(key: &[u8], seed: u32) -> u32 {
        let mut h = 0x811c_9dc5_u32 ^ seed.wrapping_mul(0x0100_0193);
        for &b in key {
            h ^= u32::from(b);
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }
}

/// Confirmed trading-path block (exact, paired with bloom at publish time).
#[derive(Debug, Clone)]
pub struct TradingPathBlock {
    pub market_id: MarketId,
    pub reason: BlacklistReason,
    pub scope: BlacklistScope,
}

/// Bloom + exact confirm tables for blacklist checks (no live `DashMap` on hot path).
#[derive(Debug, Clone)]
pub struct BlacklistSnapshot {
    pub market_bloom: BloomFilter512,
    pub token_bloom: BloomFilter512,
    trading_path_blocks: Vec<TradingPathBlock>,
    blacklisted_tokens: Vec<TokenId>,
}

impl BlacklistSnapshot {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            market_bloom: BloomFilter512::default(),
            token_bloom: BloomFilter512::default(),
            trading_path_blocks: Vec::new(),
            blacklisted_tokens: Vec::new(),
        }
    }

    #[must_use]
    pub fn may_contain_market(&self, id: &MarketId) -> bool {
        self.market_bloom.may_contain(id.as_str().as_bytes())
    }

    #[must_use]
    pub fn may_contain_token(&self, id: &TokenId) -> bool {
        self.token_bloom.may_contain(id.as_str().as_bytes())
    }

    /// Exact confirm when bloom is positive; `None` if not blocked at trading path.
    #[must_use]
    pub fn trading_path_block_detail(&self, market_id: &MarketId) -> Option<String> {
        if !self.may_contain_market(market_id) {
            return None;
        }
        self.trading_path_blocks
            .iter()
            .find(|b| &b.market_id == market_id)
            .map(|b| format!("{} (scope: {})", b.reason, b.scope))
    }

    #[must_use]
    pub fn is_token_blacklisted(&self, token_id: &TokenId) -> bool {
        if !self.may_contain_token(token_id) {
            return false;
        }
        self.blacklisted_tokens.iter().any(|t| t == token_id)
    }

    pub(crate) const fn from_parts(
        market_bloom: BloomFilter512,
        token_bloom: BloomFilter512,
        trading_path_blocks: Vec<TradingPathBlock>,
        blacklisted_tokens: Vec<TokenId>,
    ) -> Self {
        Self {
            market_bloom,
            token_bloom,
            trading_path_blocks,
            blacklisted_tokens,
        }
    }
}

/// Circuit breaker + manual halt gates frozen at snapshot time.
#[derive(Debug, Clone)]
pub struct CircuitBreakerSnapshot {
    pub circuit_breaker: CircuitBreakerGate,
    pub manual_halt: ManualHaltGate,
}

/// Daily accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct DailyAccountingSnapshot {
    pub daily_loss: Usd,
    pub daily_fee: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_remaining: Usd,
}

/// Weekly accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct WeeklyAccountingSnapshot {
    pub weekly_loss: Usd,
}

/// Hourly accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct HourlyAccountingSnapshot {
    pub hourly_loss: Usd,
    pub hourly_fee: Usd,
}

/// Drawdown guard parameters frozen at snapshot time.
///
/// Sizing factor is computed at check time using live equity from metrics.
#[derive(Debug, Clone, Copy)]
pub struct DrawdownSnapshot {
    pub hwm: Usd,
    pub max_drawdown_pct: Decimal,
    pub reduction_factor: Decimal,
}

impl DrawdownSnapshot {
    #[must_use]
    pub fn evaluate(&self, current_equity: Usd) -> (Decimal, DrawdownAction) {
        let guard =
            DrawdownGuard::from_snapshot(self.hwm, self.max_drawdown_pct, self.reduction_factor);
        guard.evaluate(current_equity)
    }

    #[must_use]
    pub fn sizing_factor(&self, current_equity: Usd) -> Decimal {
        let guard =
            DrawdownGuard::from_snapshot(self.hwm, self.max_drawdown_pct, self.reduction_factor);
        guard.sizing_factor(current_equity)
    }
}

/// Immutable copy of all internal state needed for a single pre-trade decision.
#[derive(Debug, Clone)]
pub struct RiskSnapshot {
    pub circuit_breaker: CircuitBreakerSnapshot,
    pub daily: DailyAccountingSnapshot,
    pub weekly: WeeklyAccountingSnapshot,
    pub hourly: HourlyAccountingSnapshot,
    pub drawdown: DrawdownSnapshot,
    pub total_potential_loss: Usd,
    pub blacklist: BlacklistSnapshot,
}

impl RiskSnapshot {
    #[must_use]
    pub fn zeroed() -> Self {
        Self {
            circuit_breaker: CircuitBreakerSnapshot {
                circuit_breaker: CircuitBreakerGate {
                    allows_trading: true,
                    is_probe: false,
                },
                manual_halt: ManualHaltGate::Clear,
            },
            daily: DailyAccountingSnapshot {
                daily_loss: Usd::ZERO,
                daily_fee: Usd::ZERO,
                daily_pnl: Usd::ZERO,
                daily_budget_remaining: Usd::ZERO,
            },
            weekly: WeeklyAccountingSnapshot {
                weekly_loss: Usd::ZERO,
            },
            hourly: HourlyAccountingSnapshot {
                hourly_loss: Usd::ZERO,
                hourly_fee: Usd::ZERO,
            },
            drawdown: DrawdownSnapshot {
                hwm: Usd::ZERO,
                max_drawdown_pct: Decimal::ZERO,
                reduction_factor: Decimal::ONE,
            },
            total_potential_loss: Usd::ZERO,
            blacklist: BlacklistSnapshot::empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_no_false_negatives_on_inserted_keys() {
        let mut bloom = BloomFilter512::default();
        bloom.insert(b"market-a");
        assert!(bloom.may_contain(b"market-a"));
        assert!(!bloom.may_contain(b"market-b"));
    }
}
