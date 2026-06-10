//! Blacklist management for the risk engine.
//!
//! [`BlacklistManager`] maintains an in-memory projection of blacklisted
//! markets and tokens. Backed by `DashMap` for lock-free concurrent reads
//! on the hot path. Permanent entries loaded from config; temporary entries
//! are added at runtime and garbage-collected on TTL expiry.

use crate::{
    clock::Clock,
    snapshot::{BlacklistSnapshot, BloomFilter512, TradingPathBlock},
    types::BlacklistKey,
};
use dashmap::DashMap;
use num_traits::ToPrimitive;
use oxide_arb_models::{
    domain::blacklist::BlacklistInfo,
    enums::{
        blacklist::BlacklistCheckResult,
        risk::{BlacklistReason, BlacklistScope},
    },
    runtime_config::RiskConfig,
    types::{MarketId, TokenId},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

/// Concurrent blacklist projection with lazy TTL eviction.
pub struct BlacklistManager {
    entries: DashMap<BlacklistKey, BlacklistInfo>,
    market_miss_blacklist_count: AtomicU32,
    market_miss_blacklist_duration_secs: AtomicU64,
    clock: Arc<dyn Clock>,
}

impl BlacklistManager {
    /// Construct a new manager and pre-populate permanent blacklist entries
    /// from the config.
    #[must_use]
    pub fn new(config: &RiskConfig, clock: Arc<dyn Clock>) -> Self {
        let manager = Self {
            entries: DashMap::new(),
            market_miss_blacklist_count: AtomicU32::new(config.market_miss_blacklist_count),
            market_miss_blacklist_duration_secs: AtomicU64::new(
                config.market_miss_blacklist_duration_secs,
            ),
            clock,
        };
        manager.merge_permanent_entries(config);

        if !manager.entries.is_empty() {
            tracing::info!(
                permanent_count = manager.entries.len(),
                "blacklist manager initialized with permanent entries"
            );
        }
        manager
    }

    /// Hot-reload blacklist parameters (runtime-config activation).
    ///
    /// Auto-blacklist thresholds apply immediately. Permanent lists are
    /// **merged**: new config entries are added, but entries added at runtime
    /// through the blacklist API (or by auto-blacklisting) are never removed —
    /// removal stays an explicit, audited operator action.
    pub fn reload(&self, config: &RiskConfig) {
        self.market_miss_blacklist_count
            .store(config.market_miss_blacklist_count, Ordering::Relaxed);
        self.market_miss_blacklist_duration_secs.store(
            config.market_miss_blacklist_duration_secs,
            Ordering::Relaxed,
        );
        self.merge_permanent_entries(config);
    }

    /// Consecutive-miss count at which a market is auto-blacklisted.
    #[must_use]
    pub fn miss_threshold(&self) -> u32 {
        self.market_miss_blacklist_count.load(Ordering::Relaxed)
    }

    /// Insert config-declared permanent entries that are not already present.
    fn merge_permanent_entries(&self, config: &RiskConfig) {
        let now = self.clock.now();
        for market_str in &config.permanent_blacklist_markets {
            let market_id = MarketId::new(market_str);
            let key = BlacklistKey::Market(market_id.clone());
            self.entries.entry(key).or_insert_with(|| BlacklistInfo {
                market_id,
                token_id: None,
                scope: BlacklistScope::Full,
                reason: BlacklistReason::Manual,
                expires_at: None,
                created_at: now,
                miss_count: 0,
                updated_at: now,
            });
        }

        for token_str in &config.permanent_blacklist_tokens {
            let token_id = TokenId::new(token_str);
            let key = BlacklistKey::Token(token_id.clone());
            self.entries.entry(key).or_insert_with(|| BlacklistInfo {
                market_id: MarketId::new("_token_blacklist_"),
                token_id: Some(token_id),
                scope: BlacklistScope::Full,
                reason: BlacklistReason::Manual,
                expires_at: None,
                created_at: now,
                miss_count: 0,
                updated_at: now,
            });
        }
    }

    /// Startup recovery: load persisted entries, filtering expired ones.
    pub fn load_entries(&self, entries: Vec<BlacklistInfo>) {
        let now = self.clock.now();
        let mut loaded = 0usize;

        for entry in entries {
            if entry.is_expired(now) {
                continue;
            }
            let key = entry.token_id.as_ref().map_or_else(
                || BlacklistKey::Market(entry.market_id.clone()),
                |token_id| BlacklistKey::Token(token_id.clone()),
            );
            self.entries.insert(key, entry);
            loaded += 1;
        }

        tracing::info!(loaded, "blacklist entries loaded from persistence");
    }

    /// Check whether a market is blacklisted at the `required_scope` level.
    ///
    /// Performs lazy TTL eviction: if an expired entry is found it is removed
    /// and the result is `Clear`.
    #[must_use]
    pub fn check(
        &self,
        market_id: &MarketId,
        required_scope: BlacklistScope,
    ) -> BlacklistCheckResult {
        let key = BlacklistKey::Market(market_id.clone());

        if let Some(entry_ref) = self.entries.get(&key) {
            let entry = entry_ref.value();
            let now = self.clock.now();

            if entry.is_expired(now) {
                drop(entry_ref);
                self.entries.remove(&key);
                return BlacklistCheckResult::Clear;
            }

            if entry.scope >= required_scope {
                return BlacklistCheckResult::Blocked {
                    reason: entry.reason,
                    scope: entry.scope,
                    expires_at: entry.expires_at,
                };
            }
        }

        BlacklistCheckResult::Clear
    }

    /// Add a temporary blacklist entry with TTL.
    ///
    /// If an existing entry for the same market is present, the scope and
    /// expiry are upgraded (never downgraded).
    pub fn add_temporary(
        &self,
        market_id: MarketId,
        token_id: Option<TokenId>,
        scope: BlacklistScope,
        reason: BlacklistReason,
        duration: Duration,
        miss_count: u32,
    ) -> BlacklistInfo {
        let now = self.clock.now();
        let expires_at = now
            + chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::hours(1));

        let key = token_id.as_ref().map_or_else(
            || BlacklistKey::Market(market_id.clone()),
            |token_id| BlacklistKey::Token(token_id.clone()),
        );

        let entry = if let Some(mut existing) = self.entries.get_mut(&key) {
            if existing.is_permanent() {
                return existing.clone();
            }
            if scope > existing.scope {
                existing.scope = scope;
            }
            if Some(expires_at) > existing.expires_at {
                existing.expires_at = Some(expires_at);
            }
            existing.miss_count = ToPrimitive::to_i32(&miss_count).unwrap_or(i32::MAX);
            existing.clone()
        } else {
            let new_entry = BlacklistInfo {
                market_id,
                token_id,
                scope,
                reason,
                expires_at: Some(expires_at),
                created_at: now,
                miss_count: ToPrimitive::to_i32(&miss_count).unwrap_or(i32::MAX),
                updated_at: now,
            };
            self.entries.insert(key, new_entry.clone());
            new_entry
        };

        tracing::warn!(
            market_id = %entry.market_id,
            scope = %entry.scope,
            reason = %entry.reason,
            expires_at = ?entry.expires_at,
            "temporary blacklist entry added/upgraded"
        );

        entry
    }

    /// Add a permanent (never-expiring) blacklist entry.
    pub fn add_permanent(&self, market_id: MarketId, reason: BlacklistReason) -> BlacklistInfo {
        let now = self.clock.now();
        let key = BlacklistKey::Market(market_id.clone());

        let entry = BlacklistInfo {
            market_id,
            token_id: None,
            scope: BlacklistScope::Full,
            reason,
            expires_at: None,
            created_at: now,
            miss_count: 0,
            updated_at: now,
        };

        self.entries.insert(key, entry.clone());

        tracing::warn!(
            market_id = %entry.market_id,
            reason = %entry.reason,
            "permanent blacklist entry added"
        );

        entry
    }

    /// Remove a blacklist entry by market ID. Returns `true` if an entry
    /// was actually removed.
    #[must_use]
    pub fn remove(&self, market_id: &MarketId) -> bool {
        let key = BlacklistKey::Market(market_id.clone());
        let removed = self.entries.remove(&key).is_some();
        if removed {
            tracing::info!(market_id = %market_id, "blacklist entry removed");
        }
        removed
    }

    /// Garbage-collect expired entries. Permanent entries are never removed.
    /// Returns the number of entries evicted.
    pub fn gc(&self) -> usize {
        let now = self.clock.now();
        let before = self.entries.len();

        self.entries
            .retain(|_, entry| entry.is_permanent() || !entry.is_expired(now));

        let evicted = before - self.entries.len();
        if evicted > 0 {
            tracing::info!(
                evicted,
                remaining = self.entries.len(),
                "blacklist GC completed"
            );
        }
        evicted
    }

    /// Auto-blacklist a market if its consecutive miss count exceeds the
    /// configured threshold.
    ///
    /// Returns `Some(entry)` if a new blacklist entry was created, `None`
    /// if the threshold was not met or the market is already blacklisted.
    pub fn maybe_auto_blacklist(
        &self,
        market_id: &MarketId,
        consecutive_misses: u32,
    ) -> Option<BlacklistInfo> {
        if consecutive_misses < self.market_miss_blacklist_count.load(Ordering::Relaxed) {
            return None;
        }

        let key = BlacklistKey::Market(market_id.clone());
        if self.entries.contains_key(&key) {
            return None;
        }

        let duration = Duration::from_secs(
            self.market_miss_blacklist_duration_secs
                .load(Ordering::Relaxed),
        );

        let entry = self.add_temporary(
            market_id.clone(),
            None,
            BlacklistScope::TradingPath,
            BlacklistReason::ConsecutiveFokFailures,
            duration,
            consecutive_misses,
        );

        tracing::warn!(
            market_id = %market_id,
            consecutive_misses,
            "market auto-blacklisted due to consecutive misses"
        );

        Some(entry)
    }

    /// Check whether a token is blacklisted (permanent token blacklist).
    #[must_use]
    pub fn is_token_blacklisted(&self, token_id: &TokenId) -> bool {
        let key = BlacklistKey::Token(token_id.clone());
        if let Some(entry_ref) = self.entries.get(&key) {
            let now = self.clock.now();
            if entry_ref.value().is_expired(now) {
                drop(entry_ref);
                self.entries.remove(&key);
                return false;
            }
            return true;
        }
        false
    }

    /// Snapshot of all active (non-expired) entries.
    #[must_use]
    pub fn active_entries(&self) -> Vec<BlacklistInfo> {
        let now = self.clock.now();
        self.entries
            .iter()
            .filter(|r| !r.value().is_expired(now))
            .map(|r| r.value().clone())
            .collect()
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        let now = self.clock.now();
        self.entries
            .iter()
            .filter(|r| !r.value().is_expired(now))
            .count()
    }

    /// Build bloom + exact confirm tables for [`RiskSnapshot`] publish.
    #[must_use]
    pub fn build_bloom_snapshot(&self) -> BlacklistSnapshot {
        let now = self.clock.now();
        let mut market_bloom = BloomFilter512::default();
        let mut token_bloom = BloomFilter512::default();
        let mut trading_path_blocks = Vec::new();
        let mut blacklisted_tokens = Vec::new();

        for entry in &self.entries {
            let info = entry.value();
            if info.is_expired(now) {
                continue;
            }
            match entry.key() {
                BlacklistKey::Market(market_id) => {
                    market_bloom.insert(market_id.as_str().as_bytes());
                    if info.scope >= BlacklistScope::TradingPath {
                        trading_path_blocks.push(TradingPathBlock {
                            market_id: market_id.clone(),
                            reason: info.reason,
                            scope: info.scope,
                        });
                    }
                }
                BlacklistKey::Token(token_id) => {
                    token_bloom.insert(token_id.as_str().as_bytes());
                    blacklisted_tokens.push(token_id.clone());
                }
            }
        }

        BlacklistSnapshot::from_parts(
            market_bloom,
            token_bloom,
            trading_path_blocks,
            blacklisted_tokens,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{enums::risk::BlacklistReason, runtime_config::RiskConfig};

    #[test]
    fn bloom_snapshot_blocks_trading_path_market() {
        let clock = crate::clock::utc_clock();
        let manager = BlacklistManager::new(&RiskConfig::default(), clock);
        manager.add_permanent(MarketId::new("blocked"), BlacklistReason::Manual);
        let snap = manager.build_bloom_snapshot();
        assert!(snap.may_contain_market(&MarketId::new("blocked")));
        assert!(
            snap.trading_path_block_detail(&MarketId::new("blocked"))
                .is_some()
        );
        assert!(
            snap.trading_path_block_detail(&MarketId::new("clear"))
                .is_none()
        );
    }
}
