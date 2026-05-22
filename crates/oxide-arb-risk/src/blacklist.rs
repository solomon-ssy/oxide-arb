//! Blacklist management for the risk engine.
//!
//! [`BlacklistManager`] maintains an in-memory projection of blacklisted
//! markets and tokens. Backed by `DashMap` for lock-free concurrent reads
//! on the hot path. Permanent entries loaded from config; temporary entries
//! are added at runtime and garbage-collected on TTL expiry.

use crate::types::BlacklistKey;
use chrono::Utc;
use dashmap::DashMap;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::blacklist::{BlacklistCheckResult, BlacklistEntry};
use oxide_arb_models::enums::risk::{BlacklistReason, BlacklistScope};
use oxide_arb_models::types::{MarketId, TokenId};
use std::time::Duration;

/// Concurrent blacklist projection with lazy TTL eviction.
pub struct BlacklistManager {
    entries: DashMap<BlacklistKey, BlacklistEntry>,
    config: RiskConfig,
}

impl BlacklistManager {
    /// Construct a new manager and pre-populate permanent blacklist entries
    /// from the config.
    #[must_use]
    pub fn new(config: &RiskConfig) -> Self {
        let entries = DashMap::new();

        let now = Utc::now();
        for market_str in &config.permanent_blacklist_markets {
            let market_id = MarketId::new(market_str);
            let key = BlacklistKey::Market(market_id.clone());
            let entry = BlacklistEntry {
                market_id,
                token_id: None,
                scope: BlacklistScope::Full,
                reason: BlacklistReason::Manual,
                expires_at: None,
                created_at: now,
                miss_count: 0,
            };
            entries.insert(key, entry);
        }

        for token_str in &config.permanent_blacklist_tokens {
            let token_id = TokenId::new(token_str);
            let key = BlacklistKey::Token(token_id.clone());
            let entry = BlacklistEntry {
                market_id: MarketId::new("_token_blacklist_"),
                token_id: Some(token_id),
                scope: BlacklistScope::Full,
                reason: BlacklistReason::Manual,
                expires_at: None,
                created_at: now,
                miss_count: 0,
            };
            entries.insert(key, entry);
        }

        if !entries.is_empty() {
            tracing::info!(
                permanent_count = entries.len(),
                "blacklist manager initialized with permanent entries"
            );
        }

        Self {
            entries,
            config: config.clone(),
        }
    }

    /// Startup recovery: load persisted entries, filtering expired ones.
    pub fn load_entries(&self, entries: Vec<BlacklistEntry>) {
        let now = Utc::now();
        let mut loaded = 0usize;

        for entry in entries {
            if entry.is_expired(now) {
                continue;
            }
            let key = BlacklistKey::Market(entry.market_id.clone());
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
            let now = Utc::now();

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
    ) -> BlacklistEntry {
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::hours(1));

        let key = BlacklistKey::Market(market_id.clone());

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
            existing.miss_count = miss_count;
            existing.clone()
        } else {
            let new_entry = BlacklistEntry {
                market_id,
                token_id,
                scope,
                reason,
                expires_at: Some(expires_at),
                created_at: now,
                miss_count,
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
    pub fn add_permanent(&self, market_id: MarketId, reason: BlacklistReason) -> BlacklistEntry {
        let now = Utc::now();
        let key = BlacklistKey::Market(market_id.clone());

        let entry = BlacklistEntry {
            market_id,
            token_id: None,
            scope: BlacklistScope::Full,
            reason,
            expires_at: None,
            created_at: now,
            miss_count: 0,
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
        let now = Utc::now();
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
    ) -> Option<BlacklistEntry> {
        if consecutive_misses < self.config.market_miss_blacklist_count {
            return None;
        }

        let key = BlacklistKey::Market(market_id.clone());
        if self.entries.contains_key(&key) {
            return None;
        }

        let duration = Duration::from_secs(self.config.market_miss_blacklist_duration_secs);

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
            let now = Utc::now();
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
    pub fn active_entries(&self) -> Vec<BlacklistEntry> {
        let now = Utc::now();
        self.entries
            .iter()
            .filter(|r| !r.value().is_expired(now))
            .map(|r| r.value().clone())
            .collect()
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        let now = Utc::now();
        self.entries
            .iter()
            .filter(|r| !r.value().is_expired(now))
            .count()
    }
}
