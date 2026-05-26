//! Zero-alloc [`TokenId`] interning for WS hot paths.

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use oxide_arb_models::types::TokenId;
use polymarket_client_sdk_v2::types::U256;

/// Process-wide intern pool for CLOB decimal token ids.
pub static TOKEN_INTERN: LazyLock<TokenInternPool> = LazyLock::new(TokenInternPool::new);

/// Thread-safe intern pool keyed by [`U256`] and string asset ids.
#[derive(Debug, Default)]
pub struct TokenInternPool {
    by_u256: DashMap<U256, TokenId>,
    by_str: DashMap<Arc<str>, TokenId>,
}

impl TokenInternPool {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern a CLOB `token_id` (decimal U256) — cache hit avoids string allocation.
    #[must_use]
    pub fn intern_u256(&self, asset_id: U256) -> TokenId {
        if let Some(existing) = self.by_u256.get(&asset_id) {
            return existing.clone();
        }
        let token = TokenId::new(asset_id.to_string());
        self.by_u256.insert(asset_id, token.clone());
        token
    }

    /// Intern a string asset id (cache hit avoids heap alloc).
    #[must_use]
    pub fn intern_str(&self, asset_id: &str) -> TokenId {
        if let Some(existing) = self.by_str.get(asset_id) {
            return existing.clone();
        }
        let key: Arc<str> = Arc::from(asset_id);
        if let Some(existing) = self.by_str.get(key.as_ref()) {
            return existing.clone();
        }
        let token = TokenId::new(key.as_ref());
        self.by_str.insert(key, token.clone());
        token
    }
}

#[inline]
pub fn intern_u256(asset_id: U256) -> TokenId {
    TOKEN_INTERN.intern_u256(asset_id)
}

#[inline]
pub fn intern_str(asset_id: &str) -> TokenId {
    TOKEN_INTERN.intern_str(asset_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_str_is_stable() {
        let pool = TokenInternPool::new();
        let a = pool.intern_str("12345");
        let b = pool.intern_str("12345");
        assert_eq!(a.as_str(), b.as_str());
    }

    #[test]
    fn intern_u256_is_stable() {
        let pool = TokenInternPool::new();
        let id = U256::from(42_u64);
        let a = pool.intern_u256(id);
        let b = pool.intern_u256(id);
        assert_eq!(a.as_str(), b.as_str());
    }
}
