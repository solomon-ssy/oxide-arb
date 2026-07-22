//! Immutable catalog index and stable per-token runtime slots.

use std::{
    hint::spin_loop,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};

use ahash::AHashMap;
use alloy::primitives::U256;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use quant_pivot_api::ws::TokenKeyResolver;
use quant_pivot_models::{
    domain::market::{EventRegistryInfo, MarketRegistryInfo, book::BookSnapshot},
    enums::market::MarketStatus,
    types::{EventId, MarketId, TokenId, TokenKey},
};

/// Dense metadata parallel to [`DataPlaneIndex::token_slots`].
#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub token_id: TokenId,
    pub market_id: MarketId,
}

/// Dense YES/NO token routing for one market.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketTokenPair {
    pub yes: TokenKey,
    pub no: TokenKey,
}

/// One immutable, atomically published view of all catalog routing metadata.
pub struct DataPlaneIndex {
    token_keys_by_u256: AHashMap<U256, TokenKey>,
    token_keys_by_id: AHashMap<TokenId, TokenKey>,
    token_metadata: Arc<[TokenMetadata]>,
    token_slots: Arc<[Arc<TokenSlot>]>,
    market_tokens: AHashMap<MarketId, MarketTokenPair>,
    markets: AHashMap<MarketId, Arc<MarketRegistryInfo>>,
    events: AHashMap<EventId, Arc<EventRegistryInfo>>,
    active_markets: Arc<[MarketId]>,
}

impl DataPlaneIndex {
    fn empty() -> Self {
        Self {
            token_keys_by_u256: AHashMap::new(),
            token_keys_by_id: AHashMap::new(),
            token_metadata: Arc::from([]),
            token_slots: Arc::from([]),
            market_tokens: AHashMap::new(),
            markets: AHashMap::new(),
            events: AHashMap::new(),
            active_markets: Arc::from([]),
        }
    }

    #[must_use]
    pub fn token_key(&self, token_id: &TokenId) -> Option<TokenKey> {
        self.token_keys_by_id.get(token_id).copied()
    }

    #[must_use]
    pub fn token_key_u256(&self, token_id: U256) -> Option<TokenKey> {
        self.token_keys_by_u256.get(&token_id).copied()
    }

    #[must_use]
    pub fn token_metadata(&self, token: TokenKey) -> Option<&TokenMetadata> {
        self.token_metadata.get(token.index())
    }

    #[must_use]
    pub fn token_slot(&self, token: TokenKey) -> Option<&TokenSlot> {
        self.token_slots.get(token.index()).map(Arc::as_ref)
    }

    #[must_use]
    pub fn token_count(&self) -> usize {
        self.token_metadata.len()
    }

    #[must_use]
    pub fn market_token_pair(&self, market_id: &MarketId) -> Option<MarketTokenPair> {
        self.market_tokens.get(market_id).copied()
    }

    #[must_use]
    pub fn market(&self, market_id: &MarketId) -> Option<&Arc<MarketRegistryInfo>> {
        self.markets.get(market_id)
    }

    #[must_use]
    pub fn event(&self, event_id: &EventId) -> Option<&Arc<EventRegistryInfo>> {
        self.events.get(event_id)
    }

    #[must_use]
    pub fn active_markets(&self) -> &[MarketId] {
        &self.active_markets
    }

    #[must_use]
    pub fn active_markets_owned(&self) -> Arc<[MarketId]> {
        Arc::clone(&self.active_markets)
    }

    #[must_use]
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TokenSlotState {
    Unseen = 0,
    Fresh = 1,
    Invalid = 2,
}

impl TokenSlotState {
    const fn from_raw(value: u8) -> Self {
        match value {
            1 => Self::Fresh,
            2 => Self::Invalid,
            _ => Self::Unseen,
        }
    }
}

/// Coherent freshness fields sampled through the slot's odd/even version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenFreshness {
    pub sequence: u64,
    pub session_generation: u64,
    pub freshness_tick: u64,
    pub state: TokenSlotState,
    pub latency_tick: u64,
}

/// Stable runtime state addressed directly by [`TokenKey`].
pub struct TokenSlot {
    pub published: ArcSwap<BookSnapshot>,
    pub sequence: AtomicU64,
    pub session_generation: AtomicU64,
    pub freshness_version: AtomicU64,
    pub freshness_tick: AtomicU64,
    pub state: AtomicU8,
    pub latency_tick: AtomicU64,
}

impl TokenSlot {
    fn new() -> Self {
        Self {
            published: ArcSwap::from_pointee(BookSnapshot::new(Arc::from([]), Arc::from([]), 0, 0)),
            sequence: AtomicU64::new(0),
            session_generation: AtomicU64::new(0),
            freshness_version: AtomicU64::new(0),
            freshness_tick: AtomicU64::new(0),
            state: AtomicU8::new(TokenSlotState::Unseen as u8),
            latency_tick: AtomicU64::new(0),
        }
    }

    fn begin_freshness_write(&self) -> u64 {
        let mut version = self.freshness_version.load(Ordering::Acquire);
        loop {
            if version & 1 == 1 {
                spin_loop();
                version = self.freshness_version.load(Ordering::Acquire);
                continue;
            }
            match self.freshness_version.compare_exchange_weak(
                version,
                version.wrapping_add(1),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return version,
                Err(observed) => version = observed,
            }
        }
    }

    fn end_freshness_write(&self, even_version: u64) {
        self.freshness_version
            .store(even_version.wrapping_add(2), Ordering::Release);
    }

    pub fn mark_fresh(
        &self,
        sequence: u64,
        session_generation: u64,
        freshness_tick: u64,
        latency_tick: u64,
    ) {
        let version = self.begin_freshness_write();
        self.sequence.store(sequence, Ordering::Relaxed);
        self.session_generation
            .store(session_generation, Ordering::Relaxed);
        self.freshness_tick.store(freshness_tick, Ordering::Relaxed);
        self.latency_tick.store(latency_tick, Ordering::Relaxed);
        self.state
            .store(TokenSlotState::Fresh as u8, Ordering::Relaxed);
        self.end_freshness_write(version);
    }

    pub fn publish(
        &self,
        snapshot: Arc<BookSnapshot>,
        sequence: u64,
        session_generation: u64,
        freshness_tick: u64,
        latency_tick: u64,
    ) {
        let version = self.begin_freshness_write();
        self.published.store(snapshot);
        self.sequence.store(sequence, Ordering::Relaxed);
        self.session_generation
            .store(session_generation, Ordering::Relaxed);
        self.freshness_tick.store(freshness_tick, Ordering::Relaxed);
        self.latency_tick.store(latency_tick, Ordering::Relaxed);
        self.state
            .store(TokenSlotState::Fresh as u8, Ordering::Relaxed);
        self.end_freshness_write(version);
    }

    pub fn invalidate(&self, session_generation: u64) {
        let version = self.begin_freshness_write();
        self.session_generation
            .store(session_generation, Ordering::Relaxed);
        self.freshness_tick.store(0, Ordering::Relaxed);
        self.state
            .store(TokenSlotState::Invalid as u8, Ordering::Relaxed);
        self.end_freshness_write(version);
    }

    #[must_use]
    pub fn freshness(&self) -> TokenFreshness {
        loop {
            let before = self.freshness_version.load(Ordering::Acquire);
            if before & 1 == 1 {
                spin_loop();
                continue;
            }
            let snapshot = TokenFreshness {
                sequence: self.sequence.load(Ordering::Relaxed),
                session_generation: self.session_generation.load(Ordering::Relaxed),
                freshness_tick: self.freshness_tick.load(Ordering::Relaxed),
                state: TokenSlotState::from_raw(self.state.load(Ordering::Relaxed)),
                latency_tick: self.latency_tick.load(Ordering::Relaxed),
            };
            let after = self.freshness_version.load(Ordering::Acquire);
            if before == after {
                return snapshot;
            }
        }
    }
}

struct DataPlaneWriter {
    token_keys_by_u256: AHashMap<U256, TokenKey>,
    token_keys_by_id: AHashMap<TokenId, TokenKey>,
    token_metadata: Vec<TokenMetadata>,
    token_slots: Vec<Arc<TokenSlot>>,
    market_tokens: AHashMap<MarketId, MarketTokenPair>,
    markets: AHashMap<MarketId, Arc<MarketRegistryInfo>>,
    events: AHashMap<EventId, Arc<EventRegistryInfo>>,
}

impl DataPlaneWriter {
    fn empty() -> Self {
        Self {
            token_keys_by_u256: AHashMap::new(),
            token_keys_by_id: AHashMap::new(),
            token_metadata: Vec::new(),
            token_slots: Vec::new(),
            market_tokens: AHashMap::new(),
            markets: AHashMap::new(),
            events: AHashMap::new(),
        }
    }

    fn register_token(&mut self, token_id: &TokenId, market_id: &MarketId) -> Option<TokenKey> {
        if let Some(key) = self.token_keys_by_id.get(token_id).copied() {
            self.token_metadata[key.index()].market_id = market_id.clone();
            return Some(key);
        }
        let raw_key = u32::try_from(self.token_metadata.len()).ok()?;
        let key = TokenKey::new(raw_key);
        self.token_keys_by_id.insert(token_id.clone(), key);
        if let Ok(u256) = U256::from_str(token_id.as_str()) {
            self.token_keys_by_u256.insert(u256, key);
        }
        self.token_metadata.push(TokenMetadata {
            token_id: token_id.clone(),
            market_id: market_id.clone(),
        });
        self.token_slots.push(Arc::new(TokenSlot::new()));
        Some(key)
    }

    fn register_market(&mut self, entry: MarketRegistryInfo) -> bool {
        if entry.resolve_token_pair().is_err() {
            return false;
        }
        let Some(yes) = self.register_token(&entry.token_yes, &entry.market_id) else {
            return false;
        };
        let Some(no) = self.register_token(&entry.token_no, &entry.market_id) else {
            return false;
        };
        self.market_tokens
            .insert(entry.market_id.clone(), MarketTokenPair { yes, no });
        self.markets
            .insert(entry.market_id.clone(), Arc::new(entry));
        true
    }

    fn snapshot(&self) -> DataPlaneIndex {
        let mut active_markets = self
            .markets
            .values()
            .filter(|entry| entry.status == MarketStatus::Active)
            .map(|entry| entry.market_id.clone())
            .collect::<Vec<_>>();
        active_markets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        DataPlaneIndex {
            token_keys_by_u256: self.token_keys_by_u256.clone(),
            token_keys_by_id: self.token_keys_by_id.clone(),
            token_metadata: Arc::from(self.token_metadata.clone()),
            token_slots: Arc::from(self.token_slots.clone()),
            market_tokens: self.market_tokens.clone(),
            markets: self.markets.clone(),
            events: self.events.clone(),
            active_markets: Arc::from(active_markets),
        }
    }
}

/// Cold-path writer plus the atomically published live data-plane index.
pub struct DataPlane {
    index: ArcSwap<DataPlaneIndex>,
    writer: Mutex<DataPlaneWriter>,
    epoch: Instant,
}

impl DataPlane {
    #[must_use]
    pub fn new() -> Self {
        Self {
            index: ArcSwap::from_pointee(DataPlaneIndex::empty()),
            writer: Mutex::new(DataPlaneWriter::empty()),
            epoch: Instant::now(),
        }
    }

    pub fn register_markets(&self, entries: Vec<MarketRegistryInfo>) -> usize {
        if entries.is_empty() {
            return 0;
        }
        let mut writer = self.writer.lock();
        let mut registered = 0;
        for entry in entries {
            if writer.register_market(entry) {
                registered += 1;
            }
        }
        self.index.store(Arc::new(writer.snapshot()));
        registered
    }

    pub fn register_events(&self, events: impl IntoIterator<Item = EventRegistryInfo>) {
        let mut writer = self.writer.lock();
        for event in events {
            writer
                .events
                .insert(event.event_id.clone(), Arc::new(event));
        }
        self.index.store(Arc::new(writer.snapshot()));
    }

    #[cfg(test)]
    pub fn register_test_tokens(&self, token_ids: &[TokenId]) {
        let mut writer = self.writer.lock();
        let market_id = MarketId::new("test-market");
        for token_id in token_ids {
            let _ = writer.register_token(token_id, &market_id);
        }
        self.index.store(Arc::new(writer.snapshot()));
    }

    #[must_use]
    pub fn index_owned(&self) -> Arc<DataPlaneIndex> {
        self.index.load_full()
    }

    #[must_use]
    pub fn token_key(&self, token_id: &TokenId) -> Option<TokenKey> {
        self.index.load().token_key(token_id)
    }

    #[must_use]
    pub fn token_key_u256(&self, token_id: U256) -> Option<TokenKey> {
        self.index.load().token_key_u256(token_id)
    }

    pub fn with_index<R>(&self, read: impl FnOnce(&DataPlaneIndex) -> R) -> R {
        let index = self.index.load();
        read(&index)
    }

    pub fn with_slot<R>(&self, token: TokenKey, read: impl FnOnce(&TokenSlot) -> R) -> Option<R> {
        let index = self.index.load();
        index.token_slot(token).map(read)
    }

    #[must_use]
    pub fn now_tick(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1)
    }
}

impl Default for DataPlane {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenKeyResolver for DataPlane {
    fn resolve(&self, token: U256) -> Option<TokenKey> {
        self.token_key_u256(token)
    }
}

#[cfg(test)]
mod tests {
    use std::{slice, sync::Arc, thread};

    use alloy::primitives::U256;

    use super::{DataPlane, TokenSlot, TokenSlotState};
    use quant_pivot_models::types::TokenId;

    #[test]
    fn unknown_token_fails_closed() {
        let data_plane = DataPlane::new();
        assert!(data_plane.token_key(&TokenId::new("unknown")).is_none());
    }

    #[test]
    fn token_keys_and_slots_stay_stable_across_snapshot_rebuilds() {
        let data_plane = DataPlane::new();
        let first = TokenId::new("42");
        data_plane.register_test_tokens(slice::from_ref(&first));
        let first_key = data_plane.token_key(&first).expect("first key");
        let first_index = data_plane.index_owned();
        let first_slot = Arc::clone(&first_index.token_slots[first_key.index()]);

        let second = TokenId::new("99");
        data_plane.register_test_tokens(&[second, first.clone()]);
        let rebuilt_key = data_plane.token_key(&first).expect("rebuilt key");
        let rebuilt_index = data_plane.index_owned();
        let rebuilt_slot = &rebuilt_index.token_slots[rebuilt_key.index()];

        assert_eq!(first_key, rebuilt_key);
        assert_eq!(
            data_plane.token_key_u256(U256::from(42_u64)),
            Some(first_key)
        );
        assert!(Arc::ptr_eq(&first_slot, rebuilt_slot));
    }

    #[test]
    fn freshness_snapshot_is_coherent_and_invalidates() {
        let slot = Arc::new(TokenSlot::new());
        slot.mark_fresh(9, 3, 101, 102);
        assert_eq!(
            slot.freshness(),
            super::TokenFreshness {
                sequence: 9,
                session_generation: 3,
                freshness_tick: 101,
                state: TokenSlotState::Fresh,
                latency_tick: 102,
            }
        );
        slot.invalidate(4);
        let invalid = slot.freshness();
        assert_eq!(invalid.session_generation, 4);
        assert_eq!(invalid.state, TokenSlotState::Invalid);
        assert_eq!(invalid.freshness_tick, 0);
    }

    #[test]
    fn concurrent_freshness_reads_never_observe_torn_fields() {
        let slot = Arc::new(TokenSlot::new());
        let writer_slot = Arc::clone(&slot);
        let writer = thread::spawn(move || {
            for value in 1..=10_000 {
                writer_slot.mark_fresh(value, value, value, value);
            }
        });

        while !writer.is_finished() {
            let snapshot = slot.freshness();
            if snapshot.state == TokenSlotState::Fresh {
                assert_eq!(snapshot.sequence, snapshot.session_generation);
                assert_eq!(snapshot.sequence, snapshot.freshness_tick);
                assert_eq!(snapshot.sequence, snapshot.latency_tick);
            }
        }
        writer.join().expect("freshness writer");
    }
}
