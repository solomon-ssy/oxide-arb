//! Token-to-shard routing over resident shard actors.
//!
//! Each shard is spawned exactly once and lives until shutdown; the router
//! keeps the assignment ledger and pushes the **full** desired token set to
//! the owning shard over a `tokio::sync::watch` channel after every change.
//! Full-state publication is idempotent and self-healing: a missed or
//! coalesced update is corrected by the next one.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;
use quant_pivot_models::types::TokenId;
use tokio::sync::{watch, watch::Sender};

use super::shard::{ShardDeps, WsShard};

/// One spawned shard: command channel plus its assignment ledger entry.
struct ShardSlot {
    tokens_tx: Sender<Arc<HashSet<TokenId>>>,
    assigned: HashSet<TokenId>,
}

/// Mutable routing state guarded by one lock (assignments must stay
/// consistent with the per-shard sets).
#[derive(Default)]
struct RouterLedger {
    assignments: HashMap<TokenId, usize>,
    shards: Vec<ShardSlot>,
}

/// Routes token subscriptions across resident shards.
pub struct ShardRouter {
    max_per_shard: usize,
    ledger: Mutex<RouterLedger>,
    deps: ShardDeps,
}

impl ShardRouter {
    pub(super) fn new(max_per_shard: usize, deps: ShardDeps) -> Self {
        Self {
            max_per_shard: max_per_shard.max(1),
            ledger: Mutex::new(RouterLedger::default()),
            deps,
        }
    }

    /// Assign `tokens` to shards (no-op for already-assigned tokens) and push
    /// the updated full set to every affected shard.
    pub fn assign_tokens(&self, tokens: &[TokenId]) {
        let mut ledger = self.ledger.lock();
        let mut dirty: HashSet<usize> = HashSet::new();

        for token in tokens {
            if ledger.assignments.contains_key(token) {
                continue;
            }
            let shard_id = self.pick_shard(&mut ledger);
            ledger.assignments.insert(token.clone(), shard_id);
            ledger.shards[shard_id].assigned.insert(token.clone());
            dirty.insert(shard_id);
        }

        publish(&ledger, &dirty);
        drop(ledger);
    }

    /// Remove `tokens` from their shards and push the shrunk sets.
    pub fn remove_tokens(&self, tokens: &[TokenId]) {
        let mut ledger = self.ledger.lock();
        let mut dirty: HashSet<usize> = HashSet::new();

        for token in tokens {
            let Some(shard_id) = ledger.assignments.remove(token) else {
                continue;
            };
            if let Some(slot) = ledger.shards.get_mut(shard_id) {
                slot.assigned.remove(token);
                dirty.insert(shard_id);
            }
        }

        publish(&ledger, &dirty);
        drop(ledger);
    }

    /// Force each owning shard touched by `tokens` to establish one fresh stream session.
    ///
    /// A session-level invalidation can contain hundreds of tokens from the
    /// same shard. Publishing once per shard avoids a reconnect storm while
    /// preserving the full-state watch contract.
    pub fn restart_tokens(&self, tokens: &[TokenId]) {
        let ledger = self.ledger.lock();
        let dirty = tokens
            .iter()
            .filter_map(|token| ledger.assignments.get(token).copied())
            .collect::<HashSet<_>>();
        let publications = dirty
            .into_iter()
            .map(|shard_id| {
                let slot = &ledger.shards[shard_id];
                (slot.tokens_tx.clone(), Arc::new(slot.assigned.clone()))
            })
            .collect::<Vec<_>>();
        drop(ledger);
        for (tokens_tx, assigned) in publications {
            tokens_tx.send_replace(assigned);
        }
    }

    /// Number of shards spawned so far.
    pub fn shard_count(&self) -> usize {
        self.ledger.lock().shards.len()
    }

    /// First shard with spare capacity, or a freshly spawned one.
    fn pick_shard(&self, ledger: &mut RouterLedger) -> usize {
        if let Some(shard_id) = ledger
            .shards
            .iter()
            .position(|slot| slot.assigned.len() < self.max_per_shard)
        {
            return shard_id;
        }
        self.spawn_shard(ledger)
    }

    /// Spawn a new resident shard actor (exactly once per `shard_id`).
    fn spawn_shard(&self, ledger: &mut RouterLedger) -> usize {
        let shard_id = ledger.shards.len();
        let (tokens_tx, tokens_rx) = watch::channel(Arc::new(HashSet::new()));
        self.deps.health.register(shard_id);
        tokio::spawn(WsShard::new(shard_id, tokens_rx, self.deps.clone()).run_loop());
        ledger.shards.push(ShardSlot {
            tokens_tx,
            assigned: HashSet::new(),
        });
        tracing::info!(shard_id, "WS shard spawned");
        shard_id
    }

    /// Current desired token set of a shard (test observability).
    #[cfg(test)]
    fn shard_tokens(&self, shard_id: usize) -> HashSet<TokenId> {
        self.ledger.lock().shards[shard_id]
            .tokens_tx
            .borrow()
            .as_ref()
            .clone()
    }
}

/// Push the full desired set to every dirty shard (last write wins).
fn publish(ledger: &RouterLedger, dirty: &HashSet<usize>) {
    for &shard_id in dirty {
        let slot = &ledger.shards[shard_id];
        slot.tokens_tx.send_replace(Arc::new(slot.assigned.clone()));
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, atomic::AtomicU64},
        time::{Duration, Instant},
    };

    use polymarket_client_sdk_v2::types::U256;
    use quant_pivot_models::types::TokenKey;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::ws::{health::ShardHealthBoard, reconnect::ReconnectPolicy};

    fn test_router(max_per_shard: usize) -> ShardRouter {
        let (output_tx, _output_rx) = flume::bounded(64);
        ShardRouter::new(
            max_per_shard,
            ShardDeps {
                output_tx,
                ingress_budget: Arc::new(Semaphore::new(256)),
                ws_url: "ws://test".into(),
                shutdown: CancellationToken::new(),
                message_epoch: Arc::new(Instant::now()),
                last_message_tick: Arc::new(AtomicU64::new(0)),
                token_resolver: Arc::new(|token: U256| Some(TokenKey::new(token.to::<u32>()))),
                on_session_invalidated: None,
                on_book_level_rejected: None,
                reconnect_policy: ReconnectPolicy::default(),
                sdk_initial_backoff: Duration::from_secs(1),
                sdk_max_backoff: Duration::from_secs(30),
                connect_limiter: Arc::new(Semaphore::new(4)),
                health: Arc::new(ShardHealthBoard::default()),
            },
        )
    }

    fn tok(s: &str) -> TokenId {
        TokenId::new(s)
    }

    #[tokio::test]
    async fn repeated_assigns_reuse_the_resident_shard() {
        let router = test_router(8);

        router.assign_tokens(&[tok("1"), tok("2")]);
        assert_eq!(router.shard_count(), 1);

        // Second assignment to the same shard must not spawn a new task and
        // must publish the merged full set.
        router.assign_tokens(&[tok("2"), tok("3")]);
        assert_eq!(router.shard_count(), 1, "no duplicate shard spawn");
        assert_eq!(
            router.shard_tokens(0),
            HashSet::from([tok("1"), tok("2"), tok("3")])
        );
        drop(router);
    }

    #[tokio::test]
    async fn remove_publishes_the_shrunk_set() {
        let router = test_router(8);
        router.assign_tokens(&[tok("1"), tok("2")]);

        router.remove_tokens(&[tok("1")]);
        assert_eq!(router.shard_tokens(0), HashSet::from([tok("2")]));

        // Removing the rest empties the set (shard parks, stays resident).
        router.remove_tokens(&[tok("2")]);
        assert!(router.shard_tokens(0).is_empty());
        assert_eq!(router.shard_count(), 1);
        drop(router);
    }

    #[tokio::test]
    async fn overflow_spawns_a_second_shard_and_freed_capacity_is_reused() {
        let router = test_router(2);
        router.assign_tokens(&[tok("1"), tok("2"), tok("3")]);
        assert_eq!(router.shard_count(), 2);
        assert_eq!(router.shard_tokens(1), HashSet::from([tok("3")]));

        // Freeing capacity on shard 0 lets the next token land there instead
        // of spawning a third shard.
        router.remove_tokens(&[tok("1")]);
        router.assign_tokens(&[tok("4")]);
        assert_eq!(router.shard_count(), 2);
        assert_eq!(router.shard_tokens(0), HashSet::from([tok("2"), tok("4")]));
        drop(router);
    }
}
