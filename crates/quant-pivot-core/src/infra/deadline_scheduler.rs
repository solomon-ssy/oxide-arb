//! Generic deadline scheduler: precise per-entity TTL wakes backed by the DB.
//!
//! The database is the **single source of truth** for deadlines (the `expires_at`
//! columns on intents and reports). This scheduler is a latency optimization on
//! top of it: it loads upcoming deadlines within a bounded horizon into a
//! [`DelayQueue`] and fires the (idempotent, DB-transactional) expiry action
//! exactly when a deadline elapses, instead of waiting for a coarse poll.
//!
//! It is **not** authoritative — correctness never depends on it:
//! - every fire runs the existing batch expiry, which re-checks the DB, so a
//!   missed or duplicated wake only affects latency;
//! - a separate periodic sweep (`PeriodicTask`) remains the durable backstop;
//! - at boot (and every reconcile) the queue is rebuilt from the DB.
//!
//! `load` returns `(key, deadline)` pairs for entities due within the horizon;
//! `fire` runs the idempotent batch expiry and returns how many it expired.

use std::{collections::HashMap, future::poll_fn, hash::Hash, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::QuantResult;
use tokio_util::{
    sync::CancellationToken,
    time::{DelayQueue, delay_queue::Key},
};

/// How far ahead deadlines are pre-loaded into the queue, bounding its size.
/// Deadlines beyond this are picked up by a later reconcile.
const LOOKAHEAD_HORIZON: Duration = Duration::from_mins(15);

/// Run a deadline scheduler until `token` is cancelled.
///
/// - `reconcile`: how often to reload upcoming deadlines from the DB (also the
///   max idle wait when the queue is empty).
/// - `load`: DB query returning `(key, deadline)` for entities due at or before
///   the passed horizon (`now + LOOKAHEAD_HORIZON`).
/// - `fire`: idempotent batch expiry of everything currently due; returns the
///   count expired.
pub async fn run<K, L, LFut, F, FFut>(
    name: &str,
    reconcile: Duration,
    token: CancellationToken,
    load: L,
    fire: F,
) where
    K: Eq + Hash + Clone + Send,
    L: Fn(DateTime<Utc>) -> LFut,
    LFut: Future<Output = QuantResult<Vec<(K, DateTime<Utc>)>>>,
    F: Fn() -> FFut,
    FFut: Future<Output = QuantResult<u32>>,
{
    let mut queue: DelayQueue<K> = DelayQueue::new();
    let mut scheduled: HashMap<K, Key> = HashMap::new();

    loop {
        reconcile_once(name, &load, &fire, &mut queue, &mut scheduled).await;

        // Service precise wakes until the next reconcile tick (or cancellation).
        loop {
            tokio::select! {
                biased;
                () = token.cancelled() => return,
                expired = poll_fn(|cx| queue.poll_expired(cx)), if !queue.is_empty() => {
                    if let Some(expired) = expired {
                        scheduled.remove(expired.get_ref());
                        if let Err(error) = fire().await {
                            tracing::warn!(task = name, %error, "deadline fire failed");
                        }
                    }
                }
                () = tokio::time::sleep(reconcile) => break,
            }
        }
    }
}

/// Expire anything already due, then (re)load upcoming deadlines into the queue.
async fn reconcile_once<K, L, LFut, F, FFut>(
    name: &str,
    load: &L,
    fire: &F,
    queue: &mut DelayQueue<K>,
    scheduled: &mut HashMap<K, Key>,
) where
    K: Eq + Hash + Clone + Send,
    L: Fn(DateTime<Utc>) -> LFut,
    LFut: Future<Output = QuantResult<Vec<(K, DateTime<Utc>)>>>,
    F: Fn() -> FFut,
    FFut: Future<Output = QuantResult<u32>>,
{
    if let Err(error) = fire().await {
        tracing::warn!(task = name, %error, "deadline reconcile sweep failed");
    }

    let now = Utc::now();
    let horizon = now
        + ChronoDuration::from_std(LOOKAHEAD_HORIZON)
            .unwrap_or_else(|_| ChronoDuration::seconds(900));
    match load(horizon).await {
        Ok(items) => {
            for (key, at) in items {
                if scheduled.contains_key(&key) {
                    continue;
                }
                let delay = (at - now).to_std().unwrap_or(Duration::ZERO);
                let qkey = queue.insert(key.clone(), delay);
                scheduled.insert(key, qkey);
            }
        }
        Err(error) => tracing::warn!(task = name, %error, "deadline reconcile load failed"),
    }
}
