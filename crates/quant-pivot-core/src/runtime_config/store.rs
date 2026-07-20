//! Lock-free in-process snapshot of the active runtime configuration.

use arc_swap::{ArcSwap, Guard};
use parking_lot::Mutex;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot},
    types::{ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration},
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedPolicyBundle {
    pub generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyBundlePublication {
    Published,
    AlreadyCurrent,
    OlderIgnored,
}

/// Process-wide holder of the active [`DecisionPolicySnapshot`].
///
/// Hot-path readers call [`Self::load`] (lock-free, no refcount bump for short
/// borrows); tasks that hold the snapshot across awaits use [`Self::current`].
/// Writes go exclusively through the
/// [`PolicySnapshotApplicator`](super::PolicySnapshotApplicator) after a
/// durable, audited activation.
pub struct DecisionPolicyStore {
    inner: ArcSwap<DecisionPolicySnapshot>,
    publication: Mutex<Option<PublishedPolicyBundle>>,
}

impl DecisionPolicyStore {
    #[must_use]
    pub fn new(initial: DecisionPolicySnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
            publication: Mutex::new(None),
        }
    }

    #[must_use]
    pub fn new_active(bundle: ActivePolicyBundle) -> Self {
        let metadata = PublishedPolicyBundle {
            generation: bundle.generation,
            decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            snapshot_hash: bundle.snapshot_hash,
        };
        Self {
            inner: ArcSwap::from_pointee(bundle.snapshot),
            publication: Mutex::new(Some(metadata)),
        }
    }

    /// Lock-free snapshot borrow for short, synchronous reads.
    #[must_use]
    #[inline]
    pub fn load(&self) -> Guard<Arc<DecisionPolicySnapshot>> {
        self.inner.load()
    }

    /// Owned snapshot for reads held across await points or task boundaries.
    #[must_use]
    #[inline]
    pub fn current(&self) -> Arc<DecisionPolicySnapshot> {
        self.inner.load_full()
    }

    #[must_use]
    pub fn current_bundle(&self) -> Option<PublishedPolicyBundle> {
        self.publication.lock().clone()
    }

    /// Install a new active snapshot (used by [`PolicySnapshotPort`] implementations).
    pub fn replace(&self, config: DecisionPolicySnapshot) {
        let mut publication = self.publication.lock();
        self.inner.store(Arc::new(config));
        *publication = None;
    }

    /// Swap the active snapshot. Crate-private: only the applicator writes.
    pub(crate) fn swap(&self, config: Arc<DecisionPolicySnapshot>) {
        self.inner.store(config);
    }

    pub(crate) fn publish_committed(
        &self,
        bundle: ActivePolicyBundle,
        before_store: impl FnOnce(&Arc<DecisionPolicySnapshot>),
    ) -> Result<PolicyBundlePublication, ControlError> {
        let mut current = self.publication.lock();
        if let Some(published) = current.as_ref() {
            if bundle.generation < published.generation {
                return Ok(PolicyBundlePublication::OlderIgnored);
            }
            if bundle.generation == published.generation {
                if bundle.decision_policy_snapshot_id == published.decision_policy_snapshot_id
                    && bundle.snapshot_hash == published.snapshot_hash
                {
                    return Ok(PolicyBundlePublication::AlreadyCurrent);
                }
                return Err(ControlError::Precondition(
                    "same policy bundle generation resolved to a different snapshot identity or hash"
                        .to_owned(),
                ));
            }
        }
        let snapshot = Arc::new(bundle.snapshot);
        before_store(&snapshot);
        self.inner.store(Arc::clone(&snapshot));
        *current = Some(PublishedPolicyBundle {
            generation: bundle.generation,
            decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            snapshot_hash: bundle.snapshot_hash,
        });
        drop(current);
        Ok(PolicyBundlePublication::Published)
    }
}

#[cfg(test)]
mod tests {
    use super::{DecisionPolicyStore, PolicyBundlePublication, PublishedPolicyBundle};
    use quant_pivot_models::{
        runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot},
        types::{DecisionPolicySnapshotId, PolicyBundleGeneration},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn bundle(generation: i64, book_age_increment: u64) -> ActivePolicyBundle {
        let mut snapshot = DecisionPolicySnapshot::default();
        snapshot.recommendation.data_quality.max_book_age_ms += book_age_increment;
        let snapshot_hash = snapshot
            .persistence_hash()
            .expect("hash test policy snapshot");
        ActivePolicyBundle::from_parts(
            PolicyBundleGeneration::try_new(generation).expect("positive test generation"),
            DecisionPolicySnapshotId::from_v7(),
            snapshot_hash,
            snapshot,
        )
    }

    fn metadata(bundle: &ActivePolicyBundle) -> PublishedPolicyBundle {
        PublishedPolicyBundle {
            generation: bundle.generation,
            decision_policy_snapshot_id: bundle.decision_policy_snapshot_id.clone(),
            snapshot_hash: bundle.snapshot_hash.clone(),
        }
    }

    #[test]
    fn publication_is_monotonic_idempotent_and_rejects_generation_forks() {
        let base = bundle(1, 0);
        let committed = bundle(2, 1);
        let store = DecisionPolicyStore::new_active(base.clone());
        let propagated = AtomicUsize::new(0);

        assert_eq!(
            store
                .publish_committed(committed.clone(), |_| {
                    propagated.fetch_add(1, Ordering::SeqCst);
                })
                .expect("publish committed generation"),
            PolicyBundlePublication::Published
        );
        assert_eq!(store.current_bundle(), Some(metadata(&committed)));
        assert_eq!(
            store.current().recommendation.data_quality.max_book_age_ms,
            committed
                .snapshot
                .recommendation
                .data_quality
                .max_book_age_ms
        );

        assert_eq!(
            store
                .publish_committed(committed.clone(), |_| {
                    propagated.fetch_add(1, Ordering::SeqCst);
                })
                .expect("replay exact committed generation"),
            PolicyBundlePublication::AlreadyCurrent
        );
        assert_eq!(
            store
                .publish_committed(base, |_| {
                    propagated.fetch_add(1, Ordering::SeqCst);
                })
                .expect("ignore older committed generation"),
            PolicyBundlePublication::OlderIgnored
        );
        assert_eq!(propagated.load(Ordering::SeqCst), 1);

        let fork = bundle(2, 2);
        let error = store
            .publish_committed(fork, |_| {
                propagated.fetch_add(1, Ordering::SeqCst);
            })
            .expect_err("same generation with different identity must fail closed");
        assert!(error.to_string().contains("same policy bundle generation"));
        assert_eq!(store.current_bundle(), Some(metadata(&committed)));
        assert_eq!(propagated.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn durable_bundle_recovers_publish_crash_restart_and_delayed_instances() {
        let base = bundle(1, 0);
        let committed = bundle(2, 1);

        // Instance A committed to the database and exited before publishing.
        // Restart bootstraps directly from the DB-authoritative committed bundle.
        let restarted = DecisionPolicyStore::new_active(committed.clone());
        assert_eq!(restarted.current_bundle(), Some(metadata(&committed)));

        // Two other processes still hold the previous generation. A later
        // reconciler read of the same durable bundle converges both stores.
        let delayed_one = DecisionPolicyStore::new_active(base.clone());
        let delayed_two = DecisionPolicyStore::new_active(base);
        assert_eq!(
            delayed_one
                .publish_committed(committed.clone(), |_| {})
                .expect("reconcile first delayed instance"),
            PolicyBundlePublication::Published
        );
        assert_eq!(
            delayed_two
                .publish_committed(committed.clone(), |_| {})
                .expect("reconcile second delayed instance"),
            PolicyBundlePublication::Published
        );
        assert_eq!(delayed_one.current_bundle(), Some(metadata(&committed)));
        assert_eq!(delayed_two.current_bundle(), Some(metadata(&committed)));
        assert_eq!(delayed_one.current(), delayed_two.current());
    }
}
