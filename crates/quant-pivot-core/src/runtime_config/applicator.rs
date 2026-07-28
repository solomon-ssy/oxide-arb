//! Runtime-config activation applicator with prepared, governed publication.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use quant_pivot_error::control::{ControlError, RuntimeApplyStage};
use quant_pivot_models::{
    domain::ports::{CommittedPolicyApplyPort, PolicySnapshotPort, PreparedPolicySnapshot},
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, PolicyApplyDegradedCause, PolicyApplyReadiness,
        PolicyBundleIdentity,
    },
    types::DecisionPolicySnapshotId,
};

use super::store::DecisionPolicyStore;
use crate::{
    execution::breaker::ExecutionBreaker,
    governance::BiasTableApplicator,
    ingest::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter,
    },
    service::model_serving_generation::ModelServingGenerationStore,
};

#[derive(Clone)]
pub struct PolicySnapshotSubscribers {
    pub market_filter: Arc<MarketFilter>,
    pub market_cache: Arc<MarketCache>,
    pub data_quality: Arc<BookDataQualityService>,
    /// Favorite-longshot bias-table snapshot bound to the factor plane.
    /// Reloaded (and content-hash verified) on activation; a bad ref fails the
    /// activation closed.
    pub bias_table: Arc<BiasTableApplicator>,
}

enum ApplyStart {
    Proceed,
    AlreadyApplied(PolicyApplyReadiness),
}

/// Sole process-local owner of durable-bundle convergence and readiness.
pub struct CommittedPolicyApplicator {
    target: Arc<dyn PolicySnapshotPort>,
    state: Mutex<PolicyApplyReadiness>,
}

impl CommittedPolicyApplicator {
    #[must_use]
    pub fn new(target: Arc<dyn PolicySnapshotPort>, initial: PolicyBundleIdentity) -> Self {
        Self {
            target,
            state: Mutex::new(PolicyApplyReadiness::Ready { applied: initial }),
        }
    }

    fn degraded_error(
        desired: PolicyBundleIdentity,
        applied: PolicyBundleIdentity,
        stage: RuntimeApplyStage,
        detail: impl Into<String>,
    ) -> ControlError {
        ControlError::CommittedGenerationApply {
            desired_generation: desired.generation.get(),
            applied_generation: applied.generation.get(),
            stage,
            detail: detail.into(),
        }
    }

    fn begin_apply(&self, desired: PolicyBundleIdentity) -> Result<ApplyStart, ControlError> {
        let mut state = self.state.lock();
        let tracked_desired = state.desired();
        let applied = state.applied();
        if desired.generation < tracked_desired.generation {
            return Err(Self::degraded_error(
                desired,
                applied,
                RuntimeApplyStage::GenerationMismatch,
                format!(
                    "stale committed bundle is older than tracked desired generation {}",
                    tracked_desired.generation
                ),
            ));
        }
        if desired.generation == tracked_desired.generation && desired != tracked_desired {
            *state = PolicyApplyReadiness::Degraded {
                desired,
                applied,
                cause: PolicyApplyDegradedCause::GenerationMismatch,
            };
            return Err(Self::degraded_error(
                desired,
                applied,
                RuntimeApplyStage::GenerationMismatch,
                "same generation resolved to a different snapshot identity or hash",
            ));
        }
        if desired == applied && state.is_ready() {
            return Ok(ApplyStart::AlreadyApplied(*state));
        }
        *state = PolicyApplyReadiness::Degraded {
            desired,
            applied,
            cause: PolicyApplyDegradedCause::Applying,
        };
        drop(state);
        Ok(ApplyStart::Proceed)
    }

    fn record_failure(
        &self,
        desired: PolicyBundleIdentity,
        cause: PolicyApplyDegradedCause,
    ) -> PolicyBundleIdentity {
        let mut state = self.state.lock();
        let applied = state.applied();
        if state.desired() == desired {
            *state = PolicyApplyReadiness::Degraded {
                desired,
                applied,
                cause,
            };
        }
        applied
    }

    fn record_success(&self, applied: PolicyBundleIdentity) -> PolicyApplyReadiness {
        let mut state = self.state.lock();
        if state.desired() == applied {
            *state = PolicyApplyReadiness::Ready { applied };
        } else if applied.generation > state.applied().generation
            && let PolicyApplyReadiness::Degraded { desired, cause, .. } = *state
        {
            *state = PolicyApplyReadiness::Degraded {
                desired,
                applied,
                cause,
            };
        }
        *state
    }

    fn validate_bundle(bundle: &ActivePolicyBundle) -> Result<(), String> {
        let actual_hash = bundle
            .snapshot
            .persistence_hash()
            .map_err(|error| error.to_string())?;
        if actual_hash != bundle.snapshot_hash {
            return Err(format!(
                "committed snapshot hash {actual_hash} differs from bundle hash {}",
                bundle.snapshot_hash
            ));
        }
        let expected_id = DecisionPolicySnapshotId::from_content_hash(&actual_hash);
        if expected_id != bundle.decision_policy_snapshot_id {
            return Err(format!(
                "committed snapshot identity {} differs from content-addressed identity \
                 {expected_id}",
                bundle.decision_policy_snapshot_id
            ));
        }
        Ok(())
    }

    fn publish_started(
        &self,
        prepared: PreparedPolicySnapshot,
        bundle: ActivePolicyBundle,
        desired: PolicyBundleIdentity,
    ) -> Result<PolicyApplyReadiness, ControlError> {
        if let Err(detail) = Self::validate_bundle(&bundle) {
            let applied =
                self.record_failure(desired, PolicyApplyDegradedCause::GenerationMismatch);
            return Err(Self::degraded_error(
                desired,
                applied,
                RuntimeApplyStage::GenerationMismatch,
                detail,
            ));
        }
        if let Err(error) = prepared.publish_bundle(bundle) {
            let applied = self.record_failure(desired, PolicyApplyDegradedCause::PublishFailed);
            return Err(Self::degraded_error(
                desired,
                applied,
                RuntimeApplyStage::Publish,
                error.to_string(),
            ));
        }
        Ok(self.record_success(desired))
    }
}

#[async_trait]
impl CommittedPolicyApplyPort for CommittedPolicyApplicator {
    async fn apply_committed(
        &self,
        bundle: ActivePolicyBundle,
    ) -> Result<PolicyApplyReadiness, ControlError> {
        let desired = PolicyBundleIdentity::from(&bundle);
        match self.begin_apply(desired)? {
            ApplyStart::AlreadyApplied(readiness) => return Ok(readiness),
            ApplyStart::Proceed => {}
        }
        if let Err(detail) = Self::validate_bundle(&bundle) {
            let applied =
                self.record_failure(desired, PolicyApplyDegradedCause::GenerationMismatch);
            return Err(Self::degraded_error(
                desired,
                applied,
                RuntimeApplyStage::GenerationMismatch,
                detail,
            ));
        }
        let prepared = match self.target.prepare(bundle.snapshot.clone()).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let applied = self.record_failure(desired, PolicyApplyDegradedCause::PrepareFailed);
                return Err(Self::degraded_error(
                    desired,
                    applied,
                    RuntimeApplyStage::Prepare,
                    error.to_string(),
                ));
            }
        };
        self.publish_started(prepared, bundle, desired)
    }

    fn publish_prepared(
        &self,
        prepared: PreparedPolicySnapshot,
        bundle: ActivePolicyBundle,
    ) -> Result<PolicyApplyReadiness, ControlError> {
        let desired = PolicyBundleIdentity::from(&bundle);
        match self.begin_apply(desired)? {
            ApplyStart::AlreadyApplied(readiness) => Ok(readiness),
            ApplyStart::Proceed => self.publish_started(prepared, bundle, desired),
        }
    }

    fn readiness(&self) -> PolicyApplyReadiness {
        *self.state.lock()
    }
}

pub struct PolicySnapshotApplicator {
    store: Arc<DecisionPolicyStore>,
    subscribers: PolicySnapshotSubscribers,
    /// Execution breaker, late-bound after the execution bundle is assembled
    /// (the breaker is built after governance). `None` until
    /// [`Self::attach_execution_breaker`] is called. Activations hot-swap its
    /// venue-health / daily-loss thresholds without a restart.
    execution_breaker: Mutex<Option<Arc<ExecutionBreaker>>>,
    /// Complete active/shadow/category generation owner. It is assembled after
    /// the research-plane registry, then attached before the reconciler starts.
    model_serving: Mutex<Option<Arc<ModelServingGenerationStore>>>,
}

impl PolicySnapshotApplicator {
    #[must_use]
    pub const fn new(
        store: Arc<DecisionPolicyStore>,
        subscribers: PolicySnapshotSubscribers,
    ) -> Self {
        Self {
            store,
            subscribers,
            execution_breaker: Mutex::new(None),
            model_serving: Mutex::new(None),
        }
    }

    /// Bind the execution breaker so activations hot-reload its thresholds.
    pub fn attach_execution_breaker(&self, breaker: Arc<ExecutionBreaker>) {
        *self.execution_breaker.lock() = Some(breaker);
    }

    /// Bind the sole atomic serving-generation owner before policy
    /// reconciliation begins.
    ///
    /// # Errors
    ///
    /// Rejects a second owner instead of silently replacing the live
    /// publication target.
    pub fn attach_model_serving(
        &self,
        generations: Arc<ModelServingGenerationStore>,
    ) -> Result<(), ControlError> {
        let mut current = self.model_serving.lock();
        if current.is_some() {
            return Err(ControlError::Precondition(
                "model serving generation owner is already attached".to_owned(),
            ));
        }
        *current = Some(generations);
        drop(current);
        Ok(())
    }

    fn preflight_internal(candidate: &DecisionPolicySnapshot) -> Result<(), ControlError> {
        if !candidate.uses_current_resource_schemas() {
            return Err(ControlError::Precondition(
                "unsupported runtime config schema version".to_owned(),
            ));
        }
        Ok(())
    }

    fn propagate(subs: &PolicySnapshotSubscribers, config: &Arc<DecisionPolicySnapshot>) {
        subs.market_filter
            .reload(&config.recommendation.selection.enabled_categories);
        subs.market_cache.rebuild();
        subs.data_quality
            .reload(&config.recommendation.data_quality);
    }
}

#[async_trait]
impl PolicySnapshotPort for PolicySnapshotApplicator {
    fn current(&self) -> Arc<DecisionPolicySnapshot> {
        self.store.current()
    }

    async fn prepare(
        &self,
        config: DecisionPolicySnapshot,
    ) -> Result<PreparedPolicySnapshot, ControlError> {
        Self::preflight_internal(&config)?;
        let arc = Arc::new(config);
        let generations = self.model_serving.lock().clone().ok_or_else(|| {
            ControlError::Precondition("model serving generation owner is not attached".to_owned())
        })?;
        let bias_table = self
            .subscribers
            .bias_table
            .prepare(
                &arc.profile_artifacts
                    .scoring
                    .definition
                    .structural
                    .favorite_longshot,
            )
            .await?;

        let serving_generation = generations.prepare(&arc).await?;

        let breaker = self.execution_breaker.lock().clone();
        let breaker_thresholds = breaker
            .as_ref()
            .map(|_| ExecutionBreaker::prepare_reload(&arc.execution_risk.breaker))
            .transpose()
            .map_err(ControlError::from)?;
        let store = Arc::clone(&self.store);
        let subscribers = self.subscribers.clone();
        Ok(PreparedPolicySnapshot::new_governed(
            arc,
            move |committed_bundle| {
                let bundle = committed_bundle.ok_or_else(|| {
                    ControlError::Precondition(
                        "runtime policy publication requires a durable committed bundle".to_owned(),
                    )
                })?;
                let serving_bundle = PolicyBundleIdentity::from(&bundle);
                store
                    .publish_committed(bundle, move |config| {
                        generations.publish_committed(serving_generation, &serving_bundle)?;
                        subscribers.bias_table.publish(bias_table);
                        if let (Some(breaker), Some(thresholds)) = (breaker, breaker_thresholds) {
                            breaker.publish_reload(thresholds);
                        }
                        Self::propagate(&subscribers, config);
                        Ok(())
                    })
                    .map(|_outcome| ())
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use quant_pivot_error::control::{ControlError, RuntimeApplyStage};
    use quant_pivot_models::{
        domain::ports::{CommittedPolicyApplyPort, PolicySnapshotPort, PreparedPolicySnapshot},
        runtime_config::{
            ActivePolicyBundle, DecisionPolicySnapshot, PolicyApplyDegradedCause,
            PolicyApplyReadiness, PolicyBundleIdentity,
        },
        types::{DecisionPolicySnapshotId, PolicyBundleGeneration},
    };

    use super::CommittedPolicyApplicator;

    struct ScriptedPolicyPort {
        fail_prepare: AtomicBool,
        fail_publish: Arc<AtomicBool>,
        applied: Arc<Mutex<PolicyBundleIdentity>>,
    }

    impl ScriptedPolicyPort {
        fn new(initial: PolicyBundleIdentity) -> Self {
            Self {
                fail_prepare: AtomicBool::new(false),
                fail_publish: Arc::new(AtomicBool::new(false)),
                applied: Arc::new(Mutex::new(initial)),
            }
        }

        fn reject_prepare(&self, reject: bool) {
            self.fail_prepare.store(reject, Ordering::SeqCst);
        }

        fn reject_publish(&self, reject: bool) {
            self.fail_publish.store(reject, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl PolicySnapshotPort for ScriptedPolicyPort {
        fn current(&self) -> Arc<DecisionPolicySnapshot> {
            Arc::new(DecisionPolicySnapshot::default())
        }

        async fn prepare(
            &self,
            config: DecisionPolicySnapshot,
        ) -> Result<PreparedPolicySnapshot, ControlError> {
            if self.fail_prepare.load(Ordering::SeqCst) {
                return Err(ControlError::Precondition(
                    "injected committed-generation prepare failure".to_owned(),
                ));
            }
            let fail_publish = Arc::clone(&self.fail_publish);
            let applied = Arc::clone(&self.applied);
            Ok(PreparedPolicySnapshot::new_governed(
                Arc::new(config),
                move |bundle| {
                    if fail_publish.load(Ordering::SeqCst) {
                        return Err(ControlError::Precondition(
                            "injected committed-generation publish failure".to_owned(),
                        ));
                    }
                    let bundle = bundle.ok_or_else(|| {
                        ControlError::Precondition(
                            "test publication requires a committed bundle".to_owned(),
                        )
                    })?;
                    *applied.lock() = PolicyBundleIdentity::from(&bundle);
                    Ok(())
                },
            ))
        }
    }

    fn bundle(generation: i64, age_increment: u64) -> ActivePolicyBundle {
        let mut snapshot = DecisionPolicySnapshot::default();
        snapshot.recommendation.data_quality.max_book_age_ms += age_increment;
        let snapshot_hash = snapshot.persistence_hash().expect("hash test policy");
        ActivePolicyBundle::from_parts(
            PolicyBundleGeneration::try_new(generation).expect("positive test generation"),
            DecisionPolicySnapshotId::from_content_hash(&snapshot_hash),
            snapshot_hash,
            snapshot,
        )
    }

    #[tokio::test]
    async fn apply_failure_retains_old() {
        let old = bundle(1, 0);
        let next = bundle(2, 1);
        let initial = PolicyBundleIdentity::from(&old);
        let raw = Arc::new(ScriptedPolicyPort::new(initial));
        let applicator = CommittedPolicyApplicator::new(
            Arc::clone(&raw) as Arc<dyn PolicySnapshotPort>,
            initial,
        );

        raw.reject_prepare(true);
        let error = applicator
            .apply_committed(next.clone())
            .await
            .expect_err("prepare failure must degrade committed apply");
        assert!(matches!(
            error,
            ControlError::CommittedGenerationApply {
                stage: RuntimeApplyStage::Prepare,
                ..
            }
        ));
        assert_eq!(
            applicator.readiness(),
            PolicyApplyReadiness::Degraded {
                desired: PolicyBundleIdentity::from(&next),
                applied: initial,
                cause: PolicyApplyDegradedCause::PrepareFailed,
            }
        );
        assert_eq!(*raw.applied.lock(), initial);

        raw.reject_prepare(false);
        raw.reject_publish(true);
        let error = applicator
            .apply_committed(next.clone())
            .await
            .expect_err("publish failure must degrade committed apply");
        assert!(matches!(
            error,
            ControlError::CommittedGenerationApply {
                stage: RuntimeApplyStage::Publish,
                ..
            }
        ));
        assert_eq!(*raw.applied.lock(), initial);

        raw.reject_publish(false);
        assert_eq!(
            applicator
                .apply_committed(next.clone())
                .await
                .expect("exact committed retry must converge"),
            PolicyApplyReadiness::Ready {
                applied: PolicyBundleIdentity::from(&next),
            }
        );
        assert_eq!(*raw.applied.lock(), PolicyBundleIdentity::from(&next));
    }

    #[tokio::test]
    async fn generation_fork_fails_closed() {
        let old = bundle(1, 0);
        let committed = bundle(2, 1);
        let fork = bundle(2, 2);
        let initial = PolicyBundleIdentity::from(&old);
        let raw = Arc::new(ScriptedPolicyPort::new(initial));
        let applicator = CommittedPolicyApplicator::new(
            Arc::clone(&raw) as Arc<dyn PolicySnapshotPort>,
            initial,
        );

        applicator
            .apply_committed(committed.clone())
            .await
            .expect("first committed successor");
        let error = applicator
            .apply_committed(fork.clone())
            .await
            .expect_err("same-generation fork must fail closed");
        assert!(matches!(
            error,
            ControlError::CommittedGenerationApply {
                stage: RuntimeApplyStage::GenerationMismatch,
                ..
            }
        ));
        assert_eq!(
            applicator.readiness(),
            PolicyApplyReadiness::Degraded {
                desired: PolicyBundleIdentity::from(&fork),
                applied: PolicyBundleIdentity::from(&committed),
                cause: PolicyApplyDegradedCause::GenerationMismatch,
            }
        );
        assert_eq!(*raw.applied.lock(), PolicyBundleIdentity::from(&committed));
    }
}
