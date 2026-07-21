//! Runtime-config activation applicator — minimal propagation.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::ports::{PolicySnapshotPort, PreparedPolicySnapshot},
    runtime_config::DecisionPolicySnapshot,
};

use super::store::{DecisionPolicyStore, PublishedPolicyBundle};
use crate::{
    execution::breaker::ExecutionBreaker,
    governance::{BiasTableApplicator, CategoryPointerGuard, WeightOverlayApplicator},
    ingest::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter,
    },
};

#[derive(Clone)]
pub struct PolicySnapshotSubscribers {
    pub market_filter: Arc<MarketFilter>,
    pub market_cache: Arc<MarketCache>,
    pub data_quality: Arc<BookDataQualityService>,
    /// Candidate / shadow factor-weight overlay snapshot.
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Favorite-longshot bias-table snapshot bound to the factor plane.
    /// Reloaded (and content-hash verified) on activation; a bad ref fails the
    /// activation closed.
    pub bias_table: Arc<BiasTableApplicator>,
    /// Config-apply-time validator for `model.category_model_pointers`: a
    /// dangling or mis-scoped pointer fails the activation
    /// closed rather than surfacing only as a runtime fallback.
    pub category_pointer_guard: Arc<CategoryPointerGuard>,
}

pub struct PolicySnapshotApplicator {
    store: Arc<DecisionPolicyStore>,
    subscribers: PolicySnapshotSubscribers,
    /// Execution breaker, late-bound after the execution bundle is assembled
    /// (the breaker is built after governance). `None` until
    /// [`Self::attach_execution_breaker`] is called. Activations hot-swap its
    /// venue-health / daily-loss thresholds without a restart.
    execution_breaker: Mutex<Option<Arc<ExecutionBreaker>>>,
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
        }
    }

    /// Bind the execution breaker so activations hot-reload its thresholds.
    pub fn attach_execution_breaker(&self, breaker: Arc<ExecutionBreaker>) {
        *self.execution_breaker.lock() = Some(breaker);
    }

    #[must_use]
    pub fn current_bundle(&self) -> Option<PublishedPolicyBundle> {
        self.store.current_bundle()
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
        subs.weight_overlay.reload(
            &config.profile_artifacts.scoring.definition,
            &config.model_routing.model,
        );
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

        // Validate every category pointer before mutating the live snapshot:
        // a dangling or mis-scoped pointer must fail the activation closed,
        // never rely solely on the router's runtime fallback.
        self.subscribers
            .category_pointer_guard
            .validate(&arc.model_routing.model)
            .await?;

        let breaker = self.execution_breaker.lock().clone();
        let breaker_thresholds = breaker
            .as_ref()
            .map(|_| ExecutionBreaker::prepare_reload(&arc.execution_risk.breaker))
            .transpose()
            .map_err(ControlError::from)?;
        let store = Arc::clone(&self.store);
        let subscribers = self.subscribers.clone();
        let publish_config = Arc::clone(&arc);
        Ok(PreparedPolicySnapshot::new_governed(
            arc,
            move |committed_bundle| {
                let publish_dependencies = |config: &Arc<DecisionPolicySnapshot>| {
                    subscribers.bias_table.publish(bias_table);
                    if let (Some(breaker), Some(thresholds)) = (breaker, breaker_thresholds) {
                        breaker.publish_reload(thresholds);
                    }
                    Self::propagate(&subscribers, config);
                };
                if let Some(bundle) = committed_bundle {
                    store
                        .publish_committed(bundle, publish_dependencies)
                        .map(|_outcome| ())
                } else {
                    publish_dependencies(&publish_config);
                    store.swap(publish_config);
                    Ok(())
                }
            },
        ))
    }
}
