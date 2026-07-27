//! Runtime-config activation applicator with prepared, governed publication.

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

    #[must_use]
    pub(crate) fn current_bundle(&self) -> Option<PublishedPolicyBundle> {
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
                let serving_bundle = PublishedPolicyBundle {
                    generation: bundle.generation,
                    decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
                    snapshot_hash: bundle.snapshot_hash,
                };
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
