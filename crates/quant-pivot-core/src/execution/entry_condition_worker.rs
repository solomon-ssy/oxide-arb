//! Durable recommendation-level entry-condition evaluation worker.

use std::{future::pending, sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::{ChSchemaVersion, EntryConditionEvaluationEventRow},
    domain::{
        ApplyEntryConditionEvaluation, CoreEvent, CoreEventPublisher, EntryConditionInstanceInfo,
        EntryConditionLifecycleEvent,
    },
    runtime_config::EntryConditionWorkerConfig,
    types::{
        ConditionTruth, CryptoSubjectPredicateEntered, EntryConditionArtifactV1,
        EntryConditionSourceBinding, EntryConditionV1, FactorCondition, MarketEventCondition,
        MarketId, TokenId,
    },
};
use quant_pivot_repository::traits::{EntryConditionRepository, FactWriter, FactorRepository};
use quant_pivot_storage::postgres::PostgresNotificationListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    CryptoPriceInput, EntryConditionEvaluation, EntryConditionInputSet,
    EntryConditionStateDecision, ExecutablePriceInput, FactorSnapshotInput, WeatherDailyHighInput,
    decide_entry_condition_state, evaluate_entry_condition,
};
use crate::{ingest::book_store::BookStore, runtime_config::RuntimeConfigStore};

/// Supplies exactly the persisted/in-memory PIT projections consumed by the evaluator.
#[async_trait]
pub trait EntryConditionInputProvider: Send + Sync {
    async fn load(
        &self,
        artifact: &EntryConditionArtifactV1,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInputSet>;
}

/// Production input provider. Book prices come from the lock-free store; factor
/// and domain-event facts come from typed Postgres projections.
pub struct LiveEntryConditionInputProvider {
    books: Arc<BookStore>,
    conditions: Arc<dyn EntryConditionRepository>,
    factors: Arc<dyn FactorRepository>,
}

impl LiveEntryConditionInputProvider {
    #[must_use]
    pub const fn new(
        books: Arc<BookStore>,
        conditions: Arc<dyn EntryConditionRepository>,
        factors: Arc<dyn FactorRepository>,
    ) -> Self {
        Self {
            books,
            conditions,
            factors,
        }
    }

    fn load_prices(&self, token_ids: Vec<TokenId>) -> Vec<ExecutablePriceInput> {
        token_ids
            .into_iter()
            .filter_map(|token_id| {
                let snapshot = self.books.load(&token_id)?;
                let price = snapshot.best_ask()?;
                let timestamp_ms = i64::try_from(snapshot.timestamp_ms).ok()?;
                let observed_at = DateTime::from_timestamp_millis(timestamp_ms)?;
                Some(ExecutablePriceInput {
                    token_id,
                    price,
                    observed_at,
                    available_at: observed_at,
                    gap_generation: self.books.gap_generation(),
                })
            })
            .collect()
    }

    async fn load_factors(
        &self,
        market_id: &MarketId,
        required: Vec<FactorCondition>,
    ) -> QuantResult<Vec<FactorSnapshotInput>> {
        let mut groups = Vec::new();
        for condition in &required {
            let group = groups
                .iter_mut()
                .find(|(model_version_id, _)| model_version_id == &condition.model_version_id);
            if let Some((_, definition_ids)) = group {
                push_unique(definition_ids, condition.definition_id.clone());
            } else {
                groups.push((
                    condition.model_version_id.clone(),
                    vec![condition.definition_id.clone()],
                ));
            }
        }
        let mut inputs = Vec::with_capacity(required.len());
        for (model_version_id, definition_ids) in groups {
            let Some(snapshot) = self
                .factors
                .latest_snapshot_bundle(&definition_ids, market_id, &model_version_id)
                .await?
            else {
                continue;
            };
            for value in snapshot.values {
                let (Some(raw_value), Some(normalized_value)) =
                    (value.raw_value, value.normalized_score)
                else {
                    continue;
                };
                inputs.push(FactorSnapshotInput {
                    definition_id: value.factor_definition_id,
                    definition_hash: value.definition_hash,
                    model_version_id: model_version_id.clone(),
                    raw_value,
                    normalized_value: normalized_value.inner(),
                    confidence: value.confidence.inner(),
                    observed_at: snapshot.observed_at,
                    available_at: snapshot.available_at,
                    snapshot_hash: snapshot.snapshot_hash.clone(),
                });
            }
        }
        Ok(inputs)
    }

    async fn load_crypto(
        &self,
        required: Vec<CryptoSubjectPredicateEntered>,
    ) -> QuantResult<Vec<CryptoPriceInput>> {
        let mut inputs = Vec::with_capacity(required.len());
        for condition in required {
            let Some(projection) = self
                .conditions
                .find_crypto_projection(
                    &condition.source.source_id,
                    &condition.source.instrument_key,
                )
                .await?
            else {
                continue;
            };
            let (Some(previous_price), Ok(gap_generation)) = (
                projection.previous_price,
                u64::try_from(projection.gap_generation),
            ) else {
                continue;
            };
            inputs.push(CryptoPriceInput {
                source: condition.source,
                previous_price,
                current_price: projection.current_price,
                source_sequence: projection.source_sequence,
                transition_at: projection.event_time,
                available_at: projection.available_at,
                report_hash: projection.report_hash,
                gap_generation,
                source_healthy: projection.source_healthy,
            });
        }
        Ok(inputs)
    }

    async fn load_weather(
        &self,
        required: Vec<RequiredWeather>,
    ) -> QuantResult<Vec<WeatherDailyHighInput>> {
        let mut inputs = Vec::with_capacity(required.len());
        for condition in required {
            let Some(projection) = self
                .conditions
                .find_weather_projection(
                    &condition.source.source_id,
                    &condition.source.instrument_key,
                    &condition.station,
                    condition.local_date,
                )
                .await?
            else {
                continue;
            };
            let (Ok(revision), Ok(gap_generation)) = (
                u64::try_from(projection.revision),
                u64::try_from(projection.gap_generation),
            ) else {
                continue;
            };
            inputs.push(WeatherDailyHighInput {
                source: EntryConditionSourceBinding {
                    source_id: projection.source_id,
                    instrument_key: projection.instrument_key,
                    binding_hash: condition.source.binding_hash,
                },
                station: projection.station,
                local_date: projection.local_date,
                current_high: projection.current_high,
                observation_time: projection.last_observation_time,
                available_at: projection.available_at,
                revision,
                day_closed: projection.day_closed,
                report_hash: projection.last_report_hash,
                gap_generation,
                source_healthy: projection.source_healthy,
            });
        }
        Ok(inputs)
    }
}

#[async_trait]
impl EntryConditionInputProvider for LiveEntryConditionInputProvider {
    async fn load(
        &self,
        artifact: &EntryConditionArtifactV1,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInputSet> {
        let mut required = RequiredInputs::default();
        required.collect(&artifact.root);
        let prices = self.load_prices(required.prices);
        let factors = self
            .load_factors(&artifact.binding.market_id, required.factors)
            .await?;
        let crypto = self.load_crypto(required.crypto).await?;
        let weather = self.load_weather(required.weather).await?;

        Ok(EntryConditionInputSet {
            binding: artifact.binding.clone(),
            evaluated_at,
            prices,
            factors,
            crypto,
            weather,
        })
    }
}

#[derive(Default)]
struct RequiredInputs {
    prices: Vec<TokenId>,
    factors: Vec<FactorCondition>,
    crypto: Vec<CryptoSubjectPredicateEntered>,
    weather: Vec<RequiredWeather>,
}

#[derive(Clone, PartialEq, Eq)]
struct RequiredWeather {
    source: EntryConditionSourceBinding,
    station: String,
    local_date: chrono::NaiveDate,
}

impl RequiredInputs {
    fn collect(&mut self, condition: &EntryConditionV1) {
        match condition {
            EntryConditionV1::Price(condition) => {
                push_unique(&mut self.prices, condition.token_id.clone());
            }
            EntryConditionV1::Clock(_) => {}
            EntryConditionV1::Factor(condition) => {
                push_unique(&mut self.factors, condition.clone());
            }
            EntryConditionV1::MarketEvent { event: condition } => match condition {
                MarketEventCondition::CryptoSubjectPredicateEntered(condition) => {
                    push_unique(&mut self.crypto, condition.clone());
                }
                MarketEventCondition::WeatherDailyHighEnteredBand(condition) => {
                    push_unique(
                        &mut self.weather,
                        RequiredWeather {
                            source: condition.source.clone(),
                            station: condition.station.clone(),
                            local_date: condition.local_date,
                        },
                    );
                }
                MarketEventCondition::WeatherDailyHighExceededBandUpper(condition) => {
                    push_unique(
                        &mut self.weather,
                        RequiredWeather {
                            source: condition.source.clone(),
                            station: condition.station.clone(),
                            local_date: condition.local_date,
                        },
                    );
                }
                MarketEventCondition::WeatherObservationDayClosedOutsideBand(condition) => {
                    push_unique(
                        &mut self.weather,
                        RequiredWeather {
                            source: condition.source.clone(),
                            station: condition.station.clone(),
                            local_date: condition.local_date,
                        },
                    );
                }
            },
            EntryConditionV1::All { children } | EntryConditionV1::Any { children } => {
                for child in children {
                    self.collect(child);
                }
            }
        }
    }
}

fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}

/// Multi-replica-safe durable worker. Postgres lease/CAS is authoritative; all
/// process-local wakes are latency hints only.
pub struct EntryConditionWorker {
    worker_id: Uuid,
    conditions: Arc<dyn EntryConditionRepository>,
    inputs: Arc<dyn EntryConditionInputProvider>,
    evaluations: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>>,
    books: Arc<BookStore>,
    runtime_config: Arc<RuntimeConfigStore>,
    events: CoreEventPublisher,
}

impl EntryConditionWorker {
    #[must_use]
    pub fn new(
        conditions: Arc<dyn EntryConditionRepository>,
        inputs: Arc<dyn EntryConditionInputProvider>,
        evaluations: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>>,
        books: Arc<BookStore>,
        runtime_config: Arc<RuntimeConfigStore>,
        events: CoreEventPublisher,
    ) -> Self {
        Self {
            worker_id: Uuid::now_v7(),
            conditions,
            inputs,
            evaluations,
            books,
            runtime_config,
            events,
        }
    }

    pub async fn run(
        &self,
        shutdown: CancellationToken,
        mut notifications: Option<PostgresNotificationListener>,
    ) {
        loop {
            let policy = self
                .runtime_config
                .current()
                .execution
                .entry_condition
                .clone();
            if let Err(error) = self.run_pass(Utc::now(), &policy).await {
                tracing::warn!(%error, "entry-condition evaluation pass failed");
            }
            let now = Utc::now();
            let mut wake_after = StdDuration::from_millis(policy.backstop_interval_ms);
            match self.conditions.next_wakeup_at(now).await {
                Ok(Some(deadline)) => {
                    let until_deadline = deadline
                        .signed_duration_since(now)
                        .to_std()
                        .unwrap_or(StdDuration::ZERO);
                    wake_after = wake_after.min(until_deadline);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "entry-condition deadline lookup failed; using backstop");
                }
            }
            let backstop = tokio::time::sleep(wake_after);
            tokio::pin!(backstop);
            let listener_failed = {
                let notification = async {
                    match notifications.as_mut() {
                        Some(listener) => listener.recv().await,
                        None => pending().await,
                    }
                };
                tokio::pin!(notification);
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => break,
                    () = self.books.wait_for_update() => false,
                    result = &mut notification => result.is_err(),
                    () = &mut backstop => false,
                }
            };
            if listener_failed {
                tracing::warn!("entry-condition PostgreSQL wake listener failed; using backstop");
                notifications = None;
            }
        }
    }

    async fn run_pass(
        &self,
        now: DateTime<Utc>,
        policy: &EntryConditionWorkerConfig,
    ) -> QuantResult<()> {
        for instance in self
            .conditions
            .expire_due(now, policy.expiry_batch_limit)
            .await?
        {
            self.publish(&instance);
        }
        let lease_duration = chrono_duration_secs(policy.lease_duration_secs, "lease duration")?;
        for _ in 0..policy.pass_limit {
            let evaluated_at = Utc::now();
            let leased = self
                .conditions
                .lease_next(self.worker_id, evaluated_at, evaluated_at + lease_duration)
                .await?;
            let Some(instance) = leased else {
                break;
            };
            match self.evaluate_leased(instance).await {
                Ok(updated) => self.publish(&updated),
                Err(error) => {
                    tracing::warn!(%error, "leased entry condition evaluation failed");
                }
            }
        }
        Ok(())
    }

    async fn evaluate_leased(
        &self,
        instance: EntryConditionInstanceInfo,
    ) -> QuantResult<EntryConditionInstanceInfo> {
        let policy = self
            .runtime_config
            .current()
            .execution
            .entry_condition
            .clone();
        let stop_renewal = CancellationToken::new();
        let renewal = self.spawn_lease_renewal(&instance, stop_renewal.clone(), &policy)?;
        let result = self.evaluate_leased_inner(instance, &policy).await;
        stop_renewal.cancel();
        if let Err(error) = renewal.await {
            tracing::warn!(%error, "entry-condition lease renewal task failed");
        }
        result
    }

    async fn evaluate_leased_inner(
        &self,
        instance: EntryConditionInstanceInfo,
        policy: &EntryConditionWorkerConfig,
    ) -> QuantResult<EntryConditionInstanceInfo> {
        let evaluated_at = Utc::now();
        let Some(artifact_id) = instance.artifact_id.as_ref() else {
            return self
                .invalidate(
                    instance,
                    "conditional instance has no artifact id",
                    evaluated_at,
                )
                .await;
        };
        let Some(expected_hash) = instance.artifact_hash.as_ref() else {
            return self
                .invalidate(
                    instance,
                    "conditional instance has no artifact hash",
                    evaluated_at,
                )
                .await;
        };
        let Some(stored) = self.conditions.find_artifact(artifact_id).await? else {
            return self
                .invalidate(instance, "condition artifact is missing", evaluated_at)
                .await;
        };
        let artifact = match stored.payload_json.canonicalize() {
            Ok(artifact) => artifact,
            Err(error) => {
                return self
                    .invalidate(
                        instance,
                        format!("condition artifact is invalid: {error}"),
                        evaluated_at,
                    )
                    .await;
            }
        };
        let actual_hash = match artifact.canonical_content_hash() {
            Ok(hash) => hash,
            Err(error) => {
                return self
                    .invalidate(
                        instance,
                        format!("condition artifact cannot be hashed: {error}"),
                        evaluated_at,
                    )
                    .await;
            }
        };
        if &actual_hash != expected_hash || stored.content_hash != actual_hash {
            return self
                .invalidate(instance, "condition artifact hash mismatch", evaluated_at)
                .await;
        }

        let input = self.inputs.load(&artifact, evaluated_at).await?;
        let evaluation = evaluate_entry_condition(&artifact, &input)?;
        let decision = decide_entry_condition_state(
            instance.state,
            instance.confirmation_started_at,
            instance.continuity_hash.as_ref(),
            instance.last_evaluated_at,
            &artifact,
            &evaluation,
            evaluated_at,
        );
        self.persist_evaluation(
            &instance,
            policy,
            &artifact,
            evaluation,
            decision,
            evaluated_at,
        )
        .await
    }

    async fn persist_evaluation(
        &self,
        instance: &EntryConditionInstanceInfo,
        policy: &EntryConditionWorkerConfig,
        artifact: &EntryConditionArtifactV1,
        evaluation: EntryConditionEvaluation,
        decision: EntryConditionStateDecision,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInstanceInfo> {
        let revision =
            instance
                .revision
                .checked_add(1)
                .ok_or_else(|| ReportError::ContractViolation {
                    detail: "entry condition revision overflow".to_owned(),
                })?;
        let tree_json = serde_json::to_string(&evaluation.tree).map_err(|error| {
            ReportError::ContractViolation {
                detail: format!("entry condition evaluation serialization failed: {error}"),
            }
        })?;
        self.evaluations
            .write_batch(vec![EntryConditionEvaluationEventRow {
                condition_instance_id: instance.condition_instance_id.clone(),
                revision,
                evaluator_version: artifact.evaluator_version,
                evaluated_at: evaluated_at.timestamp_millis(),
                state: decision.state.as_str().to_owned(),
                truth: truth_label(&evaluation.truth).to_owned(),
                evaluation_hash: evaluation.evaluation_hash.clone(),
                input_fingerprint: evaluation.input_fingerprint.clone(),
                tree_json,
                schema_version: ChSchemaVersion::FIRST,
            }])
            .await?;
        let next_delay =
            chrono_duration_millis(policy.next_evaluation_delay_ms, "next evaluation delay")?;
        let periodic_at = evaluated_at + next_delay;
        let next_evaluation_at = earliest_future_clock_deadline(&artifact.root, evaluated_at)
            .map_or(periodic_at, |deadline| deadline.min(periodic_at));
        self.conditions
            .apply_evaluation(
                &instance.condition_instance_id,
                self.worker_id,
                ApplyEntryConditionEvaluation {
                    expected_revision: instance.revision,
                    expected_lease_epoch: instance.lease_epoch,
                    state: decision.state,
                    truth: evaluation.truth,
                    evaluation_hash: evaluation.evaluation_hash,
                    input_fingerprint: evaluation.input_fingerprint,
                    continuity_hash: evaluation.continuity_hash,
                    confirmation_started_at: decision.confirmation_started_at,
                    evaluated_at,
                    next_evaluation_at: Some(next_evaluation_at),
                },
            )
            .await
            .map_err(QuantError::from)
    }

    fn spawn_lease_renewal(
        &self,
        instance: &EntryConditionInstanceInfo,
        stop: CancellationToken,
        policy: &EntryConditionWorkerConfig,
    ) -> QuantResult<tokio::task::JoinHandle<()>> {
        let conditions = Arc::clone(&self.conditions);
        let instance_id = instance.condition_instance_id.clone();
        let worker_id = self.worker_id;
        let lease_epoch = instance.lease_epoch;
        let renew_interval = StdDuration::from_secs(policy.lease_renew_interval_secs);
        let lease_duration = chrono_duration_secs(policy.lease_duration_secs, "lease duration")?;
        Ok(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = stop.cancelled() => break,
                    () = tokio::time::sleep(renew_interval) => {}
                }
                let now = Utc::now();
                match conditions
                    .renew_lease(&instance_id, worker_id, lease_epoch, now + lease_duration)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(error) => {
                        tracing::warn!(%error, %instance_id, "entry-condition lease renewal failed");
                        break;
                    }
                }
            }
        }))
    }

    async fn invalidate(
        &self,
        instance: EntryConditionInstanceInfo,
        detail: impl Into<String>,
        now: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInstanceInfo> {
        self.conditions
            .invalidate(
                &instance.condition_instance_id,
                self.worker_id,
                instance.revision,
                instance.lease_epoch,
                detail.into(),
                now,
            )
            .await
            .map_err(Into::into)
    }

    fn publish(&self, instance: &EntryConditionInstanceInfo) {
        self.events
            .publish(CoreEvent::Condition(EntryConditionLifecycleEvent {
                condition_instance_id: instance.condition_instance_id.clone(),
                revision: instance.revision,
                state: instance.state,
                truth: instance.truth_json.clone(),
                evaluation_hash: instance.evaluation_hash.clone(),
            }));
    }
}

fn earliest_future_clock_deadline(
    node: &EntryConditionV1,
    evaluated_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match node {
        EntryConditionV1::Clock(condition) if condition.deadline_at > evaluated_at => {
            Some(condition.deadline_at)
        }
        EntryConditionV1::All { children } | EntryConditionV1::Any { children } => children
            .iter()
            .filter_map(|child| earliest_future_clock_deadline(child, evaluated_at))
            .min(),
        EntryConditionV1::Price(_)
        | EntryConditionV1::Clock(_)
        | EntryConditionV1::Factor(_)
        | EntryConditionV1::MarketEvent { .. } => None,
    }
}

fn chrono_duration_secs(value: u64, field: &str) -> QuantResult<Duration> {
    let value = i64::try_from(value)
        .map_err(|error| QuantError::config(format!("{field} does not fit i64: {error}")))?;
    Ok(Duration::seconds(value))
}

fn chrono_duration_millis(value: u64, field: &str) -> QuantResult<Duration> {
    let value = i64::try_from(value)
        .map_err(|error| QuantError::config(format!("{field} does not fit i64: {error}")))?;
    Ok(Duration::milliseconds(value))
}

const fn truth_label(truth: &ConditionTruth) -> &'static str {
    match truth {
        ConditionTruth::Satisfied => "satisfied",
        ConditionTruth::Unsatisfied => "unsatisfied",
        ConditionTruth::Unavailable(_) => "unavailable",
    }
}
