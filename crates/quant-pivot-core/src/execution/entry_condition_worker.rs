//! Durable recommendation-level entry-condition evaluation worker.

use std::{future::pending, slice, sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    report::ReportError,
    storage::{StorageError, entity::QUANT_FACTOR},
};
use quant_pivot_models::{
    domain::{
        quant::{ApplyEntryConditionEvaluation, EntryConditionInstanceInfo, MarketLinkageInfo},
        runtime::{CoreEvent, CoreEventPublisher, EntryConditionLifecycleEvent},
    },
    enums::domain::LinkageStatus,
    hashing::CanonicalDigest,
    runtime_config::EntryConditionWorkerConfig,
    types::{
        ConditionUnavailableReason, ContentHash, CryptoPriceInput, CryptoPriceReportInput,
        CryptoSubjectPredicateEntered, EntryConditionArtifactV1, EntryConditionBinding,
        EntryConditionInputSet, EntryConditionSourceBinding, EntryConditionV1,
        ExecutablePriceInput, FactorCondition, FactorSnapshotInput, MarketEventCondition, MarketId,
        ModelVersionId, TokenId, Usd, WeatherDailyTemperatureInput, WeatherTemperatureStatistic,
        WorkerId,
    },
};
use quant_pivot_repository::traits::{
    EntryConditionRepository, FactorRepository, MarketLinkageRepository, MarketSelectionRepository,
    ModelRegistryRepository, PolicyRepository, QuantFactReadRepository, RecommendationRepository,
};
use quant_pivot_storage::postgres::PostgresNotificationListener;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    EntryConditionEvaluation, EntryConditionStateDecision, decide_entry_condition_state,
    evaluate_entry_condition,
};
use crate::{ingest::book_store::BookStore, runtime_config::DecisionPolicyStore};

/// Supplies exactly the persisted/in-memory PIT projections consumed by the evaluator.
#[async_trait]
pub trait EntryConditionInputProvider: Send + Sync {
    async fn load(
        &self,
        artifact: &EntryConditionArtifactV1,
        instance: &EntryConditionInstanceInfo,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInputSet>;
}

/// Production input provider. Book prices come from the lock-free store; factor
/// and domain-event facts come from typed Postgres projections.
pub struct LiveEntryConditionInputProvider {
    books: Arc<BookStore>,
    conditions: Arc<dyn EntryConditionRepository>,
    factors: Arc<dyn FactorRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    recommendations: Arc<dyn RecommendationRepository>,
    linkages: Arc<dyn MarketLinkageRepository>,
    selections: Arc<dyn MarketSelectionRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    runtime_configs: Arc<dyn PolicyRepository>,
    runtime_config: Arc<DecisionPolicyStore>,
}

/// Repository and runtime dependencies for the live condition input provider.
pub struct LiveEntryConditionInputDeps {
    pub books: Arc<BookStore>,
    pub conditions: Arc<dyn EntryConditionRepository>,
    pub factors: Arc<dyn FactorRepository>,
    pub facts: Arc<dyn QuantFactReadRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub selections: Arc<dyn MarketSelectionRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub runtime_configs: Arc<dyn PolicyRepository>,
    pub runtime_config: Arc<DecisionPolicyStore>,
}

impl LiveEntryConditionInputProvider {
    #[must_use]
    pub fn new(deps: LiveEntryConditionInputDeps) -> Self {
        Self {
            books: deps.books,
            conditions: deps.conditions,
            factors: deps.factors,
            facts: deps.facts,
            recommendations: deps.recommendations,
            linkages: deps.linkages,
            selections: deps.selections,
            models: deps.models,
            runtime_configs: deps.runtime_configs,
            runtime_config: deps.runtime_config,
        }
    }

    async fn load_linkage_binding(
        &self,
        expected: &EntryConditionBinding,
    ) -> QuantResult<(Option<MarketLinkageInfo>, Option<MarketLinkageInfo>)> {
        let linkage = match expected.market_linkage_id.as_ref() {
            Some(linkage_id) => self.linkages.find_by_id(linkage_id).await?,
            None => None,
        };
        let latest = if expected.market_linkage_id.is_some() {
            self.linkages
                .latest_for_markets(slice::from_ref(&expected.market_id))
                .await?
                .into_iter()
                .next()
        } else {
            None
        };
        Ok((linkage, latest))
    }

    async fn resolve_binding(
        &self,
        expected: &EntryConditionBinding,
    ) -> QuantResult<ResolvedBinding> {
        let recommendation = self
            .recommendations
            .find_by_id(&expected.recommendation_id)
            .await?;
        let selection = self
            .selections
            .find_by_id(&expected.catalog_snapshot_id)
            .await?;
        let model = self
            .models
            .find_model_version(&expected.model_version_id)
            .await?;
        let current_runtime_config = self.runtime_configs.load_current().await?;
        let factor_ids = expected
            .factor_bindings
            .iter()
            .map(|binding| binding.definition_id)
            .collect::<Vec<_>>();
        let factors = self.factors.find_definitions_by_ids(&factor_ids).await?;
        let (linkage, latest_linkage) = self.load_linkage_binding(expected).await?;
        let reason = if recommendation.as_ref().is_none_or(|recommendation| {
            recommendation.market_id != expected.market_id
                || recommendation.token_id != expected.token_id
                || recommendation.outcome_side != expected.outcome_side
                || recommendation.evidence_refs.market_selection_id != expected.catalog_snapshot_id
                || recommendation.evidence_refs.model_version_id != expected.model_version_id
                || recommendation.evidence_refs.decision_policy_snapshot_id
                    != expected.decision_policy_snapshot_id
        }) {
            Some(ConditionUnavailableReason::BindingDrift)
        } else if selection.as_ref().is_none_or(|selection| {
            let snapshot_matches = selection.selector_hash == expected.catalog_snapshot_hash;
            let runtime_config_matches =
                selection.decision_policy_snapshot_id == expected.decision_policy_snapshot_id;
            !snapshot_matches || !runtime_config_matches
        }) {
            Some(ConditionUnavailableReason::CatalogSnapshotMismatch)
        } else if expected.market_linkage_id.is_some()
            && (linkage.as_ref().is_none_or(|linkage| {
                Some(&linkage.content_hash) != expected.market_linkage_hash.as_ref()
                    || linkage.market_id != expected.market_id
                    || !matches!(
                        linkage.status,
                        LinkageStatus::Resolved | LinkageStatus::Overridden
                    )
            }) || latest_linkage.as_ref().is_none_or(|latest| {
                Some(&latest.linkage_id) != expected.market_linkage_id.as_ref()
                    || Some(&latest.content_hash) != expected.market_linkage_hash.as_ref()
            }))
        {
            Some(ConditionUnavailableReason::MarketLinkageMismatch)
        } else if model.is_none()
            || current_runtime_config.as_ref().is_none_or(|config| {
                config.decision_policy_snapshot_id != expected.decision_policy_snapshot_id
            })
            || !runtime_model_is_active(&self.runtime_config, &expected.model_version_id)
        {
            Some(ConditionUnavailableReason::BindingDrift)
        } else if factors.len() != expected.factor_bindings.len()
            || expected.factor_bindings.iter().any(|binding| {
                factors
                    .iter()
                    .find(|definition| definition.factor_definition_id == binding.definition_id)
                    .is_none_or(|definition| definition.definition_hash != binding.definition_hash)
            })
        {
            Some(ConditionUnavailableReason::FactorDefinitionMismatch)
        } else {
            None
        };
        let revision = CanonicalDigest::content_hash_json(&(
            expected,
            selection.as_ref().map(|value| &value.selector_hash),
            linkage.as_ref().map(|value| &value.content_hash),
            latest_linkage.as_ref().map(|value| &value.content_hash),
            model.as_ref().map(|value| &value.artifact_hash),
            current_runtime_config
                .as_ref()
                .map(|value| &value.snapshot_hash),
            factors
                .iter()
                .map(|value| (&value.factor_definition_id, &value.definition_hash))
                .collect::<Vec<_>>(),
        ))
        .map_err(QuantError::from)?;
        Ok(ResolvedBinding {
            binding: expected.clone(),
            revision,
            unavailable_reason: reason,
        })
    }

    fn load_prices(&self, token_ids: Vec<TokenId>) -> Vec<ExecutablePriceInput> {
        token_ids
            .into_iter()
            .filter_map(|token_id| {
                let snapshot = self.books.load_fresh_by_id(&token_id).ok()?;
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
        available_by: DateTime<Utc>,
    ) -> QuantResult<Vec<FactorSnapshotInput>> {
        let mut groups = Vec::new();
        for condition in &required {
            let group = groups
                .iter_mut()
                .find(|(model_version_id, _)| model_version_id == &condition.model_version_id);
            if let Some((_, definition_ids)) = group {
                push_unique(definition_ids, condition.definition_id);
            } else {
                groups.push((condition.model_version_id, vec![condition.definition_id]));
            }
        }
        let mut inputs = Vec::with_capacity(required.len());
        for (model_version_id, definition_ids) in groups {
            let Some(snapshot) = self
                .factors
                .latest_snapshot_bundle(&definition_ids, market_id, &model_version_id, available_by)
                .await?
            else {
                continue;
            };
            if snapshot.observed_at > available_by || snapshot.available_at > available_by {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!(
                        "factor snapshot {} exceeded entry-condition cutoff {available_by}",
                        snapshot.snapshot_hash
                    ),
                )
                .into());
            }
            for value in snapshot.values {
                let (Some(raw_value), Some(normalized_value)) =
                    (value.raw_value, value.normalized_score)
                else {
                    continue;
                };
                inputs.push(FactorSnapshotInput {
                    definition_id: value.factor_definition_id,
                    definition_hash: value.definition_hash,
                    model_version_id,
                    raw_value,
                    normalized_value: normalized_value.inner(),
                    confidence: value.confidence.inner(),
                    observed_at: snapshot.observed_at,
                    available_at: snapshot.available_at,
                    snapshot_hash: snapshot.snapshot_hash,
                });
            }
        }
        Ok(inputs)
    }

    async fn load_crypto(
        &self,
        instance: &EntryConditionInstanceInfo,
        required: Vec<CryptoSubjectPredicateEntered>,
        evaluated_at: DateTime<Utc>,
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
            let Ok(gap_generation) = u64::try_from(projection.gap_generation) else {
                continue;
            };
            let prior = instance
                .fold_state_json
                .crypto
                .iter()
                .find(|state| state.source == condition.source);
            let from = prior
                .and_then(|state| state.last_available_at)
                .unwrap_or(instance.created_at)
                .timestamp_millis();
            let to = evaluated_at
                .timestamp_millis()
                .checked_add(1)
                .ok_or_else(|| ReportError::ContractViolation {
                    detail: "entry condition evaluation timestamp overflow".to_owned(),
                })?;
            let mut rows = self
                .facts
                .crypto_reports_between(
                    vec![condition.source.instrument_key.clone()],
                    from,
                    to,
                    evaluated_at.timestamp_millis(),
                )
                .await?;
            rows.retain(|row| {
                row.source_id == condition.source.source_id
                    && row.instrument_key == condition.source.instrument_key
            });
            if prior.is_none()
                && let Some(baseline) = self
                    .facts
                    .crypto_price_report_at(
                        &condition.source.source_id,
                        &condition.source.instrument_key,
                        instance.created_at.timestamp_millis(),
                        instance.created_at.timestamp_millis(),
                    )
                    .await?
            {
                rows.push(baseline);
            }
            rows.sort_by(|left, right| {
                (
                    left.available_at,
                    left.event_time,
                    left.source_sequence,
                    &left.report_hash,
                )
                    .cmp(&(
                        right.available_at,
                        right.event_time,
                        right.source_sequence,
                        &right.report_hash,
                    ))
            });
            rows.dedup_by(|left, right| {
                left.source_sequence == right.source_sequence
                    && left.report_hash == right.report_hash
            });
            let reports = rows
                .into_iter()
                .filter_map(|row| {
                    Some(CryptoPriceReportInput {
                        source_sequence: row.source_sequence,
                        price: Usd::new(Decimal::from(row.price)),
                        event_at: DateTime::from_timestamp_millis(row.event_time)?,
                        available_at: DateTime::from_timestamp_millis(row.available_at)?,
                        report_hash: row.report_hash,
                    })
                })
                .collect();
            inputs.push(CryptoPriceInput {
                source: condition.source,
                reports,
                gap_generation,
                source_healthy: projection.source_healthy,
            });
        }
        Ok(inputs)
    }

    async fn load_weather(
        &self,
        required: Vec<RequiredWeather>,
    ) -> QuantResult<Vec<WeatherDailyTemperatureInput>> {
        let mut inputs = Vec::with_capacity(required.len());
        for condition in required {
            let Some(projection) = self
                .conditions
                .find_weather_projection(
                    &condition.source.source_id,
                    &condition.source.instrument_key,
                    &condition.station,
                    condition.local_date,
                    condition.temperature_statistic,
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
            inputs.push(WeatherDailyTemperatureInput {
                source: EntryConditionSourceBinding {
                    source_id: projection.source_id,
                    instrument_key: projection.instrument_key,
                    binding_hash: condition.source.binding_hash,
                },
                station: projection.station,
                local_date: projection.local_date,
                temperature_statistic: projection.temperature_statistic,
                current_extreme: projection.current_extreme,
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
        instance: &EntryConditionInstanceInfo,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<EntryConditionInputSet> {
        let resolved_binding = self.resolve_binding(&artifact.binding).await?;
        let mut required = RequiredInputs::default();
        required.collect(&artifact.root);
        let prices = self.load_prices(required.prices);
        let factors = self
            .load_factors(&artifact.binding.market_id, required.factors, evaluated_at)
            .await?;
        let crypto = self
            .load_crypto(instance, required.crypto, evaluated_at)
            .await?;
        let weather = self.load_weather(required.weather).await?;

        Ok(EntryConditionInputSet {
            binding: resolved_binding.binding,
            binding_revision: resolved_binding.revision,
            binding_unavailable_reason: resolved_binding.unavailable_reason,
            fold_state: instance.fold_state_json.clone(),
            evaluated_at,
            prices,
            factors,
            crypto,
            weather,
        })
    }
}

struct ResolvedBinding {
    binding: EntryConditionBinding,
    revision: ContentHash,
    unavailable_reason: Option<ConditionUnavailableReason>,
}

fn runtime_model_is_active(
    runtime_config: &DecisionPolicyStore,
    expected: &ModelVersionId,
) -> bool {
    let config = runtime_config.load();
    config
        .model_routing
        .model
        .active_model_version_id
        .as_ref()
        .into_iter()
        .chain(config.model_routing.model.category_model_pointers.values())
        .any(|reference| &reference.id == expected)
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
    local_date: NaiveDate,
    temperature_statistic: WeatherTemperatureStatistic,
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
                MarketEventCondition::WeatherDailyTemperatureEnteredBand(condition) => {
                    push_unique(
                        &mut self.weather,
                        RequiredWeather {
                            source: condition.source.clone(),
                            station: condition.station.clone(),
                            local_date: condition.local_date,
                            temperature_statistic: condition.temperature_statistic,
                        },
                    );
                }
                MarketEventCondition::WeatherDailyTemperatureCrossedTerminalBound(condition) => {
                    push_unique(
                        &mut self.weather,
                        RequiredWeather {
                            source: condition.source.clone(),
                            station: condition.station.clone(),
                            local_date: condition.local_date,
                            temperature_statistic: condition.temperature_statistic,
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
                            temperature_statistic: condition.temperature_statistic,
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
    worker_id: WorkerId,
    conditions: Arc<dyn EntryConditionRepository>,
    inputs: Arc<dyn EntryConditionInputProvider>,
    books: Arc<BookStore>,
    runtime_config: Arc<DecisionPolicyStore>,
    events: CoreEventPublisher,
}

impl EntryConditionWorker {
    #[must_use]
    pub fn new(
        conditions: Arc<dyn EntryConditionRepository>,
        inputs: Arc<dyn EntryConditionInputProvider>,
        books: Arc<BookStore>,
        runtime_config: Arc<DecisionPolicyStore>,
        events: CoreEventPublisher,
    ) -> Self {
        Self {
            worker_id: WorkerId::from_v7(),
            conditions,
            inputs,
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
                .operational_control
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
            .operational_control
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

        let input = self.inputs.load(&artifact, &instance, evaluated_at).await?;
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
        let tree_json = serde_json::to_string(&evaluation.tree).map_err(|error| {
            ReportError::ContractViolation {
                detail: format!("entry condition evaluation serialization failed: {error}"),
            }
        })?;
        let next_delay =
            chrono_duration_millis(policy.next_evaluation_delay_ms, "next evaluation delay")?;
        let periodic_at = evaluated_at + next_delay;
        let next_evaluation_at = earliest_future_clock_deadline(&artifact.root, evaluated_at)
            .map_or(periodic_at, |deadline| deadline.min(periodic_at));
        let outcome = self
            .conditions
            .apply_evaluation(
                &instance.condition_instance_id,
                self.worker_id,
                ApplyEntryConditionEvaluation {
                    expected_revision: instance.revision,
                    expected_lease_epoch: instance.lease_epoch,
                    state: decision.state,
                    truth: evaluation.truth.clone(),
                    evaluation_hash: evaluation.evaluation_hash,
                    input_fingerprint: evaluation.input_fingerprint,
                    continuity_hash: evaluation.continuity_hash,
                    fold_state: evaluation.fold_state.clone(),
                    confirmation_started_at: decision.confirmation_started_at,
                    evaluated_at,
                    next_evaluation_at: Some(next_evaluation_at),
                    evaluator_version: artifact.evaluator_version,
                    tree_json,
                },
            )
            .await
            .map_err(QuantError::from)?;
        Ok(outcome.instance)
    }

    fn spawn_lease_renewal(
        &self,
        instance: &EntryConditionInstanceInfo,
        stop: CancellationToken,
        policy: &EntryConditionWorkerConfig,
    ) -> QuantResult<JoinHandle<()>> {
        let conditions = Arc::clone(&self.conditions);
        let instance_id = instance.condition_instance_id;
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
                condition_instance_id: instance.condition_instance_id,
                revision: instance.revision,
                state: instance.state,
                truth: instance.truth_json.clone(),
                evaluation_hash: instance.evaluation_hash,
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
