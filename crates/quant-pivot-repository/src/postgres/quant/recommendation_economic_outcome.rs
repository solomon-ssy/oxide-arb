//! `PostgreSQL` WORM outcome repository and durable horizon queue.

use std::{
    cmp,
    collections::{HashMap, HashSet},
    fmt::Display,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_ECONOMIC_OUTCOME_RECONCILIATION_TASK, QUANT_RECOMMENDATION_ECONOMIC_OUTCOME},
};
use quant_pivot_models::{
    domain::quant::{
        EconomicOutcomeReconciliationResult, EconomicOutcomeReplayContext,
        EconomicOutcomeTaskClaim, EconomicOutcomeTaskSettlement, NewRecommendationEconomicOutcome,
        RecommendationEconomicOutcomeInfo, RecommendationResolutionOutcomeInfo,
    },
    entities::{
        quant_economic_outcome_reconciliation_task::{
            ActiveModel as TaskActiveModel, Column as TaskColumn, Entity as TaskEntity,
            Model as TaskModel,
        },
        quant_feature_vector::{
            Column as FeatureVectorColumn, Entity as FeatureVectorEntity,
            Model as FeatureVectorModel,
        },
        quant_recommendation::{
            Column as RecommendationColumn, Entity as RecommendationEntity,
            Model as RecommendationModel,
        },
        quant_recommendation_economic_outcome::{Entity, Model},
        quant_recommendation_report::{
            Column as ReportColumn, Entity as ReportEntity, Model as RecommendationReportModel,
        },
        quant_recommendation_resolution_outcome::{
            Column as ResolutionColumn, Entity as ResolutionEntity,
        },
        quant_report_route_run::{Column as RouteRunColumn, Entity as RouteRunEntity},
        quant_research_readiness_evidence::Entity as ReadinessEvidenceEntity,
        quant_trade_policy_artifact::Entity as TradePolicyEntity,
        research_profile_artifact::{Column as ProfileColumn, Entity as ResearchProfileEntity},
    },
    enums::quant::{OutcomeReconciliationTaskStatus, OutcomeSide},
    types::{
        ContentHash, FeatureVectorId, RecommendationId, RecommendationPolicyProvenance,
        RecommendationReportId, ReportRouteRunId, ResearchReadinessEvidencePayload, WorkerId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, ExprTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
    sea_query::{Alias, Expr, Func, LockBehavior, LockType, OnConflict, Query},
};

use crate::{
    postgres::primitives::statement_timestamp, traits::RecommendationEconomicOutcomeRepository,
};

const MAX_ERROR_CHARS: usize = 4_096;
const MAX_LEASE_SECS: u64 = 3_600;
const MAX_QUEUE_BATCH: u64 = 4_096;
const MAX_CANDIDATE_PAGE: u64 = 64;
const MAX_RETRY_SECS: u64 = 86_400;
const MAX_SOURCE_LATENESS_SECS: u64 = 604_800;

pub struct PgRecommendationEconomicOutcomeRepository {
    db: DatabaseConnection,
}

struct FrozenEconomicBoundary {
    replay_until: DateTime<Utc>,
    source_cutoff_at: DateTime<Utc>,
    resolution_outcome_hash: Option<ContentHash>,
}

struct EconomicClaimLineage {
    recommendations: HashMap<RecommendationId, RecommendationModel>,
    reports: HashMap<RecommendationReportId, RecommendationReportModel>,
    features: HashMap<FeatureVectorId, FeatureVectorModel>,
    resolutions: HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
}

impl EconomicClaimLineage {
    async fn load(
        transaction: &DatabaseTransaction,
        tasks: &[TaskModel],
        available_through: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        let ids = tasks
            .iter()
            .filter(|row| row.replay_until.is_none() && row.horizon_at > available_through)
            .map(|row| row.recommendation_id)
            .collect::<Vec<_>>();
        let resolution_ids = tasks
            .iter()
            .filter(|row| {
                row.resolution_outcome_hash.is_some()
                    || (row.replay_until.is_none() && row.horizon_at > available_through)
            })
            .map(|row| row.recommendation_id)
            .collect::<Vec<_>>();
        let recommendations = if ids.is_empty() {
            HashMap::new()
        } else {
            RecommendationEntity::find()
                .filter(RecommendationColumn::RecommendationId.is_in(ids))
                .all(transaction)
                .await?
                .into_iter()
                .map(|row| (row.recommendation_id, row))
                .collect::<HashMap<_, _>>()
        };
        let report_ids = recommendations
            .values()
            .map(|row| row.recommendation_report_id)
            .collect::<HashSet<_>>();
        let feature_ids = recommendations
            .values()
            .map(|row| row.evidence_refs.feature_vector_id)
            .collect::<HashSet<_>>();
        let reports = if report_ids.is_empty() {
            HashMap::new()
        } else {
            ReportEntity::find()
                .filter(ReportColumn::RecommendationReportId.is_in(report_ids))
                .all(transaction)
                .await?
                .into_iter()
                .map(|row| (row.recommendation_report_id, row))
                .collect()
        };
        let features = if feature_ids.is_empty() {
            HashMap::new()
        } else {
            FeatureVectorEntity::find()
                .filter(FeatureVectorColumn::FeatureVectorId.is_in(feature_ids))
                .all(transaction)
                .await?
                .into_iter()
                .map(|row| (row.feature_vector_id, row))
                .collect()
        };
        let resolutions = if resolution_ids.is_empty() {
            HashMap::new()
        } else {
            ResolutionEntity::find()
                .filter(ResolutionColumn::RecommendationId.is_in(resolution_ids))
                .all(transaction)
                .await?
                .into_iter()
                .map(|row| (row.recommendation_id, row.into()))
                .collect()
        };
        Ok(Self {
            recommendations,
            reports,
            features,
            resolutions,
        })
    }

    fn boundary(
        &self,
        task: &TaskModel,
        available_through: DateTime<Utc>,
        lateness: Duration,
    ) -> Result<Option<FrozenEconomicBoundary>, StorageError> {
        if let (Some(replay_until), Some(source_cutoff_at)) =
            (task.replay_until, task.source_cutoff_at)
        {
            if let Some(expected_hash) = task.resolution_outcome_hash {
                let resolution = self.resolution(task.recommendation_id)?;
                if resolution.outcome_hash != expected_hash {
                    return Err(PgRecommendationEconomicOutcomeRepository::queue_invariant(
                        "frozen economic resolution hash changed",
                    ));
                }
                if resolution.available_at > available_through {
                    return Ok(None);
                }
            }
            return Ok(
                (replay_until <= available_through).then_some(FrozenEconomicBoundary {
                    replay_until,
                    source_cutoff_at,
                    resolution_outcome_hash: task.resolution_outcome_hash,
                }),
            );
        }
        if task.replay_until.is_some()
            || task.source_cutoff_at.is_some()
            || task.resolution_outcome_hash.is_some()
        {
            return Err(PgRecommendationEconomicOutcomeRepository::queue_invariant(
                "economic replay boundary is only partially frozen",
            ));
        }
        if task.horizon_at <= available_through {
            return Ok(Some(FrozenEconomicBoundary {
                replay_until: task.horizon_at,
                source_cutoff_at: task.horizon_at.checked_add_signed(lateness).ok_or_else(
                    || {
                        PgRecommendationEconomicOutcomeRepository::queue_invariant(
                            "economic source cutoff overflowed UTC",
                        )
                    },
                )?,
                resolution_outcome_hash: None,
            }));
        }
        let Some(resolution) = self.resolutions.get(&task.recommendation_id) else {
            return Ok(None);
        };
        let resolution = self.resolution(resolution.recommendation_id)?;
        let recommendation = self
            .recommendations
            .get(&task.recommendation_id)
            .ok_or_else(|| {
                PgRecommendationEconomicOutcomeRepository::queue_invariant(
                    "economic task lost its recommendation",
                )
            })?;
        let report = self
            .reports
            .get(&recommendation.recommendation_report_id)
            .ok_or_else(|| {
                PgRecommendationEconomicOutcomeRepository::queue_invariant(
                    "economic task lost its report",
                )
            })?;
        let feature = self
            .features
            .get(&recommendation.evidence_refs.feature_vector_id)
            .ok_or_else(|| {
                PgRecommendationEconomicOutcomeRepository::queue_invariant(
                    "early economic task lost its frozen feature boundary",
                )
            })?;
        PgRecommendationEconomicOutcomeRepository::validate_replay_feature(
            feature,
            recommendation,
            report,
        )?;
        feature
            .decision_boundary
            .validate()
            .map_err(PgRecommendationEconomicOutcomeRepository::queue_invariant)?;
        let identity_matches = resolution.market_id == recommendation.market_id
            && resolution.token_id == recommendation.token_id;
        let observed_after_decision = resolution.source_observed_at > report.decision_at;
        if !identity_matches || !observed_after_decision {
            return Err(PgRecommendationEconomicOutcomeRepository::queue_invariant(
                "economic resolution identity or decision timeline differs",
            ));
        }
        let lag = Duration::from_std(feature.decision_boundary.knowledge_lag())
            .map_err(PgRecommendationEconomicOutcomeRepository::queue_invariant)?;
        let effective_visible_at =
            resolution
                .resolved_at
                .checked_add_signed(lag)
                .ok_or_else(|| {
                    PgRecommendationEconomicOutcomeRepository::queue_invariant(
                        "economic resolution visibility overflowed UTC",
                    )
                })?;
        let replay_until = cmp::max(effective_visible_at, resolution.source_observed_at);
        let source_anchor = cmp::max(replay_until, resolution.available_at);
        if replay_until >= task.horizon_at || source_anchor > available_through {
            return Ok(None);
        }
        Ok(Some(FrozenEconomicBoundary {
            replay_until,
            source_cutoff_at: source_anchor.checked_add_signed(lateness).ok_or_else(|| {
                PgRecommendationEconomicOutcomeRepository::queue_invariant(
                    "early economic source cutoff overflowed UTC",
                )
            })?,
            resolution_outcome_hash: Some(resolution.outcome_hash),
        }))
    }

    fn resolution(
        &self,
        id: RecommendationId,
    ) -> Result<&RecommendationResolutionOutcomeInfo, StorageError> {
        let resolution = self.resolutions.get(&id).ok_or_else(|| {
            PgRecommendationEconomicOutcomeRepository::queue_invariant(
                "frozen economic resolution projection is missing",
            )
        })?;
        resolution
            .validate()
            .map_err(PgRecommendationEconomicOutcomeRepository::queue_invariant)?;
        Ok(resolution)
    }
}

impl PgRecommendationEconomicOutcomeRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn exact_retry(stored: &Model, incoming: &NewRecommendationEconomicOutcome) -> bool {
        stored.recommendation_id == incoming.recommendation_id
            && stored.recommendation_report_id == incoming.recommendation_report_id
            && stored.report_route_run_id == incoming.report_route_run_id
            && stored.decision_policy_snapshot_id == incoming.decision_policy_snapshot_id
            && stored.economic_tier_id == incoming.economic_tier_id
            && stored.model_version_id == incoming.model_version_id
            && stored.trade_policy_artifact_id == incoming.trade_policy_artifact_id
            && stored.research_profile_artifact_id == incoming.research_profile_artifact_id
            && stored.state == incoming.state
            && stored.decision_at == incoming.decision_at
            && stored.horizon_at == incoming.horizon_at
            && stored.source_available_until == incoming.source_available_until
            && stored.replay_kernel_version == incoming.replay_kernel_version
            && stored.payload_json == incoming.payload_json
            && stored.evidence_hash == incoming.evidence_hash
            && stored.available_at == incoming.available_at
    }

    pub(crate) async fn enqueue_report_txn(
        transaction: &DatabaseTransaction,
        report_id: &RecommendationReportId,
    ) -> Result<u64, StorageError> {
        let report = ReportEntity::find_by_id(*report_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_recommendation_report", report_id))?;
        let recommendations = RecommendationEntity::find()
            .filter(RecommendationColumn::RecommendationReportId.eq(*report_id))
            .order_by_asc(RecommendationColumn::RecommendationId)
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .filter(|recommendation| {
                matches!(
                    recommendation.trade_plan.policy.as_ref(),
                    RecommendationPolicyProvenance::TradePolicy { .. }
                )
            })
            .collect::<Vec<_>>();
        if recommendations.is_empty() {
            return Ok(0);
        }
        let route_ids = recommendations
            .iter()
            .map(|recommendation| recommendation.report_route_run_id)
            .collect::<Vec<_>>();
        let routes = RouteRunEntity::find()
            .filter(RouteRunColumn::ReportRouteRunId.is_in(route_ids))
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let profile_ids = routes
            .iter()
            .filter_map(|route| route.research_profile_artifact_id.clone())
            .collect::<Vec<_>>();
        let profiles = ResearchProfileEntity::find()
            .filter(ProfileColumn::ResearchProfileArtifactId.is_in(profile_ids))
            .all(transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|profile| (profile.research_profile_artifact_id.clone(), profile))
            .collect::<HashMap<_, _>>();
        let horizons = routes
            .into_iter()
            .map(|route| {
                let profile_id = route.research_profile_artifact_id.ok_or_else(|| {
                    Self::queue_invariant("economic outcome Route has no research profile")
                })?;
                let profile = profiles.get(&profile_id).ok_or_else(|| {
                    Self::queue_invariant("economic outcome Route lost its research profile")
                })?;
                let seconds = i64::try_from(profile.spec.target_horizon_secs).map_err(|error| {
                    Self::queue_invariant(format!("economic horizon exceeds chrono: {error}"))
                })?;
                Ok((route.report_route_run_id, Duration::seconds(seconds)))
            })
            .collect::<Result<HashMap<ReportRouteRunId, Duration>, StorageError>>()?;
        let now = statement_timestamp(transaction).await?;
        for recommendation in &recommendations {
            let horizon = horizons
                .get(&recommendation.report_route_run_id)
                .ok_or_else(|| Self::queue_invariant("recommendation lost its Route horizon"))?;
            let horizon_at = report
                .decision_at
                .checked_add_signed(*horizon)
                .ok_or_else(|| Self::queue_invariant("economic horizon overflowed UTC"))?;
            TaskEntity::insert(TaskActiveModel {
                recommendation_id: ActiveValue::Set(recommendation.recommendation_id),
                horizon_at: ActiveValue::Set(horizon_at),
                replay_until: ActiveValue::Set(None),
                resolution_outcome_hash: ActiveValue::Set(None),
                source_cutoff_at: ActiveValue::Set(None),
                status: ActiveValue::Set(OutcomeReconciliationTaskStatus::Pending),
                attempt_count: ActiveValue::Set(0),
                claim_owner: ActiveValue::Set(None),
                lease_expires_at: ActiveValue::Set(None),
                next_attempt_at: ActiveValue::Set(None),
                last_error: ActiveValue::Set(None),
                completed_at: ActiveValue::Set(None),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
            })
            .on_conflict(
                OnConflict::column(TaskColumn::RecommendationId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
            let stored = TaskEntity::find_by_id(recommendation.recommendation_id)
                .one(transaction)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| Self::queue_invariant("economic horizon task insert disappeared"))?;
            if stored.horizon_at != horizon_at {
                return Err(Self::queue_invariant(
                    "economic horizon task changed immutable horizon",
                ));
            }
        }
        u64::try_from(recommendations.len())
            .map_err(|error| Self::queue_invariant(format!("task count overflow: {error}")))
    }

    fn claim_durations(
        lease_secs: u64,
        source_lateness_secs: u64,
        limit: u64,
    ) -> Result<(Duration, Duration), StorageError> {
        if lease_secs == 0 || lease_secs > MAX_LEASE_SECS {
            return Err(Self::queue_invariant(format!(
                "lease_secs must be within 1..={MAX_LEASE_SECS}"
            )));
        }
        if source_lateness_secs == 0 || source_lateness_secs > MAX_SOURCE_LATENESS_SECS {
            return Err(Self::queue_invariant(format!(
                "source_lateness_secs must be within 1..={MAX_SOURCE_LATENESS_SECS}"
            )));
        }
        if limit == 0 || limit > MAX_QUEUE_BATCH {
            return Err(Self::queue_invariant(format!(
                "claim limit must be within 1..={MAX_QUEUE_BATCH}"
            )));
        }
        let lease = i64::try_from(lease_secs)
            .map(Duration::seconds)
            .map_err(|error| Self::queue_invariant(format!("lease overflow: {error}")))?;
        let lateness = i64::try_from(source_lateness_secs)
            .map(Duration::seconds)
            .map_err(|error| Self::queue_invariant(format!("lateness overflow: {error}")))?;
        Ok((lease, lateness))
    }

    fn retry_duration(delay_secs: u64, error: &str) -> Result<Duration, StorageError> {
        if delay_secs == 0
            || delay_secs > MAX_RETRY_SECS
            || error.trim().is_empty()
            || error.chars().count() > MAX_ERROR_CHARS
        {
            return Err(Self::queue_invariant(format!(
                "retry delay must be within 1..={MAX_RETRY_SECS} and error within 1..={MAX_ERROR_CHARS} characters"
            )));
        }
        i64::try_from(delay_secs)
            .map(Duration::seconds)
            .map_err(|error| Self::queue_invariant(format!("retry delay overflow: {error}")))
    }

    fn queue_invariant(detail: impl Display) -> StorageError {
        StorageError::invariant_violation(
            Some(QUANT_ECONOMIC_OUTCOME_RECONCILIATION_TASK),
            detail.to_string(),
        )
    }

    fn outcome_invariant(detail: impl Display) -> StorageError {
        StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_ECONOMIC_OUTCOME),
            detail.to_string(),
        )
    }

    fn owns_claim(
        row: &TaskModel,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
        now: DateTime<Utc>,
    ) -> bool {
        row.status == OutcomeReconciliationTaskStatus::Delivering
            && row.claim_owner == Some(worker_id)
            && row.attempt_count == claim.attempt_count
            && row.lease_expires_at.is_some_and(|expires| expires > now)
            && row.recommendation_id == claim.recommendation_id
            && row.horizon_at == claim.horizon_at
            && row.replay_until == Some(claim.replay_until)
            && row.source_cutoff_at == Some(claim.source_cutoff_at)
            && row.resolution_outcome_hash == claim.resolution_outcome_hash
            && claim.source_available_until <= now
            && claim.source_available_until <= claim.source_cutoff_at
            && claim.source_available_until >= claim.replay_until
    }

    async fn settle_claim(
        transaction: &DatabaseTransaction,
        active: TaskActiveModel,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
    ) -> Result<bool, StorageError> {
        let result = TaskEntity::update_many()
            .set(active)
            .filter(TaskColumn::RecommendationId.eq(claim.recommendation_id))
            .filter(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Delivering))
            .filter(TaskColumn::ClaimOwner.eq(worker_id))
            .filter(TaskColumn::AttemptCount.eq(claim.attempt_count))
            .filter(
                Expr::col((TaskEntity, TaskColumn::LeaseExpiresAt))
                    .gt(Func::cust(Alias::new("STATEMENT_TIMESTAMP"))),
            )
            .exec(transaction)
            .await?;
        Ok(result.rows_affected == 1)
    }

    fn validate_replay_feature(
        feature: &FeatureVectorModel,
        recommendation: &RecommendationModel,
        report: &RecommendationReportModel,
    ) -> Result<(), StorageError> {
        let snapshot = &feature.decision_capture.snapshot;
        let selection = &snapshot.selection;
        let expected_token = match recommendation.outcome_side {
            OutcomeSide::Yes => Some(&selection.primary_token_id),
            OutcomeSide::No => selection.secondary_token_id.as_ref(),
        };
        let boundary_matches = feature.decision_boundary == snapshot.boundary;
        let valid = feature.market_id == recommendation.market_id
            && feature.decision_at == report.decision_at
            && boundary_matches
            && snapshot.market_id == feature.market_id
            && selection.market_id == feature.market_id
            && feature.token_id.as_ref() == Some(&selection.primary_token_id)
            && snapshot.token_id == selection.primary_token_id
            && expected_token == Some(&recommendation.token_id);
        if !valid {
            return Err(Self::queue_invariant(format!(
                "economic replay feature boundary differs from recommendation identity: recommendation={} side={:?} recommendation_market={} recommendation_token={} report_decision_at={} feature={} feature_market={} feature_token={:?} feature_decision_at={} capture_market={} capture_token={} primary_token={} secondary_token={:?}",
                recommendation.recommendation_id,
                recommendation.outcome_side,
                recommendation.market_id,
                recommendation.token_id,
                report.decision_at,
                feature.feature_vector_id,
                feature.market_id,
                feature.token_id,
                feature.decision_at,
                snapshot.market_id,
                snapshot.token_id,
                selection.primary_token_id,
                selection.secondary_token_id,
            )));
        }
        Ok(())
    }
}

impl PgRecommendationEconomicOutcomeRepository {
    async fn persist_outcome(
        transaction: &DatabaseTransaction,
        outcome: NewRecommendationEconomicOutcome,
        now: DateTime<Utc>,
    ) -> Result<(RecommendationEconomicOutcomeInfo, bool), StorageError> {
        outcome.verify().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_ECONOMIC_OUTCOME),
                error.to_string(),
            )
        })?;
        if outcome.available_at > now
            || outcome.source_available_until > now
            || outcome
                .payload_json
                .detail
                .terminal_at()
                .is_some_and(|terminal| terminal > now)
        {
            return Err(Self::outcome_invariant(
                "economic outcome cannot become visible in the future",
            ));
        }
        let recommendation = RecommendationEntity::find_by_id(outcome.recommendation_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_recommendation", outcome.recommendation_id)
            })?;
        if let Some(stored) = Entity::find_by_id(outcome.recommendation_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
        {
            let stored_info: RecommendationEconomicOutcomeInfo = stored.clone().into();
            stored_info.verify().map_err(Self::outcome_invariant)?;
            if Self::exact_retry(&stored, &outcome) {
                return Ok((stored_info, false));
            }
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_ECONOMIC_OUTCOME,
                Some(&outcome.recommendation_id),
                "economic outcome replay changed immutable content",
            ));
        }
        let report = ReportEntity::find_by_id(recommendation.recommendation_report_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_recommendation_report",
                    recommendation.recommendation_report_id,
                )
            })?;
        let route_run = RouteRunEntity::find_by_id(recommendation.report_route_run_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_report_route_run",
                    recommendation.report_route_run_id,
                )
            })?;
        let profile_id = outcome.research_profile_artifact_id.clone();
        let profile = ResearchProfileEntity::find_by_id(profile_id.clone())
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("research_profile_artifact", &profile_id))?;
        let horizon_secs = i64::try_from(profile.spec.target_horizon_secs).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_ECONOMIC_OUTCOME),
                format!("profile horizon exceeds chrono range: {error}"),
            )
        })?;
        let expected_horizon = report
            .decision_at
            .checked_add_signed(Duration::seconds(horizon_secs))
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_ECONOMIC_OUTCOME),
                    "economic outcome horizon overflows chrono",
                )
            })?;
        if outcome.recommendation_report_id != recommendation.recommendation_report_id
            || outcome.report_route_run_id != recommendation.report_route_run_id
            || outcome.economic_tier_id != recommendation.economic_tier_id
            || outcome.decision_policy_snapshot_id != report.decision_policy_snapshot_id
            || outcome.decision_at != report.decision_at
            || outcome.horizon_at != expected_horizon
            || route_run.model_version_id != Some(outcome.model_version_id)
            || route_run.trade_policy_artifact_id != Some(outcome.trade_policy_artifact_id)
            || route_run.research_profile_artifact_id != Some(profile_id)
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_ECONOMIC_OUTCOME),
                "economic outcome lineage differs from frozen recommendation Route",
            ));
        }
        let stored = Entity::insert(outcome.into_active_model())
            .exec_with_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        Ok((stored.into(), true))
    }
}

#[async_trait::async_trait]
impl RecommendationEconomicOutcomeRepository for PgRecommendationEconomicOutcomeRepository {
    async fn insert(
        &self,
        outcome: NewRecommendationEconomicOutcome,
    ) -> Result<RecommendationEconomicOutcomeInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = statement_timestamp(&transaction).await?;
        let (stored, _) = Self::persist_outcome(&transaction, outcome, now).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(stored)
    }

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationEconomicOutcomeInfo>, StorageError> {
        Entity::find_by_id(*recommendation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|outcome| outcome.map(Into::into))
    }

    async fn replay_context(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<EconomicOutcomeReplayContext, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let recommendation = RecommendationEntity::find_by_id(*recommendation_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_recommendation", recommendation_id))?;
        let report = ReportEntity::find_by_id(recommendation.recommendation_report_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_recommendation_report",
                    recommendation.recommendation_report_id,
                )
            })?;
        let route_run = RouteRunEntity::find_by_id(recommendation.report_route_run_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_report_route_run",
                    recommendation.report_route_run_id,
                )
            })?;
        let profile_id = route_run
            .research_profile_artifact_id
            .clone()
            .ok_or_else(|| {
                Self::queue_invariant("economic replay Route has no research profile")
            })?;
        let profile = ResearchProfileEntity::find_by_id(profile_id.clone())
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("research_profile_artifact", &profile_id))?;
        let policy_id = route_run.trade_policy_artifact_id.ok_or_else(|| {
            Self::queue_invariant("economic replay Route has no trade policy artifact")
        })?;
        let policy = TradePolicyEntity::find_by_id(policy_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_trade_policy_artifact", policy_id))?;
        let latency_id = policy.payload_json.fit_contract.latency_evidence_id;
        let latency = ReadinessEvidenceEntity::find_by_id(latency_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_research_readiness_evidence", latency_id)
            })?;
        if latency.payload_hash != policy.payload_json.fit_contract.latency_profile_hash {
            return Err(Self::queue_invariant(
                "economic replay latency evidence hash differs from its trade policy",
            ));
        }
        let ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency_profile) =
            latency.payload_json
        else {
            return Err(Self::queue_invariant(
                "economic replay latency evidence has the wrong payload kind",
            ));
        };
        let feature =
            FeatureVectorEntity::find_by_id(recommendation.evidence_refs.feature_vector_id)
                .one(&transaction)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::not_found(
                        "quant_feature_vector",
                        recommendation.evidence_refs.feature_vector_id,
                    )
                })?;
        Self::validate_replay_feature(&feature, &recommendation, &report)?;
        let resolution_outcome = ResolutionEntity::find_by_id(*recommendation_id)
            .one(&transaction)
            .await?
            .map(RecommendationResolutionOutcomeInfo::from);
        if let Some(resolution) = &resolution_outcome {
            resolution.validate().map_err(Self::queue_invariant)?;
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(EconomicOutcomeReplayContext {
            recommendation: recommendation.into(),
            report: report.into(),
            route_run: route_run.into(),
            profile_spec: profile.spec,
            trade_policy: policy.into(),
            latency_profile,
            decision_boundary: feature.decision_boundary,
            resolution_outcome,
        })
    }

    async fn enqueue_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<u64, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let inserted = Self::enqueue_report_txn(&transaction, report_id).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn claim_due(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        source_lateness_secs: u64,
        limit: u64,
    ) -> Result<Vec<EconomicOutcomeTaskClaim>, StorageError> {
        let (lease, lateness) = Self::claim_durations(lease_secs, source_lateness_secs, limit)?;
        let transaction = self.db.begin().await?;
        let now = statement_timestamp(&transaction).await?;
        let available_through = cmp::min(available_through, now);
        let due = Condition::any()
            .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Pending))
            .add(
                Condition::all()
                    .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Retrying))
                    .add(TaskColumn::NextAttemptAt.lte(now)),
            )
            .add(
                Condition::all()
                    .add(TaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Delivering))
                    .add(TaskColumn::LeaseExpiresAt.lte(now)),
            );
        let resolved = Query::select()
            .column((ResolutionEntity, ResolutionColumn::RecommendationId))
            .from(ResolutionEntity)
            .and_where(Expr::col(ResolutionColumn::AvailableAt).lte(available_through))
            .and_where(Expr::col(ResolutionColumn::ResolvedAt).lte(available_through))
            .to_owned();
        let claim_limit = usize::try_from(limit).map_err(Self::queue_invariant)?;
        let page_limit = cmp::min(limit, MAX_CANDIDATE_PAGE);
        let mut scanned = 0_u64;
        let mut cursor: Option<(DateTime<Utc>, RecommendationId)> = None;
        let mut claims = Vec::with_capacity(claim_limit);
        // Scan a bounded keyset page at a time. Only candidates needing an
        // initial early-terminal proof load full lineage; a mature or already
        // frozen horizon never reloads feature documents.
        while scanned < MAX_QUEUE_BATCH && claims.len() < claim_limit {
            let mut query = TaskEntity::find()
                .filter(
                    Condition::any()
                        .add(TaskColumn::HorizonAt.lte(available_through))
                        .add(TaskColumn::ReplayUntil.lte(available_through))
                        .add(Expr::col(TaskColumn::RecommendationId).in_subquery(resolved.clone())),
                )
                .filter(due.clone());
            if let Some((horizon_at, recommendation_id)) = cursor {
                query = query.filter(
                    Condition::any()
                        .add(TaskColumn::HorizonAt.gt(horizon_at))
                        .add(
                            Condition::all()
                                .add(TaskColumn::HorizonAt.eq(horizon_at))
                                .add(TaskColumn::RecommendationId.gt(recommendation_id)),
                        ),
                );
            }
            let rows = query
                .order_by_asc(TaskColumn::HorizonAt)
                .order_by_asc(TaskColumn::RecommendationId)
                .limit(cmp::min(page_limit, MAX_QUEUE_BATCH - scanned))
                .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
                .all(&transaction)
                .await?;
            if rows.is_empty() {
                break;
            }
            scanned += u64::try_from(rows.len()).map_err(Self::queue_invariant)?;
            cursor = rows
                .last()
                .map(|row| (row.horizon_at, row.recommendation_id));
            let lineage =
                EconomicClaimLineage::load(&transaction, &rows, available_through).await?;
            for row in rows {
                if claims.len() == claim_limit {
                    break;
                }
                let Some(boundary) = lineage.boundary(&row, available_through, lateness)? else {
                    continue;
                };
                let attempt_count = row
                    .attempt_count
                    .checked_add(1)
                    .ok_or_else(|| Self::queue_invariant("economic task attempt overflow"))?;
                let mut active = row.clone().into_active_model();
                active.replay_until = ActiveValue::Set(Some(boundary.replay_until));
                active.source_cutoff_at = ActiveValue::Set(Some(boundary.source_cutoff_at));
                active.resolution_outcome_hash = ActiveValue::Set(boundary.resolution_outcome_hash);
                active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Delivering);
                active.attempt_count = ActiveValue::Set(attempt_count);
                active.claim_owner = ActiveValue::Set(Some(worker_id));
                active.lease_expires_at = ActiveValue::Set(Some(now + lease));
                active.next_attempt_at = ActiveValue::Set(None);
                active.updated_at = ActiveValue::Set(now);
                active.update(&transaction).await?;
                claims.push(EconomicOutcomeTaskClaim {
                    recommendation_id: row.recommendation_id,
                    horizon_at: row.horizon_at,
                    replay_until: boundary.replay_until,
                    resolution_outcome_hash: boundary.resolution_outcome_hash,
                    source_cutoff_at: boundary.source_cutoff_at,
                    source_available_until: cmp::min(available_through, boundary.source_cutoff_at),
                    attempt_count,
                });
            }
        }
        transaction.commit().await?;
        Ok(claims)
    }

    async fn complete_task(
        &self,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
        outcome: NewRecommendationEconomicOutcome,
    ) -> Result<EconomicOutcomeReconciliationResult, StorageError> {
        let transaction = self.db.begin().await?;
        let row = TaskEntity::find_by_id(claim.recommendation_id)
            .lock_exclusive()
            .one(&transaction)
            .await?;
        let now = statement_timestamp(&transaction).await?;
        let Some(row) = row.filter(|row| Self::owns_claim(row, claim, worker_id, now)) else {
            return Ok(EconomicOutcomeReconciliationResult::ClaimLost);
        };
        let identity_matches = outcome.recommendation_id == claim.recommendation_id
            && outcome.horizon_at == claim.horizon_at;
        let source_covers_replay = outcome.source_available_until >= claim.replay_until;
        let source_within_claim = outcome.source_available_until <= claim.source_available_until;
        let terminal_within_replay = outcome
            .payload_json
            .detail
            .terminal_at()
            .is_none_or(|terminal| terminal <= claim.replay_until);
        if !identity_matches
            || !source_covers_replay
            || !source_within_claim
            || !terminal_within_replay
        {
            return Err(Self::outcome_invariant(
                "economic outcome differs from its frozen lease boundary",
            ));
        }
        let existing = Entity::find_by_id(claim.recommendation_id)
            .one(&transaction)
            .await?;
        let (stored, inserted) = if let Some(existing) = existing {
            let stored: RecommendationEconomicOutcomeInfo = existing.clone().into();
            stored.verify().map_err(Self::outcome_invariant)?;
            if stored.available_at > now || !Self::exact_retry(&existing, &outcome) {
                return Err(Self::outcome_invariant(
                    "existing economic outcome is not an exact immutable retry",
                ));
            }
            (stored, false)
        } else {
            if outcome.source_available_until != claim.source_available_until {
                return Err(Self::outcome_invariant(
                    "new economic outcome source cutoff differs from its claim",
                ));
            }
            let outcome = outcome
                .with_available_at(now)
                .map_err(Self::outcome_invariant)?;
            Self::persist_outcome(&transaction, outcome, now).await?
        };
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Completed);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.next_attempt_at = ActiveValue::Set(None);
        active.last_error = ActiveValue::Set(None);
        active.completed_at = ActiveValue::Set(Some(now));
        active.updated_at = ActiveValue::Set(now);
        // Recheck the actual statement clock after lineage validation and the
        // WORM insert. Losing the lease here rolls back both writes atomically.
        if !Self::settle_claim(&transaction, active, claim, worker_id).await? {
            transaction.rollback().await?;
            return Ok(EconomicOutcomeReconciliationResult::ClaimLost);
        }
        transaction.commit().await?;
        Ok(if inserted {
            EconomicOutcomeReconciliationResult::Inserted(stored)
        } else {
            EconomicOutcomeReconciliationResult::AlreadyPresent(stored)
        })
    }

    async fn retry_task(
        &self,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
        delay_secs: u64,
        error: String,
    ) -> Result<EconomicOutcomeTaskSettlement, StorageError> {
        let transaction = self.db.begin().await?;
        let row = TaskEntity::find_by_id(claim.recommendation_id)
            .lock_exclusive()
            .one(&transaction)
            .await?;
        let now = statement_timestamp(&transaction).await?;
        let Some(row) = row.filter(|row| Self::owns_claim(row, claim, worker_id, now)) else {
            return Ok(EconomicOutcomeTaskSettlement::ClaimLost);
        };
        let delay = Self::retry_duration(delay_secs, &error)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OutcomeReconciliationTaskStatus::Retrying);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.next_attempt_at = ActiveValue::Set(Some(now + delay));
        active.last_error = ActiveValue::Set(Some(error));
        active.completed_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(now);
        if !Self::settle_claim(&transaction, active, claim, worker_id).await? {
            transaction.rollback().await?;
            return Ok(EconomicOutcomeTaskSettlement::ClaimLost);
        }
        transaction.commit().await?;
        Ok(EconomicOutcomeTaskSettlement::Retried)
    }
}
