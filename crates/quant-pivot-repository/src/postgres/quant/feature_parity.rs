//! Postgres feature-parity run lifecycle and append-only latch ledger.

use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    hashing::CanonicalDigestError,
    storage::{
        StorageError,
        entity::{QUANT_FEATURE_PARITY_RUN, QUANT_FEATURE_PARITY_STATE, QUANT_RESEARCH_JOB},
    },
};
use quant_pivot_models::{
    domain::{
        api::FeatureParityRunListQuery,
        pagination::{PageWindow, Paginated},
        quant::{
            CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityStateInfo,
            FrozenFeatureParityCandidate, FrozenFeatureParitySubject, FrozenFeatureParitySubjectId,
            ModelRunParityEvidence, NewFeatureParityRun, NewFeatureParityState,
            NewFrozenModelParitySubject, NewResearchJob, ResearchJobInfo,
            parity_candidate_membership_hash, parity_selection_hash, report_parity_evidence_hash,
            report_parity_generation_hash,
        },
    },
    entities::{
        quant_feature_parity_candidate::{
            ActiveModel as QuantFeatureParityCandidateActiveModel,
            Entity as QuantFeatureParityCandidateEntity,
        },
        quant_feature_parity_run::{
            Column as QuantFeatureParityRunColumn, Entity as QuantFeatureParityRunEntity,
        },
        quant_feature_parity_state::{
            Column as QuantFeatureParityStateColumn, Entity as QuantFeatureParityStateEntity,
        },
        quant_feature_parity_subject::{
            ActiveModel, Column as QuantFeatureParitySubjectColumn,
            Entity as QuantFeatureParitySubjectEntity,
        },
        quant_market_selection::{
            Column as QuantMarketSelectionColumn, Entity as QuantMarketSelectionEntity,
        },
        quant_market_selection_member::{
            Column as QuantMarketSelectionMemberColumn, Entity as QuantMarketSelectionMemberEntity,
        },
        quant_model_run::{Column as QuantModelRunColumn, Entity as QuantModelRunEntity},
        quant_recommendation_report::{Column, Entity, Model},
        quant_research_job::Entity as QuantResearchJobEntity,
    },
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
        FeatureParityStateTransition, ModelRunKind, ModelRunStatus, ParitySubjectKind,
        ResearchJobKind,
    },
    types::{
        ContentHash, DiagnosticCode, FeatureParityCandidateId, FeatureParityRunId,
        FeatureParityStateId, FeatureParitySubjectId, MarketSelectionId, ModelRunId,
        ModelVersionId, RecommendationReportId, ResearchJobParams, RoleCode, TrainingDatasetId,
    },
};
use sea_orm::{
    ActiveValue, ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IntoActiveModel, LoaderTrait, QueryFilter, QueryOrder,
    TransactionTrait, sea_query::Expr,
};

use crate::{
    postgres::{error, primitives, query::paginate_mapped},
    traits::{EnqueueFrozenFeatureParityOutcome, FeatureParityLatchActor, FeatureParityRepository},
};

pub(super) const LATCH_ADVISORY_LOCK_KEY: i64 = 0x_11_06_50_41;

/// Postgres-backed parity run and latch repository.
pub struct PgFeatureParityRepository {
    db: DatabaseConnection,
}

impl PgFeatureParityRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

struct ServingSubjectSeed {
    identity: ServingSubjectIdentity,
    generation: ContentHash,
    decision_at: DateTime<Utc>,
    selection_id: MarketSelectionId,
    evidence_hash: ContentHash,
}

enum ServingSubjectIdentity {
    ModelRun(ModelRunId),
    RecommendationReport(RecommendationReportId),
}

fn map_parity_hash_error(error: &CanonicalDigestError, context: &'static str) -> StorageError {
    StorageError::invariant_violation(
        Some(QUANT_FEATURE_PARITY_RUN),
        format!("{context} canonical hash failed: {error}"),
    )
}

impl PgFeatureParityRepository {
    async fn freeze_full_window(
        txn: &DatabaseTransaction,
        run: &NewFeatureParityRun,
    ) -> Result<Vec<ServingSubjectSeed>, StorageError> {
        if run.kind != FeatureParityRunKind::Full
            || run.report_id.is_some()
            || run.model_version_id.is_some()
            || run.training_dataset_id.is_some()
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_RUN),
                "frozen serving-window enqueue requires an unbound full parity run",
            ));
        }
        let reports = Entity::find()
            .filter(Column::DecisionAt.gte(run.window_start))
            .filter(Column::DecisionAt.lt(run.window_end))
            .order_by_asc(Column::DecisionAt)
            .order_by_asc(Column::RecommendationReportId)
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        let model_runs = QuantModelRunEntity::find()
            .filter(QuantModelRunColumn::RunKind.eq(ModelRunKind::LiveInference))
            .filter(QuantModelRunColumn::Status.eq(ModelRunStatus::Succeeded))
            .filter(QuantModelRunColumn::WindowStart.gte(run.window_start))
            .filter(QuantModelRunColumn::WindowStart.lt(run.window_end))
            .order_by_asc(QuantModelRunColumn::WindowStart)
            .order_by_asc(QuantModelRunColumn::ModelRunId)
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        let report_by_run = reports
            .iter()
            .filter_map(|report| report.model_run_id.as_ref().map(|run_id| (*run_id, report)))
            .collect::<HashMap<_, _>>();
        let model_run_ids = model_runs
            .iter()
            .map(|model_run| model_run.model_run_id)
            .collect::<HashSet<_>>();
        for run_id in report_by_run.keys() {
            if !model_run_ids.contains(run_id) {
                return Err(StorageError::state_conflict(
                    QUANT_FEATURE_PARITY_RUN,
                    Some(&run.run_id),
                    format!(
                        "serving report references model run {run_id} outside the frozen full window"
                    ),
                ));
            }
        }

        let mut seeds = Vec::with_capacity(model_runs.len() + reports.len());
        for model_run in model_runs {
            let selection_id = model_run.market_selection_id.ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_model_run",
                    Some(&model_run.model_run_id),
                    "successful live model run has no market selection",
                )
            })?;
            let generation = model_run.output_hash.ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_model_run",
                    Some(&model_run.model_run_id),
                    "successful live model run has no output hash",
                )
            })?;
            let evidence_hash = ModelRunParityEvidence {
                model_run_id: &model_run.model_run_id,
                input_hash: &model_run.input_hash,
                output_hash: &generation,
                model_version_id: &model_run.model_version_id,
                decision_policy_snapshot_id: &model_run.decision_policy_snapshot_id,
            }
            .content_hash()
            .map_err(|error| map_parity_hash_error(&error, "model-run evidence"))?;
            seeds.push(ServingSubjectSeed {
                identity: ServingSubjectIdentity::ModelRun(model_run.model_run_id),
                generation,
                decision_at: model_run.window_start,
                selection_id,
                evidence_hash,
            });
        }
        for report in reports
            .into_iter()
            .filter(|report| report.model_run_id.is_none())
        {
            let generation = report_parity_generation_hash(
                &report.recommendation_report_id,
                report.decision_at,
                report.created_at,
            )
            .map_err(|error| map_parity_hash_error(&error, "report generation"))?;
            let evidence_hash = report_parity_evidence_hash(
                &generation,
                &report.model_version_id,
                &report.decision_policy_snapshot_id,
                &report.market_selection_id,
                &report.data_quality_snapshot_ref,
                &report.portfolio_plan_id,
            )
            .map_err(|error| map_parity_hash_error(&error, "report evidence"))?;
            seeds.push(ServingSubjectSeed {
                identity: ServingSubjectIdentity::RecommendationReport(
                    report.recommendation_report_id,
                ),
                generation,
                decision_at: report.decision_at,
                selection_id: report.market_selection_id,
                evidence_hash,
            });
        }
        seeds.sort_by_key(|seed| {
            let (kind, id) = match &seed.identity {
                ServingSubjectIdentity::ModelRun(id) => ("model_run", id.to_string()),
                ServingSubjectIdentity::RecommendationReport(id) => {
                    ("recommendation_report", id.to_string())
                }
            };
            (seed.decision_at, kind, id)
        });
        Ok(seeds)
    }
}

impl PgFeatureParityRepository {
    pub(super) async fn insert_frozen_report_subject(
        txn: &DatabaseTransaction,
        run_id: &FeatureParityRunId,
        report: &Model,
    ) -> Result<(), StorageError> {
        let seed = if let Some(model_run_id) = report.model_run_id.as_ref() {
            let model_run = QuantModelRunEntity::find_by_id(*model_run_id)
                .one(txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| StorageError::not_found("quant_model_run", model_run_id))?;
            if model_run.run_kind != ModelRunKind::LiveInference
                || model_run.status != ModelRunStatus::Succeeded
                || model_run.window_start != report.decision_at
                || model_run.market_selection_id.as_ref() != Some(&report.market_selection_id)
                || model_run.decision_policy_snapshot_id != report.decision_policy_snapshot_id
                || model_run.model_version_id.as_ref() != Some(&report.model_version_id)
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEATURE_PARITY_RUN,
                    Some(run_id),
                    "sampled parity report does not bind an exact successful live model run",
                ));
            }
            let generation = model_run.output_hash.ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_model_run",
                    Some(model_run_id),
                    "successful live model run has no output hash",
                )
            })?;
            let evidence_hash = ModelRunParityEvidence {
                model_run_id: &model_run.model_run_id,
                input_hash: &model_run.input_hash,
                output_hash: &generation,
                model_version_id: &model_run.model_version_id,
                decision_policy_snapshot_id: &model_run.decision_policy_snapshot_id,
            }
            .content_hash()
            .map_err(|error| map_parity_hash_error(&error, "model-run evidence"))?;
            ServingSubjectSeed {
                identity: ServingSubjectIdentity::ModelRun(model_run.model_run_id),
                generation,
                decision_at: model_run.window_start,
                selection_id: report.market_selection_id,
                evidence_hash,
            }
        } else {
            let generation = report_parity_generation_hash(
                &report.recommendation_report_id,
                report.decision_at,
                report.created_at,
            )
            .map_err(|error| map_parity_hash_error(&error, "report generation"))?;
            let evidence_hash = report_parity_evidence_hash(
                &generation,
                &report.model_version_id,
                &report.decision_policy_snapshot_id,
                &report.market_selection_id,
                &report.data_quality_snapshot_ref,
                &report.portfolio_plan_id,
            )
            .map_err(|error| map_parity_hash_error(&error, "report evidence"))?;
            ServingSubjectSeed {
                identity: ServingSubjectIdentity::RecommendationReport(
                    report.recommendation_report_id,
                ),
                generation,
                decision_at: report.decision_at,
                selection_id: report.market_selection_id,
                evidence_hash,
            }
        };
        Self::insert_frozen_subjects(txn, run_id, vec![seed]).await
    }
}

impl PgFeatureParityRepository {
    async fn insert_frozen_subjects(
        txn: &DatabaseTransaction,
        run_id: &FeatureParityRunId,
        seeds: Vec<ServingSubjectSeed>,
    ) -> Result<(), StorageError> {
        let selection_ids = seeds
            .iter()
            .map(|seed| seed.selection_id)
            .collect::<Vec<_>>();
        let selections = QuantMarketSelectionEntity::find()
            .filter(QuantMarketSelectionColumn::MarketSelectionId.is_in(selection_ids.clone()))
            .all(txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|selection| (selection.market_selection_id, selection))
            .collect::<HashMap<_, _>>();
        let members = QuantMarketSelectionMemberEntity::find()
            .filter(QuantMarketSelectionMemberColumn::MarketSelectionId.is_in(selection_ids))
            .order_by_asc(QuantMarketSelectionMemberColumn::MarketSelectionId)
            .order_by_asc(QuantMarketSelectionMemberColumn::MarketId)
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        let mut members_by_selection: HashMap<MarketSelectionId, Vec<_>> = HashMap::new();
        for member in members {
            members_by_selection
                .entry(member.market_selection_id)
                .or_default()
                .push(member);
        }

        for seed in seeds {
            let selection = selections.get(&seed.selection_id).ok_or_else(|| {
                StorageError::not_found("quant_market_selection", seed.selection_id)
            })?;
            let members = members_by_selection
                .get(&seed.selection_id)
                .cloned()
                .unwrap_or_default();
            let market_ids = members
                .iter()
                .map(|member| member.market_id.clone())
                .collect::<Vec<_>>();
            let selection_hash =
                parity_selection_hash(&seed.selection_id, &selection.selector_hash, &market_ids)
                    .map_err(|error| map_parity_hash_error(&error, "selection membership"))?;
            let subject_id = FeatureParitySubjectId::from_v7();
            let (subject_kind, model_run_id, recommendation_report_id) = match seed.identity {
                ServingSubjectIdentity::ModelRun(id) => {
                    (ParitySubjectKind::ModelRun, Some(id), None)
                }
                ServingSubjectIdentity::RecommendationReport(id) => {
                    (ParitySubjectKind::RecommendationReport, None, Some(id))
                }
            };
            QuantFeatureParitySubjectEntity::insert(ActiveModel {
                parity_subject_id: Set(subject_id),
                run_id: Set(*run_id),
                subject_kind: Set(subject_kind),
                model_run_id: Set(model_run_id),
                recommendation_report_id: Set(recommendation_report_id),
                model_version_id: Set(None),
                training_dataset_id: Set(None),
                market_selection_id: Set(Some(seed.selection_id)),
                subject_generation: Set(seed.generation),
                decision_at: Set(Some(seed.decision_at)),
                selection_hash: Set(Some(selection_hash)),
                evidence_hash: Set(seed.evidence_hash),
                created_at: ActiveValue::NotSet,
            })
            .exec(txn)
            .await
            .map_err(StorageError::from)?;
            for (ordinal, member) in members.into_iter().enumerate() {
                let ordinal = i32::try_from(ordinal).map_err(|_| {
                    StorageError::invariant_violation(
                        Some("quant_feature_parity_candidate"),
                        "selection membership exceeds i32 ordinal capacity",
                    )
                })?;
                let membership_hash =
                    parity_candidate_membership_hash(&selection_hash, &member.market_id, ordinal)
                        .map_err(|error| {
                        map_parity_hash_error(&error, "parity candidate membership")
                    })?;
                QuantFeatureParityCandidateEntity::insert(QuantFeatureParityCandidateActiveModel {
                    parity_candidate_id: Set(FeatureParityCandidateId::from_v7()),
                    parity_subject_id: Set(subject_id),
                    market_id: Set(member.market_id),
                    ordinal: Set(ordinal),
                    membership_hash: Set(membership_hash),
                    created_at: ActiveValue::NotSet,
                })
                .exec(txn)
                .await
                .map_err(StorageError::from)?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FeatureParityRepository for PgFeatureParityRepository {
    async fn create_run(
        &self,
        run: NewFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        validate_new_run(&run)?;
        let run_id = run.run_id.to_string();
        QuantFeatureParityRunEntity::insert(run.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| error::map_unique(error, QUANT_FEATURE_PARITY_RUN, &run_id))
            .map(Into::into)
    }

    async fn create_frozen_model_run(
        &self,
        run: NewFeatureParityRun,
        subject: NewFrozenModelParitySubject,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        validate_new_run(&run)?;
        if run.kind != FeatureParityRunKind::Full
            || run.report_id.is_some()
            || run.model_version_id.as_ref() != Some(&subject.model_version_id)
            || run.training_dataset_id.as_ref() != Some(&subject.training_dataset_id)
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_RUN),
                "offline frozen parity subject must exactly bind its full model/dataset run",
            ));
        }
        let run_id = run.run_id;
        let run_key = run_id.to_string();
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let run_model = QuantFeatureParityRunEntity::insert(run.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|error| error::map_unique(error, QUANT_FEATURE_PARITY_RUN, &run_key))?;
        QuantFeatureParitySubjectEntity::insert(ActiveModel {
            parity_subject_id: Set(FeatureParitySubjectId::from_v7()),
            run_id: Set(run_id),
            subject_kind: Set(ParitySubjectKind::ModelVersion),
            model_run_id: Set(None),
            recommendation_report_id: Set(None),
            model_version_id: Set(Some(subject.model_version_id)),
            training_dataset_id: Set(Some(subject.training_dataset_id)),
            market_selection_id: Set(None),
            subject_generation: Set(subject.subject_generation),
            decision_at: Set(None),
            selection_hash: Set(None),
            evidence_hash: Set(subject.evidence_hash),
            created_at: ActiveValue::NotSet,
        })
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(run_model.into())
    }

    async fn enqueue_run(
        &self,
        run: NewFeatureParityRun,
        job: NewResearchJob,
    ) -> Result<(FeatureParityRunInfo, ResearchJobInfo), StorageError> {
        validate_new_run(&run)?;
        if job.kind != ResearchJobKind::FeatureParity {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "parity run must be paired with a feature_parity research job",
            ));
        }
        let expected_run_id = run.run_id.to_string();
        let ResearchJobParams::FeatureParity(params) = &job.params_json else {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "feature_parity job requires typed feature parity params",
            ));
        };
        if params.parity_run_id != run.run_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "feature_parity job params must reference the same parity_run_id",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let run_model = QuantFeatureParityRunEntity::insert(run.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|error| {
                error::map_unique(error, QUANT_FEATURE_PARITY_RUN, &expected_run_id)
            })?;
        let job_model = QuantResearchJobEntity::insert(job.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((run_model.into(), job_model.into()))
    }

    async fn enqueue_frozen_full(
        &self,
        run: NewFeatureParityRun,
        job: NewResearchJob,
    ) -> Result<EnqueueFrozenFeatureParityOutcome, StorageError> {
        validate_new_run(&run)?;
        if job.kind != ResearchJobKind::FeatureParity {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "parity run must be paired with a feature_parity research job",
            ));
        }
        let expected_run_id = run.run_id.to_string();
        let ResearchJobParams::FeatureParity(params) = &job.params_json else {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "feature_parity job requires typed feature parity params",
            ));
        };
        if params.parity_run_id != run.run_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "feature_parity job params must reference the same parity_run_id",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let seeds = Self::freeze_full_window(&txn, &run).await?;
        if seeds.is_empty() {
            txn.rollback().await.map_err(StorageError::from)?;
            return Ok(EnqueueFrozenFeatureParityOutcome::NotEligible);
        }
        let run_id = run.run_id;
        let run_model = QuantFeatureParityRunEntity::insert(run.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|error| {
                error::map_unique(error, QUANT_FEATURE_PARITY_RUN, &expected_run_id)
            })?;
        Self::insert_frozen_subjects(&txn, &run_id, seeds).await?;
        let job_model = QuantResearchJobEntity::insert(job.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(EnqueueFrozenFeatureParityOutcome::Enqueued {
            run: Box::new(run_model.into()),
            job: Box::new(job_model.into()),
        })
    }

    async fn load_frozen_subjects(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<Vec<FrozenFeatureParitySubject>, StorageError> {
        let subjects = QuantFeatureParitySubjectEntity::find()
            .filter(QuantFeatureParitySubjectColumn::RunId.eq(*run_id))
            .order_by_asc(QuantFeatureParitySubjectColumn::DecisionAt)
            .order_by_asc(QuantFeatureParitySubjectColumn::SubjectKind)
            .order_by_asc(QuantFeatureParitySubjectColumn::ModelRunId)
            .order_by_asc(QuantFeatureParitySubjectColumn::RecommendationReportId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let candidates = subjects
            .load_many(QuantFeatureParityCandidateEntity, &self.db)
            .await
            .map_err(StorageError::from)?;
        subjects
            .into_iter()
            .zip(candidates)
            .map(|(subject, mut candidates)| {
                candidates.sort_by_key(|candidate| candidate.ordinal);
                let subject_id = match (
                    subject.subject_kind,
                    subject.model_run_id,
                    subject.recommendation_report_id,
                    subject.model_version_id,
                    subject.training_dataset_id,
                ) {
                    (ParitySubjectKind::ModelRun, Some(id), None, None, None) => {
                        FrozenFeatureParitySubjectId::ModelRun(id)
                    }
                    (ParitySubjectKind::RecommendationReport, None, Some(id), None, None) => {
                        FrozenFeatureParitySubjectId::RecommendationReport(id)
                    }
                    (
                        ParitySubjectKind::ModelVersion,
                        None,
                        None,
                        Some(model_version_id),
                        Some(training_dataset_id),
                    ) => FrozenFeatureParitySubjectId::ModelVersion {
                        model_version_id,
                        training_dataset_id,
                    },
                    _ => {
                        return Err(StorageError::invariant_violation(
                            Some("quant_feature_parity_subject"),
                            "subject kind and typed foreign-key identity disagree",
                        ));
                    }
                };
                Ok(FrozenFeatureParitySubject {
                    subject_id,
                    market_selection_id: subject.market_selection_id,
                    subject_generation: subject.subject_generation,
                    decision_at: subject.decision_at,
                    selection_hash: subject.selection_hash,
                    evidence_hash: subject.evidence_hash,
                    candidates: candidates
                        .into_iter()
                        .map(|candidate| FrozenFeatureParityCandidate {
                            market_id: candidate.market_id,
                            ordinal: candidate.ordinal,
                            membership_hash: candidate.membership_hash,
                        })
                        .collect(),
                })
            })
            .collect()
    }

    async fn find_run(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        find_run_on(&self.db, run_id).await
    }

    async fn page_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> Result<Paginated<FeatureParityRunInfo>, StorageError> {
        let mut condition = Condition::all();
        if let Some(kind) = query.kind {
            condition = condition.add(QuantFeatureParityRunColumn::Kind.eq(kind));
        }
        if let Some(status) = query.status {
            condition = condition.add(QuantFeatureParityRunColumn::Status.eq(status));
        }
        if let Some(report_id) = query.report_id.as_ref() {
            condition = condition.add(QuantFeatureParityRunColumn::ReportId.eq(*report_id));
        }
        if let Some(model_version_id) = query.model_version_id.as_ref() {
            condition =
                condition.add(QuantFeatureParityRunColumn::ModelVersionId.eq(*model_version_id));
        }
        if let Some(training_dataset_id) = query.training_dataset_id.as_ref() {
            condition = condition
                .add(QuantFeatureParityRunColumn::TrainingDatasetId.eq(*training_dataset_id));
        }
        if let Some(from) = query.from {
            condition = condition.add(QuantFeatureParityRunColumn::CreatedAt.gte(from));
        }
        if let Some(to) = query.to {
            condition = condition.add(QuantFeatureParityRunColumn::CreatedAt.lt(to));
        }
        paginate_mapped(
            QuantFeatureParityRunEntity::find()
                .filter(condition)
                .order_by_desc(QuantFeatureParityRunColumn::CreatedAt)
                .order_by_desc(QuantFeatureParityRunColumn::RunId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn latest_run(
        &self,
        kind: FeatureParityRunKind,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        QuantFeatureParityRunEntity::find()
            .filter(QuantFeatureParityRunColumn::Kind.eq(kind))
            .order_by_desc(QuantFeatureParityRunColumn::CreatedAt)
            .order_by_desc(QuantFeatureParityRunColumn::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_unbound_full(&self) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        QuantFeatureParityRunEntity::find()
            .filter(QuantFeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Full))
            .filter(QuantFeatureParityRunColumn::ReportId.is_null())
            .filter(QuantFeatureParityRunColumn::ModelVersionId.is_null())
            .filter(QuantFeatureParityRunColumn::TrainingDatasetId.is_null())
            .order_by_desc(QuantFeatureParityRunColumn::CreatedAt)
            .order_by_desc(QuantFeatureParityRunColumn::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_full_window(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        QuantFeatureParityRunEntity::find()
            .filter(QuantFeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Full))
            .filter(QuantFeatureParityRunColumn::WindowStart.eq(window_start))
            .filter(QuantFeatureParityRunColumn::WindowEnd.eq(window_end))
            .filter(QuantFeatureParityRunColumn::ReportId.is_null())
            .filter(QuantFeatureParityRunColumn::ModelVersionId.is_null())
            .filter(QuantFeatureParityRunColumn::TrainingDatasetId.is_null())
            .filter(QuantFeatureParityRunColumn::Status.is_in([
                FeatureParityRunStatus::Queued,
                FeatureParityRunStatus::Running,
                FeatureParityRunStatus::PendingMaterialization,
            ]))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_sampled_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        QuantFeatureParityRunEntity::find()
            .filter(QuantFeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Sampled))
            .filter(QuantFeatureParityRunColumn::ReportId.eq(*report_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_full_for_model(
        &self,
        model_version_id: &ModelVersionId,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        QuantFeatureParityRunEntity::find()
            .filter(QuantFeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Full))
            .filter(QuantFeatureParityRunColumn::ModelVersionId.eq(*model_version_id))
            .filter(QuantFeatureParityRunColumn::TrainingDatasetId.eq(*training_dataset_id))
            .order_by_desc(QuantFeatureParityRunColumn::CreatedAt)
            .order_by_desc(QuantFeatureParityRunColumn::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn mark_running(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        let result = QuantFeatureParityRunEntity::update_many()
            .col_expr(
                QuantFeatureParityRunColumn::Status,
                primitives::enum_value(&FeatureParityRunStatus::Running),
            )
            .col_expr(
                QuantFeatureParityRunColumn::StartedAt,
                primitives::timestamp_once(QuantFeatureParityRunColumn::StartedAt),
            )
            .filter(QuantFeatureParityRunColumn::RunId.eq(*run_id))
            .filter(
                Condition::any()
                    .add(QuantFeatureParityRunColumn::Status.eq(FeatureParityRunStatus::Queued))
                    .add(
                        QuantFeatureParityRunColumn::Status
                            .eq(FeatureParityRunStatus::PendingMaterialization),
                    ),
            )
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            return Err(run_transition_conflict(&self.db, run_id, "running").await);
        }
        require_run_on(&self.db, run_id).await
    }

    async fn complete_run(
        &self,
        run_id: &FeatureParityRunId,
        result: CompleteFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        validate_completion(&result)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let finished_at = if result.status.is_terminal() {
            Expr::current_timestamp()
        } else {
            Expr::value(Option::<DateTime<Utc>>::None)
        };
        let mut statement = QuantFeatureParityRunEntity::update_many()
            .col_expr(
                QuantFeatureParityRunColumn::Status,
                primitives::enum_value(&result.status),
            )
            .col_expr(
                QuantFeatureParityRunColumn::TotalCount,
                Expr::value(result.total_count),
            )
            .col_expr(
                QuantFeatureParityRunColumn::ComparedCount,
                Expr::value(result.compared_count),
            )
            .col_expr(
                QuantFeatureParityRunColumn::MatchedCount,
                Expr::value(result.matched_count),
            )
            .col_expr(
                QuantFeatureParityRunColumn::MismatchedCount,
                Expr::value(result.mismatched_count),
            )
            .col_expr(
                QuantFeatureParityRunColumn::PendingMaterializationCount,
                Expr::value(result.pending_materialization_count),
            )
            .col_expr(
                QuantFeatureParityRunColumn::FeatureContractHash,
                Expr::value(result.feature_contract_hash),
            )
            .col_expr(
                QuantFeatureParityRunColumn::TransformHash,
                Expr::value(result.transform_hash),
            )
            .col_expr(
                QuantFeatureParityRunColumn::FailureCode,
                Expr::value(result.failure_code.clone()),
            )
            .col_expr(
                QuantFeatureParityRunColumn::FailureDetail,
                Expr::value(result.failure_detail.clone()),
            )
            .col_expr(QuantFeatureParityRunColumn::FinishedAt, finished_at);
        if result.status == FeatureParityRunStatus::PendingMaterialization {
            statement = statement.col_expr(
                QuantFeatureParityRunColumn::PendingSince,
                primitives::timestamp_once(QuantFeatureParityRunColumn::PendingSince),
            );
        }
        let update = statement
            .filter(QuantFeatureParityRunColumn::RunId.eq(*run_id))
            .filter(QuantFeatureParityRunColumn::Status.eq(FeatureParityRunStatus::Running))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if update.rows_affected == 0 {
            let error = run_transition_conflict(&txn, run_id, result.status.as_str()).await;
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(error);
        }
        if result.status == FeatureParityRunStatus::Mismatched {
            Self::acquire_latch_lock(&txn).await?;
            Self::append_open_state(
                &txn,
                run_id,
                FeatureParityStateTransition::DeterministicMismatch,
                "deterministic online/replay mismatch",
            )
            .await?;
        }
        let completed = require_run_on(&txn, run_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(completed)
    }

    async fn mark_containment_complete(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        let result = QuantFeatureParityRunEntity::update_many()
            .col_expr(
                QuantFeatureParityRunColumn::ContainmentCompletedAt,
                primitives::timestamp_once(QuantFeatureParityRunColumn::ContainmentCompletedAt),
            )
            .filter(QuantFeatureParityRunColumn::RunId.eq(*run_id))
            .filter(QuantFeatureParityRunColumn::Status.is_in([
                FeatureParityRunStatus::Mismatched,
                FeatureParityRunStatus::Failed,
            ]))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            let current = require_run_on(&self.db, run_id).await?;
            if current.containment_completed_at.is_some()
                && matches!(
                    current.status,
                    FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed
                )
            {
                return Ok(current);
            }
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                format!(
                    "containment completion requires mismatched/failed status, found {}",
                    current.status.as_str()
                ),
            ));
        }
        require_run_on(&self.db, run_id).await
    }

    async fn current_state(&self) -> Result<Option<FeatureParityStateInfo>, StorageError> {
        current_state_on(&self.db).await
    }

    async fn open_latch(
        &self,
        cause_run_id: &FeatureParityRunId,
        transition: FeatureParityStateTransition,
        reason: String,
    ) -> Result<FeatureParityStateInfo, StorageError> {
        if matches!(
            transition,
            FeatureParityStateTransition::BootstrapProof
                | FeatureParityStateTransition::GovernedAcknowledge
        ) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_STATE),
                "opening the latch requires a failure transition",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        Self::acquire_latch_lock(&txn).await?;
        let cause = require_run_on(&txn, cause_run_id).await?;
        let expected = match transition {
            FeatureParityStateTransition::DeterministicMismatch => {
                FeatureParityRunStatus::Mismatched
            }
            FeatureParityStateTransition::IntegrityFailure => FeatureParityRunStatus::Failed,
            FeatureParityStateTransition::BootstrapProof
            | FeatureParityStateTransition::GovernedAcknowledge => unreachable!(),
        };
        if cause.status != expected {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(cause_run_id),
                format!(
                    "latch transition {} requires run status {}, found {}",
                    transition.as_str(),
                    expected.as_str(),
                    cause.status.as_str()
                ),
            ));
        }
        let state = Self::append_open_state(&txn, cause_run_id, transition, &reason).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(state)
    }

    async fn record_integrity_failure(
        &self,
        source_run_id: &FeatureParityRunId,
        reason: String,
    ) -> Result<(FeatureParityRunInfo, FeatureParityStateInfo), StorageError> {
        if reason.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_STATE),
                "governance integrity failure reason must not be empty",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        Self::acquire_latch_lock(&txn).await?;
        let source = require_run_on(&txn, source_run_id).await?;
        if source.kind != FeatureParityRunKind::Full
            || source.status != FeatureParityRunStatus::Passed
        {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(source_run_id),
                "governance integrity incident must derive from the passed full permit used by the switch",
            ));
        }
        let now = Utc::now();
        let incident_id = FeatureParityRunId::from_v7();
        let incident = NewFeatureParityRun {
            run_id: incident_id,
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Failed,
            window_start: source.window_start,
            window_end: source.window_end,
            report_id: source.report_id,
            model_version_id: source.model_version_id,
            training_dataset_id: source.training_dataset_id,
            triggered_by: "system:model_governance".to_owned(),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            reason: reason.clone(),
            total_count: 0,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: source.feature_contract_hash,
            transform_hash: source.transform_hash,
            failure_code: Some(DiagnosticCode::new("rollback_pointer_recovery_failed")),
            failure_detail: Some(reason.clone()),
            started_at: Some(now),
            pending_since: None,
            containment_completed_at: Some(now),
            finished_at: Some(now),
        };
        let incident: FeatureParityRunInfo =
            QuantFeatureParityRunEntity::insert(incident.into_active_model())
                .exec_with_returning(&txn)
                .await
                .map_err(StorageError::from)?
                .into();
        let state = Self::append_open_state(
            &txn,
            &incident_id,
            FeatureParityStateTransition::IntegrityFailure,
            &reason,
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((incident, state))
    }

    async fn acknowledge_latch(
        &self,
        recovery_run_id: &FeatureParityRunId,
        actor: FeatureParityLatchActor,
    ) -> Result<FeatureParityStateInfo, StorageError> {
        if actor.reason.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_STATE),
                "acknowledgement reason must not be empty",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        Self::acquire_latch_lock(&txn).await?;
        let recovery = require_run_on(&txn, recovery_run_id).await?;
        validate_recovery_run(&recovery)?;
        let current = current_state_on(&txn).await?;
        if let Some(current) = current.as_ref() {
            if current.state == FeatureParityLatchState::Clear {
                validate_clear_acknowledgement(current, recovery_run_id)?;
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(current.clone());
            }
            let incident_states = open_incident_states_on(&txn, current).await?;
            let mut causes = Vec::with_capacity(incident_states.len());
            for incident_state in incident_states {
                let cause_run_id = incident_state.cause_run_id.as_ref().ok_or_else(|| {
                    StorageError::state_conflict(
                        QUANT_FEATURE_PARITY_STATE,
                        Some(&incident_state.state_id),
                        "open parity incident has no causal run",
                    )
                })?;
                let cause = require_run_on(&txn, cause_run_id).await?;
                causes.push((incident_state, cause));
            }
            validate_open_latch_recoveries(&causes, &recovery)?;
        } else {
            validate_bootstrap_recovery(&recovery)?;
        }
        let next = NewFeatureParityState {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: current.as_ref().and_then(|row| row.cause_run_id),
            recovery_run_id: Some(*recovery_run_id),
            previous_state_id: current.as_ref().map(|row| row.state_id),
            actor: actor.actor,
            acting_role: Some(RoleCode::new(actor.acting_role)),
            reason: actor.reason,
        };
        let inserted = QuantFeatureParityStateEntity::insert(next.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }
}

fn validate_new_run(run: &NewFeatureParityRun) -> Result<(), StorageError> {
    if run.status != FeatureParityRunStatus::Queued {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "new parity run must start queued",
        ));
    }
    if run.window_end <= run.window_start {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "window_end must be later than window_start",
        ));
    }
    if run.reason.trim().is_empty()
        || run.acting_role.as_str().trim().is_empty()
        || run.triggered_by.trim().is_empty()
        || run.feature_contract_hash.is_none()
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "reason, acting_role, triggered_by, and feature_contract_hash are required",
        ));
    }
    if run.total_count != 0
        || run.compared_count != 0
        || run.matched_count != 0
        || run.mismatched_count != 0
        || run.pending_materialization_count != 0
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "queued parity run counters must start at zero",
        ));
    }
    if run.started_at.is_some()
        || run.pending_since.is_some()
        || run.containment_completed_at.is_some()
        || run.finished_at.is_some()
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "queued parity run cannot pre-populate execution timestamps",
        ));
    }
    Ok(())
}

fn validate_completion(result: &CompleteFeatureParityRun) -> Result<(), StorageError> {
    if result.total_count < 0
        || result.compared_count < 0
        || result.matched_count < 0
        || result.mismatched_count < 0
        || result.pending_materialization_count < 0
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "parity counters must be non-negative",
        ));
    }
    if result.compared_count != result.matched_count + result.mismatched_count
        || result.total_count != result.compared_count + result.pending_materialization_count
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "compared_count must equal matched_count + mismatched_count and total_count must equal compared_count + pending_materialization_count",
        ));
    }
    if result.feature_contract_hash.is_none() {
        return Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "feature_contract_hash is required for every parity result",
        ));
    }
    match result.status {
        FeatureParityRunStatus::Passed
            if result.total_count > 0
                && result.matched_count == result.total_count
                && result.mismatched_count == 0
                && result.pending_materialization_count == 0
                && result.transform_hash.is_some()
                && result.failure_code.is_none()
                && result.failure_detail.is_none() =>
        {
            Ok(())
        }
        FeatureParityRunStatus::Mismatched if result.mismatched_count > 0 => Ok(()),
        FeatureParityRunStatus::PendingMaterialization
            if result.pending_materialization_count > 0 =>
        {
            Ok(())
        }
        FeatureParityRunStatus::Failed
            if result
                .failure_code
                .as_ref()
                .is_some_and(|code| !code.as_str().is_empty())
                && result
                    .failure_detail
                    .as_deref()
                    .is_some_and(|detail| !detail.is_empty()) =>
        {
            Ok(())
        }
        _ => Err(StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "completion counters/details do not satisfy the target status",
        )),
    }
}

fn validate_recovery_run(run: &FeatureParityRunInfo) -> Result<(), StorageError> {
    if run.kind != FeatureParityRunKind::Full
        || run.status != FeatureParityRunStatus::Passed
        || run.total_count <= 0
        || run.compared_count != run.total_count
        || run.matched_count != run.total_count
        || run.mismatched_count != 0
        || run.pending_materialization_count != 0
        || run.feature_contract_hash.is_none()
        || run.transform_hash.is_none()
        || run.finished_at.is_none()
    {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "latch recovery requires a finished, non-empty full pass with feature/transform commitments and zero mismatch/pending rows",
        ));
    }
    Ok(())
}

/// The guarded bootstrap state has no causal serving incident. Its first clear
/// transition therefore accepts only a subject-bound proof over one immutable
/// model and its exact frozen dataset; an unbound runtime replay cannot
/// initialize production admission. The run must also have completed strictly
/// after its own durable initialization, so a pre-existing/backfilled terminal
/// row cannot be used as a bootstrap permit.
fn validate_bootstrap_recovery(run: &FeatureParityRunInfo) -> Result<(), StorageError> {
    if run.report_id.is_some()
        || run.model_version_id.is_none()
        || run.training_dataset_id.is_none()
    {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "uninitialized latch recovery requires a frozen model+dataset-bound full proof",
        ));
    }
    let finished_at = run.finished_at.ok_or_else(|| {
        StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "bootstrap recovery run has no completion timestamp",
        )
    })?;
    if finished_at < run.created_at {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "bootstrap recovery run cannot complete before it was initialized",
        ));
    }
    Ok(())
}

/// Acknowledge is idempotent only for the exact recovery proof that minted the
/// current clear generation. The advisory latch lock serializes concurrent
/// cold-start requests; a waiter presenting a different run observes the first
/// clear row and is rejected instead of being reported as accepted.
fn validate_clear_acknowledgement(
    current: &FeatureParityStateInfo,
    recovery_run_id: &FeatureParityRunId,
) -> Result<(), StorageError> {
    if current.recovery_run_id.as_ref() == Some(recovery_run_id) {
        return Ok(());
    }
    Err(StorageError::state_conflict(
        QUANT_FEATURE_PARITY_STATE,
        Some(&current.state_id),
        format!(
            "parity latch is already clear from recovery run {}; acknowledgement with different run {recovery_run_id} is not idempotent",
            current
                .recovery_run_id
                .as_ref()
                .map_or_else(|| "<missing>".to_owned(), ToString::to_string)
        ),
    ))
}

#[cfg(test)]
fn validate_open_latch_recovery(
    state: &FeatureParityStateInfo,
    cause: &FeatureParityRunInfo,
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    validate_open_latch_recoveries(&[(state.clone(), cause.clone())], recovery)
}

/// Validate one governed proof against the complete unresolved incident set.
///
/// Every open transition since the latest clear generation remains an
/// independent deterministic cause. A later incident never replaces an older
/// one: the recovery proof must cover their window union, satisfy every subject
/// scope, and complete after the most recent incident was durably opened.
fn validate_open_latch_recoveries(
    incidents: &[(FeatureParityStateInfo, FeatureParityRunInfo)],
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    let Some((first_state, first_cause)) = incidents.first() else {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_STATE,
            Option::<&str>::None,
            "open parity latch has no unresolved deterministic causes",
        ));
    };
    let mut window_start = first_cause.window_start;
    let mut window_end = first_cause.window_end;
    let mut latest_opened_at = first_state.created_at;
    let mut cause_ids = Vec::with_capacity(incidents.len());

    for (state, cause) in incidents {
        if !matches!(
            cause.status,
            FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed
        ) {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                format!(
                    "open latch causal run must be mismatched/failed, found {}",
                    cause.status.as_str()
                ),
            ));
        }
        if cause.containment_completed_at.is_none() {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                "causal parity run has not completed report/intent containment",
            ));
        }
        validate_recovery_scope(cause, recovery)?;
        window_start = window_start.min(cause.window_start);
        window_end = window_end.max(cause.window_end);
        latest_opened_at = latest_opened_at.max(state.created_at);
        cause_ids.push(cause.run_id.to_string());
    }

    if recovery.window_start > window_start || recovery.window_end < window_end {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&recovery.run_id),
            format!(
                "recovery window [{}, {}) must cover unresolved cause union [{}, {}) for runs [{}]",
                recovery.window_start,
                recovery.window_end,
                window_start,
                window_end,
                cause_ids.join(", ")
            ),
        ));
    }
    let finished_at = recovery.finished_at.ok_or_else(|| {
        StorageError::state_conflict(
            QUANT_FEATURE_PARITY_RUN,
            Some(&recovery.run_id),
            "passed full parity run has no completion timestamp",
        )
    })?;
    if finished_at <= latest_opened_at {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_STATE,
            Some(&first_state.state_id),
            "recovery full run must complete after every unresolved incident was opened",
        ));
    }
    Ok(())
}

fn validate_recovery_scope(
    cause: &FeatureParityRunInfo,
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    match (
        cause.report_id.as_ref(),
        cause.model_version_id.as_ref(),
        cause.training_dataset_id.as_ref(),
    ) {
        // Serving runtime incidents (report-bound sampled or unbound scheduled
        // full) may only be cleared by an unbound runtime full replay. An
        // offline frozen-artifact proof is a different evidence population.
        (Some(_), Some(_), None) | (None, None, None) => {
            if recovery.report_id.is_some()
                || recovery.model_version_id.is_some()
                || recovery.training_dataset_id.is_some()
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEATURE_PARITY_RUN,
                    Some(&recovery.run_id),
                    "serving parity latch recovery requires an unbound runtime full run",
                ));
            }
        }
        // Offline model/dataset integrity incidents must be recovered against
        // that exact immutable subject, never by unrelated live traffic.
        (None, Some(model_version_id), Some(training_dataset_id)) => {
            if recovery.report_id.is_some()
                || recovery.model_version_id.as_ref() != Some(model_version_id)
                || recovery.training_dataset_id.as_ref() != Some(training_dataset_id)
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEATURE_PARITY_RUN,
                    Some(&recovery.run_id),
                    format!(
                        "offline parity recovery must bind exact model {model_version_id} and training dataset {training_dataset_id}"
                    ),
                ));
            }
        }
        _ => {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                "causal parity run has an invalid report/model/dataset scope",
            ));
        }
    }
    Ok(())
}

impl PgFeatureParityRepository {
    async fn acquire_latch_lock(txn: &DatabaseTransaction) -> Result<(), StorageError> {
        primitives::advisory_xact_lock(txn, LATCH_ADVISORY_LOCK_KEY).await
    }
}

impl PgFeatureParityRepository {
    pub(crate) async fn verify_clear_latch_generation(
        txn: &DatabaseTransaction,
        expected_state_id: &FeatureParityStateId,
    ) -> Result<(), StorageError> {
        Self::acquire_latch_lock(txn).await?;
        let current = current_state_on(txn).await?.ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_FEATURE_PARITY_STATE,
                Option::<&str>::None,
                "feature parity latch is uninitialized at risk-increasing commit",
            )
        })?;
        if current.state != FeatureParityLatchState::Clear || &current.state_id != expected_state_id
        {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_STATE,
                Some(&current.state_id),
                format!(
                    "parity commit permit {} is stale; current latch state is {} generation {}",
                    expected_state_id,
                    current.state.as_str(),
                    current.state_id
                ),
            ));
        }
        Ok(())
    }
}

impl PgFeatureParityRepository {
    async fn append_open_state(
        txn: &DatabaseTransaction,
        cause_run_id: &FeatureParityRunId,
        transition: FeatureParityStateTransition,
        reason: &str,
    ) -> Result<FeatureParityStateInfo, StorageError> {
        let current = current_state_on(txn).await?;
        if let Some(current) = current.as_ref()
            && current.state == FeatureParityLatchState::Open
            && current.cause_run_id.as_ref() == Some(cause_run_id)
        {
            return Ok(current.clone());
        }
        let next = NewFeatureParityState {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Open,
            transition,
            cause_run_id: Some(*cause_run_id),
            recovery_run_id: None,
            previous_state_id: current.as_ref().map(|row| row.state_id),
            actor: None,
            acting_role: None,
            reason: reason.to_owned(),
        };
        QuantFeatureParityStateEntity::insert(next.into_active_model())
            .exec_with_returning(txn)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}

/// Walk the append-only `previous_state_id` chain for the current open
/// generation and return each unique deterministic cause. Traversing the
/// explicit chain avoids timestamp ties and guarantees that a later incident
/// cannot hide an earlier, still-unresolved cause.
async fn open_incident_states_on<C>(
    db: &C,
    current: &FeatureParityStateInfo,
) -> Result<Vec<FeatureParityStateInfo>, StorageError>
where
    C: ConnectionTrait,
{
    let mut cursor = Some(current.clone());
    let mut seen_state_ids = BTreeSet::new();
    let mut seen_cause_ids = BTreeSet::new();
    let mut incidents = Vec::new();

    while let Some(state) = cursor {
        if !seen_state_ids.insert(state.state_id.to_string()) {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                "feature parity state ledger contains a previous_state_id cycle",
            ));
        }
        if state.state == FeatureParityLatchState::Clear {
            break;
        }
        if !matches!(
            state.transition,
            FeatureParityStateTransition::DeterministicMismatch
                | FeatureParityStateTransition::IntegrityFailure
        ) {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                format!(
                    "open parity generation contains non-incident transition {}",
                    state.transition.as_str()
                ),
            ));
        }
        let cause_run_id = state.cause_run_id.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                "open parity incident has no causal run",
            )
        })?;
        if seen_cause_ids.insert(cause_run_id.to_string()) {
            incidents.push(state.clone());
        }

        cursor = match state.previous_state_id {
            Some(previous_state_id) => Some(
                QuantFeatureParityStateEntity::find_by_id(previous_state_id)
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .map(Into::into)
                    .ok_or_else(|| {
                        StorageError::state_conflict(
                            QUANT_FEATURE_PARITY_STATE,
                            Some(&state.state_id),
                            format!(
                                "previous parity state {previous_state_id} is missing from the append-only ledger"
                            ),
                        )
                    })?,
            ),
            None => None,
        };
    }

    if incidents.is_empty() {
        return Err(StorageError::state_conflict(
            QUANT_FEATURE_PARITY_STATE,
            Some(&current.state_id),
            "open parity latch has no unresolved deterministic causes",
        ));
    }
    Ok(incidents)
}

async fn current_state_on<C>(db: &C) -> Result<Option<FeatureParityStateInfo>, StorageError>
where
    C: ConnectionTrait,
{
    QuantFeatureParityStateEntity::find()
        .order_by_desc(QuantFeatureParityStateColumn::CreatedAt)
        .order_by_desc(QuantFeatureParityStateColumn::StateId)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn find_run_on<C>(
    db: &C,
    run_id: &FeatureParityRunId,
) -> Result<Option<FeatureParityRunInfo>, StorageError>
where
    C: ConnectionTrait,
{
    QuantFeatureParityRunEntity::find_by_id(*run_id)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn require_run_on<C>(
    db: &C,
    run_id: &FeatureParityRunId,
) -> Result<FeatureParityRunInfo, StorageError>
where
    C: ConnectionTrait,
{
    find_run_on(db, run_id)
        .await?
        .ok_or_else(|| StorageError::not_found(QUANT_FEATURE_PARITY_RUN, run_id))
}

async fn run_transition_conflict<C>(
    db: &C,
    run_id: &FeatureParityRunId,
    target: &str,
) -> StorageError
where
    C: ConnectionTrait,
{
    match find_run_on(db, run_id).await {
        Ok(Some(run)) => StorageError::illegal_transition(
            QUANT_FEATURE_PARITY_RUN,
            Some(run_id),
            run.status.as_str(),
            target,
        ),
        Ok(None) => StorageError::not_found(QUANT_FEATURE_PARITY_RUN, run_id),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::types::{
        ContentHash, DiagnosticCode, ModelVersionId, RecommendationReportId, RoleCode,
        TrainingDatasetId,
    };

    use super::*;

    fn hash() -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", "a".repeat(64))).expect("content hash")
    }

    fn parity_run(
        kind: FeatureParityRunKind,
        status: FeatureParityRunStatus,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> FeatureParityRunInfo {
        let now = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        FeatureParityRunInfo {
            run_id: FeatureParityRunId::from_v7(),
            kind,
            status,
            window_start,
            window_end,
            report_id: None,
            model_version_id: None,
            training_dataset_id: None,
            triggered_by: "test".to_owned(),
            requested_by: Some("risk-owner".to_owned()),
            acting_role: RoleCode::new("risk_owner"),
            reason: "test".to_owned(),
            total_count: 2,
            compared_count: 2,
            matched_count: i64::from(status == FeatureParityRunStatus::Passed) * 2,
            mismatched_count: i64::from(status == FeatureParityRunStatus::Mismatched),
            pending_materialization_count: 0,
            feature_contract_hash: Some(hash()),
            transform_hash: (status == FeatureParityRunStatus::Passed).then(hash),
            failure_code: (status == FeatureParityRunStatus::Failed)
                .then(|| DiagnosticCode::new("integrity_failure")),
            failure_detail: (status == FeatureParityRunStatus::Failed).then(|| "failed".to_owned()),
            started_at: Some(now - Duration::minutes(1)),
            pending_since: None,
            containment_completed_at: None,
            finished_at: status.is_terminal().then_some(now),
            created_at: now - Duration::minutes(2),
            updated_at: now,
        }
    }

    fn open_state(
        cause_run_id: &FeatureParityRunId,
        created_at: DateTime<Utc>,
    ) -> FeatureParityStateInfo {
        FeatureParityStateInfo {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Open,
            transition: FeatureParityStateTransition::DeterministicMismatch,
            cause_run_id: Some(*cause_run_id),
            recovery_run_id: None,
            previous_state_id: None,
            actor: None,
            acting_role: None,
            reason: "deterministic mismatch".to_owned(),
            created_at,
        }
    }

    fn clear_state(recovery_run_id: &FeatureParityRunId) -> FeatureParityStateInfo {
        FeatureParityStateInfo {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: Some(*recovery_run_id),
            previous_state_id: None,
            actor: Some("risk-owner".to_owned()),
            acting_role: Some(RoleCode::new("risk_owner")),
            reason: "bootstrap parity verified".to_owned(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 11, 12, 1, 0).unwrap(),
        }
    }

    #[test]
    fn recovery_requires_completed_finish() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut cause = parity_run(
            FeatureParityRunKind::Sampled,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(1),
            base,
        );
        cause.report_id = Some(RecommendationReportId::from_v7());
        cause.model_version_id = Some(ModelVersionId::from_v7());
        let state = open_state(&cause.run_id, base + Duration::minutes(1));
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(2),
            base + Duration::minutes(2),
        );
        recovery.finished_at = Some(base + Duration::minutes(3));

        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("containment is mandatory");
        assert!(error.to_string().contains("containment"));

        cause.containment_completed_at = Some(base + Duration::seconds(30));
        recovery.window_start = cause.window_start + Duration::seconds(1);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must cover the causal start");
        assert!(error.to_string().contains("must cover"));

        recovery.window_start = cause.window_start;
        recovery.window_end = cause.window_end - Duration::seconds(1);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must cover the causal end");
        assert!(error.to_string().contains("must cover"));

        recovery.window_end = cause.window_end;
        recovery.finished_at = Some(state.created_at);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must finish after latch open");
        assert!(
            error
                .to_string()
                .contains("after every unresolved incident")
        );

        recovery.finished_at = Some(state.created_at + Duration::milliseconds(1));
        validate_open_latch_recovery(&state, &cause, &recovery)
            .expect("covered, contained, later full run is valid");
    }

    #[test]
    fn recovery_covers_not_latest() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut older = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(4),
            base - Duration::hours(3),
        );
        older.containment_completed_at = Some(base - Duration::hours(2));
        let mut latest = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Failed,
            base - Duration::hours(1),
            base,
        );
        latest.containment_completed_at = Some(base + Duration::seconds(1));
        let older_state = open_state(&older.run_id, base - Duration::hours(2));
        let mut latest_state = open_state(&latest.run_id, base + Duration::seconds(2));
        latest_state.transition = FeatureParityStateTransition::IntegrityFailure;

        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            latest.window_start,
            latest.window_end,
        );
        recovery.finished_at = Some(base + Duration::minutes(1));
        let incidents = vec![(latest_state, latest), (older_state, older)];

        let error = validate_open_latch_recoveries(&incidents, &recovery)
            .expect_err("latest-only proof must not erase the older cause");
        assert!(error.to_string().contains("unresolved cause union"));

        recovery.window_start = base - Duration::hours(4);
        validate_open_latch_recoveries(&incidents, &recovery)
            .expect("one full replay covers the complete pending window union");
    }

    #[test]
    fn recovery_run_no_mismatch() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(1),
            base,
        );
        validate_recovery_run(&recovery).expect("complete full pass");

        recovery.kind = FeatureParityRunKind::Sampled;
        assert!(validate_recovery_run(&recovery).is_err());
        recovery.kind = FeatureParityRunKind::Full;
        recovery.pending_materialization_count = 1;
        assert!(validate_recovery_run(&recovery).is_err());

        recovery.pending_materialization_count = 0;
        recovery.feature_contract_hash = None;
        assert!(validate_recovery_run(&recovery).is_err());

        recovery.feature_contract_hash = Some(hash());
        recovery.finished_at = None;
        assert!(validate_recovery_run(&recovery).is_err());
    }

    #[test]
    fn uninitialized_latch_requires_proof() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(1),
            base,
        );

        let error = validate_bootstrap_recovery(&recovery)
            .expect_err("unbound runtime full cannot initialize the latch");
        assert!(error.to_string().contains("model+dataset-bound"));

        recovery.model_version_id = Some(ModelVersionId::from_v7());
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        validate_bootstrap_recovery(&recovery).expect("frozen subject proof is valid");

        recovery.report_id = Some(RecommendationReportId::from_v7());
        assert!(validate_bootstrap_recovery(&recovery).is_err());
        recovery.report_id = None;

        recovery.finished_at = Some(recovery.created_at - Duration::microseconds(1));
        let error = validate_bootstrap_recovery(&recovery)
            .expect_err("proof must not finish before durable initialization");
        assert!(error.to_string().contains("before it was initialized"));

        recovery.finished_at = Some(recovery.created_at);
        validate_bootstrap_recovery(&recovery)
            .expect("equal PostgreSQL timestamp precision still represents a later transition");
    }

    #[test]
    fn clear_acknowledgement_idempotent_run() {
        let recovery_run_id = FeatureParityRunId::from_v7();
        let state = clear_state(&recovery_run_id);
        validate_clear_acknowledgement(&state, &recovery_run_id)
            .expect("same recovery acknowledgement is idempotent");

        let different = FeatureParityRunId::from_v7();
        let error = validate_clear_acknowledgement(&state, &different)
            .expect_err("concurrent different proof must not be accepted");
        assert!(error.to_string().contains("different run"));
    }

    #[test]
    fn serving_recovery_cannot_full() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut cause = parity_run(
            FeatureParityRunKind::Sampled,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(1),
            base,
        );
        cause.report_id = Some(RecommendationReportId::from_v7());
        cause.model_version_id = Some(ModelVersionId::from_v7());
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            cause.window_start,
            cause.window_end,
        );

        validate_recovery_scope(&cause, &recovery).expect("unbound runtime full");
        recovery.model_version_id = Some(ModelVersionId::from_v7());
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        let error = validate_recovery_scope(&cause, &recovery)
            .expect_err("offline subject proof cannot clear serving latch");
        assert!(error.to_string().contains("unbound runtime full"));
    }

    #[test]
    fn offline_recovery_requires_subject() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let model_version_id = ModelVersionId::from_v7();
        let training_dataset_id = TrainingDatasetId::from_v7();
        let mut cause = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Failed,
            base - Duration::hours(1),
            base,
        );
        cause.model_version_id = Some(model_version_id);
        cause.training_dataset_id = Some(training_dataset_id);
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            cause.window_start,
            cause.window_end,
        );
        recovery.model_version_id = Some(model_version_id);
        recovery.training_dataset_id = Some(training_dataset_id);

        validate_recovery_scope(&cause, &recovery).expect("exact offline subject");
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        let error = validate_recovery_scope(&cause, &recovery)
            .expect_err("different dataset cannot clear offline latch");
        assert!(error.to_string().contains("exact model"));
    }
}
