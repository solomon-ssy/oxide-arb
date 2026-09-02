//! Short real-database leases exercise renewal, handoff, and bounded drain.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Error as AnyhowError, Result, bail, ensure};
use async_trait::async_trait;
use quant_pivot_core::{
    app::{ports::feedback_mutation::FeedbackCycleFreezePlan, research_job::ResearchJobEngine},
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    service::feedback_coordinator::{
        FeedbackCoordinator, FeedbackCoordinatorBudget, FeedbackCoordinatorConfig,
        FeedbackCoordinatorDeps, FeedbackShadowCancellationPort, FeedbackStagePort,
        FeedbackStagePreparation, FeedbackStageSuccess,
    },
};
use quant_pivot_error::{QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::TrainingDatasetListQuery,
        quant::{
            FeedbackCycleInfo, FeedbackStageEventInput, FeedbackStageJobIdentity,
            NewFeedbackStageEvent, ResearchJobInfo, TrainingDatasetInfo,
        },
        runtime::CoreEventPublisher,
    },
    enums::quant::{DatasetPurpose, FeedbackCycleStatus, FeedbackStage, FeedbackStageEventKind},
    types::{FeedbackCycleId, WorkerId},
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgExecutionAttemptOutcomeRepository, PgFeedbackCycleRepository,
        PgFeedbackSchedulerRepository, PgModelRunRepository,
        PgRecommendationExecutionRollupRepository, PgResearchJobRepository,
        PgResolutionObservationRepository, PgSourceSliceRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, FeedbackCycleCasOutcome, FeedbackCycleGeneration,
        FeedbackCycleLeaseGuard, FeedbackCycleRepository, ModelRunRepository,
        SourceSliceRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    training::DatasetParquetCodec,
};
use sea_orm::DatabaseConnection;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{Duration, FixtureCancellationOwner};
use crate::{
    postgres::{setup_pg, with_postgres_suite},
    production_stack::{ProductionStackFixture, pause_feedback_schedulers},
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        research_browser_seed::BrowserResearchFixture,
    },
};

const LEASE_SECS: u64 = 3;
const CANCEL_REASON: &str = "fixture_owner_cancelled";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_lifecycle_stays_owned() -> Result<()> {
    Box::pin(with_postgres_suite(async {
        let (pool, _database) = setup_pg().await;
        let directory = TempDir::with_prefix("quant-pivot-cancellation-owner-")?;
        let artifacts: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(
            Arc::new(LocalArtifactStore::new(directory.path().to_owned())),
        ));
        let verification = timeout(
            Duration::from_mins(3),
            Box::pin(CancellationCase::verify(pool.connection(), &artifacts)),
        )
        .await
        .context("fixture cancellation lifecycle exceeded its bounded budget")
        .and_then(|result| result);
        drop(artifacts);
        let cleanup = directory.close();
        verification?;
        cleanup.context("remove cancellation fixture artifacts")
    }))
    .await?
}

struct CancellationCase<'a> {
    db: &'a DatabaseConnection,
    repository: PgFeedbackCycleRepository,
    cycle_id: FeedbackCycleId,
}

impl<'a> CancellationCase<'a> {
    async fn verify(db: &'a DatabaseConnection, artifacts: &Arc<dyn ArtifactStore>) -> Result<()> {
        let (_, research) =
            Box::pin(ProductionStackFixture::GovernedFeedback.seed_research_fixture(db, artifacts))
                .await?;
        pause_feedback_schedulers(db).await?;
        let case = Self {
            db,
            repository: PgFeedbackCycleRepository::new(db.clone()),
            cycle_id: research
                .governed_cancellation_cycle_id
                .context("governed cancellation cycle")?,
        };
        case.verify_lineage(&research, artifacts).await?;
        case.verify_shutdown().await?;
        case.verify_cancellation().await
    }

    async fn cycle(&self) -> Result<FeedbackCycleInfo> {
        self.repository
            .find_cycle(&self.cycle_id)
            .await?
            .context("read cancellation fixture cycle")
    }

    async fn verify_shutdown(&self) -> Result<()> {
        let claim = self
            .repository
            .claim_cycle(WorkerId::from_v7(), LEASE_SECS)
            .await?
            .context("claim short fixture lease")?;
        ensure!(claim.cycle.feedback_cycle_id == self.cycle_id);
        let original = claim.cycle.clone();
        let owner = FixtureCancellationOwner::start(self.db.clone(), claim, LEASE_SECS);
        let verification = self.renewal_holds(&original).await;
        let shutdown = owner.shutdown().await;
        verification?;
        shutdown?;
        let released = self.cycle().await?;
        ensure!(released.status == FeedbackCycleStatus::Running);
        ensure!(released.cancel_requested_at.is_none());
        ensure!(released.lease_owner == original.lease_owner);
        ensure!(released.generation > original.generation);
        ensure!(
            released
                .lease_expires_at
                .context("released lease deadline")?
                <= self.repository.database_time().await?
        );
        Ok(())
    }

    async fn renewal_holds(&self, original: &FeedbackCycleInfo) -> Result<()> {
        let expires = original
            .lease_expires_at
            .context("initial lease deadline")?;
        timeout(Duration::from_secs(10), async {
            loop {
                let cycle = self.cycle().await?;
                let now = self.repository.database_time().await?;
                ensure!(cycle.lease_owner == original.lease_owner);
                ensure!(cycle.status == FeedbackCycleStatus::Running);
                ensure!(cycle.lease_expires_at > Some(now));
                if now > expires && cycle.generation >= original.generation + 3 {
                    ensure!(
                        self.repository
                            .claim_cycle(WorkerId::from_v7(), LEASE_SECS)
                            .await?
                            .is_none(),
                        "healthy renewal must exclude the production recovery claimant"
                    );
                    return Ok(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("fixture failed to renew beyond its original short lease")?
    }

    async fn verify_cancellation(&self) -> Result<()> {
        let claim = self
            .repository
            .claim_cycle(WorkerId::from_v7(), LEASE_SECS)
            .await?
            .context("reclaim explicitly drained fixture lease")?;
        let original = claim.cycle.clone();
        ensure!(original.feedback_cycle_id == self.cycle_id);
        let owner = FixtureCancellationOwner::start(self.db.clone(), claim, LEASE_SECS);
        let verification = async {
            let cancelled = self.request_cancel().await?;
            ensure!(cancelled.status == FeedbackCycleStatus::Running);
            ensure!(cancelled.generation > original.generation);
            timeout(Duration::from_secs(5), async {
                loop {
                    let cycle = self.cycle().await?;
                    if cycle.generation > cancelled.generation
                        && cycle.lease_expires_at <= Some(self.repository.database_time().await?)
                    {
                        ensure!(cycle.cancel_requested_at == cancelled.cancel_requested_at);
                        return Ok::<_, AnyhowError>(());
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .context("cancel request did not release its fixture-owned lease")??;
            Ok::<_, AnyhowError>(cancelled)
        }
        .await;
        let shutdown = owner.shutdown().await;
        let cancelled = verification?;
        shutdown?;
        self.finish_cancelled(&cancelled).await
    }

    async fn request_cancel(&self) -> Result<FeedbackCycleInfo> {
        let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: self.cycle_id,
            event_sequence: 2,
            stage: FeedbackStage::TruthFreeze,
            event_kind: FeedbackStageEventKind::CancellationRequested,
            trigger_family: None,
            research_job_id: None,
            actor: Some("cancellation_owner_test".to_owned()),
            reason_code: Some(CANCEL_REASON.to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: self.repository.database_time().await?,
        })?;
        for attempt in 0..3 {
            let cycle = self.cycle().await?;
            match self
                .repository
                .request_cancel(FeedbackCycleGeneration::from(&cycle), event.clone())
                .await
            {
                Ok((FeedbackCycleCasOutcome::Applied(cycle), _)) => return Ok(cycle),
                Err(StorageError::StateConflict { .. }) if attempt < 2 => {
                    // The live renewal owner may have advanced this exact generation.
                }
                other => bail!("unexpected cancellation result: {other:?}"),
            }
        }
        bail!("cancellation CAS retry budget exhausted")
    }

    async fn finish_cancelled(&self, requested: &FeedbackCycleInfo) -> Result<()> {
        let stage = Arc::new(CancelOnlyStage::default());
        let coordinator = stage.coordinator(self.db.clone())?;
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let mut task = tokio::spawn(async move { coordinator.run(shutdown).await });
        let verification = timeout(Duration::from_secs(10), async {
            loop {
                let cycle = self.cycle().await?;
                if cycle.status == FeedbackCycleStatus::Cancelled {
                    ensure!(cycle.idempotency_key == requested.idempotency_key);
                    ensure!(cycle.idempotency_hash == requested.idempotency_hash);
                    ensure!(cycle.generation > requested.generation);
                    ensure!(cycle.cancel_requested_at == requested.cancel_requested_at);
                    ensure!(cycle.lease_owner.is_none() && cycle.lease_expires_at.is_none());
                    ensure!(cycle.completed_at >= requested.cancel_requested_at);
                    ensure!(cycle.terminal_reason_code.as_deref() == Some(CANCEL_REASON));
                    ensure!(stage.preparations.load(Ordering::SeqCst) == 0);
                    ensure!(stage.releases.load(Ordering::SeqCst) == 1);
                    return Ok::<_, AnyhowError>(());
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("production coordinator did not terminalize released cancellation")
        .and_then(|result| result);
        cancellation.cancel();
        let drained = timeout(Duration::from_secs(5), &mut task).await;
        if drained.is_err() {
            task.abort();
            let aborted = task.await;
            ensure!(
                aborted.is_err_and(|error| error.is_cancelled()),
                "timed-out production coordinator did not abort cleanly"
            );
        }
        verification?;
        drained.context("drain production cancellation coordinator")??;
        Ok(())
    }

    async fn verify_lineage(
        &self,
        research: &BrowserResearchFixture,
        artifacts: &Arc<dyn ArtifactStore>,
    ) -> Result<()> {
        for cycle_id in [research.feedback_cycle_id, self.cycle_id] {
            let cycle = self
                .repository
                .find_cycle(&cycle_id)
                .await?
                .context("seeded feedback cycle")?;
            let profile = cycle
                .profile_ref
                .resolve_builtin_research_profile()
                .map_err(AnyhowError::msg)?;
            let plan = FeedbackCycleFreezePlan::derive_at_cutoff(
                &profile,
                cycle.champion_model_spec_id,
                cycle.champion_model_spec_definition_hash,
                cycle.decision_policy_snapshot_id,
                cycle.decision_policy_snapshot_hash,
                cycle.label_cutoff,
            )?;
            let datasets = PgTrainingDatasetRepository::new(self.db.clone())
                .page(TrainingDatasetListQuery {
                    model_spec_id: Some(cycle.champion_model_spec_id),
                    purpose: Some(DatasetPurpose::Evaluation),
                    ..TrainingDatasetListQuery::default()
                })
                .await?;
            ensure!(
                !datasets.has_next,
                "bounded fixture dataset query must be complete"
            );
            let matching = datasets
                .items
                .iter()
                .filter(|dataset| dataset.pit_cutoff == cycle.label_cutoff)
                .collect::<Vec<_>>();
            ensure!(
                matching.len() == 1,
                "cycle requires one same-plan evaluation dataset"
            );
            ensure!(
                matching[0].source_lineage.decision_policy_snapshot_id
                    == cycle.decision_policy_snapshot_id
            );
            ensure!(
                matching[0].source_lineage.runtime_config_hash
                    == cycle.decision_policy_snapshot_hash
            );
            self.verify_dataset(matching[0], &plan, artifacts).await?;
        }
        self.verify_backtest(research).await?;
        Ok(())
    }

    async fn verify_backtest(&self, research: &BrowserResearchFixture) -> Result<()> {
        let dataset = PgTrainingDatasetRepository::new(self.db.clone())
            .find_by_id(&research.evaluation_dataset_id)
            .await?
            .context("backtest evaluation dataset")?;
        let report = PgBacktestReportRepository::new(self.db.clone())
            .find_by_id(&research.backtest_report_id)
            .await?
            .context("same-plan backtest")?;
        let run = PgModelRunRepository::new(self.db.clone())
            .find_by_id(&report.model_run_id)
            .await?
            .context("same-plan backtest model run")?;
        ensure!(report.window_start == dataset.window_start);
        ensure!(report.window_end == dataset.window_end);
        ensure!(run.window_start == dataset.window_start);
        ensure!(run.window_end == dataset.window_end);
        Ok(())
    }

    async fn verify_dataset(
        &self,
        dataset: &TrainingDatasetInfo,
        plan: &FeedbackCycleFreezePlan,
        artifacts: &Arc<dyn ArtifactStore>,
    ) -> Result<()> {
        ensure!(dataset.window_start == plan.evaluation().window_start());
        ensure!(dataset.window_end == plan.evaluation().cutoff());
        ensure!(dataset.source_lineage.source_window_start == plan.source_start());
        ensure!(dataset.source_lineage.source_window_end == plan.label_cutoff());
        ensure!(dataset.source_lineage.research_program_hash == plan.research_program_hash());
        let source = PgSourceSliceRepository::new(self.db.clone())
            .find_by_id(&dataset.source_slice_id)
            .await?
            .context("frozen source ledger")?;
        ensure!(source.window_start == plan.source_start());
        ensure!(source.window_end == plan.label_cutoff());
        ensure!(source.pit_cutoff == plan.label_cutoff());
        ensure!(source.research_program_hash == plan.research_program_hash());
        let source_manifest = source.manifest.context("frozen source manifest")?;
        dataset.source_lineage.verify_manifest(&source_manifest)?;
        ensure!(source_manifest.window_start == plan.source_start());
        ensure!(source_manifest.window_end == plan.label_cutoff());
        ensure!(source_manifest.research_program_hash == plan.research_program_hash());
        let bytes = artifacts
            .get(
                dataset
                    .parquet_uri
                    .as_ref()
                    .context("dataset artifact URI")?,
            )
            .await?;
        let decoded = DatasetParquetCodec::decode_with_manifest(&bytes)?;
        ensure!(Some(&decoded.manifest) == dataset.manifest.as_ref());
        ensure!(decoded.examples.len() == 80);
        ensure!(decoded.examples.iter().all(|example| {
            example.decision_at() >= dataset.window_start
                && example.decision_at() < dataset.window_end
                && example.decision_at() < plan.label_cutoff()
        }));
        Ok(())
    }
}

#[derive(Default)]
struct CancelOnlyStage {
    preparations: AtomicUsize,
    releases: AtomicUsize,
}

impl CancelOnlyStage {
    fn coordinator(self: &Arc<Self>, db: DatabaseConnection) -> Result<FeedbackCoordinator> {
        let (events, _receiver) = CoreEventPublisher::bounded(16);
        Ok(FeedbackCoordinator::new(FeedbackCoordinatorDeps {
            cycles: Arc::new(PgFeedbackCycleRepository::new(db.clone())),
            scheduler: Arc::new(PgFeedbackSchedulerRepository::new(db.clone())),
            resolutions: Arc::new(PgResolutionObservationRepository::new(db.clone())),
            attempts: Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
            rollups: Arc::new(PgRecommendationExecutionRollupRepository::new(db.clone())),
            jobs: ResearchJobEngine::new(Arc::new(PgResearchJobRepository::new(db)), events),
            stages: Arc::clone(self) as Arc<dyn FeedbackStagePort>,
            shadow_cancellation: Arc::clone(self) as Arc<dyn FeedbackShadowCancellationPort>,
            metrics: Arc::new(MetricsHub::new()),
            alerts: Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
                Vec::new(),
            )))),
            config: FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
                poll_interval: Duration::from_secs(1),
                lease_heartbeat: Duration::from_secs(1),
                lease_ttl: Duration::from_secs(LEASE_SECS),
                max_inflight: 1,
                stuck_after: Duration::from_secs(4),
                alert_timeout: Duration::from_secs(1),
                alert_dedupe_secs: 60,
                shutdown_drain: Duration::from_secs(2),
            })?,
        }))
    }
}

#[async_trait]
impl FeedbackStagePort for CancelOnlyStage {
    async fn prepare(
        &self,
        _cycle: &FeedbackCycleInfo,
        _lease: FeedbackCycleLeaseGuard,
        _identity: FeedbackStageJobIdentity,
    ) -> QuantResult<FeedbackStagePreparation> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        Err(FeedbackError::InvalidCoordinatorState {
            detail: "cancelled fixture must not enqueue research work".to_owned(),
        }
        .into())
    }

    async fn succeeded(
        &self,
        _cycle: &FeedbackCycleInfo,
        _job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        Err(FeedbackError::InvalidCoordinatorState {
            detail: "cancelled fixture has no successful research stage".to_owned(),
        }
        .into())
    }
}

#[async_trait]
impl FeedbackShadowCancellationPort for CancelOnlyStage {
    async fn release_cycle(&self, cycle: &FeedbackCycleInfo, reason_code: &str) -> QuantResult<()> {
        if cycle.cancel_requested_at.is_none() || reason_code != CANCEL_REASON {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator cancellation lost its exact committed request".to_owned(),
            }
            .into());
        }
        self.releases.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
