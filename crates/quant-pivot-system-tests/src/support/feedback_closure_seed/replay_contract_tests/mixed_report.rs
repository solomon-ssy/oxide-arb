//! Real source-side economics and risk rejection, not a feedback-closure canary.

mod boundary_parity;
mod pit_readiness;
mod scenario_refit;
mod source_side;

use std::{sync::Arc, time::Duration as StdDuration};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    ingest::{data_plane_index::DataPlane, market_registry::MarketRegistry},
    pit::platform::ch_historical::DurablePitSource,
    prefetch::market_candidates::MarketCandidateProvider,
    report::{BuildReportRequest, ReportBuilder, ReportTrigger},
};
use quant_pivot_models::{
    config::ClickHouseConfig,
    domain::{
        data_plane::{DecisionClock, DecisionSource, ExchangeHistoryFrontier},
        quant::RouteHistoryLineage,
    },
    runtime_config::{BuyModelRoute, CryptoDomainConfig, WeatherDomainConfig},
    types::{
        CorrelationId, DomainInstrumentKey, FinalizedExecutionEvidence, ReportRunId, Usd,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCatalogLedgerRepository, PgClobMarketInfoRepository, PgExchangeHistoryRepository,
        PgFeatureRepository, PgMarketLinkageRepository, PgPolicyRepository,
    },
    traits::{ClobMarketInfoRepository, ExchangeHistoryRepository, FeatureRepository},
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use serde_json::{json, to_value};
use tempfile::TempDir;
use tokio::{
    task::JoinSet,
    time::{Instant, timeout_at},
};

use self::{scenario_refit::ScenarioRefit, source_side::SourceSideComposer};

use super::super::{
    ClosureFactWriters, ClosureHistoryInterval, ClosureMarketSource, ClosureSeedSetup,
    CohortMarketFacts, CohortSourceFacts, FeedbackClosureFixture, FeedbackClosureSeedRequest,
    PreparedMarketSources, closure_crypto_observation, closure_execution_history_rows,
    prepare_market_sources, seed_catalog_baseline, seed_report_catalogs,
};
use crate::{
    postgres::PostgresClock,
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture,
        execution_pg_seed::{
            CalibrationEvidencePreset, FeedbackServingFixtureConfig, seed_feedback_serving_infra,
        },
        portfolio_scenario_fixtures::finalize_feedback_portfolio,
        production_history::{DeterministicPolygonChain, MODEL_CONFIRMATION_BLOCKS},
        report_pipeline_harness::{
            HarnessOptions, ReportBuilderHarnessInput, ReportEvidenceWriters, account_factory,
            build_model_runner, build_report_builder, calibration_artifact_loader,
            ensure_harness_execution_account, publish_pooled_control_model,
        },
        research_browser_seed::seed_closure_feedback_research,
        trade_policy_fixtures::FixtureBookTiming,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crypto_cutoff_reads_visible() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let result = tokio::time::timeout(StdDuration::from_secs(30), async {
        let facts = ClosureFactWriters::connect(&stack.clickhouse_config).await?;
        let decision_at = "2026-07-01T12:00:02Z".parse::<DateTime<Utc>>()?;
        let boundary = DecisionClock::new(FixtureBookTiming::REPORT_LAG_SECS).serving_boundary(
            decision_at,
            CryptoDomainConfig::default().availability_lag_secs,
            WeatherDomainConfig::default().availability_lag_secs,
        )?;
        let cutoff = boundary.cutoff_for(DecisionSource::DomainCrypto);
        let row = closure_crypto_observation(&boundary)?;
        ensure!(
            facts
                .fact_read
                .domain_observation_at(
                    &row.instrument_key,
                    &row.metric,
                    cutoff.timestamp_millis(),
                    decision_at.timestamp_millis(),
                )
                .await?
                .is_none(),
            "fresh cutoff fixture must not contain a previous candle"
        );
        ClosureFactWriters::write_batches(facts.domain_observations.as_ref(), vec![row.clone()])
            .await?;
        let visible = facts
            .fact_read
            .domain_observation_at(
                &row.instrument_key,
                &row.metric,
                cutoff.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await?
            .with_context(|| {
                format!(
                    "closed Crypto candle is not visible: decision={decision_at} cutoff={cutoff} event_ms={} publish_ms={} ingestion_ms={}",
                    row.event_time, row.publish_time, row.ingestion_time
                )
            })?;
        ensure!(to_value(&visible)? == to_value(&row)?, "PIT candle readback changed its exact source row");

        // Independent instrument identities prevent another valid candle from
        // hiding a late-publication or late-ingestion rejection.
        for (instrument, late_publication) in [
            ("BINANCE:ETHUSDT:1m", true),
            ("BINANCE:SOLUSDT:1m", false),
        ] {
            let mut future = row.clone();
            future.instrument_key = DomainInstrumentKey::new(instrument);
            if late_publication {
                future.publish_time = cutoff.timestamp_millis() + 1;
            } else {
                future.ingestion_time = decision_at.timestamp_millis() + 1;
            }
            let instrument = future.instrument_key.clone();
            ClosureFactWriters::write_batches(facts.domain_observations.as_ref(), vec![future])
                .await?;
            ensure!(
                facts
                    .fact_read
                    .domain_observation_at(
                        &instrument,
                        &row.metric,
                        cutoff.timestamp_millis(),
                        decision_at.timestamp_millis(),
                    )
                    .await?
                    .is_none(),
                "future Crypto source became visible: {instrument} publication={late_publication}"
            );
        }
        Ok::<(), AnyhowError>(())
    })
    .await
    .context("Crypto cutoff proof exceeded its bounded read budget")
    .and_then(|result| result);
    let shutdown = Box::pin(stack.shutdown()).await;
    result?;
    shutdown?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_side_preserves_risk() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let directory = TempDir::with_prefix("quant-pivot-mixed-sizing-")?;
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(Arc::new(
        LocalArtifactStore::new(directory.path().to_owned()),
    )));
    let deadline = Instant::now() + StdDuration::from_mins(10);
    let result = Box::pin(MixedReportFixture::run(&stack, &store, deadline)).await;
    drop(store);
    let shutdown = Box::pin(stack.shutdown()).await;
    let cleanup = directory.close();
    result?;
    shutdown?;
    cleanup.context("remove mixed report artifacts")?;
    Ok(())
}

struct MixedReportFixture {
    setup: ClosureSeedSetup,
    fixture: FeedbackClosureFixture,
    polygon: Arc<DeterministicPolygonChain>,
}

impl MixedReportFixture {
    async fn run(
        stack: &SystemStack,
        store: &Arc<dyn ArtifactStore>,
        deadline: Instant,
    ) -> Result<()> {
        let fixture = Box::pin(Self::bootstrap(
            stack.postgres.connection().clone(),
            stack.clickhouse_config.clone(),
            Arc::clone(store),
            deadline,
        ))
        .await?;
        timeout_at(deadline, Box::pin(fixture.verify_risk(stack, store)))
            .await
            .context("single mixed report exceeded its bounded fixture budget")?
    }

    async fn verify_risk(&self, stack: &SystemStack, store: &Arc<dyn ArtifactStore>) -> Result<()> {
        let db = stack.postgres.connection();
        ensure_harness_execution_account(db).await;
        let evidence = ReportEvidenceWriters::default();
        let model_runner = Box::pin(build_model_runner(
            db,
            store,
            Arc::clone(&evidence.model_inputs),
        ))
        .await;
        let registry = Arc::new(MarketRegistry::new(Arc::new(DataPlane::new())));
        let accounts = account_factory(
            db,
            registry,
            &HarnessOptions {
                collateral: Usd::new(dec!(555.56)),
                ..HarnessOptions::default()
            },
        );
        let facts = Arc::clone(&self.setup.fact_writers.fact_read);
        let pit = Arc::new(DurablePitSource::new(
            Arc::clone(&facts),
            Arc::new(PgCatalogLedgerRepository::new(db.clone())),
            Arc::new(PgClobMarketInfoRepository::new(db.clone())),
        ));
        let crypto = self
            .setup
            .report_universe
            .routes
            .iter()
            .find(|route| route.route == BuyModelRoute::Crypto)
            .context("Crypto route")?;
        let composer = SourceSideComposer::load(db, &crypto.version).await?;
        let builder = build_report_builder(ReportBuilderHarnessInput {
            db,
            runtime_config_repo: Arc::new(PgPolicyRepository::new(db.clone())),
            candidate_provider: Arc::new(MarketCandidateProvider::new(
                pit,
                Arc::new(PgMarketLinkageRepository::new(db.clone())),
                Arc::clone(&facts),
            )),
            model_runner,
            account_factory: accounts,
            artifact_store: Arc::clone(store),
            calibration_loader: calibration_artifact_loader(db),
            feature_writer: evidence.features,
            exchange_history_repo: Arc::new(PgExchangeHistoryRepository::new(db.clone())),
            fact_read: facts,
            composer: Arc::new(composer),
        });
        let decision_at = Self::future_crypto_decision(db).await?;
        self.persist_sources(db, decision_at).await?;
        println!(
            "mixed-report-policy {}",
            json!({
                "admission": self.setup.policy.snapshot.execution_risk.portfolio.admission,
                "account_collateral_usd": "555.56",
                "market_count": self.fixture.report_market_ids().len(),
                "book_count": self.fixture.report_book_snapshots()?.len(),
                "decision_at": decision_at,
                "knowledge_lag_secs": FixtureBookTiming::REPORT_LAG_SECS,
                "crypto_lag_secs": self.setup.policy.snapshot.profile_artifacts.domain.definition.crypto.availability_lag_secs,
            })
        );
        let report = builder
            .build(BuildReportRequest {
                report_run_id: ReportRunId::from_v7(),
                trigger: ReportTrigger::AdHoc {
                    request_id: CorrelationId::new("mixed-sizing-regression"),
                },
                trigger_time: decision_at,
                top_n_override: Some(10),
                knowledge_lag_secs_override: Some(FixtureBookTiming::REPORT_LAG_SECS),
            })
            .await?;
        let vector_ids = report
            .transaction
            .data_quality_snapshot
            .tokens_json
            .0
            .iter()
            .map(|record| record.feature_vector_id)
            .collect::<Vec<_>>();
        let vectors = PgFeatureRepository::new(db.clone())
            .find_by_ids(&vector_ids)
            .await?;
        ensure!(
            vectors.len() == 10
                && vectors.iter().all(|vector| vector
                    .payload
                    .generic
                    .contains_key(&FeatureName::new("ts.price_reversal"))),
            "all ten markets must retain real computed features"
        );
        ensure!(
            report.funnel_rows.len() == 10,
            "mixed report did not retain all ten market funnel rows"
        );
        // This pre-promotion Weather source is not the full retained forward
        // history. The canonical retained harness, not this risk regression,
        // owns the mandatory ten-published-recommendation closure criterion.
        Ok(())
    }

    async fn future_crypto_decision(db: &DatabaseConnection) -> Result<DateTime<Utc>> {
        // Exercise the actual minute rollover while keeping the source and
        // report clocks real. The next future minute+2 is less than 60s away;
        // the 62s owner budget includes bounded database reads and polling.
        tokio::time::timeout(StdDuration::from_secs(62), async {
            let now = tokio::time::timeout(StdDuration::from_secs(2), db.statement_time())
                .await
                .context("read mixed-report scheduling clock")?;
            let minute = now.timestamp().div_euclid(60) * 60;
            let current = DateTime::from_timestamp(minute + 2, 0)
                .context("mixed minute boundary is outside UTC")?;
            let decision_at = if current > now {
                current
            } else {
                current + Duration::minutes(1)
            };
            loop {
                let observed = tokio::time::timeout(StdDuration::from_secs(2), db.statement_time())
                    .await
                    .context("read mixed-report minute boundary clock")?;
                if observed >= decision_at {
                    return Ok::<DateTime<Utc>, AnyhowError>(decision_at);
                }
                tokio::time::sleep(StdDuration::from_millis(20)).await;
            }
        })
        .await
        .context("mixed-report real minute boundary exceeded its 62s budget")?
    }

    /// Own one runtime task so deep model decoding does not inherit caller poll frames.
    async fn bootstrap(
        db: DatabaseConnection,
        clickhouse_config: ClickHouseConfig,
        artifacts: Arc<dyn ArtifactStore>,
        deadline: Instant,
    ) -> Result<Self> {
        ensure!(
            Instant::now() < deadline,
            "owned bootstrap deadline already elapsed"
        );
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { Box::pin(Self::load(&db, &clickhouse_config, &artifacts)).await });
        let joined = match timeout_at(deadline, tasks.join_next()).await {
            Ok(joined) => joined.context("owned bootstrap task disappeared")?,
            Err(elapsed) => {
                tasks.abort_all();
                while let Some(drained) = tasks.join_next().await {
                    if let Err(error) = drained
                        && !error.is_cancelled()
                    {
                        return Err(error).context("timed-out bootstrap task failed during drain");
                    }
                }
                return Err(elapsed)
                    .context("owned bootstrap exceeded the original fixture deadline");
            }
        };
        let fixture = joined.context("owned bootstrap task failed")??;
        ensure!(
            Instant::now() < deadline,
            "owned bootstrap completed after its original fixture deadline"
        );
        Ok(fixture)
    }

    async fn load(
        db: &DatabaseConnection,
        clickhouse_config: &ClickHouseConfig,
        artifacts: &Arc<dyn ArtifactStore>,
    ) -> Result<Self> {
        let governed = Box::pin(seed_feedback_serving_infra(
            db,
            artifacts,
            FeedbackServingFixtureConfig {
                book_timing: FixtureBookTiming::closure()?,
                required_shadow_window_secs: 900,
                shadow_diff_threshold: Decimal::ONE,
                feedback_budget_usd: dec!(555.56),
                outcome_reconciliation_enabled: true,
                outcome_reconciliation_sweep_secs: 10,
                ad_hoc_report_enabled: true,
                knowledge_lag_secs: 90,
            },
        ))
        .await;
        Box::pin(publish_pooled_control_model(
            db,
            artifacts,
            governed.pooled_model_version_id,
            governed.template.decision_policy_snapshot_id,
        ))
        .await;
        let research = Box::pin(seed_closure_feedback_research(
            db,
            artifacts,
            &governed.template,
            governed.champion_model_version_id,
        ))
        .await?;
        let infra = Box::pin(finalize_feedback_portfolio(
            db,
            artifacts,
            governed.template,
            research.model_version_id,
            research.evaluation_dataset_id,
            FixtureBookTiming::closure()?,
            CalibrationEvidencePreset::WeakBinarySignal,
        ))
        .await?;
        let infra = Box::pin(ScenarioRefit::apply(db, artifacts, infra)).await?;
        let polygon = Arc::new(DeterministicPolygonChain::new());
        let head = polygon.head();
        let execution_evidence = FinalizedExecutionEvidence::runtime(
            true,
            Some(
                head.block_number
                    .checked_sub(MODEL_CONFIRMATION_BLOCKS)
                    .context("confirmed head")?,
            ),
            Some(DateTime::from_timestamp(head.timestamp, 0).context("head clock")?),
        );
        let setup = Box::pin(ClosureSeedSetup::load(&FeedbackClosureSeedRequest {
            db,
            clickhouse_config,
            artifact_store: artifacts,
            infra: &infra,
            champion_model_version_id: research.model_version_id,
            historical_feedback_cycle_id: research.feedback_cycle_id,
            report_resolves_at: db.statement_time().await + Duration::hours(48),
            runtime_finalized_execution_evidence: execution_evidence.clone(),
            polygon: &polygon,
        }))
        .await?;
        seed_catalog_baseline(db, db.statement_time().await - Duration::days(2)).await?;
        let cohorts = Box::pin(seed_report_catalogs(
            db,
            setup.capability_registry_hash,
            db.statement_time().await + Duration::hours(48),
        ))
        .await?;
        let fixture = FeedbackClosureFixture::new(
            research.feedback_cycle_id,
            &[],
            cohorts,
            Arc::from([]),
            Arc::clone(&setup.fact_writers),
            Arc::clone(&setup.replay),
            execution_evidence,
        );
        Ok(Self {
            setup,
            fixture,
            polygon,
        })
    }

    async fn persist_sources(
        &self,
        db: &DatabaseConnection,
        decision_at: DateTime<Utc>,
    ) -> Result<()> {
        let sources = self
            .fixture
            .report_cohorts
            .iter()
            .flat_map(|cohort| cohort.markets.iter().cloned())
            .collect::<Vec<ClosureMarketSource>>();
        let lag = FixtureBookTiming::REPORT_LAG_SECS;
        let PreparedMarketSources {
            facts,
            market_infos,
            ..
        } = prepare_market_sources(
            &self.setup.replay,
            &sources,
            decision_at,
            lag,
            Decimal::ZERO,
        )?;
        let history_repo = PgExchangeHistoryRepository::new(db.clone());
        let plan = history_repo
            .load_plan(137)
            .await?
            .context("mixed source history plan")?;
        let execution = closure_execution_history_rows(
            &sources,
            decision_at,
            lag,
            Decimal::ZERO,
            &self.polygon,
            ClosureHistoryInterval {
                frontier: ExchangeHistoryFrontier::Activation,
                from_block: Some(u64::try_from(plan.activation_from_block)?),
                policy_hash: Some(plan.policy_hash),
            },
        )?;
        let market_repo = PgClobMarketInfoRepository::new(db.clone());
        for info in market_infos {
            market_repo.insert_observation(info).await?;
        }
        let CohortMarketFacts {
            books,
            microstructure,
            sessions,
            domain_observations,
            weather_observations,
            weather_forecasts,
        } = facts;
        let history = self
            .setup
            .fact_writers
            .commit_sources(CohortSourceFacts {
                books,
                microstructure,
                sessions,
                domain_observations,
                weather_observations,
                weather_forecasts,
                executions: execution.executions,
                participants: execution.participants,
                acceptances: vec![execution.acceptance],
                history: execution.history,
            })
            .await?;
        ensure!(
            matches!(
                history.route_history(db, decision_at).await?,
                RouteHistoryLineage::Runtime { .. }
            ),
            "mixed source must freeze actual Runtime head"
        );
        Ok(())
    }
}
