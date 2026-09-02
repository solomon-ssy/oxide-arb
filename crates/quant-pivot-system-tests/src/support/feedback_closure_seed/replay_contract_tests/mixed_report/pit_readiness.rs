//! Real ingress readiness must include the complete cohort at the report PIT boundary.

use std::{collections::HashMap, mem, sync::Arc, time::Duration};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use quant_pivot_api::ws::{
    ClobWsManager, ClobWsManagerHooks, SubscriptionSource, TokenKeyResolver,
    TransportRetirementHook, WsSessionInvalidationHook,
};
use quant_pivot_core::{
    app::{InfraBundle, task_id::TaskId, task_registry::AppRunner},
    ingest::{
        book_store::BookStore,
        data_pipeline::{DataPipeline, DataPipelineDeps},
        data_plane_index::DataPlane,
        event_source::PipelineEventSource,
        market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
    service::system_status_nudge::SystemStatusNudge,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig,
    domain::data_plane::{DecisionClock, DecisionSource},
    types::TokenId,
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::MixedReportFixture;
use crate::{
    performance::upstream::DeterministicClobServer,
    postgres::PostgresClock,
    production_stack::ProductionStack,
    stack::SystemStack,
    support::{
        artifact_store::VersionedArtifactStoreFixture, trade_policy_fixtures::FixtureBookTiming,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cohort_is_pit_visible() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let directory = TempDir::with_prefix("quant-pivot-pit-readiness-")?;
    let store: Arc<dyn ArtifactStore> = Arc::new(VersionedArtifactStoreFixture::new(Arc::new(
        LocalArtifactStore::new(directory.path().to_owned()),
    )));
    let result = Box::pin(BookIngressFixture::verify(&stack, &store)).await;
    drop(store);
    let shutdown = Box::pin(stack.shutdown()).await;
    let cleanup = directory.close();
    println!(
        "pit-cohort-cleanup stack_ok={} artifacts_ok={}",
        shutdown.is_ok(),
        cleanup.is_ok()
    );
    result?;
    shutdown?;
    cleanup.context("remove this PIT cohort fixture's artifacts")?;
    Ok(())
}

struct BookIngressFixture {
    clob: DeterministicClobServer,
    root: CancellationToken,
    infra: InfraBundle,
    books: Arc<BookStore>,
    markets: Arc<MarketRegistry>,
    manager: Arc<ClobWsManager>,
    runner: JoinHandle<QuantResult<()>>,
    tokens: Vec<TokenId>,
}

impl BookIngressFixture {
    async fn verify(stack: &SystemStack, store: &Arc<dyn ArtifactStore>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_mins(10);
        let bootstrap = Box::pin(MixedReportFixture::bootstrap(
            stack.postgres.connection().clone(),
            stack.clickhouse_config.clone(),
            Arc::clone(store),
            deadline,
        ));
        println!(
            "pit-cohort-phase bootstrap_start fixture_bytes={} ingress_bytes={} bootstrap_future_bytes={}",
            mem::size_of::<MixedReportFixture>(),
            mem::size_of::<Self>(),
            mem::size_of_val(bootstrap.as_ref().get_ref())
        );
        let fixture = bootstrap.await?;
        println!("pit-cohort-phase bootstrap_complete");
        Box::pin(Self::run_prepared(stack, &fixture, deadline)).await
    }

    async fn run_prepared(
        stack: &SystemStack,
        fixture: &MixedReportFixture,
        deadline: Instant,
    ) -> Result<()> {
        let ingress = timeout_at(deadline, Box::pin(Self::start(stack, fixture)))
            .await
            .context("PIT ingress setup exceeded its original fixture budget")??;
        Box::pin(Self::verify_and_shutdown(ingress, stack, fixture, deadline)).await
    }

    async fn verify_and_shutdown(
        ingress: Self,
        stack: &SystemStack,
        fixture: &MixedReportFixture,
        deadline: Instant,
    ) -> Result<()> {
        let result = timeout_at(deadline, Box::pin(ingress.verify_boundary(stack, fixture)))
            .await
            .context("PIT cohort observation exceeded its original fixture budget")
            .and_then(|result| result);
        let shutdown = Box::pin(ingress.shutdown()).await;
        println!("pit-cohort-ingress-cleanup ok={}", shutdown.is_ok());
        result.and(shutdown)
    }

    async fn start(stack: &SystemStack, fixture: &MixedReportFixture) -> Result<Self> {
        println!("pit-cohort-phase ingress_start");
        let clob = DeterministicClobServer::start_keepalive(Duration::from_secs(
            FixtureBookTiming::FEED_PERIOD_SECS,
        ))
        .await?;
        let tokens = fixture
            .fixture
            .report_book_snapshots()?
            .into_iter()
            .map(|snapshot| snapshot.token_id)
            .collect::<Vec<_>>();
        ensure!(
            tokens.len() == 20,
            "real binary markets require all twenty tokens"
        );
        let root = CancellationToken::new();
        let metrics = Arc::new(MetricsHub::new());
        let mut deploy = DeployConfig::default();
        deploy.db.postgres = stack.postgres_config.clone();
        deploy.db.clickhouse = stack.clickhouse_config.clone();
        deploy.cache.redis = stack.redis_config.clone();
        deploy.polymarket.clob_ws_url = clob.base_url();
        deploy.market_data.websocket.engine_max_subscription_tokens = tokens.len();
        println!("pit-cohort-phase infra_assemble_start");
        let infra = Box::pin(InfraBundle::assemble(&deploy, Arc::clone(&metrics))).await?;
        println!("pit-cohort-phase infra_assemble_complete");
        let plane = Arc::new(DataPlane::new());
        let books = Arc::new(BookStore::new(Arc::clone(&plane), Arc::clone(&metrics)));
        let markets = Arc::new(MarketRegistry::new(Arc::clone(&plane)));
        markets.register_events(
            fixture
                .fixture
                .catalogs
                .iter()
                .map(|catalog| catalog.registry_event.clone()),
        );
        markets.register_markets(
            fixture
                .fixture
                .catalogs
                .iter()
                .flat_map(|catalog| catalog.markets.iter().map(|market| market.info.clone()))
                .collect(),
        );
        let invalidation_books = Arc::clone(&books);
        let invalidation: WsSessionInvalidationHook = Arc::new(move |tokens| {
            invalidation_books.invalidate_ids(tokens);
        });
        let (retirement_tx, retirement_rx) = flume::bounded(1_024);
        let retirement_root = root.clone();
        let retirement: TransportRetirementHook = Arc::new(move |event| {
            if retirement_tx
                .send_timeout(event, Duration::from_millis(100))
                .is_err()
            {
                retirement_root.cancel();
            }
        });
        let manager = Arc::new(ClobWsManager::new(
            &deploy.polymarket,
            &deploy.market_data.websocket,
            root.clone(),
            plane as Arc<dyn TokenKeyResolver>,
            ClobWsManagerHooks {
                on_session_invalidated: Some(invalidation),
                on_transport_retired: Some(retirement),
                ..ClobWsManagerHooks::default()
            },
        ));
        let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
            event_source: Arc::clone(&manager) as Arc<dyn PipelineEventSource>,
            book_store: Arc::clone(&books),
            market_registry: Arc::clone(&markets),
            metrics,
            book_fact_writer: Arc::clone(&infra.book_fact_writer),
            shutdown: root.clone(),
            status_nudge: SystemStatusNudge::default(),
            retirement_rx,
            durable_publish_observer: None,
        }));
        let mut runner = AppRunner::new(root.clone());
        infra.register_fact_writer_tasks(&mut runner);
        runner.spawn_critical(TaskId::DataPipeline, move |_token| async move {
            pipeline.run().await
        });
        println!("pit-cohort-phase runner_start");
        let runner = tokio::spawn(runner.run());
        manager.subscribe_tokens(SubscriptionSource::Engine, &tokens);
        Ok(Self {
            clob,
            root,
            infra,
            books,
            markets,
            manager,
            runner,
            tokens,
        })
    }

    async fn verify_boundary(
        &self,
        stack: &SystemStack,
        fixture: &MixedReportFixture,
    ) -> Result<()> {
        let refresh = self.clob.refresh_handle();
        refresh
            .wait_for_token_owners(&self.tokens, Duration::from_mins(2))
            .await?;
        let database_before = stack.postgres.connection().statement_time().await;
        println!("pit-cohort-phase install_start database_before={database_before}");
        let install = ProductionStack::install_report_books(
            stack.postgres.connection(),
            &fixture.fixture,
            &refresh,
            FixtureBookTiming::REPORT_LAG_SECS,
        )
        .await;
        let returned_at = match install {
            Ok(returned_at) => returned_at,
            Err(error) => {
                let memory = self
                    .books
                    .diagnostic_books()
                    .into_iter()
                    .filter(|(_, token, _)| self.tokens.contains(token))
                    .take(20)
                    .map(|(_, token, book)| {
                        let snapshot = book.snapshot.as_ref();
                        json!({
                            "token": token,
                            "timestamp_ms": snapshot.map(|row| row.timestamp_ms),
                            "version": snapshot.map(|row| row.version),
                            "best_bid": snapshot.and_then(|row| row.summary.best_bid).map(|price| price.to_string()),
                            "best_ask": snapshot.and_then(|row| row.summary.best_ask).map(|price| price.to_string()),
                            "freshness": format!("{:?}", book.freshness),
                            "availability": format!("{:?}", book.availability),
                        })
                    })
                    .collect::<Vec<_>>();
                return Err(
                    error.context(format!("PIT ingress memory diagnostics: {}", json!(memory)))
                );
            }
        };
        let database_after = stack.postgres.connection().statement_time().await;
        ensure!(
            database_before <= returned_at && returned_at <= database_after,
            "the readiness return clock must be bracketed by the real PostgreSQL clock"
        );
        let decision_at = returned_at;
        let boundary =
            DecisionClock::new(FixtureBookTiming::REPORT_LAG_SECS).boundary(decision_at)?;
        let facts = &fixture.setup.fact_writers.fact_read;
        let current = facts
            .book_ledger_snapshots_at(
                self.tokens.clone(),
                decision_at.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await?;
        let pit = facts
            .book_ledger_snapshots_at(
                self.tokens.clone(),
                boundary.cutoff_for(DecisionSource::Book).timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await?;
        let by_token = pit
            .iter()
            .map(|row| (&row.token_id, row))
            .collect::<HashMap<_, _>>();
        let identical = current
            .iter()
            .filter(|row| {
                by_token.get(&row.token_id).is_some_and(|visible| {
                    visible.stream_session_id == row.stream_session_id
                        && visible.token_sequence == row.token_sequence
                        && visible.event_hash == row.event_hash
                })
            })
            .count();
        println!(
            "pit-cohort-boundary {}",
            json!({
            "returned_at": returned_at, "decision_at": decision_at,
            "database_before": database_before, "database_after": database_after,
                "book_cutoff": boundary.cutoff_for(DecisionSource::Book),
                "current_count": current.len(), "pit_count": pit.len(), "same_cohort_count": identical,
                "tokens": current.iter().map(|row| {
                    let visible = by_token.get(&row.token_id);
                    json!({"token": row.token_id, "current_session": row.stream_session_id,
                        "current_sequence": row.token_sequence, "current_event_ms": row.venue_event_time,
                        "current_persisted_ms": row.persisted_time,
                        "pit_session": visible.map(|old| old.stream_session_id),
                        "pit_sequence": visible.map(|old| old.token_sequence),
                        "pit_event_ms": visible.map(|old| old.venue_event_time),
                        "pit_persisted_ms": visible.map(|old| old.persisted_time)})
                }).collect::<Vec<_>>()
            })
        );
        ensure!(
            current.len() == 20 && pit.len() == 20 && by_token.len() == 20,
            "both real CH views must contain every token, not a missing-source shortcut"
        );
        ensure!(
            identical == 20,
            "report readiness returned before its complete exact periodic cohort was PIT visible: same={identical}/20"
        );
        Ok(())
    }

    async fn shutdown(self) -> Result<()> {
        let Self {
            clob,
            root,
            infra,
            books,
            markets,
            manager,
            runner,
            ..
        } = self;
        root.cancel();
        let runner_result = timeout(Duration::from_mins(1), runner)
            .await
            .context("PIT ingress runner did not drain")
            .and_then(|result| result.context("PIT ingress runner panicked"))
            .and_then(|result| result.map_err(AnyhowError::from));
        let upstream_result = clob.shutdown().await;
        drop((manager, books, markets, infra));
        runner_result.and(upstream_result)
    }
}
