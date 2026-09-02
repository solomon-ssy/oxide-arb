//! Real worker/PG/CH proof that rolling history preserves the initial fit window.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use quant_pivot_core::{
    app::exchange_history_worker::{
        ExchangeHistoryProgressHandle, ExchangeHistoryWorker, ExchangeHistoryWriters,
    },
    observability::metrics_hub::MetricsHub,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::{
        ExchangeHistoryAttestorConfig, FinalizedExchangeHistoryConfig, HyperSyncConfig,
        PolygonRpcEndpoint,
    },
    domain::data_plane::{
        CreateHistoryFitSeal, ExchangeHistoryFrontier, ExchangeHistoryPlanInfo,
        HistorySealChunkRef, NewHistoryFitSeal,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, HistoryFitSealId, POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID,
        builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{PgExchangeHistoryRepository, PgMarketRepository},
    traits::{ExchangeHistoryRepository, QuantFactReadRepository},
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};

use super::{
    DeterministicPolygonChain, HYPERSYNC_TOKEN, HistoryUpstreams, MODEL_CONFIRMATION_BLOCKS,
};
use crate::{
    postgres::PostgresClock, stack::SystemStack,
    support::policy_fixtures::bootstrap_default_policy_bundle,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_history_preserves_fit() -> Result<()> {
    let stack = Box::pin(SystemStack::start()).await?;
    let polygon = Arc::new(DeterministicPolygonChain::new());
    // Seal only the empty source registration. The attested chain clock keeps advancing.
    polygon.freeze();
    let case = HistoryCutpointCase {
        stack: &stack,
        polygon,
    };
    let verification = tokio::time::timeout(Duration::from_mins(6), Box::pin(case.verify()))
        .await
        .context("rolling history cutpoint proof exceeded its bounded deadline")
        .and_then(|result| result);
    drop(case);
    let cleanup = Box::pin(stack.shutdown()).await;
    verification?;
    cleanup?;
    Ok(())
}

struct HistoryCutpointCase<'a> {
    stack: &'a SystemStack,
    polygon: Arc<DeterministicPolygonChain>,
}

impl HistoryCutpointCase<'_> {
    async fn verify(&self) -> Result<()> {
        let db = self.stack.postgres.connection();
        bootstrap_default_policy_bundle(
            db,
            "history-cutpoint-test",
            "verify complete frozen fit and rolling source windows",
        )
        .await;
        let repository = PgExchangeHistoryRepository::new(db.clone());
        let plan = Box::pin(self.initial_pass()).await?;
        self.await_new_head(plan.activation_through_block).await?;
        let head = Box::pin(HistoryUpstreams::serving_head(
            db,
            &self.stack.clickhouse_config,
            &self.polygon,
        ))
        .await?;
        ensure!(
            head.seal.accepted_through_block > plan.activation_through_block,
            "catch-up must continue beyond the frozen fit target"
        );
        let accepted = repository
            .accepted_from(
                ExchangeHistoryFrontier::Activation,
                plan.activation_from_block,
            )
            .await?;
        ensure!(
            accepted
                .iter()
                .all(|chunk| chunk.to_block <= plan.activation_through_block
                    || chunk.from_block > plan.activation_through_block),
            "production worker emitted an accepted chunk across the initial cutpoint"
        );
        let prefix = accepted
            .iter()
            .take_while(|chunk| chunk.to_block <= plan.activation_through_block)
            .map(|chunk| {
                Ok(HistorySealChunkRef {
                    chunk_id: chunk.chunk_id,
                    frontier: chunk.frontier,
                    state_revision: chunk.state_revision.context("accepted state revision")?,
                    from_block: chunk.from_block,
                    to_block: chunk.to_block,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            prefix
                .first()
                .is_some_and(|chunk| chunk.from_block == plan.activation_from_block)
                && prefix
                    .last()
                    .is_some_and(|chunk| chunk.to_block == plan.activation_through_block),
            "initial fit window must be an exact complete-chunk prefix"
        );
        let tail = accepted
            .iter()
            .find(|chunk| chunk.from_block == plan.activation_through_block + 1)
            .context("live tail must resume at initial target plus one")?;
        let tail_ref = HistorySealChunkRef {
            chunk_id: tail.chunk_id,
            frontier: tail.frontier,
            state_revision: tail.state_revision.context("live tail state revision")?,
            from_block: tail.from_block,
            to_block: tail.to_block,
        };
        let pool = Arc::new(ClickHousePool::connect(&self.stack.clickhouse_config).await?);
        ChQuantFactReadRepository::new(pool)
            .validate_execution_history_chunks(head.chunks.clone())
            .await?;
        let profile = builtin_research_profiles()
            .map_err(AnyhowError::msg)?
            .into_iter()
            .find(|profile| {
                profile.profile_ref.id.as_str() == POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID
            })
            .context("Pooled profile")?;
        let mut command = CreateHistoryFitSeal {
            seal: NewHistoryFitSeal {
                fit_seal_id: HistoryFitSealId::from_v7(),
                seal_hash: ContentHash::from_bytes([0; 32]),
                plan_id: plan.plan_id,
                window_from_block: plan.activation_from_block,
                window_to_block: plan.activation_through_block,
                policy_hash: plan.policy_hash,
                profile_hash: profile.profile_ref.content_hash,
                cohort_hash: CanonicalDigest::content_hash_json(&profile.spec.cohort_contract)?,
                created_at: db.statement_time().await,
            },
            chunks: prefix,
        };
        command.seal.seal_hash = command.derive_hash()?;
        let sealed = repository.create_fit_seal(command.clone()).await?;
        ensure!(
            repository
                .validate_fit_seal(sealed.seal.fit_seal_id, sealed.seal.seal_hash)
                .await?
                == sealed,
            "complete initial prefix did not round-trip as a valid fit seal"
        );
        let mut crossing = command;
        crossing.seal.fit_seal_id = HistoryFitSealId::from_v7();
        crossing.chunks.push(tail_ref);
        crossing.seal.seal_hash = crossing.derive_hash()?;
        ensure!(
            matches!(
                repository.create_fit_seal(crossing).await,
                Err(StorageError::InvariantViolation {
                    entity: Some("quant_history_seal"),
                    ..
                })
            ),
            "fit validation must still reject a complete live-tail proof outside its frozen window"
        );
        self.await_new_head(head.seal.accepted_through_block)
            .await?;
        let advanced = Box::pin(HistoryUpstreams::serving_head(
            db,
            &self.stack.clickhouse_config,
            &self.polygon,
        ))
        .await?;
        ensure!(
            advanced.seal.accepted_through_block > head.seal.accepted_through_block,
            "fit cutpoint must not freeze live serving freshness"
        );
        ensure!(
            repository.load_plan(137).await?.as_ref() == Some(&plan),
            "rolling catch-up changed the immutable plan"
        );
        ensure!(
            repository
                .validate_fit_seal(sealed.seal.fit_seal_id, sealed.seal.seal_hash)
                .await?
                == sealed,
            "new live chunks invalidated the unchanged complete initial fit prefix"
        );
        Ok(())
    }

    async fn await_new_head(&self, previous: i64) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .polygon
                .head()
                .block_number
                .saturating_sub(MODEL_CONFIRMATION_BLOCKS)
                <= u64::try_from(previous)?
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<(), AnyhowError>(())
        })
        .await
        .context("live source clock did not advance")?
    }

    async fn initial_pass(&self) -> Result<ExchangeHistoryPlanInfo> {
        let db = self.stack.postgres.connection();
        let upstreams = HistoryUpstreams::start(Arc::clone(&self.polygon)).await?;
        let config = FinalizedExchangeHistoryConfig {
            enabled: true,
            max_blocks_per_chunk: 50_000,
            hot_window_blocks_per_tick: 1,
            hypersync: HyperSyncConfig {
                endpoint: upstreams.hypersync.uri(),
                api_token: HYPERSYNC_TOKEN.into(),
                ..HyperSyncConfig::default()
            },
            attestor: ExchangeHistoryAttestorConfig {
                rpc_endpoint: PolygonRpcEndpoint::Public {
                    url: upstreams.attestor.uri(),
                },
                max_blocks_per_log_request: 50_000,
                ..ExchangeHistoryAttestorConfig::default()
            },
            ..FinalizedExchangeHistoryConfig::default()
        };
        let repository = Arc::new(PgExchangeHistoryRepository::new(db.clone()));
        let pool = Arc::new(ClickHousePool::connect(&self.stack.clickhouse_config).await?);
        let writer = Arc::new(ChWriteManager::new(
            self.stack.clickhouse_config.max_concurrent_inserts,
            &self.stack.clickhouse_config.io,
        ));
        let worker = ExchangeHistoryWorker::connect(
            Arc::clone(&repository) as Arc<dyn ExchangeHistoryRepository>,
            Arc::new(PgMarketRepository::new(db.clone())),
            ExchangeHistoryWriters {
                raw_logs: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_exchange_log_raw",
                )),
                events: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_exchange_event",
                )),
                fee_charges: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_exchange_fee_charge",
                )),
                matches: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_exchange_match",
                )),
                executions: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_market_execution",
                )),
                participants: Arc::new(ChFactWriter::new(
                    Arc::clone(&pool),
                    Arc::clone(&writer),
                    "quant_execution_participant",
                )),
                acceptance: Arc::new(ChFactWriter::new(
                    pool,
                    writer,
                    "quant_exchange_history_acceptance",
                )),
            },
            config,
            ExchangeHistoryProgressHandle::fresh_boot(),
            Arc::new(MetricsHub::new()),
        )?;
        worker.probe().await?;
        worker.run_once().await?;
        let plan = repository
            .load_plan(137)
            .await?
            .context("production worker must freeze its own initial plan")?;
        let accepted = repository
            .accepted_from(
                ExchangeHistoryFrontier::Activation,
                plan.activation_from_block,
            )
            .await?;
        ensure!(
            accepted.len() == 1
                && accepted[0].from_block == plan.activation_from_block
                && accepted[0].to_block == plan.activation_from_block
                && accepted[0].to_block < plan.activation_through_block,
            "initial scheduling pass must leave genuine catch-up work unfinished"
        );
        Ok(plan)
    }
}
