//! Real `PostgreSQL` acceptance-deadline tests for the successor observer.
//!
//! The valid source-unavailable economic censor is only an existing-row fixture.
//! These tests do not claim executable economics or full successor closure.

use std::{slice, time::Duration};

use anyhow::{Context, Error as AnyhowError, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_core::service::recommendation_economic_outcome::{
    RecommendationEconomicReplayAdapter, RecommendationEconomicReplayBinding,
};
use quant_pivot_models::{
    clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
    domain::quant::EconomicOutcomeReconciliationResult,
    entities::{
        quant_economic_outcome_reconciliation_task::Entity as EconomicTaskEntity,
        quant_recommendation::{Entity as RecommendationEntity, Model as RecommendationModel},
    },
    enums::quant::RecommendationEconomicOutcomeState,
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmBlockHash, EvmTransactionHash, MarketId, PayoutRatio, RecommendationId,
        TokenId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgRecommendationEconomicOutcomeRepository, PgRecommendationResolutionOutcomeRepository,
    },
    traits::{RecommendationEconomicOutcomeRepository, RecommendationResolutionOutcomeRepository},
};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, SqlxPostgresConnector, Statement,
    TransactionTrait,
};
use serde_json::Value as JsonValue;
use sqlx::postgres::PgPoolOptions;
use tokio::time::{Instant, timeout};

use super::SuccessorOutcomeVerifier;
use crate::{
    postgres::{PostgresClock, setup_pg, with_postgres_suite},
    support::{
        economic_outcome_fixtures::seed_report_at,
        execution_pg_seed::{fixture_no_token_id, seed_shared_demo_infra},
        feedback_closure_seed::{FeedbackReportResolutionEvidence, FeedbackResolutionFactEvidence},
    },
};

struct ObserverFixture {
    recommendation: RecommendationModel,
    resolution_plane: FeedbackReportResolutionEvidence,
    foreign_id: RecommendationId,
}

impl ObserverFixture {
    async fn new(db: &DatabaseConnection) -> Result<Self> {
        let infra = Box::pin(seed_shared_demo_infra(db)).await;
        let decision_at = db.statement_time().await - ChronoDuration::days(2);
        let ids = Box::pin(seed_report_at(db, &infra, decision_at)).await?;
        let economics = PgRecommendationEconomicOutcomeRepository::new(db.clone());
        let worker = WorkerId::from_v7();
        let claims = economics
            .claim_due(db.statement_time().await, worker, 60, 300, 1)
            .await?;
        assert_eq!(claims.len(), 1);
        let claim = claims[0];
        assert_eq!(claim.recommendation_id, ids.recommendation);
        assert_eq!(claim.source_available_until, claim.source_cutoff_at);
        let context = economics.replay_context(&ids.recommendation).await?;
        let binding = RecommendationEconomicReplayBinding {
            recommendation_id: ids.recommendation,
            recommendation_report_id: context.report.recommendation_report_id,
            report_route_run_id: context.route_run.report_route_run_id,
            decision_policy_snapshot_id: context.report.decision_policy_snapshot_id,
            economic_tier_id: context.recommendation.economic_tier_id,
            model_version_id: context
                .route_run
                .model_version_id
                .context("fixture model")?,
            trade_policy_artifact_id: context
                .route_run
                .trade_policy_artifact_id
                .context("fixture policy")?,
            research_profile_artifact_id: context
                .route_run
                .research_profile_artifact_id
                .context("fixture profile")?,
            decision_at: context.report.decision_at,
            horizon_at: claim.horizon_at,
            replay_until: claim.replay_until,
            resolution_outcome_hash: claim.resolution_outcome_hash,
            source_cutoff_at: claim.source_cutoff_at,
            source_available_until: claim.source_available_until,
            replay_input_hash: CanonicalDigest::content_hash_json(
                &context.recommendation.evidence_refs,
            )?,
            available_at: claim.source_available_until,
        };
        let censored = RecommendationEconomicReplayAdapter::censor_unavailable(binding)?;
        let result = economics.complete_task(claim, worker, censored).await?;
        let EconomicOutcomeReconciliationResult::Inserted(economic) = result else {
            bail!("fixture must insert its canonical source-unavailable censor")
        };
        assert_eq!(economic.state, RecommendationEconomicOutcomeState::Censored);

        let fact = MarketResolutionRow::seal(MarketResolutionFactInput {
            market_id: MarketId::new(&ids.market),
            token_ids: [
                TokenId::new(&ids.token),
                fixture_no_token_id(&ids.market, &ids.token),
            ],
            payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
            resolved_at: (decision_at + ChronoDuration::minutes(1)).timestamp_millis(),
            observed_at: (decision_at + ChronoDuration::minutes(2)).timestamp_millis(),
            source_block_number: 42,
            source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))?,
            source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))?,
            source_log_index: 1,
            source_checkpoint_hash: ContentHash::from_bytes([61; 32]),
        })?;
        let resolutions = PgRecommendationResolutionOutcomeRepository::new(db.clone());
        resolutions
            .reconcile_fact(&ids.recommendation, &fact)
            .await?;
        let resolution = resolutions
            .find_by_recommendation(&ids.recommendation)
            .await?
            .context("fixture canonical resolution")?;
        let resolved_at =
            DateTime::<Utc>::from_timestamp_millis(fact.resolved_at).context("resolution clock")?;
        let observed_at = DateTime::<Utc>::from_timestamp_millis(fact.observed_at)
            .context("observation clock")?;
        assert_eq!(resolution.resolved_at, resolved_at);
        assert_eq!(resolution.source_observed_at, observed_at);
        let resolution_plane = FeedbackReportResolutionEvidence {
            report_id: ids.report,
            report_decision_at: ids.decision_at,
            resolved_at,
            observed_at,
            facts: vec![FeedbackResolutionFactEvidence {
                market_id: fact.market_id,
                resolved_outcome: "yes".to_owned(),
                resolved_at,
                observed_at,
                source_checkpoint_hash: fact.source_checkpoint_hash,
                resolution_fact_hash: fact.resolution_fact_hash,
            }],
        };
        let foreign = Box::pin(seed_report_at(
            db,
            &infra,
            decision_at + ChronoDuration::seconds(1),
        ))
        .await?;
        let recommendation = RecommendationEntity::find_by_id(ids.recommendation)
            .one(db)
            .await?
            .context("fixture recommendation")?;
        Ok(Self {
            recommendation,
            resolution_plane,
            foreign_id: foreign.recommendation,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounds_real_postgres_reads() -> Result<()> {
    Box::pin(with_postgres_suite(async {
        timeout(
            Duration::from_mins(1),
            Box::pin(async {
                let (pool, _scenario) = setup_pg().await;
                let fixture = Box::pin(ObserverFixture::new(pool.connection())).await?;
                let options = pool
                    .connection()
                    .get_postgres_connection_pool()
                    .connect_options();
                // Reuse only this disposable scenario's typed connection options.
                // Never print a connection URL or change the production pool.
                let single = PgPoolOptions::new()
                    .max_connections(1)
                    .min_connections(1)
                    .acquire_timeout(Duration::from_secs(30))
                    .connect_with((*options).clone())
                    .await?;
                let db = SqlxPostgresConnector::from_sqlx_postgres_pool(single.clone());
                let verifier = SuccessorOutcomeVerifier {
                    db: &db,
                    recommendations: slice::from_ref(&fixture.recommendation),
                    resolution_plane: &fixture.resolution_plane,
                    decision_at: fixture.resolution_plane.report_decision_at,
                };
                let id = fixture.recommendation.recommendation_id;
                let before = EconomicTaskEntity::find_by_id(id)
                    .one(&db)
                    .await?
                    .context("task before observation")?;
                let foreign_before = EconomicTaskEntity::find_by_id(fixture.foreign_id)
                    .one(&db)
                    .await?
                    .context("foreign task before observation")?;
                let held = db.begin().await?;
                assert_eq!(single.size(), 1);
                assert_eq!(single.num_idle(), 0);
                let deadline = Instant::now() + Duration::from_millis(200);
                // The transaction stays held until both acceptance and diagnostics
                // finish. This proves cancellation of a real repository acquire,
                // not cancellation of SQL already executing on the server.
                let blocked = timeout(Duration::from_secs(5), verifier.observe(deadline)).await;
                held.rollback().await?;
                let blocked_error = blocked
                    .context("blocked observer must finish without its pool lease")?
                    .err()
                    .context("blocked acquire must never accept existing rows")?;
                let blocked_detail = blocked_error.to_string();
                assert!(
                    blocked_detail.contains(
                        "production outcome reconciliation did not project all post-report"
                    )
                );
                assert!(blocked_detail.contains("resolutions=0 economics=0 expected=1"));
                assert!(blocked_detail.contains("capture_timeout"));
                timeout(
                    Duration::from_secs(1),
                    db.execute_raw(Statement::from_string(DbBackend::Postgres, "SELECT 1")),
                )
                .await
                .context("single pool remains usable after acquire cancellation")??;
                assert_eq!(
                    EconomicTaskEntity::find_by_id(id).one(&db).await?,
                    Some(before.clone())
                );

                let (resolutions, economics) = verifier
                    .observe(Instant::now() + Duration::from_secs(2))
                    .await?;
                assert_eq!(resolutions.len(), 1);
                assert!(resolutions.contains_key(&id));
                assert_eq!(economics.len(), 1);
                assert_eq!(
                    economics.get(&id).context("observed fixture censor")?.state,
                    RecommendationEconomicOutcomeState::Censored
                );

                let expired = verifier
                    .observe(Instant::now() - Duration::from_millis(1))
                    .await
                    .err()
                    .context("already expired deadline must reject even existing rows")?;
                let expired_detail = expired.to_string();
                assert!(
                    expired_detail.contains(
                        "production outcome reconciliation did not project all post-report"
                    )
                );
                let (_, diagnostic) = expired_detail
                    .split_once(" diagnostics=")
                    .context("expired observation retains its primary error and diagnostics")?;
                let diagnostic: JsonValue = serde_json::from_str(diagnostic)?;
                let records = diagnostic["recommendations"]
                    .as_array()
                    .context("exact diagnostic targets")?;
                assert_eq!(records.len(), 1);
                assert_eq!(
                    records[0]["recommendation_id"].as_str(),
                    Some(id.to_string().as_str())
                );
                assert_eq!(records[0]["task"]["status"], "completed");
                assert!(
                    !diagnostic
                        .to_string()
                        .contains(&fixture.foreign_id.to_string())
                );
                assert_eq!(
                    EconomicTaskEntity::find_by_id(id).one(&db).await?,
                    Some(before)
                );
                assert_eq!(
                    EconomicTaskEntity::find_by_id(fixture.foreign_id)
                        .one(&db)
                        .await?,
                    Some(foreign_before)
                );
                single.close().await;
                Ok::<(), AnyhowError>(())
            }),
        )
        .await
        .context("bounded real PostgreSQL successor-observer regression")?
    }))
    .await?
}
