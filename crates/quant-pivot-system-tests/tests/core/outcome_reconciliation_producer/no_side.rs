//! Selected-token economic replay through the real worker and atomic PG ledger.

use std::{collections::BTreeMap, sync::Arc, time::Duration as StdDuration};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    app::outcome_reconciliation_worker::OutcomeReconciliationWorker,
    service::{
        feedback_cohort::evaluate_feedback_cohort,
        recommendation_economic_outcome::CanonicalRecommendationEconomicReplaySource,
    },
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookStreamSessionRow, ChDigest, ChSchemaVersion,
        MarketResolutionFactInput, MarketResolutionRow, QuantSignalCandidateEventRow,
    },
    domain::quant::{
        FeedbackCohortDecision, FeedbackCohortEvidence, FeedbackCohortPageQuery,
        FeedbackCohortSnapshot, FeedbackCohortWindow, RecommendationEconomicStateDetail,
    },
    enums::{
        clickhouse::{
            ChCanonicalBookEventType, ChOutcomeSide, ChStreamSessionEndReason, ChStreamSessionState,
        },
        common::TickSize,
        market::MarketStatus,
        quant::{
            CohortCensorReason, FeedbackCohort, OutcomeReconciliationTaskStatus, OutcomeSide,
            RecommendationEconomicOutcomeState,
        },
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        ContentHash, PayoutRatio, Price, Probability, Shares, SignalCandidateId, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgClobMarketInfoRepository, PgExecutionAttemptOutcomeRepository,
        PgFeedbackCohortRepository, PgMarketRepository, PgRecommendationEconomicOutcomeRepository,
        PgRecommendationResolutionOutcomeRepository,
    },
    traits::{
        ClobMarketInfoRepository, FeedbackCohortRepository, MarketRepository,
        QuantFactReadRepository, RecommendationEconomicOutcomeRepository,
        RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{self, PostgresClock, setup_pg},
    support::{
        economic_outcome_fixtures::EconomicReportSeed,
        execution_pg_seed::{
            ExecutionTxnIds, fixture_no_token_id, fixture_profile_ref, seed_shared_demo_infra,
        },
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait};
use uuid::Uuid;

use super::{
    EconomicTaskEntity, MemoryResolutionFacts, ScriptedResolutionSource, SharedDemoInfra, block,
    block_hash, pass_config, seed_cursor, service_with_economic_source, transaction_hash,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_side_economic_contracts() {
    tokio::time::timeout(
        StdDuration::from_mins(4),
        Box::pin(postgres::with_postgres_suite(async {
            NoSideEconomics::verify()
                .await
                .expect("No-side worker contracts");
        })),
    )
    .await
    .expect("bounded No-side suite")
    .expect("No-side PostgreSQL suite");
}

struct NoSideEconomics;

impl NoSideEconomics {
    async fn verify() -> Result<()> {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let infra = seed_shared_demo_infra(&db).await;
        let decision_at = DateTime::from_timestamp_millis(
            (db.statement_time().await - Duration::seconds(5)).timestamp_millis(),
        )
        .context("decision clock")?;
        let facts = Arc::new(MemoryResolutionFacts::default());
        let market_info = Arc::new(PgClobMarketInfoRepository::new(db.clone()));
        let cases = Self::prepare_cases(&db, &infra, &facts, &market_info, decision_at).await?;
        Self::reconcile(&db, facts, market_info, decision_at, &cases).await
    }

    async fn prepare_cases(
        db: &DatabaseConnection,
        infra: &SharedDemoInfra,
        facts: &MemoryResolutionFacts,
        market_info: &PgClobMarketInfoRepository,
        decision_at: DateTime<Utc>,
    ) -> Result<Vec<(ExecutionTxnIds, TokenId, bool, bool)>> {
        let mut cases = Vec::new();
        for (ordinal, (no_wins, missing_no)) in [(true, false), (false, false), (true, true)]
            .into_iter()
            .enumerate()
        {
            let case_decision = decision_at + Duration::milliseconds(i64::try_from(ordinal)? * 10);
            let prepared = EconomicReportSeed {
                decision_at: case_decision,
                knowledge_lag_secs: 0,
                outcome_side: OutcomeSide::No,
            }
            .prepare(db, infra)
            .await?;
            let ids = &prepared.ids;
            let selected = fixture_no_token_id(&ids.market, &ids.token);
            assert_ne!(selected, TokenId::new(&ids.token));
            assert_eq!(prepared.transaction.recommendations[0].token_id, selected);
            assert_eq!(
                prepared.transaction.recommendations[0].outcome_side,
                OutcomeSide::No
            );
            let info = Self::market_info(ids, case_decision)?;
            info.validate().map_err(AnyhowError::msg)?;
            market_info.insert_observation(info).await?;
            let yes = Self::book(ids, TokenId::new(&ids.token), dec!(0.90))?;
            let no = Self::book(ids, selected.clone(), dec!(0.40))?;
            for row in [&yes, &no] {
                assert_eq!(
                    ContentHash::from(row.event_hash),
                    row.canonical_event_hash()?
                );
                assert_eq!(row.venue_event_time, case_decision.timestamp_millis());
            }
            facts
                .sessions
                .lock()
                .expect("session facts")
                .push(Self::session(&yes)?);
            facts.books.lock().expect("book facts").push(yes);
            if !missing_no {
                facts
                    .sessions
                    .lock()
                    .expect("session facts")
                    .push(Self::session(&no)?);
                facts.books.lock().expect("book facts").push(no);
            }
            facts
                .signals
                .lock()
                .expect("signal facts")
                .push(Self::signal(ids, selected.clone()));
            let fact = MarketResolutionRow::seal(MarketResolutionFactInput {
                market_id: ids.market.as_str().into(),
                token_ids: [TokenId::new(&ids.token), selected.clone()],
                payout_ratios: if no_wins {
                    [PayoutRatio::ZERO, PayoutRatio::ONE]
                } else {
                    [PayoutRatio::ONE, PayoutRatio::ZERO]
                },
                resolved_at: (case_decision + Duration::seconds(1)).timestamp_millis(),
                observed_at: (case_decision + Duration::seconds(1)).timestamp_millis(),
                source_block_number: 101,
                source_block_hash: block_hash('a'),
                source_transaction_hash: transaction_hash('b'),
                source_log_index: u64::try_from(ordinal)?,
                source_checkpoint_hash: CanonicalDigest::content_hash_json(&(
                    ids.recommendation,
                    no_wins,
                ))?,
            })?;
            facts.persist(vec![fact])?;
            let ids = Box::pin(prepared.publish(db)).await?;
            PgMarketRepository::new(db.clone())
                .update_status(
                    &ids.market.as_str().into(),
                    MarketStatus::Settled,
                    Some(if no_wins { "No" } else { "Yes" }),
                )
                .await?;
            cases.push((ids, selected, no_wins, missing_no));
        }
        Ok(cases)
    }

    async fn reconcile(
        db: &DatabaseConnection,
        facts: Arc<MemoryResolutionFacts>,
        market_info: Arc<PgClobMarketInfoRepository>,
        decision_at: DateTime<Utc>,
        cases: &[(ExecutionTxnIds, TokenId, bool, bool)],
    ) -> Result<()> {
        seed_cursor(db, 103, decision_at + Duration::seconds(3)).await;
        let outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
            as Arc<dyn RecommendationEconomicOutcomeRepository>;
        let source = Arc::new(CanonicalRecommendationEconomicReplaySource::new(
            Arc::clone(&outcomes),
            Arc::clone(&facts) as Arc<dyn QuantFactReadRepository>,
            market_info,
            Arc::new(ComputeExecutor::new()?),
        ));
        let service = Arc::new(service_with_economic_source(
            db,
            Arc::new(ScriptedResolutionSource {
                head: block(103, decision_at + Duration::seconds(3), '0'),
                scan: None,
            }),
            Arc::clone(&facts),
            Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
            Arc::clone(&outcomes),
            source,
        ));
        let worker = OutcomeReconciliationWorker::new(service);
        worker
            .run_once(pass_config(db.statement_time().await))
            .await?;
        // The real worker visits economics before resolution. The first pass
        // publishes the resolution owner; only a fresh subsequent cutoff can
        // authorize the early economic claim while the horizon is still future.
        for (ids, _, _, _) in cases {
            assert!(outcomes.find_by_id(&ids.recommendation).await?.is_none());
            assert!(
                PgRecommendationResolutionOutcomeRepository::new(db.clone())
                    .find_by_recommendation(&ids.recommendation)
                    .await?
                    .is_some()
            );
        }
        worker
            .run_once(pass_config(db.statement_time().await))
            .await?;
        let truth_cutoff = db.statement_time().await;
        let snapshot = FeedbackCohortSnapshot::try_new(
            FeedbackCohortWindow::try_new(fixture_profile_ref(), decision_at, truth_cutoff)?,
            truth_cutoff,
        )?;
        let page = PgFeedbackCohortRepository::new(db.clone())
            .list_page(FeedbackCohortPageQuery::try_new(
                FeedbackCohort::PolicyEvaluation,
                snapshot.clone(),
                None,
                10,
            )?)
            .await?;
        ensure!(
            page.candidates().len() == cases.len(),
            "cohort must expose every original recommendation: found={} expected={}",
            page.candidates().len(),
            cases.len()
        );
        for (ids, selected, no_wins, missing_no) in cases {
            let resolution = PgRecommendationResolutionOutcomeRepository::new(db.clone())
                .find_by_recommendation(&ids.recommendation)
                .await?
                .context("resolution WORM")?;
            assert_eq!(resolution.token_id, *selected);
            assert_eq!(
                resolution.token_payout_ratio,
                if *no_wins {
                    PayoutRatio::ONE
                } else {
                    PayoutRatio::ZERO
                }
            );
            let task = EconomicTaskEntity::find_by_id(ids.recommendation)
                .one(db)
                .await?
                .context("economic task")?;
            let economic = outcomes.find_by_id(&ids.recommendation).await?;
            let candidate = page
                .candidates()
                .iter()
                .find(|row| row.context().recommendation_id() == ids.recommendation)
                .context("PolicyEvaluation candidate")?;
            assert_eq!(candidate.context().token_id(), selected);
            let decision = evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &snapshot,
                candidate.context(),
                candidate.resolution_outcome(),
                candidate.execution_rollup(),
                candidate.economic_outcome(),
            )?;
            if *missing_no {
                assert!(
                    economic.is_none(),
                    "missing No book must not fall back to the Yes decoy"
                );
                assert_ne!(task.status, OutcomeReconciliationTaskStatus::Completed);
                assert_eq!(
                    decision,
                    FeedbackCohortDecision::Censored(
                        CohortCensorReason::EconomicOutcomeUnavailableAtCutoff
                    )
                );
                continue;
            }
            let economic = economic.with_context(|| {
                format!("atomic economic WORM: task={task:?} resolution={resolution:?}")
            })?;
            economic.verify()?;
            assert_eq!(task.status, OutcomeReconciliationTaskStatus::Completed);
            assert_eq!(
                economic.state,
                RecommendationEconomicOutcomeState::ResolvedBeforeHorizon
            );
            assert!(
                matches!(economic.payload_json.detail, RecommendationEconomicStateDetail::ResolvedBeforeHorizon { entered_at: Some(_), payout_ratio, .. } if payout_ratio == resolution.token_payout_ratio)
            );
            let amounts = &economic.payload_json.amounts;
            assert_eq!(amounts.entry_filled_shares, Shares::new(dec!(62.5)));
            assert_eq!(amounts.exited_shares, amounts.entry_filled_shares);
            assert_eq!(amounts.entry_cost_usd, Usd::new(dec!(25)));
            assert_eq!(
                amounts.resolution_payout_usd,
                Usd::new(if *no_wins { dec!(62.5) } else { Decimal::ZERO })
            );
            assert_eq!(
                amounts.net_pnl_usd,
                Some(Usd::new(if *no_wins { dec!(37.5) } else { dec!(-25) }))
            );
            assert!(
                matches!(decision, FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation { economic: evidence, .. }) if evidence.net_return_bps == amounts.net_return_bps && evidence.evidence_hash == economic.evidence_hash)
            );
        }
        let reads = facts.book_reads.lock().expect("book read trace");
        assert!(!reads.is_empty());
        assert!(
            reads
                .iter()
                .all(|token| cases.iter().any(|(_, selected, _, _)| token == selected))
        );
        drop(reads);
        Ok(())
    }

    fn book(ids: &ExecutionTxnIds, token: TokenId, ask: Decimal) -> Result<BookL2LedgerRow> {
        let at = ids.decision_at.timestamp_millis();
        Ok(BookL2LedgerRow {
            stream_session_id: Uuid::now_v7(),
            shard_id: 0,
            token_id: token,
            market_id: Some(ids.market.as_str().into()),
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: vec![Price::new(ask - dec!(0.01)).into()],
            bid_sizes: vec![Shares::new(dec!(1000)).into()],
            ask_prices: vec![Price::new(ask).into()],
            ask_sizes: vec![Shares::new(dec!(1000)).into()],
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            trade_transaction_hash: None,
            venue_event_time: at,
            ingress_time: at,
            persisted_time: at,
            event_hash: ChDigest::new([0; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
        .seal()?)
    }

    fn session(row: &BookL2LedgerRow) -> Result<BookStreamSessionRow> {
        let sequence = serde_json::to_string(&BTreeMap::from([(
            row.token_id.as_str(),
            row.token_sequence,
        )]))?;
        Ok(BookStreamSessionRow {
            stream_session_id: row.stream_session_id,
            shard_id: 0,
            ledger_sequence: 1,
            state: ChStreamSessionState::Open,
            end_reason: ChStreamSessionEndReason::None,
            subscription_token_hash: CanonicalDigest::content_hash_json(&row.token_id)?,
            subscription_token_count: 1,
            received_sequence_json: sequence.clone(),
            persisted_sequence_json: sequence,
            opened_at: row.venue_event_time,
            recorded_at: row.persisted_time,
            schema_version: ChSchemaVersion::FIRST,
        })
    }

    fn signal(ids: &ExecutionTxnIds, token: TokenId) -> QuantSignalCandidateEventRow {
        QuantSignalCandidateEventRow {
            event_time: ids.decision_at.timestamp_millis(),
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ids.model_run,
            model_version_id: ids.model_version,
            market_id: ids.market.as_str().into(),
            token_id: token,
            side: ChOutcomeSide::No,
            score: Probability::new(dec!(0.9)).into(),
            confidence: Probability::ONE.into(),
            expected_return_bps: Bps::new(dec!(1000)).into(),
            entry_price: Price::new(dec!(0.4)).into(),
            target_price: Price::new(dec!(0.8)).into(),
            stop_price: Price::new(dec!(0.1)).into(),
            route_rank: 1,
            rejection_reason: String::new(),
            ingestion_time: ids.decision_at.timestamp_millis(),
        }
    }

    fn market_info(ids: &ExecutionTxnIds, at: DateTime<Utc>) -> Result<ClobMarketInfoVersion> {
        let no = fixture_no_token_id(&ids.market, &ids.token);
        let raw = serde_json::json!({ "c": ids.market, "t": [{"t": ids.token, "o": "Yes"}, {"t": no, "o": "No"}], "mts": "0.01", "mos": "1", "nr": false, "fd": {"r":"0", "e":1, "to":true}, "mbf":0, "tbf":0 });
        Ok(ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: ids.market.as_str().into(),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: TokenId::new(&ids.token),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: no,
                    outcome: "No".to_owned(),
                },
            ],
            tick_size: TickSize::Hundredth,
            minimum_order_size: Shares::ONE,
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: Decimal::ZERO,
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at: at,
            available_at: at,
            payload_hash: CanonicalDigest::content_hash_json(&raw)?,
            raw_payload: raw,
        })
    }
}
