//! Report lifecycle seeding owned by system tests.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration};
use quant_pivot_models::{
    clickhouse::{ChUsd, QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    config::ClickHouseConfig,
    domain::quant::{
        NewReportFactDelivery, NewReportTransaction, RecommendationReportInfo, ReportRunClaim,
    },
    entities::quant_report_run::ActiveModel,
    enums::quant::{
        RecommendationReportStatus, RecommendationStatus, ReportFactDeliveryStatus,
        ReportRunStatus, ReportTriggerKind,
    },
    hashing::CanonicalDigest,
    runtime_config::ReportDeliveryPolicy,
    types::{
        REPORT_FACT_BUNDLE_FORMAT_VERSION, ReportFactBundleV2,
        ReportFactNotificationRecommendationV2, ReportFactNotificationV2,
        ReportFactTableCommitment, ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage,
        ReportTriggerKey, Usd, WorkerId,
    },
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    postgres::{PgRecommendationReportRepository, PgReportRunRepository},
    traits::{FactWriter, RecommendationReportRepository, ReportRunRepository},
};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection};

use quant_pivot_research::artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};

const RECOMMENDATION_TABLE: &str = "quant_report_recommendation_fact";
const FUNNEL_TABLE: &str = "quant_report_market_funnel";

/// Seal and write the production report facts for a small system fixture.
pub async fn materialize_report_facts(
    artifacts: &Arc<dyn ArtifactStore>,
    clickhouse: &ClickHouseConfig,
    transaction: &mut NewReportTransaction,
) -> Result<()> {
    let report = &transaction.report;
    let recommendation_rows = transaction
        .recommendations
        .iter()
        .map(|recommendation| {
            let economics = recommendation.economics_json;
            Ok(QuantReportRecommendationFactRow {
                event_time: report.decision_at.timestamp_millis(),
                recommendation_report_id: report.recommendation_report_id,
                recommendation_id: recommendation.recommendation_id,
                report_route_run_id: recommendation.report_route_run_id,
                economic_tier_id: recommendation.economic_tier_id,
                route: recommendation.route.as_str().to_owned(),
                rank: u32::try_from(recommendation.rank)?,
                market_id: recommendation.market_id.clone(),
                token_id: recommendation.token_id.clone(),
                side: recommendation.outcome_side.into(),
                profit_probability_bps: economics
                    .profit_probability_bps
                    .inner()
                    .to_i64()
                    .context("profit probability bps must fit i64")?,
                nominal_expected_net_usd: ChUsd::from(economics.nominal_expected_net_usd),
                robust_expected_net_usd: ChUsd::from(economics.robust_expected_net_usd),
                max_loss_usd: ChUsd::from(economics.max_loss_usd),
                cvar_contribution_usd: ChUsd::from(economics.cvar_contribution_usd),
                capital_occupancy_usd_hours: ChUsd::from(Usd::new(
                    economics.capital_occupancy_usd_hours.inner(),
                )),
                marginal_portfolio_value_usd: ChUsd::from(economics.marginal_portfolio_value_usd),
                hard_reserved_cash_usd: ChUsd::from(
                    recommendation.trade_plan.sizing.hard_reserved_cash_usd,
                ),
                valid_until: recommendation.valid_until.timestamp_millis(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let funnel_rows = transaction
        .recommendations
        .iter()
        .map(|recommendation| {
            let route_run = transaction
                .route_runs
                .iter()
                .find(|run| run.report_route_run_id == recommendation.report_route_run_id)
                .context("recommendation Route run must exist")?;
            let lineage = route_run.lineage_json.as_ref();
            let terminal_stage = ReportFunnelStage::Published;
            let primary_reason = ReportFunnelReason::Published;
            let secondary_diagnostics = ReportFunnelDiagnostics::None {};
            let mut row = ReportMarketFunnelRow {
                event_time: report.decision_at.timestamp_millis(),
                recommendation_report_id: report.recommendation_report_id,
                market_selection_id: report.market_selection_id,
                decision_policy_snapshot_id: report.decision_policy_snapshot_id,
                report_route_run_id: Some(route_run.report_route_run_id),
                route: Some(route_run.route.as_str().to_owned()),
                model_version_id: lineage.map(|value| value.model_version_id),
                model_run_id: lineage.and_then(|value| value.model_run_id),
                market_id: recommendation.market_id.clone(),
                event_id: recommendation.event_id.clone(),
                primary_token_id: recommendation
                    .evidence_refs
                    .book_snapshot_ref
                    .token_id
                    .clone(),
                terminal_stage: terminal_stage.as_str().to_owned(),
                primary_reason: primary_reason.as_str().to_owned(),
                secondary_diagnostics_json: serde_json::to_string(&secondary_diagnostics)?,
                feature_vector_id: Some(recommendation.evidence_refs.feature_vector_id),
                signal_candidate_id: Some(recommendation.evidence_refs.signal_candidate_id),
                recommendation_id: Some(recommendation.recommendation_id),
                row_hash: String::new(),
                ingestion_time: report.decision_at.timestamp_millis(),
            };
            row.seal_hash()?;
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;
    seal_report_facts(
        artifacts,
        transaction,
        recommendation_rows.clone(),
        funnel_rows.clone(),
    )
    .await?;

    let pool = Arc::new(ClickHousePool::connect(clickhouse).await?);
    let manager = Arc::new(ChWriteManager::new(clickhouse.max_concurrent_inserts));
    if !recommendation_rows.is_empty() {
        ChFactWriter::new(
            Arc::clone(&pool),
            Arc::clone(&manager),
            RECOMMENDATION_TABLE,
        )
        .write_batch(recommendation_rows)
        .await?;
    }
    if !funnel_rows.is_empty() {
        ChFactWriter::new(pool, manager, FUNNEL_TABLE)
            .write_batch(funnel_rows)
            .await?;
    }
    Ok(())
}

/// Seal a production-format report-fact bundle for a deterministic system fixture.
pub async fn seal_report_facts(
    artifacts: &Arc<dyn ArtifactStore>,
    transaction: &mut NewReportTransaction,
    mut recommendation_rows: Vec<QuantReportRecommendationFactRow>,
    mut funnel_rows: Vec<ReportMarketFunnelRow>,
) -> Result<()> {
    let report = &transaction.report;
    recommendation_rows.sort_by(|left, right| {
        left.rank.cmp(&right.rank).then_with(|| {
            left.recommendation_id
                .as_uuid()
                .cmp(&right.recommendation_id.as_uuid())
        })
    });
    funnel_rows.sort_by(|left, right| left.market_id.cmp(&right.market_id));
    ensure!(
        recommendation_rows
            .iter()
            .all(|row| row.recommendation_report_id == report.recommendation_report_id)
            && funnel_rows
                .iter()
                .all(|row| row.recommendation_report_id == report.recommendation_report_id),
        "report-fact fixture contains a row for another report"
    );
    let recommendation_hash = CanonicalDigest::content_hash_json(&recommendation_rows)?;
    let funnel_hash = CanonicalDigest::content_hash_json(&funnel_rows)?;
    let recommendation_row_count = u64::try_from(recommendation_rows.len())?;
    let funnel_row_count = u64::try_from(funnel_rows.len())?;
    let notification_top3 = transaction
        .recommendations
        .iter()
        .take(3)
        .map(|recommendation| ReportFactNotificationRecommendationV2 {
            market_id: recommendation.market_id.to_string(),
            outcome_side: recommendation.outcome_side,
            route: recommendation.route,
            profit_probability_bps: recommendation.economics_json.profit_probability_bps,
            robust_expected_net_usd: recommendation.economics_json.robust_expected_net_usd,
            marginal_portfolio_value_usd: recommendation
                .economics_json
                .marginal_portfolio_value_usd,
            hard_reserved_cash_usd: recommendation.trade_plan.sizing.hard_reserved_cash_usd,
        })
        .collect();
    let bundle = ReportFactBundleV2 {
        format_version: REPORT_FACT_BUNDLE_FORMAT_VERSION,
        recommendation_report_id: report.recommendation_report_id,
        created_at: report.decision_at,
        delivery_policy: ReportDeliveryPolicy::StoreOnly,
        notify_operators: false,
        notification: ReportFactNotificationV2 {
            kind: report.report_kind,
            status: report.status.as_str().to_owned(),
            runtime_mode: report.runtime_mode,
            published_count: report.summary_json.published_recommendation_count,
            total_hard_reserved_cash_usd: report.summary_json.total_hard_reserved_cash_usd,
            top3: notification_top3,
            warnings: report.summary_json.warnings.clone(),
            empty_reason: report.summary_json.empty_reason,
        },
        recommendation_commitment: ReportFactTableCommitment {
            table: RECOMMENDATION_TABLE.to_owned(),
            row_count: recommendation_row_count,
            row_chain_hash: recommendation_hash,
        },
        funnel_commitment: ReportFactTableCommitment {
            table: FUNNEL_TABLE.to_owned(),
            row_count: funnel_row_count,
            row_chain_hash: funnel_hash,
        },
        recommendation_rows,
        funnel_rows,
    };
    let bytes = serde_json::to_vec(&bundle).context("serialize system-test report-fact bundle")?;
    let bundle_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let bundle_uri = artifacts
        .put(
            ArtifactKey::new(ArtifactNamespace::ReportFacts, bundle_hash.hex(), "json")?,
            &bytes,
        )
        .await?;
    let persisted = artifacts.get(&bundle_uri).await?;
    ensure!(
        persisted == bytes,
        "system-test report-fact artifact failed read-after-write verification"
    );
    transaction.fact_delivery = Some(NewReportFactDelivery {
        recommendation_report_id: report.recommendation_report_id,
        status: ReportFactDeliveryStatus::Pending,
        bundle_uri,
        bundle_hash,
        bundle_bytes: i64::try_from(bytes.len())?,
        recommendation_row_count: i64::try_from(recommendation_row_count)?,
        recommendation_row_chain_hash: recommendation_hash,
        funnel_row_count: i64::try_from(funnel_row_count)?,
        funnel_row_chain_hash: funnel_hash,
    });
    Ok(())
}

/// Persist a complete Prepared artifact, verify its delivery, and atomically
/// publish it. Fixtures must not bypass the durable run/publication FSM.
pub async fn persist_and_publish_report(
    db: &DatabaseConnection,
    transaction: NewReportTransaction,
    trigger_key: &str,
    knowledge_lag_secs: i64,
) -> RecommendationReportInfo {
    let prepared = persist_prepared_report(db, transaction, trigger_key, knowledge_lag_secs).await;
    let now = PgReportRunRepository::new(db.clone())
        .database_time()
        .await
        .expect("read database time for report publication");
    let report_id = prepared.recommendation_report_id;
    let repository = PgRecommendationReportRepository::new(db.clone());
    let delivery_worker = WorkerId::from_v7();
    let claimed = repository
        .claim_fact_delivery(delivery_worker, 600)
        .await
        .expect("claim report delivery")
        .expect("seeded report delivery is claimable");
    assert_eq!(claimed.recommendation_report_id, report_id);
    repository
        .verify_and_publish_report(&report_id, delivery_worker, now)
        .await
        .expect("publish seeded report")
        .into_applied()
        .expect("seeded report delivery claim must remain held")
        .report
}

/// Persist only the Prepared artifact and Pending delivery for delivery-worker tests.
pub async fn persist_prepared_report(
    db: &DatabaseConnection,
    mut transaction: NewReportTransaction,
    trigger_key: &str,
    knowledge_lag_secs: i64,
) -> RecommendationReportInfo {
    let clock = PgReportRunRepository::new(db.clone());
    let latest_availability_at = transaction
        .recommendations
        .iter()
        .map(|recommendation| recommendation.created_at)
        .fold(transaction.report.created_at, DateTime::max);
    let now = loop {
        let now = clock
            .database_time()
            .await
            .expect("read database time for report fixture");
        if now >= latest_availability_at {
            break now;
        }
        let wait = (latest_availability_at - now)
            .to_std()
            .expect("positive database clock wait");
        tokio::time::sleep(wait).await;
    };
    let worker_id = WorkerId::from_v7();
    let lease_expires_at = now + Duration::minutes(10);
    let report_run_id = transaction.report.report_run_id;
    transaction.report.status = RecommendationReportStatus::Prepared;
    transaction.report.published_at = None;
    transaction.report.successor_report_id = None;
    transaction.report.superseded_at = None;
    transaction.report.obsoleted_at = None;
    transaction.report.revoked_at = None;
    transaction.report.expired_at = None;
    transaction.report.status_reason = None;
    for recommendation in &mut transaction.recommendations {
        recommendation.status = RecommendationStatus::Prepared;
    }

    ActiveModel {
        report_run_id: ActiveValue::Set(report_run_id),
        trigger_kind: ActiveValue::Set(ReportTriggerKind::Scheduled),
        trigger_key: ActiveValue::Set(
            ReportTriggerKey::parse(trigger_key).expect("valid report fixture trigger key"),
        ),
        schedule_id: ActiveValue::Set(Some("test_fixture".into())),
        request_id: ActiveValue::Set(None),
        retry_of_run_id: ActiveValue::Set(None),
        scheduled_for: ActiveValue::Set(Some(transaction.report.decision_at)),
        requested_at: ActiveValue::Set(transaction.report.decision_at),
        status: ActiveValue::Set(ReportRunStatus::Running),
        started_at: ActiveValue::Set(Some(transaction.report.decision_at)),
        decision_at: ActiveValue::Set(Some(transaction.report.decision_at)),
        heartbeat_at: ActiveValue::Set(Some(now)),
        lease_expires_at: ActiveValue::Set(Some(lease_expires_at)),
        finished_at: ActiveValue::Set(None),
        lease_owner: ActiveValue::Set(Some(worker_id)),
        decision_policy_snapshot_id: ActiveValue::Set(Some(
            transaction.report.decision_policy_snapshot_id,
        )),
        top_n: ActiveValue::Set(Some(transaction.report.top_n)),
        knowledge_lag_secs: ActiveValue::Set(Some(knowledge_lag_secs)),
        output_report_id: ActiveValue::Set(None),
        terminal_reason: ActiveValue::Set(None),
        error_code: ActiveValue::Set(None),
        error_summary: ActiveValue::Set(None),
    }
    .insert(db)
    .await
    .expect("seed running report run");

    let repository = PgRecommendationReportRepository::new(db.clone());
    repository
        .create_prepared_report(
            ReportRunClaim {
                report_run_id,
                lease_owner: worker_id,
                lease_expires_at,
            },
            transaction,
        )
        .await
        .expect("seed prepared report")
        .report
}
