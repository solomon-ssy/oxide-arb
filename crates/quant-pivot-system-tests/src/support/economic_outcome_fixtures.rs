//! Historical report fixtures for economic queue and worker clock contracts.
//!
//! These fixtures seal a typed feature/capture through the production projection
//! and publish through the report FSM. Their explicit synthetic source handles
//! support isolated repository/worker tests, not an ingestion or replay canary.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{
        data_plane::{DecisionClock, DecisionSource},
        quant::NewReportTransaction,
    },
    entities::quant_economic_outcome_reconciliation_task::Entity as EconomicTaskEntity,
    enums::{
        catalog::CatalogTimestampQuality,
        feature::EvidenceSourceKind,
        quant::{DataQualityStatus, OutcomeSide, RecommendationReportStatus},
    },
    hashing::CanonicalDigest,
    types::{
        BookSnapshotRef, BookSnapshotSource, CatalogDecisionRef, CatalogEventChangeId,
        CatalogMarketChangeId, CatalogSyncBatchId, DecisionCaptureEvidence,
        DecisionSnapshotEvidence, EventId, EvidenceSourceRef, FeatureCell, FeatureStaleness,
        FeatureValue, FinalizedExecutionEvidence, MarketId, Probability, SchemaVersion,
        SelectionMemberEvidence, TokenId, stable_name::FeatureName,
    },
};
use quant_pivot_repository::{postgres::PgFeatureRepository, traits::FeatureRepository};
use quant_pivot_research::features::FeatureVector;
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait};
use uuid::Uuid;

use super::{
    execution_pg_seed::{
        ExecutionTxnIds, ReportBuildOptions, ReportSeedConfig, SharedDemoInfra,
        build_custom_report_transaction, fixture_no_token_id, prepare_report_on_infra,
    },
    report_lifecycle_seed::persist_and_publish_report,
};
use crate::postgres::PostgresClock;

const KNOWLEDGE_LAG_SECS: u64 = 10;

/// Publish one report at a real historical decision time with its exact feature owner.
///
/// Prepare shared model infrastructure before selecting `decision_at` so setup
/// time cannot consume a test's deliberately bounded source-lateness window.
pub async fn seed_report_at(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    decision_at: DateTime<Utc>,
) -> Result<ExecutionTxnIds> {
    let prepared = EconomicReportSeed {
        decision_at,
        knowledge_lag_secs: KNOWLEDGE_LAG_SECS,
        outcome_side: OutcomeSide::Yes,
    }
    .prepare(db, infra)
    .await?;
    Box::pin(prepared.publish(db)).await
}

/// Explicit side and decision clock for isolated economic-source contracts.
pub struct EconomicReportSeed {
    pub decision_at: DateTime<Utc>,
    pub knowledge_lag_secs: u64,
    pub outcome_side: OutcomeSide,
}

/// Prepared immutable graph retained until its source fixture is ready.
pub struct PreparedEconomicReport {
    pub ids: ExecutionTxnIds,
    pub transaction: NewReportTransaction,
    trigger_key: String,
    knowledge_lag_secs: u64,
}

impl PreparedEconomicReport {
    pub async fn publish(self, db: &DatabaseConnection) -> Result<ExecutionTxnIds> {
        let report = persist_and_publish_report(
            db,
            self.transaction,
            &self.trigger_key,
            i64::try_from(self.knowledge_lag_secs)?,
        )
        .await;
        ensure!(
            report.status == RecommendationReportStatus::Published,
            "economic fixture report was not published: {:?}",
            report.status
        );
        ensure!(
            EconomicTaskEntity::find_by_id(self.ids.recommendation)
                .one(db)
                .await?
                .is_some(),
            "published economic fixture has no atomically enqueued task"
        );
        Ok(self.ids)
    }
}

impl EconomicReportSeed {
    pub async fn prepare(
        &self,
        db: &DatabaseConnection,
        infra: &SharedDemoInfra,
    ) -> Result<PreparedEconomicReport> {
        let decision_at = self.decision_at;
        ensure!(
            decision_at < db.statement_time().await,
            "economic fixture decision must be historical"
        );
        let identity = Uuid::now_v7();
        let digest = identity.simple();
        let config = ReportSeedConfig {
            event_id: format!("economic-clock-{identity}"),
            market_id: format!("0x{digest}{digest}"),
            market_question: "Will the economic clock fixture resolve Yes?".to_owned(),
            market_slug: format!("economic-clock-{identity}"),
            token_id: identity.as_u128().to_string(),
            trigger_key: format!("scheduled:economic-clock:{identity}"),
        };
        let ids = prepare_report_on_infra(db, infra, &config, decision_at).await;
        let mut options = ReportBuildOptions::published_single(&ids);
        let recommendation = options
            .recommendations
            .first_mut()
            .context("economic fixture recommendation")?;
        let boundary = DecisionClock::new(self.knowledge_lag_secs).boundary(decision_at)?;
        boundary.validate()?;
        let cutoff = boundary.cutoff_for(DecisionSource::Book);
        let market_id = MarketId::new(&ids.market);
        let token_id = TokenId::new(&ids.token);
        let event_id = EventId::new(&ids.event);
        let source_hash = CanonicalDigest::content_hash_json(&(
            "economic-clock-synthetic-source",
            &market_id,
            &token_id,
            cutoff,
        ))?;
        let book_ref = BookSnapshotRef {
            token_id: token_id.clone(),
            source: BookSnapshotSource::CanonicalL2 {
                stream_session_id: identity,
                token_sequence: 1,
                source_event_hash: source_hash,
                event_time_ms: cutoff.timestamp_millis(),
                ingestion_time_ms: cutoff.timestamp_millis(),
            },
            content_hash: source_hash,
        };
        recommendation.evidence_refs.book_snapshot_ref = book_ref.clone();
        recommendation.market_context.book_age_ms = self.knowledge_lag_secs * 1_000;
        let capture = DecisionCaptureEvidence {
            snapshot: DecisionSnapshotEvidence {
                boundary: boundary.clone(),
                market_id: market_id.clone(),
                event_id: event_id.clone(),
                token_id: token_id.clone(),
                catalog: CatalogDecisionRef {
                    catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
                    market_change_id: CatalogMarketChangeId::from_v7(),
                    event_change_id: CatalogEventChangeId::from_v7(),
                    market_content_hash: source_hash,
                    event_content_hash: source_hash,
                    membership_hash: source_hash,
                    market_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
                    market_available_at: cutoff,
                    event_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
                    event_available_at: cutoff,
                    market_timestamp_quality: CatalogTimestampQuality::Source,
                    event_timestamp_quality: CatalogTimestampQuality::Source,
                },
                book_snapshot_ref: book_ref,
                book_effective_at: cutoff,
                book_available_at: cutoff,
                selection: SelectionMemberEvidence {
                    market_id: market_id.clone(),
                    event_id,
                    category: recommendation.identity.category,
                    primary_token_id: token_id.clone(),
                    secondary_token_id: Some(fixture_no_token_id(&ids.market, &ids.token)),
                    liquidity_usd: Some(recommendation.market_context.depth_usd),
                    volume_24h_usd: recommendation.market_context.volume_24h_usd,
                    source_refs: Vec::new(),
                },
            },
            finalized_execution_evidence: FinalizedExecutionEvidence::not_required(),
            identity: recommendation.identity.clone(),
            market_context: recommendation.market_context.clone(),
            data_quality: DataQualityStatus::Fresh,
            liquidity_score: Probability::ONE,
        };
        let vector = FeatureVector {
            market_id,
            token_id: Some(token_id),
            decision_at,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::from([(
                FeatureName::from_static("book.mid"),
                FeatureCell::observed(
                    FeatureValue::Probability(Probability::new(dec!(0.6))),
                    Some(EvidenceSourceRef {
                        source_kind: EvidenceSourceKind::Book,
                        reference: format!("economic-clock:{identity}"),
                        effective_at: cutoff,
                        available_at: Some(cutoff),
                    }),
                    FeatureStaleness::Known {
                        age_ms: self.knowledge_lag_secs * 1_000,
                    },
                ),
            )]),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        };
        let mut feature = vector.try_to_new(&boundary, &capture)?;
        feature.feature_vector_id = recommendation.evidence_refs.feature_vector_id;
        PgFeatureRepository::new(db.clone()).create(feature).await?;
        if self.outcome_side == OutcomeSide::No {
            let no_token = fixture_no_token_id(&ids.market, &ids.token);
            recommendation.token_id = no_token.clone();
            recommendation.outcome_side = OutcomeSide::No;
            "No".clone_into(&mut recommendation.identity.outcome_name);
            recommendation.economic_tier_json.token_id = no_token.clone();
            recommendation.economic_tier_json.outcome_side = OutcomeSide::No;
            recommendation.evidence_refs.book_snapshot_ref.token_id = no_token;
        }
        ids.complete_model_run(db).await;
        let transaction = build_custom_report_transaction(&ids, options);
        Ok(PreparedEconomicReport {
            ids,
            transaction,
            trigger_key: config.trigger_key,
            knowledge_lag_secs: self.knowledge_lag_secs,
        })
    }
}
