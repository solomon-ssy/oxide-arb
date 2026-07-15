//! Crash-recoverable report fact delivery and verification worker.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_error::{
    QuantResult,
    report::ReportError,
    storage::{StorageError, entity::QUANT_RECOMMENDATION_REPORT},
};
use quant_pivot_models::{
    clickhouse::{QuantRecommendationEventRow, ReportMarketFunnelRow},
    domain::{RecommendationReportInfo, ReportFactDeliveryInfo},
    enums::quant::{RecommendationReportStatus, ReportFactDeliveryStatus},
    hashing::CanonicalDigest,
    types::{
        ContentHash, REPORT_FACT_BUNDLE_FORMAT_VERSION, ReportFactBundleV1,
        ReportFactTableCommitment,
    },
};
use quant_pivot_repository::traits::RecommendationReportRepository;
use quant_pivot_research::artifact::ArtifactStore;
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    publisher::ReportPublisher,
    types::{NotificationRecommendation, ReportNotificationPayload},
};

const RECOMMENDATION_TABLE: &str = "quant_recommendation_event";
const FUNNEL_TABLE: &str = "quant_report_market_funnel";
const CHUNK_ROWS: usize = 10_000;
const MAX_DELIVERY_ATTEMPTS: i32 = 8;
const LEASE_DURATION: Duration = Duration::from_mins(1);
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct ReportFactDeliveryDeps {
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub clickhouse: Arc<ClickHousePool>,
    pub write_manager: Arc<ChWriteManager>,
    pub publisher: Arc<ReportPublisher>,
}

/// Delivers immutable report bundles and makes reports actionable only after
/// exact two-table verification.
pub struct ReportFactDeliveryWorker {
    deps: ReportFactDeliveryDeps,
    worker_id: Uuid,
}

impl ReportFactDeliveryWorker {
    #[must_use]
    pub fn new(deps: ReportFactDeliveryDeps) -> Self {
        Self {
            deps,
            worker_id: Uuid::new_v4(),
        }
    }

    pub async fn run(&self, token: CancellationToken) -> QuantResult<()> {
        loop {
            if token.is_cancelled() {
                return Ok(());
            }
            if !self.process_one().await? {
                tokio::select! {
                    () = token.cancelled() => return Ok(()),
                    () = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
                }
            }
        }
    }

    /// Claim and settle at most one bundle. Returns whether work was claimed.
    pub async fn process_one(&self) -> QuantResult<bool> {
        let now = Utc::now();
        let lease_expires_at = now
            + chrono::Duration::from_std(LEASE_DURATION).map_err(|error| {
                ReportError::InvariantViolation {
                    stage: "report_fact_delivery",
                    detail: error.to_string(),
                }
            })?;
        let delivery = self
            .deps
            .reports
            .claim_fact_delivery(self.worker_id, now, lease_expires_at)
            .await?;
        let Some(delivery) = delivery else {
            return self.announce_one(now, lease_expires_at).await;
        };

        match self.deliver(&delivery).await {
            Ok(()) => {
                self.deps
                    .reports
                    .verify_fact_delivery(
                        &delivery.recommendation_report_id,
                        self.worker_id,
                        Utc::now(),
                    )
                    .await?;
            }
            Err(error) => {
                let status = if delivery.attempt_count >= MAX_DELIVERY_ATTEMPTS {
                    ReportFactDeliveryStatus::Failed
                } else {
                    ReportFactDeliveryStatus::Retrying
                };
                self.deps
                    .reports
                    .fail_fact_delivery(
                        &delivery.recommendation_report_id,
                        self.worker_id,
                        status,
                        &error.to_string(),
                    )
                    .await?;
                tracing::error!(
                    report_id = %delivery.recommendation_report_id,
                    attempt = delivery.attempt_count,
                    terminal = status == ReportFactDeliveryStatus::Failed,
                    %error,
                    "report fact delivery failed"
                );
            }
        }
        Ok(true)
    }

    async fn announce_one(
        &self,
        now: chrono::DateTime<Utc>,
        lease_expires_at: chrono::DateTime<Utc>,
    ) -> QuantResult<bool> {
        let Some(delivery) = self
            .deps
            .reports
            .claim_fact_announcement(self.worker_id, now, lease_expires_at)
            .await?
        else {
            return Ok(false);
        };
        let result = async {
            let bundle = self.load_bundle(&delivery).await?;
            let report = self.load_publishable_report(&delivery).await?;
            self.deps
                .publisher
                .publish_verified(
                    &report,
                    &notification_payload(&bundle),
                    bundle.delivery_policy,
                    bundle.notify_operators,
                )
                .await;
            self.deps
                .reports
                .acknowledge_fact_announcement(
                    &delivery.recommendation_report_id,
                    self.worker_id,
                    Utc::now(),
                )
                .await?;
            QuantResult::Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::error!(
                report_id = %delivery.recommendation_report_id,
                %error,
                "verified report announcement failed; lease expiry will retry"
            );
        }
        Ok(true)
    }

    async fn deliver(&self, delivery: &ReportFactDeliveryInfo) -> QuantResult<()> {
        let bundle = self.load_bundle(delivery).await?;
        self.write_recommendation_chunks(&delivery.bundle_hash, &bundle.recommendation_rows)
            .await?;
        self.write_funnel_chunks(&delivery.bundle_hash, &bundle.funnel_rows)
            .await?;
        self.verify_recommendations(delivery).await?;
        self.verify_funnel(delivery).await?;
        self.load_publishable_report(delivery).await?;
        Ok(())
    }

    async fn load_bundle(
        &self,
        delivery: &ReportFactDeliveryInfo,
    ) -> QuantResult<ReportFactBundleV1> {
        let bytes = self.deps.artifacts.get(&delivery.bundle_uri).await?;
        let byte_count =
            i64::try_from(bytes.len()).map_err(|error| ReportError::NumericOverflow {
                field: "report_fact_bundle.bundle_bytes",
                detail: error.to_string(),
            })?;
        let actual_bundle_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes))?;
        if byte_count != delivery.bundle_bytes || actual_bundle_hash != delivery.bundle_hash {
            return Err(ReportError::InvariantViolation {
                stage: "report_fact_delivery",
                detail: "report fact bundle size/hash does not match the PG outbox".to_owned(),
            }
            .into());
        }
        let bundle: ReportFactBundleV1 =
            serde_json::from_slice(&bytes).map_err(|error| ReportError::InvariantViolation {
                stage: "report_fact_delivery",
                detail: format!("invalid report fact bundle: {error}"),
            })?;
        validate_bundle(delivery, &bundle)?;
        Ok(bundle)
    }

    async fn load_publishable_report(
        &self,
        delivery: &ReportFactDeliveryInfo,
    ) -> QuantResult<RecommendationReportInfo> {
        let report = self
            .deps
            .reports
            .find_by_id(&delivery.recommendation_report_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RECOMMENDATION_REPORT,
                    &delivery.recommendation_report_id,
                )
            })?;
        if !matches!(
            report.status,
            RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
        ) {
            return Err(ReportError::InvariantViolation {
                stage: "report_fact_delivery",
                detail: format!("report is not publishable: {}", report.status),
            }
            .into());
        }
        Ok(report)
    }

    async fn write_recommendation_chunks(
        &self,
        bundle_hash: &ContentHash,
        rows: &[QuantRecommendationEventRow],
    ) -> QuantResult<()> {
        for (index, chunk) in rows.chunks(CHUNK_ROWS).enumerate() {
            let chunk_hash = CanonicalDigest::content_hash_json(chunk)?;
            let token_material =
                format!("{bundle_hash}:{RECOMMENDATION_TABLE}:{index}:{chunk_hash}");
            let token = CanonicalDigest::raw_hex(token_material.as_bytes());
            self.deps
                .write_manager
                .write_batch_deduplicated(
                    self.deps.clickhouse.client(),
                    RECOMMENDATION_TABLE,
                    &token,
                    chunk.to_vec(),
                )
                .await?;
        }
        Ok(())
    }

    async fn write_funnel_chunks(
        &self,
        bundle_hash: &ContentHash,
        rows: &[ReportMarketFunnelRow],
    ) -> QuantResult<()> {
        for (index, chunk) in rows.chunks(CHUNK_ROWS).enumerate() {
            let chunk_hash = CanonicalDigest::content_hash_json(chunk)?;
            let token_material = format!("{bundle_hash}:{FUNNEL_TABLE}:{index}:{chunk_hash}");
            let token = CanonicalDigest::raw_hex(token_material.as_bytes());
            self.deps
                .write_manager
                .write_batch_deduplicated(
                    self.deps.clickhouse.client(),
                    FUNNEL_TABLE,
                    &token,
                    chunk.to_vec(),
                )
                .await?;
        }
        Ok(())
    }

    async fn verify_recommendations(&self, delivery: &ReportFactDeliveryInfo) -> QuantResult<()> {
        let mut cursor = self
            .deps
            .clickhouse
            .client()
            .query(
                "SELECT ?fields FROM quant_recommendation_event FINAL \
                 WHERE recommendation_report_id = ? \
                 ORDER BY rank, recommendation_id",
            )
            .bind(delivery.recommendation_report_id.clone())
            .fetch::<QuantRecommendationEventRow>()
            .map_err(StorageError::from)?;
        let mut verifier = RowChainVerifier::new();
        while let Some(row) = cursor.next().await.map_err(StorageError::from)? {
            verifier.push(&row)?;
        }
        verifier.finish(
            delivery.recommendation_row_count,
            &delivery.recommendation_row_chain_hash,
            RECOMMENDATION_TABLE,
        )
    }

    async fn verify_funnel(&self, delivery: &ReportFactDeliveryInfo) -> QuantResult<()> {
        let mut cursor = self
            .deps
            .clickhouse
            .client()
            .query(
                "SELECT ?fields FROM quant_report_market_funnel FINAL \
                 WHERE recommendation_report_id = ? ORDER BY market_id",
            )
            .bind(delivery.recommendation_report_id.clone())
            .fetch::<ReportMarketFunnelRow>()
            .map_err(StorageError::from)?;
        let mut verifier = RowChainVerifier::new();
        while let Some(row) = cursor.next().await.map_err(StorageError::from)? {
            verifier.push(&row)?;
        }
        verifier.finish(
            delivery.funnel_row_count,
            &delivery.funnel_row_chain_hash,
            FUNNEL_TABLE,
        )
    }
}

fn validate_bundle(
    delivery: &ReportFactDeliveryInfo,
    bundle: &ReportFactBundleV1,
) -> QuantResult<()> {
    if bundle.format_version != REPORT_FACT_BUNDLE_FORMAT_VERSION
        || bundle.recommendation_report_id != delivery.recommendation_report_id
        || bundle.recommendation_commitment.table != RECOMMENDATION_TABLE
        || bundle.funnel_commitment.table != FUNNEL_TABLE
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_delivery",
            detail: "bundle identity, format, or table binding is invalid".to_owned(),
        }
        .into());
    }
    validate_commitment(
        &bundle.recommendation_commitment,
        &bundle.recommendation_rows,
        delivery.recommendation_row_count,
        &delivery.recommendation_row_chain_hash,
    )?;
    validate_commitment(
        &bundle.funnel_commitment,
        &bundle.funnel_rows,
        delivery.funnel_row_count,
        &delivery.funnel_row_chain_hash,
    )?;
    if bundle
        .recommendation_rows
        .iter()
        .any(|row| row.recommendation_report_id != delivery.recommendation_report_id)
        || bundle
            .funnel_rows
            .iter()
            .any(|row| row.recommendation_report_id != delivery.recommendation_report_id)
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_delivery",
            detail: "bundle contains a fact row for another report".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn validate_commitment<T: Serialize>(
    commitment: &ReportFactTableCommitment,
    rows: &[T],
    pg_count: i64,
    pg_hash: &ContentHash,
) -> QuantResult<()> {
    let row_count = u64::try_from(rows.len()).map_err(|error| ReportError::NumericOverflow {
        field: "report_fact_bundle.row_count",
        detail: error.to_string(),
    })?;
    let row_hash = CanonicalDigest::content_hash_json(rows)?;
    if commitment.row_count != row_count
        || commitment.row_chain_hash != row_hash
        || i64::try_from(row_count).ok() != Some(pg_count)
        || &row_hash != pg_hash
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_delivery",
            detail: format!("{} commitment does not match bundle/PG", commitment.table),
        }
        .into());
    }
    Ok(())
}

struct RowChainVerifier {
    hasher: blake3::Hasher,
    row_count: i64,
    first: bool,
}

impl RowChainVerifier {
    fn new() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"[");
        Self {
            hasher,
            row_count: 0,
            first: true,
        }
    }

    fn push<T: Serialize>(&mut self, row: &T) -> QuantResult<()> {
        if !self.first {
            self.hasher.update(b",");
        }
        self.first = false;
        let bytes = serde_json::to_vec(row).map_err(|error| ReportError::InvariantViolation {
            stage: "report_fact_delivery",
            detail: format!("ClickHouse verification serialization failed: {error}"),
        })?;
        self.hasher.update(&bytes);
        self.row_count =
            self.row_count
                .checked_add(1)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "report_fact_delivery.verified_row_count",
                    detail: "row count exceeds i64".to_owned(),
                })?;
        Ok(())
    }

    fn finish(
        mut self,
        expected_count: i64,
        expected_hash: &ContentHash,
        table: &'static str,
    ) -> QuantResult<()> {
        self.hasher.update(b"]");
        let actual_hash =
            ContentHash::parse(format!("blake3:{}", self.hasher.finalize().to_hex()))?;
        if self.row_count != expected_count || &actual_hash != expected_hash {
            return Err(ReportError::InvariantViolation {
                stage: "report_fact_delivery",
                detail: format!(
                    "{table} verification mismatch: count {}/{}, hash {}/{}",
                    self.row_count, expected_count, actual_hash, expected_hash
                ),
            }
            .into());
        }
        Ok(())
    }
}

fn notification_payload(bundle: &ReportFactBundleV1) -> ReportNotificationPayload {
    ReportNotificationPayload {
        report_id: bundle.recommendation_report_id.clone(),
        kind: bundle.notification.kind,
        status: bundle.notification.status.clone(),
        runtime_mode: bundle.notification.runtime_mode,
        published_count: bundle.notification.published_count,
        total_suggested_usd: bundle.notification.total_suggested_usd,
        top3: bundle
            .notification
            .top3
            .iter()
            .map(|recommendation| NotificationRecommendation {
                market_id: recommendation.market_id.clone(),
                outcome_side: recommendation.outcome_side,
                score: recommendation.score,
                suggested_usd: recommendation.suggested_usd,
            })
            .collect(),
        warnings: bundle.notification.warnings.clone(),
        empty_reason: bundle.notification.empty_reason,
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::hashing::CanonicalDigest;

    use super::RowChainVerifier;

    #[test]
    fn streaming_row_chain_matches_canonical_json_array() {
        let rows = vec![
            serde_json::json!({"rank": 1, "market": "a"}),
            serde_json::json!({"rank": 2, "market": "b"}),
        ];
        let expected = CanonicalDigest::content_hash_json(&rows).expect("row-chain hash");
        let mut verifier = RowChainVerifier::new();
        for row in &rows {
            verifier.push(row).expect("stream row");
        }
        verifier
            .finish(2, &expected, "test_rows")
            .expect("exact stream commitment");
    }

    #[test]
    fn streaming_row_chain_rejects_tampering() {
        let expected_rows = vec![serde_json::json!({"rank": 1})];
        let expected = CanonicalDigest::content_hash_json(&expected_rows).expect("row-chain hash");
        let mut verifier = RowChainVerifier::new();
        verifier
            .push(&serde_json::json!({"rank": 9}))
            .expect("stream row");
        assert!(verifier.finish(1, &expected, "test_rows").is_err());
    }
}
