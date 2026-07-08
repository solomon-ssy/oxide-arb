//! Type-safe identifiers preventing accidental mixing of different ID domains.
//!
//! Identifiers fall into two families:
//!
//! - **External string ids** ([`MarketId`], [`TokenId`], [`EventId`],
//!   [`OrderId`]) wrap `Arc<str>` via `#[derive(StrId)]`. Their
//!   value is defined by an external system or carries semantic structure, so
//!   it is **not** a UUID and is persisted as `text` / `varchar`.
//! - **Internal UUID ids** (everything else) wrap `Arc<Uuid>` via
//!   `#[derive(UuidId)]` and persist as the native Postgres `uuid` type. They
//!   are generated in-process with [`from_v7`](UuidId) — always time-ordered so
//!   inserts stay sequential and indexes stay compact; no `prefix_` string
//!   scheme is used.
//!
//! All ids use an `Arc` internally so that `clone()` is a cheap atomic
//! reference-count increment rather than a heap allocation, which matters on
//! the hot path where ids flow through channels and live in many structures.

use quant_pivot_macros::{StrId, UuidId};
use std::sync::Arc;
use uuid::Uuid;

// ── External string identifiers (Arc<str>) ───────────────────────────────

/// Polymarket `condition_id` identifying a market.
///
/// `Ord` is derived (over the inner `Arc<str>`) so the id can key a
/// deterministic `BTreeMap` for exposure / allocation aggregates.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketId(Arc<str>);

/// Polymarket event identifier.
///
/// `Ord` is derived (over the inner `Arc<str>`) so the id can key a
/// deterministic `BTreeMap` for exposure / allocation aggregates.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(Arc<str>);

/// `ERC-1155` conditional token identifier (CLOB `token_id`).
///
/// **Namespace safety**: `TokenId` and `MarketId` are distinct namespaces.
/// Never construct a `TokenId` from a `MarketId` string — this will cause
/// silent lookup failures. Polymarket `condition_id` (`MarketId`) starts with
/// "0x" (66 chars); CLOB token IDs are decimal U256 strings.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokenId(Arc<str>);

#[cfg(debug_assertions)]
impl TokenId {
    /// Debug-only validation that catches common `MarketId → TokenId` confusion.
    pub fn debug_validate(&self) {
        let s = self.as_str();
        debug_assert!(
            !(s.starts_with("0x") && s.len() == 66),
            "TokenId contains what appears to be a Polymarket condition_id (MarketId). \
             This is likely a bug — use MarketId instead. Value: {s}"
        );
    }
}

/// CLOB order identifier returned by Polymarket after submission.
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId(Arc<str>);

/// External domain data source identifier (e.g. `binance`, `chainlink`).
///
/// A stable, lowercase source label persisted on every long-format
/// `quant_domain_observation` row and on ingest cursors. New sources are a
/// pure data extension — no schema change.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DomainSourceId(Arc<str>);

impl DomainSourceId {
    /// The Binance spot kline source.
    #[must_use]
    pub fn binance() -> Self {
        Self::new("binance")
    }

    /// The Chainlink on-chain aggregator source.
    #[must_use]
    pub fn chainlink() -> Self {
        Self::new("chainlink")
    }
}

/// Canonical external instrument key, e.g. `BINANCE:BTCUSDT:1m` or
/// `CHAINLINK:BTC-USD`.
///
/// The single join key between frozen market linkages and stored domain
/// observations. Constructed only through the typed helpers in
/// [`crate::types::domain`] so the format never drifts per call site.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DomainInstrumentKey(Arc<str>);

impl OrderId {
    /// Generate a synthetic venue-order id for tests and local adapters.
    #[must_use]
    pub fn synthetic() -> Self {
        Self::new(Uuid::now_v7().to_string())
    }
}

// ── Internal UUID identifiers (Arc<Uuid>) ────────────────────────────────

/// Market selection snapshot used by a report or model run.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketSelectionId(Arc<Uuid>);

/// Point-in-time feature vector snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureVectorId(Arc<Uuid>);

/// Governed factor definition identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorDefinitionId(Arc<Uuid>);

/// Persisted factor value identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorValueId(Arc<Uuid>);

/// Governed model specification identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelSpecId(Arc<Uuid>);

/// Published model version identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelVersionId(Arc<Uuid>);

/// Model training, backtest, shadow, or inference run identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelRunId(Arc<Uuid>);

/// Candidate signal emitted before portfolio pruning.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignalCandidateId(Arc<Uuid>);

/// Frozen, point-in-time training dataset artifact identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrainingDatasetId(Arc<Uuid>);

/// One materialized training example (row) within a training dataset.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrainingExampleId(Arc<Uuid>);

/// Stored model artifact (serialized weights / model) identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelArtifactId(Arc<Uuid>);

/// Point-in-time backtest report identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BacktestReportId(Arc<Uuid>);

/// Pairwise model-comparison report identifier (baseline vs candidate replay).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelComparisonReportId(Arc<Uuid>);

/// Unified, content-addressed calibration-artifact identifier (Phase 11.3).
///
/// Shared by every empirical calibration artifact in the system:
/// `kind = ModelScore` (a [`crate::enums::quant::CalibrationKind`]) — a
/// `ProbabilityCalibrator` mapping model score → `P(win)`, fit on an
/// independent held-out calibration split — and `kind = MarketPriceBias`
/// (formerly the standalone Phase 11.2.1 `FavoriteLongshotBiasTableId`,
/// deleted, no alias) — market-implied price → empirical settlement
/// frequency, conditioned by `(category, ttr_bucket, price_bucket)`. Both
/// kinds share one table, one content-hash/split-hash contract, and one
/// reliability-report shape.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalibrationArtifactId(Arc<Uuid>);

/// Shadow comparison record identifier (shadow vs active model run).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowComparisonId(Arc<Uuid>);

/// Frozen market → external-subject linkage ledger row identifier (11.2.2).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketLinkageId(Arc<Uuid>);

/// Basis-cross-check exceedance alert row identifier (11.2.2 remediation R6).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BasisAlertId(Arc<Uuid>);

/// Durable research job identifier (async dataset build / model train / backtest).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResearchJobId(Arc<Uuid>);

/// Model-governance audit row identifier (publish / retire / rollback / promote).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelGovernanceAuditId(Arc<Uuid>);

/// Decision-time venue account / capital snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountSnapshotId(Arc<Uuid>);

/// Strategy-capital equity curve snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EquitySnapshotId(Arc<Uuid>);

/// Report-level data-quality snapshot identifier (one row per report fire).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportDataQualitySnapshotId(Arc<Uuid>);

/// Portfolio plan identifier used by a recommendation report.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PortfolioPlanId(Arc<Uuid>);

/// Immutable `TopN` recommendation report identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecommendationReportId(Arc<Uuid>);

/// Single recommendation row identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecommendationId(Arc<Uuid>);

/// Governed bridge from a recommendation to execution.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderIntentId(Arc<Uuid>);

/// Internal execution-order lifecycle identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionOrderId(Arc<Uuid>);

/// Intent-level capital allocation ledger identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapitalAllocationId(Arc<Uuid>);

/// One on-chain CTF redemption batch for a `(condition_id, funder)` pair.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettlementRedeemId(Arc<Uuid>);

/// Per-position allocation row within a settlement redemption batch.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettlementRedeemLotId(Arc<Uuid>);

/// Position-lot ledger identifier.
///
/// One lot per filled entry intent (`order_intent_id` is its natural unique
/// key); the surrogate id keeps the entity addressable independent of the
/// originating intent and lets the per-token aggregate stay a query view.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionId(Arc<Uuid>);

/// Execution-order reconciliation record identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReconciliationId(Arc<Uuid>);

/// Runtime-config version identifier used by governed config activation.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigVersionId(Arc<Uuid>);

/// Runtime-config activation identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigActivationId(Arc<Uuid>);

/// Append-only operation-log row identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuditEventId(Arc<Uuid>);

// ── RBAC identifiers (web layer) ─────────────────────────────────────────

/// RBAC user identifier.
///
/// This is the stable Casbin subject: renaming a user or changing their
/// username never invalidates their role bindings.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(Arc<Uuid>);

/// RBAC role identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleId(Arc<Uuid>);

/// RBAC menu identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MenuId(Arc<Uuid>);

/// Append-only operation-log row identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationLogId(Arc<Uuid>);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    #[test]
    fn recommendation_report_id_v7_sortable_by_time() {
        const N: usize = 50;
        let mut ids: Vec<RecommendationReportId> = Vec::with_capacity(N);
        for _ in 0..N {
            ids.push(RecommendationReportId::from_v7());
            sleep(Duration::from_millis(2));
        }
        let mut sorted = ids.clone();
        sorted.sort_by_key(RecommendationReportId::as_uuid);
        let expected: Vec<Uuid> = ids.iter().map(RecommendationReportId::as_uuid).collect();
        let got: Vec<Uuid> = sorted.iter().map(RecommendationReportId::as_uuid).collect();
        assert_eq!(got, expected, "UUID v7 must be time-ordered");
    }

    #[test]
    fn order_intent_id_from_v7_is_version_7() {
        let id = OrderIntentId::from_v7();
        assert_eq!(id.as_uuid().get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn generated_ids_are_version_7() {
        assert_eq!(
            ModelRunId::from_v7().as_uuid().get_version(),
            Some(uuid::Version::SortRand)
        );
        assert_eq!(
            ExecutionOrderId::from_v7().as_uuid().get_version(),
            Some(uuid::Version::SortRand)
        );
    }

    #[test]
    fn market_id_roundtrip_serde() {
        let id = MarketId::new("0xabc123");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""0xabc123""#);
        let back: MarketId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn uuid_id_roundtrip_serde() {
        let id = RecommendationId::from_v7();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!(r#""{}""#, id.as_uuid()));
        let back: RecommendationId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn id_display_matches_inner() {
        let id = EventId::new("event-1");
        assert_eq!(id.to_string(), "event-1");
    }

    #[test]
    fn id_from_str() {
        let id: MarketId = "test".parse().unwrap();
        assert_eq!(id.as_str(), "test");
    }

    #[test]
    fn str_id_clone_is_cheap() {
        let id = TokenId::new("token-123");
        let cloned = id.clone();
        assert_eq!(id, cloned);
        assert_eq!(
            std::ptr::from_ref::<str>(id.as_str()),
            std::ptr::from_ref::<str>(cloned.as_str())
        );
    }
}
