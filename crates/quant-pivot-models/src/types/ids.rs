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

use crate::types::ContentHash;

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

    /// The Binance aggregate-trade stream used by live crypto events.
    #[must_use]
    pub fn binance_agg_trade() -> Self {
        Self::new("binance_agg_trade")
    }

    /// Binance USD-M Futures kline source.
    #[must_use]
    pub fn binance_usdm_futures() -> Self {
        Self::new("binance_usdm_futures")
    }

    /// Binance USD-M Futures aggregate-trade stream.
    #[must_use]
    pub fn binance_usdm_futures_agg_trade() -> Self {
        Self::new("binance_usdm_futures_agg_trade")
    }

    /// Binance price updates relayed by the public Polymarket RTDS.
    #[must_use]
    pub fn polymarket_rtds_binance() -> Self {
        Self::new("polymarket_rtds_binance")
    }

    /// Chainlink price updates relayed by the public Polymarket RTDS.
    #[must_use]
    pub fn polymarket_rtds_chainlink() -> Self {
        Self::new("polymarket_rtds_chainlink")
    }

    /// The Chainlink Data Streams signed-report source.
    #[must_use]
    pub fn chainlink_data_streams() -> Self {
        Self::new("chainlink_data_streams")
    }

    /// NOAA `AviationWeather` METAR/SPECI/COR source.
    #[must_use]
    pub fn aviation_weather() -> Self {
        Self::new("aviation_weather")
    }

    /// NOAA `GHCNh` historical station source.
    #[must_use]
    pub fn ghcnh() -> Self {
        Self::new("ghcnh")
    }

    /// NOAA Global Ensemble Forecast System source.
    #[must_use]
    pub fn gefs() -> Self {
        Self::new("gefs")
    }

    /// Hong Kong Observatory Open Data API.
    #[must_use]
    pub fn hko_open_data() -> Self {
        Self::new("hko_open_data")
    }

    /// EPA `AirNow` nationwide reporting-area files.
    #[must_use]
    pub fn airnow() -> Self {
        Self::new("airnow")
    }

    /// NOAA Storm Prediction Center preliminary local storm reports.
    #[must_use]
    pub fn spc_storm_reports() -> Self {
        Self::new("spc_storm_reports")
    }

    /// NOAA NCEI final Storm Events archive.
    #[must_use]
    pub fn ncei_storm_events() -> Self {
        Self::new("ncei_storm_events")
    }

    /// NOAA NHC current tropical-cyclone advisory summary.
    #[must_use]
    pub fn nhc_advisory() -> Self {
        Self::new("nhc_advisory")
    }

    /// NOAA NHC post-analysis HURDAT2 best track.
    #[must_use]
    pub fn nhc_hurdat2() -> Self {
        Self::new("nhc_hurdat2")
    }

    /// NASA GISS GISTEMP v4 monthly global anomaly.
    #[must_use]
    pub fn nasa_gistemp() -> Self {
        Self::new("nasa_gistemp")
    }

    /// NOAA/NSIDC Sea Ice Index v4.
    #[must_use]
    pub fn nsidc_sea_ice_index() -> Self {
        Self::new("nsidc_sea_ice_index")
    }

    /// NOAA/NWS API quality-controlled station observation.
    #[must_use]
    pub fn nws_observation() -> Self {
        Self::new("nws_observation")
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

/// One committed Gamma catalog synchronization batch.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogSyncBatchId(Arc<Uuid>);

/// Content-addressed normalized Gamma event object.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogEventObjectId(Arc<Uuid>);

impl CatalogEventObjectId {
    /// Stable projection of a catalog event content identifier.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x7c48_b3cf_f8bb_4e06_b099_2db7_4d1f_4ec1);
        Self::new(Uuid::new_v5(&NAMESPACE, content_hash.as_str().as_bytes()))
    }
}

/// One append-only event change observed in a committed sync batch.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogEventChangeId(Arc<Uuid>);

/// Content-addressed normalized Gamma market object.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogMarketObjectId(Arc<Uuid>);

impl CatalogMarketObjectId {
    /// Stable projection of a catalog market content identifier.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0xb7dd_4b1e_f861_4b4f_a787_7c54_b1da_1690);
        Self::new(Uuid::new_v5(&NAMESPACE, content_hash.as_str().as_bytes()))
    }
}

/// One append-only market change observed in a committed sync batch.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogMarketChangeId(Arc<Uuid>);

/// One typed catalog input rejection attached to a failed sync attempt.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogSyncRejectionId(Arc<Uuid>);

/// Append-only point-in-time CLOB market-info observation.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClobMarketInfoVersionId(Arc<Uuid>);

/// Deterministic point-in-time source-slice materialization ledger row.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceSliceId(Arc<Uuid>);

/// Point-in-time feature vector snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureVectorId(Arc<Uuid>);

/// Governed factor definition identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorDefinitionId(Arc<Uuid>);

impl FactorDefinitionId {
    /// Project a canonical factor-definition digest into the UUID primary-key
    /// domain. The namespace is permanent: changing it would break immutable
    /// revision identity and artifact references.
    #[must_use]
    pub fn from_definition_hash(definition_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x7c9e_6a55_3f1b_4d2a_8e0f_1c2d_3e4f_5a6b);
        Self::new(Uuid::new_v5(
            &NAMESPACE,
            definition_hash.as_str().as_bytes(),
        ))
    }
}

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

/// Governed, content-addressed entry/exit policy artifact identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradePolicyArtifactId(Arc<Uuid>);

impl TradePolicyArtifactId {
    /// Deterministically project immutable policy content into its ledger id.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0001);
        Self::new(Uuid::new_v5(&NAMESPACE, content_hash.as_str().as_bytes()))
    }
}

/// Immutable, content-addressed entry-condition artifact identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntryConditionArtifactId(Arc<Uuid>);

impl EntryConditionArtifactId {
    /// Deterministically project immutable condition content into its ledger id.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0711);
        Self::new(Uuid::new_v5(&NAMESPACE, content_hash.as_str().as_bytes()))
    }
}

/// Durable recommendation-level entry-condition instance identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntryConditionInstanceId(Arc<Uuid>);

/// Append-only entry-condition lifecycle audit identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntryConditionAuditId(Arc<Uuid>);

/// One immutable domain-event envelope identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DomainEventId(Arc<Uuid>);

impl DomainEventId {
    /// Deterministic identity for an immutable event envelope.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_d011);
        Self::new(Uuid::new_v5(&NAMESPACE, content_hash.as_str().as_bytes()))
    }
}

/// Append-only governance audit row for a trade-policy artifact.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradePolicyGovernanceAuditId(Arc<Uuid>);

/// One immutable independent validation attempt for a trade-policy artifact.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradePolicyValidationRunId(Arc<Uuid>);

/// One immutable candidate/fold/path attempt in a policy-fit trial ledger.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradePolicyTrialAttemptId(Arc<Uuid>);

impl TradePolicyTrialAttemptId {
    /// Deterministic identity for one immutable ordinal in a fit job.
    /// Lease recovery therefore replays the exact same append-only row instead
    /// of manufacturing a second attempt.
    #[must_use]
    pub fn from_fit_job_ordinal(fit_job_id: &ResearchJobId, attempt_ordinal: i64) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0722);
        let identity = format!("{}:{attempt_ordinal}", fit_job_id.as_uuid());
        Self::new(Uuid::new_v5(&NAMESPACE, identity.as_bytes()))
    }
}

/// Point-in-time backtest report identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BacktestReportId(Arc<Uuid>);

/// Combinatorial Purged Cross-Validation (CPCV) backtest path-set identifier
/// (Phase 11.5): one row per `phi`-path-reconstruction run over a frozen
/// `(model_spec, training_dataset, runtime_config)` triple.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BacktestPathSetId(Arc<Uuid>);

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
/// frequency, conditioned by `(category, ttr_bucket, price_bucket)` — plus
/// `kind = WeatherStationLeadBias`, the frozen station × lead correction used
/// by Weather features. All kinds share one content-addressed ledger.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalibrationArtifactId(Arc<Uuid>);

/// Shadow comparison record identifier (shadow vs active model run).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowComparisonId(Arc<Uuid>);

/// Frozen market → external-subject linkage ledger row identifier (11.2.2).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketLinkageId(Arc<Uuid>);

/// One capability-declared domain source binding.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DomainSourceExpectationId(Arc<Uuid>);

impl DomainSourceExpectationId {
    /// Stable identity for one canonical `(source_id, instrument_key)` pair.
    #[must_use]
    pub fn from_identity_hash(identity_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x37fa_7992_019c_4ef1_a225_9f35_d5da_86d9);
        Self::new(Uuid::new_v5(&NAMESPACE, identity_hash.as_str().as_bytes()))
    }
}

/// One durable feedback-cycle run.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeedbackRunId(Arc<Uuid>);

/// One append-only stage event in a feedback run.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeedbackRunStageId(Arc<Uuid>);

/// One immutable data/concept/label drift report.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DriftReportId(Arc<Uuid>);

/// One immutable factor bundle promoted atomically with a model.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorBundleId(Arc<Uuid>);

/// One append-only model + factor governance decision.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorGovernanceAuditId(Arc<Uuid>);

/// One governed cross-profile allocation proposal.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProfileAllocationId(Arc<Uuid>);

/// Basis-cross-check exceedance alert row identifier (11.2.2 remediation R6).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BasisAlertId(Arc<Uuid>);

/// Durable research job identifier (async dataset build / model train / backtest).
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResearchJobId(Arc<Uuid>);

/// One deterministic online/offline feature-parity replay run.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureParityRunId(Arc<Uuid>);

/// One stage-level comparison evidence row in `ClickHouse`.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureParityEventId(Arc<Uuid>);

impl FeatureParityEventId {
    /// Project a canonical evidence identity into an idempotent event id.
    ///
    /// Retrying a parity attempt for the same run/evidence key must replace the
    /// prior `ClickHouse` row rather than append a second logical comparison.
    #[must_use]
    pub fn from_evidence_hash(evidence_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6f5d_35d2_946a_4dc5_8eed_9b67_83b8_79ec);
        Self::new(Uuid::new_v5(&NAMESPACE, evidence_hash.as_str().as_bytes()))
    }
}

/// One append-only transition of the governed feature-parity latch.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureParityStateId(Arc<Uuid>);

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

/// One durable report build attempt.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportRunId(Arc<Uuid>);

/// One append-only aggregate of missed report schedule occurrences.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportScheduleGapId(Arc<Uuid>);

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

/// Append-only runtime-config approval decision identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigApprovalId(Arc<Uuid>);

/// Append-only operation-log row identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuditEventId(Arc<Uuid>);

/// Append-only bootstrap lifecycle transition identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BootstrapTransitionId(Arc<Uuid>);

/// Frozen parity subject identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureParitySubjectId(Arc<Uuid>);

/// Frozen parity candidate identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeatureParityCandidateId(Arc<Uuid>);

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
    use std::{ptr, thread::sleep, time::Duration};

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
            ptr::from_ref::<str>(id.as_str()),
            ptr::from_ref::<str>(cloned.as_str())
        );
    }
}
