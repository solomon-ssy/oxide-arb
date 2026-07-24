//! Type-safe identifiers preventing accidental mixing of different ID domains.
//!
//! Identifiers fall into two families:
//!
//! - **External string ids** ([`MarketId`], [`TokenId`], [`EventId`],
//!   [`OrderId`], [`VenueTradeId`]) wrap `Arc<str>` via `#[derive(StrId)]`. Their
//!   value is defined by an external system or carries semantic structure, so
//!   it is **not** a UUID and is persisted as `text` / `varchar`.
//! - **Internal UUID ids** (everything else) wrap `Uuid` by value via
//!   `#[derive(UuidId)]` and persist as the native Postgres `uuid` type. They
//!   are generated in-process with [`from_v7`](UuidId) — always time-ordered so
//!   inserts stay sequential and indexes stay compact; no `prefix_` string
//!   scheme is used.
//!
//! External string ids share their variable-length bytes through `Arc<str>`.
//! Internal UUID ids are 16-byte `Copy` values, avoiding allocation, pointer
//! indirection, and reference-count traffic on hot paths.

use std::sync::Arc;

use quant_pivot_macros::{StrId, UuidId};
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

/// Globally unique Polymarket trade identifier returned by order placement.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VenueTradeId(Arc<str>);

/// External domain data source identifier (e.g. `binance`, `chainlink`).
///
/// A stable, lowercase source label persisted on every long-format
/// `quant_domain_observation` row and on ingest cursors. New sources are a
/// pure data extension — no schema change.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DomainSourceId(Arc<str>);

/// Stable project-owned research-profile name.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResearchProfileId(Arc<str>);

/// End-to-end request/correlation identity across HTTP and internal actions.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelationId(Arc<str>);

/// Stable RBAC role code used in governed audit context.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoleCode(Arc<str>);

/// Stable semantic operation name recorded in the append-only audit log.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationAction(Arc<str>);

/// Stable configured report-schedule identity.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReportScheduleId(Arc<str>);

/// Machine-readable diagnostic code; human detail is stored separately.
#[derive(StrId, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DiagnosticCode(Arc<str>);

impl DomainSourceId {
    /// Finalized Polygon Conditional Tokens resolution source.
    #[must_use]
    pub fn polymarket_ctf_resolution() -> Self {
        Self::new("polymarket_ctf_resolution")
    }

    /// Internal finalized Polymarket settlement accounting source.
    #[must_use]
    pub fn polymarket_settlement() -> Self {
        Self::new("polymarket_settlement")
    }

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
    pub fn binance_futures_trade() -> Self {
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

// ── Internal UUID identifiers (Uuid) ─────────────────────────────────────

/// Market selection snapshot used by a report or model run.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketSelectionId(Uuid);

/// One committed Gamma catalog synchronization batch.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogSyncBatchId(Uuid);

/// Content-addressed normalized Gamma event object.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogEventObjectId(Uuid);

impl CatalogEventObjectId {
    /// Stable projection of a catalog event content identifier.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x7c48_b3cf_f8bb_4e06_b099_2db7_4d1f_4ec1);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// One append-only event change observed in a committed sync batch.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogEventChangeId(Uuid);

/// Content-addressed normalized Gamma market object.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogMarketObjectId(Uuid);

impl CatalogMarketObjectId {
    /// Stable projection of a catalog market content identifier.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0xb7dd_4b1e_f861_4b4f_a787_7c54_b1da_1690);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// One append-only market change observed in a committed sync batch.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogMarketChangeId(Uuid);

/// One typed catalog input rejection attached to a failed sync attempt.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogSyncRejectionId(Uuid);

/// Append-only point-in-time CLOB market-info observation.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClobMarketInfoVersionId(Uuid);

/// Deterministic point-in-time source-slice materialization ledger row.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceSliceId(Uuid);

/// Point-in-time feature vector snapshot identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureVectorId(Uuid);

/// Governed factor definition identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FactorDefinitionId(Uuid);

impl FactorDefinitionId {
    /// Project a canonical factor-definition digest into the UUID primary-key
    /// domain. The namespace is permanent: changing it would break immutable
    /// revision identity and artifact references.
    #[must_use]
    pub fn from_definition_hash(definition_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x7c9e_6a55_3f1b_4d2a_8e0f_1c2d_3e4f_5a6b);
        Self::new(uuid_v5_for_content(&NAMESPACE, definition_hash))
    }
}

/// Persisted factor value identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FactorValueId(Uuid);

/// Governed model specification identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelSpecId(Uuid);

/// Published model version identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelVersionId(Uuid);

/// Model training, backtest, shadow, or inference run identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelRunId(Uuid);

/// Candidate signal emitted before portfolio pruning.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignalCandidateId(Uuid);

/// Frozen, point-in-time training dataset artifact identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrainingDatasetId(Uuid);

/// One materialized training example (row) within a training dataset.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrainingExampleId(Uuid);

/// Stored model artifact (serialized weights / model) identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelArtifactId(Uuid);

/// Governed, content-addressed entry/exit policy artifact identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradePolicyArtifactId(Uuid);

impl TradePolicyArtifactId {
    /// Deterministically project immutable policy content into its ledger id.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0001);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// Immutable, content-addressed entry-condition artifact identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryConditionArtifactId(Uuid);

impl EntryConditionArtifactId {
    /// Deterministically project immutable condition content into its ledger id.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0711);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// Durable recommendation-level entry-condition instance identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryConditionInstanceId(Uuid);

/// Append-only entry-condition lifecycle audit identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryConditionAuditId(Uuid);

/// One immutable domain-event envelope identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainEventId(Uuid);

impl DomainEventId {
    /// Deterministic identity for an immutable event envelope.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_d011);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// Append-only governance audit row for a trade-policy artifact.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradePolicyGovernanceAuditId(Uuid);

/// One immutable independent validation attempt for a trade-policy artifact.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradePolicyValidationRunId(Uuid);

/// One immutable candidate/fold/path attempt in a policy-fit trial ledger.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradePolicyTrialAttemptId(Uuid);

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
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BacktestReportId(Uuid);

/// Combinatorial Purged Cross-Validation (CPCV) backtest path-set identifier:
/// one row per `phi`-path-reconstruction run over a frozen
/// `(model_spec, training_dataset, runtime_config)` triple.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BacktestPathSetId(Uuid);

/// Pairwise model-comparison report identifier (baseline vs candidate replay).
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelComparisonReportId(Uuid);

/// Unified, content-addressed calibration-artifact identifier.
///
/// Shared by every empirical calibration artifact in the system:
/// `kind = ModelScore` (a [`crate::enums::quant::CalibrationKind`]) — a
/// `ProbabilityCalibrator` mapping model score → `P(win)`, fit on an
/// independent held-out calibration split — and `kind = MarketPriceBias`
/// (formerly the standalone `FavoriteLongshotBiasTableId`,
/// deleted, no alias) — market-implied price → empirical settlement
/// frequency, conditioned by `(category, ttr_bucket, price_bucket)` — plus
/// `kind = WeatherStationLeadBias`, the frozen station × lead correction used
/// by Weather features. All kinds share one content-addressed ledger.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CalibrationArtifactId(Uuid);

/// Shadow comparison record identifier (shadow vs active model run).
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShadowComparisonId(Uuid);

/// Frozen market → external-subject linkage ledger row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketLinkageId(Uuid);

/// One capability-declared domain source binding.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainSourceExpectationId(Uuid);

impl DomainSourceExpectationId {
    /// Stable identity for one canonical `(source_id, instrument_key)` pair.
    #[must_use]
    pub fn from_identity_hash(identity_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x37fa_7992_019c_4ef1_a225_9f35_d5da_86d9);
        Self::new(uuid_v5_for_content(&NAMESPACE, identity_hash))
    }
}

/// One durable feedback-cycle run.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedbackRunId(Uuid);

/// One append-only stage event in a feedback run.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedbackRunStageId(Uuid);

/// One immutable data/concept/label drift report.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DriftReportId(Uuid);

/// One immutable factor bundle promoted atomically with a model.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FactorBundleId(Uuid);

/// One append-only model + factor governance decision.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FactorGovernanceAuditId(Uuid);

/// One governed cross-profile allocation proposal.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProfileAllocationId(Uuid);

/// Basis-cross-check exceedance alert row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BasisAlertId(Uuid);

/// Durable research job identifier (async dataset build / model train / backtest).
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResearchJobId(Uuid);

/// One deterministic online/offline feature-parity replay run.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureParityRunId(Uuid);

/// One stage-level comparison evidence row in `ClickHouse`.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureParityEventId(Uuid);

impl FeatureParityEventId {
    /// Project a canonical evidence identity into an idempotent event id.
    ///
    /// Retrying a parity attempt for the same run/evidence key must replace the
    /// prior `ClickHouse` row rather than append a second logical comparison.
    #[must_use]
    pub fn from_evidence_hash(evidence_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x6f5d_35d2_946a_4dc5_8eed_9b67_83b8_79ec);
        Self::new(uuid_v5_for_content(&NAMESPACE, evidence_hash))
    }
}

/// One append-only transition of the governed feature-parity latch.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureParityStateId(Uuid);

/// Model-governance audit row identifier (publish / retire / rollback / promote).
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelGovernanceAuditId(Uuid);

/// Decision-time venue account / capital snapshot identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountSnapshotId(Uuid);

/// Strategy-capital equity curve snapshot identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EquitySnapshotId(Uuid);

/// Report-level data-quality snapshot identifier (one row per report fire).
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportDataQualitySnapshotId(Uuid);

/// Portfolio plan identifier used by a recommendation report.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PortfolioPlanId(Uuid);

/// Immutable `TopN` recommendation report identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecommendationReportId(Uuid);

/// One durable report build attempt.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportRunId(Uuid);

/// One append-only aggregate of missed report schedule occurrences.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportScheduleGapId(Uuid);

/// Single recommendation row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecommendationId(Uuid);

/// Governed bridge from a recommendation to execution.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderIntentId(Uuid);

/// Internal execution-order lifecycle identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionOrderId(Uuid);

/// One Polymarket trade identity attached to an execution order.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionTradeRefId(Uuid);

/// One execution-order to EVM transaction relationship.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionTransactionRefId(Uuid);

/// Immutable money-holding account identity used by order and settlement ledgers.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionAccountId(Uuid);

impl ExecutionAccountId {
    /// Deterministic identity for one canonical execution-account digest.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x1fe8_5901_622b_47cb_baaa_c2ec_0e37_5ff8);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// Intent-level capital allocation ledger identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CapitalAllocationId(Uuid);

/// One on-chain CTF redemption batch for a `(condition_id, funder)` pair.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementRedeemId(Uuid);

/// One durable approval, revocation, or redemption chain submission.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementChainSubmissionId(Uuid);

/// One immutable `SemiAuto` settlement authorization attempt.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementAuthorizationId(Uuid);

/// One governed settlement action or exact one-shot canary grant.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementGovernedActionId(Uuid);

/// Durable cursor for account-scoped external settlement observation.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementExternalCursorId(Uuid);

/// One immutable contributing lot in a settlement inventory snapshot.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementInventoryLotId(Uuid);

/// Per-position allocation row within a settlement redemption batch.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SettlementRedeemLotId(Uuid);

/// Position-lot ledger identifier.
///
/// One lot per filled entry intent (`order_intent_id` is its natural unique
/// key); the surrogate id keeps the entity addressable independent of the
/// originating intent and lets the per-token aggregate stay a query view.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PositionId(Uuid);

/// Execution-order reconciliation record identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconciliationId(Uuid);

/// Immutable revision of one governed configuration resource.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyRevisionId(Uuid);

/// Frozen bundle of policy revisions captured at a decision boundary.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecisionPolicySnapshotId(Uuid);

impl DecisionPolicySnapshotId {
    /// Deterministic identity for a content-addressed policy bundle.
    #[must_use]
    pub fn from_content_hash(content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x9ce1_c2e4_f455_4d54_97b1_49bc_9e26_72a1);
        Self::new(uuid_v5_for_content(&NAMESPACE, content_hash))
    }
}

/// Content-addressed immutable policy-profile artifact identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileArtifactId(Uuid);

impl ProfileArtifactId {
    /// Derive a stable row identity from the profile kind and canonical content hash.
    #[must_use]
    pub fn from_content_address(kind: &str, content_hash: &ContentHash) -> Self {
        const NAMESPACE: Uuid = Uuid::from_u128(0x8fe9_fea3_9f92_51d7_a432_3bcf_59f5_d6dd);
        let address = format!("{kind}:{content_hash}");
        Self::new(Uuid::new_v5(&NAMESPACE, address.as_bytes()))
    }
}

/// Governed policy-resource activation identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyActivationId(Uuid);

/// Append-only policy-resource approval decision identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyApprovalId(Uuid);

/// One-time guarded preproduction-reset plan and confirmation nonce.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreproductionResetNonce(Uuid);

/// Append-only operation-log row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuditEventId(Uuid);

/// Append-only atomic runtime-control transition identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeControlTransitionId(Uuid);

/// Frozen parity subject identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureParitySubjectId(Uuid);

/// Frozen parity candidate identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureParityCandidateId(Uuid);

// ── RBAC identifiers (web layer) ─────────────────────────────────────────

/// RBAC user identifier.
///
/// This is the stable Casbin subject: renaming a user or changing their
/// username never invalidates their role bindings.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserId(Uuid);

/// RBAC role identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoleId(Uuid);

/// RBAC menu identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MenuId(Uuid);

/// Append-only operation-log row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationLogId(Uuid);

/// Process/lease/claim owner identity used by durable workers.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(Uuid);

/// Immutable research-readiness evidence row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResearchReadinessEvidenceId(Uuid);

/// Calibration artifact publication ledger identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CalibrationArtifactPublicationId(Uuid);

/// Entry-condition evaluation outbox row identifier.
#[derive(UuidId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryConditionEvaluationOutboxId(Uuid);

fn uuid_v5_for_content(namespace: &Uuid, content_hash: &ContentHash) -> Uuid {
    let canonical = content_hash.canonical_text();
    Uuid::new_v5(namespace, canonical.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::{
        mem::{needs_drop, size_of},
        ptr,
        thread::sleep,
        time::Duration,
    };

    use bincode::{deserialize as bincode_deserialize, serialize as bincode_serialize};
    use bitcode::{deserialize as bitcode_deserialize, serialize as bitcode_serialize};
    use sea_orm::sea_query::Value;

    use super::*;

    #[test]
    fn recommendation_report_id_time() {
        const N: usize = 50;
        let mut ids: Vec<RecommendationReportId> = Vec::with_capacity(N);
        for _ in 0..N {
            ids.push(RecommendationReportId::from_v7());
            sleep(Duration::from_millis(2));
        }
        let mut sorted = ids.clone();
        sorted.sort_by_key(|id| id.as_uuid());
        let expected: Vec<Uuid> = ids
            .iter()
            .copied()
            .map(RecommendationReportId::as_uuid)
            .collect();
        let got: Vec<Uuid> = sorted
            .iter()
            .copied()
            .map(RecommendationReportId::as_uuid)
            .collect();
        assert_eq!(got, expected, "UUID v7 must be time-ordered");
    }

    #[test]
    fn order_intent_id_7() {
        let id = OrderIntentId::from_v7();
        assert_eq!(id.as_uuid().get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn generated_ids_version_7() {
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
    fn uuid_id_inline_value() {
        const UUID: Uuid = Uuid::from_u128(0x0190_6f35_2b3a_7f11_9c22_aabb_ccdd_eeff);
        const ID: RecommendationId = RecommendationId::new(UUID);

        fn assert_copy<T: Copy>() {}

        assert_copy::<RecommendationId>();
        assert_eq!(size_of::<RecommendationId>(), 16);
        assert!(!needs_drop::<RecommendationId>());
        assert_eq!(ID.as_uuid(), UUID);
        assert_eq!(ID.as_uuid_ref(), &UUID);
        assert_eq!(ID.into_uuid(), UUID);
        assert_eq!(RecommendationId::from(&ID), ID);
    }

    #[test]
    fn uuid_id_matches_string() {
        const UUID: Uuid = Uuid::from_u128(0x0190_6f35_2b3a_7f11_9c22_aabb_ccdd_eeff);
        let id = RecommendationId::new(UUID);
        let canonical = UUID.hyphenated().to_string();

        {
            let actual_wire = bincode_serialize(&id).unwrap();
            let expected_wire = bincode_serialize(&canonical).unwrap();
            assert_eq!(actual_wire, expected_wire);
            assert_eq!(
                bincode_deserialize::<RecommendationId>(&actual_wire).unwrap(),
                id
            );
        }

        {
            let actual_wire = bitcode_serialize(&id).unwrap();
            let expected_wire = bitcode_serialize(&canonical).unwrap();
            assert_eq!(actual_wire, expected_wire);
            assert_eq!(
                bitcode_deserialize::<RecommendationId>(&actual_wire).unwrap(),
                id
            );
        }
    }

    #[test]
    fn uuid_id_seaorm_uuid() {
        const UUID: Uuid = Uuid::from_u128(0x0190_6f35_2b3a_7f11_9c22_aabb_ccdd_eeff);
        let id = RecommendationId::new(UUID);

        assert_eq!(Value::from(id), Value::Uuid(Some(UUID)));
        assert_eq!(Value::from(&id), Value::Uuid(Some(UUID)));
    }

    #[test]
    fn content_address_never_drifts() {
        let hash = ContentHash::from_bytes([0; 32]);

        assert_eq!(
            CatalogEventObjectId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0xa1e6_20dc_8fc3_545e_b346_1769_4ed8_abb5)
        );
        assert_eq!(
            CatalogMarketObjectId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0x933a_3270_9dbb_5226_916d_e2d9_08b6_ba8f)
        );
        assert_eq!(
            FactorDefinitionId::from_definition_hash(&hash).as_uuid(),
            Uuid::from_u128(0x95e3_baa0_a627_53cc_b8f8_6a83_b69c_5c69)
        );
        assert_eq!(
            TradePolicyArtifactId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0xa8f8_0211_bf62_5e7b_acd9_54e2_4dfe_89df)
        );
        assert_eq!(
            EntryConditionArtifactId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0x5c55_4839_6590_5f4e_be25_bc7b_6cfa_2450)
        );
        assert_eq!(
            DomainEventId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0xaeda_7b2e_79f9_53bd_bdba_66dc_9a78_efd2)
        );
        assert_eq!(
            DomainSourceExpectationId::from_identity_hash(&hash).as_uuid(),
            Uuid::from_u128(0x45f5_9084_0ac2_56bb_886c_4f8e_77bd_903d)
        );
        assert_eq!(
            FeatureParityEventId::from_evidence_hash(&hash).as_uuid(),
            Uuid::from_u128(0x6053_a5c2_d9a7_501d_84ac_a285_b7a9_76d0)
        );
        assert_eq!(
            DecisionPolicySnapshotId::from_content_hash(&hash).as_uuid(),
            Uuid::from_u128(0xe55d_fb78_4c9a_5e5a_b252_3822_edac_2b55)
        );
        assert_eq!(
            ProfileArtifactId::from_content_address("example", &hash).as_uuid(),
            Uuid::from_u128(0xaed1_39c4_7491_5f75_8d3d_333f_9baf_ebf5)
        );
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
    fn str_id_clone_cheap() {
        let id = TokenId::new("token-123");
        let cloned = id.clone();
        assert_eq!(id, cloned);
        assert_eq!(
            ptr::from_ref::<str>(id.as_str()),
            ptr::from_ref::<str>(cloned.as_str())
        );
    }
}
