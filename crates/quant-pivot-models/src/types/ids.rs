//! Type-safe identifiers preventing accidental mixing of different ID domains.
//!
//! Identifiers fall into two families:
//!
//! - **External string ids** ([`MarketId`], [`TokenId`], [`EventId`],
//!   [`OrderId`], [`ReportId`]) wrap `Arc<str>` via `#[derive(StrId)]`. Their
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
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId(Arc<str>);

/// Polymarket event identifier.
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(Arc<str>);

/// `ERC-1155` conditional token identifier (CLOB `token_id`).
///
/// **Namespace safety**: `TokenId` and `MarketId` are distinct namespaces.
/// Never construct a `TokenId` from a `MarketId` string — this will cause
/// silent lookup failures. Polymarket `condition_id` (`MarketId`) starts with
/// "0x" (66 chars); CLOB token IDs are decimal U256 strings.
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
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
///
/// Live orders carry the venue-assigned string; dry-run / paper modes mint a
/// synthetic value via [`OrderId::synthetic`].
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId(Arc<str>);

impl OrderId {
    /// Generate a synthetic order id for dry-run / paper-trade modes.
    #[must_use]
    pub fn synthetic() -> Self {
        Self::new(Uuid::now_v7().to_string())
    }
}

/// Report snapshot identifier (e.g. `"daily_2025-06-01"`).
#[derive(StrId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportId(Arc<str>);

// ── Internal UUID identifiers (Arc<Uuid>) ────────────────────────────────

/// Point-in-time market metadata snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketPitSnapshotId(Arc<Uuid>);

/// Detection-pipeline opportunity identifier.
///
/// Uses UUID v7 so the identifier is time-ordered — sorting `OpportunityId`
/// is the same as sorting by detection instant. An opportunity that has not
/// yet passed score gates is represented by `Option::None`, never a sentinel.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpportunityId(Arc<Uuid>);

/// Unique trade identifier.
///
/// Independent from [`OpportunityId`] — correlate via opportunity fields on
/// trade records.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradeId(Arc<Uuid>);

/// Unique identifier for a single execution attempt.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionId(Arc<Uuid>);

impl Default for ExecutionId {
    fn default() -> Self {
        Self::from_v7()
    }
}

/// Unique position lifecycle identifier.
///
/// Each open/close/settle cycle for a (market, token, side) triple generates a
/// new `PositionId`, allowing full history tracking.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionId(Arc<Uuid>);

/// Exposure reservation identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReservationId(Arc<Uuid>);

/// Accounting period identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeriodId(Arc<Uuid>);

/// Potential loss ledger entry identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LedgerId(Arc<Uuid>);

/// Market resolution event identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolutionEventId(Arc<Uuid>);

/// Governed control-factor artifact identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlFactorId(Arc<Uuid>);

/// Control-factor publication identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorPublicationId(Arc<Uuid>);

/// Point-in-time materialization run identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializationRunId(Arc<Uuid>);

/// Evidence stage report identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct StageReportId(Arc<Uuid>);

/// Runtime-config version identifier used by PIT evidence manifests.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigVersionId(Arc<Uuid>);

/// Runtime-config activation identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigActivationId(Arc<Uuid>);

/// Cash/collateral balance snapshot identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BalanceSnapshotId(Arc<Uuid>);

/// Control-factor training dataset manifest identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrainingDatasetId(Arc<Uuid>);

/// Shadow decision audit identifier.
#[derive(UuidId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowDecisionId(Arc<Uuid>);

/// Append-only control-factor audit event identifier.
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
    fn opportunity_id_v7_sortable_by_time() {
        const N: usize = 50;
        let mut ids: Vec<OpportunityId> = Vec::with_capacity(N);
        for _ in 0..N {
            ids.push(OpportunityId::from_v7());
            sleep(Duration::from_millis(2));
        }
        let mut sorted = ids.clone();
        sorted.sort_by_key(OpportunityId::as_uuid);
        let expected: Vec<Uuid> = ids.iter().map(OpportunityId::as_uuid).collect();
        let got: Vec<Uuid> = sorted.iter().map(OpportunityId::as_uuid).collect();
        assert_eq!(got, expected, "UUID v7 must be time-ordered");
    }

    #[test]
    fn trade_id_from_v7_is_version_7() {
        let id = TradeId::from_v7();
        assert_eq!(id.as_uuid().get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn generated_ids_are_version_7() {
        assert_eq!(
            ExecutionId::from_v7().as_uuid().get_version(),
            Some(uuid::Version::SortRand)
        );
        assert_eq!(
            PositionId::from_v7().as_uuid().get_version(),
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
        let id = TradeId::from_v7();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!(r#""{}""#, id.as_uuid()));
        let back: TradeId = serde_json::from_str(&json).unwrap();
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
