//! String-typed identifiers preventing accidental mixing of different ID domains.
//!
//! All identifiers use `Arc<str>` internally so that `clone()` is an atomic
//! reference-count increment (O(1)) rather than a heap allocation (O(n)).
//! This matters on the hot path where `TokenId` / `MarketId` are frequently
//! passed through channels and stored in multiple data structures.

use oxide_arb_macros::TypedId;
use std::sync::Arc;
use uuid::Uuid;

/// Polymarket `condition_id` identifying a market.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId(Arc<str>);

/// Point-in-time market metadata snapshot identifier.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketPitSnapshotId(Arc<str>);

impl MarketPitSnapshotId {
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("mps_{}", Uuid::now_v7()).as_str()))
    }
}

/// Polymarket event identifier.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(Arc<str>);

/// `ERC-1155` conditional token identifier (CLOB `token_id`).
///
/// **Namespace safety**: `TokenId` and `MarketId` are distinct namespaces.
/// Never construct a `TokenId` from a `MarketId` string — this will cause
/// silent lookup failures. Polymarket `condition_id` (`MarketId`) starts with
/// "0x" (66 chars); CLOB token IDs are decimal U256 strings.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
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

/// Detection-pipeline opportunity identifier.
///
/// Uses UUID v7 so the string is time-ordered — sorting `OpportunityId`
/// lexicographically is the same as sorting by detection instant.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpportunityId(Arc<str>);

impl OpportunityId {
    /// Placeholder until score gates pass and a v7 ID is assigned.
    #[must_use]
    pub fn pending() -> Self {
        Self(Arc::from(""))
    }

    /// Generate a fresh time-ordered ID (UUID v7).
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(Uuid::now_v7().to_string().as_str()))
    }

    #[must_use]
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.as_str().is_empty()
    }
}

/// Unique trade identifier (`t_<uuid v7>`).
///
/// Independent from [`OpportunityId`] — correlate via opportunity fields on trade records.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradeId(Arc<str>);

impl TradeId {
    /// Generate a new time-ordered trade ID (UUID v7).
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(format!("t_{}", Uuid::now_v7()).as_str()))
    }
}

/// Unique identifier for a single execution attempt.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionId(Arc<str>);

impl ExecutionId {
    /// Generate a new random execution ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().to_string().as_str()))
    }
}

impl Default for ExecutionId {
    fn default() -> Self {
        Self::generate()
    }
}

/// CLOB order identifier returned by Polymarket after submission.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId(Arc<str>);

impl OrderId {
    /// Generate a new random order ID for dry-run / paper-trade modes.
    #[must_use]
    pub fn new_id() -> Self {
        Self(Arc::from(format!("o_{}", Uuid::new_v4()).as_str()))
    }
}

/// Unique position lifecycle identifier (UUID v4).
///
/// Each open/close/settle cycle for a (market, token, side) triple
/// generates a new `PositionId`, allowing full history tracking.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionId(Arc<str>);

impl PositionId {
    /// Generate a new random position ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().to_string().as_str()))
    }
}

/// Exposure reservation identifier.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReservationId(Arc<str>);

impl ReservationId {
    /// Generate a new random reservation ID.
    #[must_use]
    pub fn new_id() -> Self {
        Self(Arc::from(format!("res_{}", Uuid::new_v4()).as_str()))
    }
}

/// Accounting period identifier (UUID v4).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeriodId(Arc<str>);

impl PeriodId {
    /// Generate a new random period ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().to_string().as_str()))
    }
}

/// Potential loss ledger entry identifier (UUID v4).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LedgerId(Arc<str>);

impl LedgerId {
    /// Generate a new random ledger entry ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().to_string().as_str()))
    }
}

/// Report snapshot identifier (e.g. `"daily_2025-06-01"`).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportId(Arc<str>);

/// Governed control-factor artifact identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlFactorId(Arc<str>);

impl ControlFactorId {
    /// Generate a fresh time-ordered control-factor ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cf_{}", Uuid::now_v7()).as_str()))
    }
}

/// Control-factor publication identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorPublicationId(Arc<str>);

impl FactorPublicationId {
    /// Generate a fresh time-ordered publication ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cfp_{}", Uuid::now_v7()).as_str()))
    }
}

/// Point-in-time materialization run identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializationRunId(Arc<str>);

impl MaterializationRunId {
    /// Generate a fresh time-ordered materialization run ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cfmr_{}", Uuid::now_v7()).as_str()))
    }
}

/// Evidence stage report identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct StageReportId(Arc<str>);

impl StageReportId {
    /// Generate a fresh time-ordered stage report ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cfsr_{}", Uuid::now_v7()).as_str()))
    }
}

/// Runtime-config version identifier used by PIT evidence manifests.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigVersionId(Arc<str>);

impl RuntimeConfigVersionId {
    /// Generate a fresh time-ordered runtime-config version ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("rcv_{}", Uuid::now_v7()).as_str()))
    }
}

/// Runtime-config activation identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeConfigActivationId(Arc<str>);

impl RuntimeConfigActivationId {
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("rca_{}", Uuid::now_v7()).as_str()))
    }
}

/// Cash/collateral balance snapshot identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct BalanceSnapshotId(Arc<str>);

impl BalanceSnapshotId {
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("bs_{}", Uuid::now_v7()).as_str()))
    }
}

/// Control-factor training dataset manifest identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrainingDatasetId(Arc<str>);

impl TrainingDatasetId {
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cftd_{}", Uuid::now_v7()).as_str()))
    }
}

/// Shadow decision audit identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowDecisionId(Arc<str>);

impl ShadowDecisionId {
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cfsd_{}", Uuid::now_v7()).as_str()))
    }
}

/// Append-only control-factor audit event identifier (UUID v7).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuditEventId(Arc<str>);

impl AuditEventId {
    /// Generate a fresh time-ordered audit event ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("cfae_{}", Uuid::now_v7()).as_str()))
    }
}

// ── RBAC identifiers (Phase 6 web layer) ─────────────────────────────────────

/// RBAC user identifier (UUID v7, `usr_` prefix).
///
/// This is the stable Casbin subject: renaming a user or changing their
/// username never invalidates their role bindings.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(Arc<str>);

impl UserId {
    /// Generate a fresh time-ordered user ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("usr_{}", Uuid::now_v7()).as_str()))
    }
}

/// RBAC role identifier (UUID v7, `rol_` prefix).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleId(Arc<str>);

impl RoleId {
    /// Generate a fresh time-ordered role ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("rol_{}", Uuid::now_v7()).as_str()))
    }
}

/// RBAC menu identifier (UUID v7, `mnu_` prefix).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MenuId(Arc<str>);

impl MenuId {
    /// Generate a fresh time-ordered menu ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("mnu_{}", Uuid::now_v7()).as_str()))
    }
}

/// `user_role` join-row identifier (UUID v7, `url_` prefix).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserRoleId(Arc<str>);

impl UserRoleId {
    /// Generate a fresh time-ordered user-role binding ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("url_{}", Uuid::now_v7()).as_str()))
    }
}

/// `role_menu` join-row identifier (UUID v7, `rml_` prefix).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleMenuId(Arc<str>);

impl RoleMenuId {
    /// Generate a fresh time-ordered role-menu binding ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("rml_{}", Uuid::now_v7()).as_str()))
    }
}

/// Append-only operation-log row identifier (UUID v7, `opl_` prefix).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationLogId(Arc<str>);

impl OperationLogId {
    /// Generate a fresh time-ordered operation-log ID.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Arc::from(format!("opl_{}", Uuid::now_v7()).as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    #[test]
    fn opportunity_id_v7_sortable_by_time() {
        const N: usize = 50;
        let mut ids: Vec<OpportunityId> = Vec::with_capacity(N);
        for _ in 0..N {
            ids.push(OpportunityId::new_v7());
            sleep(Duration::from_millis(2));
        }
        let mut sorted = ids.clone();
        sorted.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        let expected: Vec<&str> = ids.iter().map(OpportunityId::as_str).collect();
        let got: Vec<&str> = sorted.iter().map(OpportunityId::as_str).collect();
        assert_eq!(
            got, expected,
            "UUID v7 must be lexicographically time-ordered"
        );
    }

    #[test]
    fn trade_id_generate_is_prefixed_uuid_v7() {
        let id = TradeId::generate();
        let s = id.as_str();
        let suffix = s.strip_prefix("t_").expect("trade id must use t_ prefix");
        let parsed = Uuid::parse_str(suffix).expect("suffix must be a UUID");
        assert_eq!(parsed.get_version(), Some(uuid::Version::SortRand));
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
    fn id_clone_is_cheap() {
        let id = TokenId::new("token-123");
        let cloned = id.clone();
        assert_eq!(id, cloned);
        assert_eq!(
            std::ptr::from_ref::<str>(id.as_str()),
            std::ptr::from_ref::<str>(cloned.as_str())
        );
    }
}
