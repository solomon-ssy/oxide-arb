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

/// Outbox event row primary key (UUID v4).
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutboxEventId(Arc<str>);

impl OutboxEventId {
    /// Generate a new random outbox event ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Arc::from(Uuid::new_v4().to_string().as_str()))
    }
}

/// Polymorphic aggregate reference in transactional outbox rows.
#[derive(TypedId, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateId(Arc<str>);

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
