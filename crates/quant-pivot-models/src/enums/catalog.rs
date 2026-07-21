//! Strongly typed `PostgreSQL` enums for the Gamma catalog ledger.

use sea_orm::{ActiveValue, IntoActiveValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A legitimate Gamma pre-listing object that cannot yet enter the canonical
/// market projection because its venue identity is incomplete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogPrelistingFilterReason {
    MissingConditionId,
}

impl CatalogPrelistingFilterReason {
    /// Stable low-cardinality observability label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingConditionId => "missing_condition_id",
        }
    }
}

pg_enum! {
    type_name = "qp_catalog_sync_kind",
    pub enum CatalogSyncKind {
        Baseline => "baseline",
        Reconcile => "reconcile",
    }
}

/// Deterministically ordered set of explicit catalog filter facts.
///
/// The domain uses a set so duplicate upstream signals cannot perturb content
/// hashes. Persistence converts it to the native `PostgreSQL` enum array used by
/// the `SeaORM` entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CatalogFilterReasonSet(u8);

impl CatalogFilterReason {
    pub const ALL: [Self; 4] = [
        Self::Inactive,
        Self::Closed,
        Self::ClobDisabled,
        Self::OrdersNotAccepted,
    ];

    const fn bit(self) -> u8 {
        match self {
            Self::Inactive => 1 << 0,
            Self::Closed => 1 << 1,
            Self::ClobDisabled => 1 << 2,
            Self::OrdersNotAccepted => 1 << 3,
        }
    }
}

impl CatalogFilterReasonSet {
    pub const EMPTY: Self = Self(0);

    pub const fn insert(&mut self, reason: CatalogFilterReason) {
        self.0 |= reason.bit();
    }

    #[must_use]
    pub const fn contains(self, reason: CatalogFilterReason) -> bool {
        self.0 & reason.bit() != 0
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn iter(self) -> impl Iterator<Item = CatalogFilterReason> {
        CatalogFilterReason::ALL
            .into_iter()
            .filter(move |reason| self.contains(*reason))
    }
}

impl FromIterator<CatalogFilterReason> for CatalogFilterReasonSet {
    fn from_iter<I: IntoIterator<Item = CatalogFilterReason>>(iter: I) -> Self {
        let mut set = Self::EMPTY;
        for reason in iter {
            set.insert(reason);
        }
        set
    }
}

impl IntoActiveValue<Vec<CatalogFilterReason>> for CatalogFilterReasonSet {
    fn into_active_value(self) -> ActiveValue<Vec<CatalogFilterReason>> {
        ActiveValue::Set(self.iter().collect())
    }
}

impl Serialize for CatalogFilterReasonSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for CatalogFilterReasonSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<CatalogFilterReason>::deserialize(deserializer)
            .map(|reasons| reasons.into_iter().collect())
    }
}

pg_enum! {
    type_name = "qp_catalog_sync_status",
    pub enum CatalogSyncStatus {
        Committed => "committed",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_catalog_sync_failure_stage",
    pub enum CatalogSyncFailureStage {
        Fetch => "fetch",
        Normalize => "normalize",
        Persist => "persist",
        Recovery => "recovery",
    }
}

pg_enum! {
    type_name = "qp_catalog_timestamp_quality",
    pub enum CatalogTimestampQuality {
        Source => "source",
        CommitTimeFallback => "commit_time_fallback",
    }
}

pg_enum! {
    type_name = "qp_catalog_change_type",
    pub enum CatalogChangeType {
        GammaScanUpsert => "gamma_scan_upsert",
        GammaIdRecheckUpsert => "gamma_id_recheck_upsert",
        GammaConfirmedTombstone => "gamma_confirmed_tombstone",
    }
}

impl CatalogChangeType {
    #[must_use]
    pub const fn is_tombstone(self) -> bool {
        matches!(self, Self::GammaConfirmedTombstone)
    }
}

pg_enum! {
    type_name = "qp_catalog_entity_kind",
    pub enum CatalogEntityKind {
        Event => "event",
        Market => "market",
        Cursor => "cursor",
    }
}

pg_enum! {
    type_name = "qp_catalog_rejection_reason",
    pub enum CatalogRejectionReason {
        EmptyConditionId => "empty_condition_id",
        MissingClobTokenIds => "missing_clob_token_ids",
        NotBinary => "not_binary",
        InvalidTokenPair => "invalid_token_pair",
        UnsupportedTickSize => "unsupported_tick_size",
        DuplicateEntityId => "duplicate_entity_id",
        MalformedEntity => "malformed_entity",
        CursorProtocolViolation => "cursor_protocol_violation",
    }
}

pg_enum! {
    type_name = "qp_catalog_filter_reason",
    pub enum CatalogFilterReason {
        Inactive => "inactive",
        Closed => "closed",
        ClobDisabled => "clob_disabled",
        OrdersNotAccepted => "orders_not_accepted",
    }
}
