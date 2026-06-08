//! Fact data-plane enums persisted in Postgres.

active_string_enum! {
    pub enum BalanceSnapshotSource {
        InternalLedger => "internal_ledger",
        ClobApi => "clob_api",
    }
}

active_string_enum! {
    pub enum ShadowDecisionType {
        WouldReject => "would_reject",
        WouldSize => "would_size",
        WouldScore => "would_score",
        NoEffect => "no_effect",
    }
}
