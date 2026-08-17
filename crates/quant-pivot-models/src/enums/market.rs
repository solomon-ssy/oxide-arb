//! Market lifecycle enums for the data pipeline.

pg_enum! {
    type_name = "qp_market_status",
    @derive(schemars::JsonSchema)
    pub enum MarketStatus {
        Discovered => "discovered",
        Active => "active",
        Filtered => "filtered",
        Paused => "paused",
        ManuallyBlocked => "manually_blocked",
        Settled => "settled",
        Delisted => "delisted",
    }
}

pg_enum! {
    type_name = "qp_event_status",
    pub enum EventStatus {
        Active => "active",
        Closed => "closed",
        Archived => "archived",
        Unknown => "unknown",
    }
}
