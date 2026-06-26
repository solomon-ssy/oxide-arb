//! Market lifecycle enums for the data pipeline.

crate::pg_enum! {
    type_name = "qp_market_status",
    pub enum MarketStatus {
        Discovered => "discovered",
        Active => "active",
        Filtered => "filtered",
        Paused => "paused",
        Settled => "settled",
        Delisted => "delisted",
    }
}

crate::pg_enum! {
    type_name = "qp_event_status",
    pub enum EventStatus {
        Active => "active",
        Closed => "closed",
        Archived => "archived",
        Unknown => "unknown",
    }
}
