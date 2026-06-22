//! Market lifecycle enums for the data pipeline.

active_string_enum! {
    /// Lifecycle state of a market in the registry.
    pub enum MarketStatus {
        Discovered => "discovered",
        Active => "active",
        Filtered => "filtered",
        Paused => "paused",
        Settled => "settled",
        Delisted => "delisted",
    }
}

active_string_enum! {
    /// Lifecycle state of an external Polymarket event.
    ///
    /// Event status is intentionally separate from [`MarketStatus`]: events model
    /// the upstream Gamma lifecycle, while markets additionally carry local
    /// registry states such as discovered, filtered, and delisted.
    pub enum EventStatus {
        /// Event is open for active market discovery and scanning.
        Active => "active",
        /// Event has closed to new trading.
        Closed => "closed",
        /// Event has been archived by the upstream source.
        Archived => "archived",
        /// Event status was not recognized by the ingestion layer.
        Unknown => "unknown",
    }
}
