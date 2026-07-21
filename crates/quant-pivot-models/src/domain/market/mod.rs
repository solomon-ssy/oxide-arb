//! Market context: market metadata and live order-book DTOs.

pub mod book;
pub mod catalog;
pub mod fee;
pub mod registry;
pub mod version;

pub use book::{
    BinaryBookPair, BookGateError, BookGateLeg, BookLevel, BookSideView, BookSnapshot,
    IMBALANCE_DEPTH_LEVELS, OrderbookSide, QuantBookSnapshot, QuantBookView, TopOfBook,
    bid_depth_down_to, top_n_share_depth, total_depth_usd,
};
pub use catalog::{EventInfo, EventTags, UpsertEvent};
pub use fee::{BuilderFeeAttribution, MarketFeeSchedule};
pub use registry::{
    CatalogMarketLeg, EventRegistryInfo, MarketInfo, MarketRegistryInfo, NegRiskLeg, NegRiskLegSet,
    TokenInfo, UpsertMarket, resolve_binary_pair_exact,
};
pub use version::{
    CATALOG_OBJECT_SCHEMA_VERSION, CatalogBatchChainInfo, CatalogBatchCommit, CatalogBatchFailure,
    CatalogEventCandidate, CatalogEventChangeInfo, CatalogMarketCandidate, CatalogMarketChangeInfo,
    CatalogSnapshotInfo, CatalogSyncBatchInfo, CatalogWindowInfo, NewCatalogEventChange,
    NewCatalogEventObject, NewCatalogMarketChange, NewCatalogMarketObject, NewCatalogSyncBatch,
    NewCatalogSyncRejection,
};
