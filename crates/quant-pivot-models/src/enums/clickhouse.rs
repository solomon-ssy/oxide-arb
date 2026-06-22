//! Strongly typed `ClickHouse` `Enum8` values for quant-pivot fact rows.

use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookEventType {
    Snapshot = 1,
    Delta = 2,
    Bbo = 3,
    TickSizeChange = 4,
    LastTrade = 5,
    MarketResolved = 6,
    ShardStatus = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChFactSource {
    WsSnapshot = 1,
    WsDelta = 2,
    WsBbo = 3,
    WsTickSize = 4,
    WsLastTrade = 5,
    WsMarketResolved = 6,
    QuantPipeline = 7,
    Execution = 8,
    WsShardStatus = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSnapshotReason {
    Startup = 1,
    Reconnect = 2,
    Gap = 3,
    Periodic = 4,
    Manual = 5,
    WsSnapshot = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookDecisionStage {
    FeatureGenerated = 1,
    FactorScored = 2,
    ModelScored = 3,
    PortfolioPruned = 4,
    RecommendationPublished = 5,
    IntentCreated = 6,
    ExecutionUpdated = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookQuality {
    Fresh = 1,
    Stale = 2,
    Crossed = 3,
    Gap = 4,
    Invalid = 5,
    Insufficient = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChBookEvidenceTier {
    ExactReplay = 1,
    DecisionContext = 2,
    AggregateOnly = 3,
    Insufficient = 4,
}
