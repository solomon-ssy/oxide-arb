//! Strongly typed `ClickHouse` `Enum8` values for fact rows.
//!
//! The `clickhouse` crate maps `Enum8` / `Enum16` through `serde_repr`, so
//! every discriminant here must match the DDL exactly.

use crate::enums::{
    audit::OpportunityAuditStage,
    calibration::{DurationBucket, PriceZone},
    common::{
        MarketCategory, SettlementAccountingStatus, SettlementTrigger, Side, StalenessLevel,
        TradeBusinessOutcome,
    },
};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSide {
    Buy = 1,
    Sell = 2,
}

impl From<Side> for ChSide {
    fn from(value: Side) -> Self {
        match value {
            Side::Buy => Self::Buy,
            Side::Sell => Self::Sell,
        }
    }
}

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
    Scanner = 7,
    Execution = 8,
    Settlement = 9,
    CalibrationUpdater = 10,
    WsShardStatus = 11,
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
pub enum ChPriceZone {
    Z95 = 1,
    Z96 = 2,
    Z97 = 3,
    Z98 = 4,
    Z99 = 5,
}

impl From<PriceZone> for ChPriceZone {
    fn from(value: PriceZone) -> Self {
        match value {
            PriceZone::Z95 => Self::Z95,
            PriceZone::Z96 => Self::Z96,
            PriceZone::Z97 => Self::Z97,
            PriceZone::Z98 => Self::Z98,
            PriceZone::Z99 => Self::Z99,
        }
    }
}

impl From<ChPriceZone> for PriceZone {
    fn from(value: ChPriceZone) -> Self {
        match value {
            ChPriceZone::Z95 => Self::Z95,
            ChPriceZone::Z96 => Self::Z96,
            ChPriceZone::Z97 => Self::Z97,
            ChPriceZone::Z98 => Self::Z98,
            ChPriceZone::Z99 => Self::Z99,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChDurationBucket {
    Short = 1,
    Medium = 2,
    Long = 3,
    VeryLong = 4,
}

impl From<DurationBucket> for ChDurationBucket {
    fn from(value: DurationBucket) -> Self {
        match value {
            DurationBucket::Short => Self::Short,
            DurationBucket::Medium => Self::Medium,
            DurationBucket::Long => Self::Long,
            DurationBucket::VeryLong => Self::VeryLong,
        }
    }
}

impl From<ChDurationBucket> for DurationBucket {
    fn from(value: ChDurationBucket) -> Self {
        match value {
            ChDurationBucket::Short => Self::Short,
            ChDurationBucket::Medium => Self::Medium,
            ChDurationBucket::Long => Self::Long,
            ChDurationBucket::VeryLong => Self::VeryLong,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChStalenessLevel {
    Fresh = 1,
    Acceptable = 2,
    Stale = 3,
    Expired = 4,
}

impl From<StalenessLevel> for ChStalenessLevel {
    fn from(value: StalenessLevel) -> Self {
        match value {
            StalenessLevel::Fresh => Self::Fresh,
            StalenessLevel::Acceptable => Self::Acceptable,
            StalenessLevel::Stale => Self::Stale,
            StalenessLevel::Expired => Self::Expired,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChMarketCategory {
    Geopolitics = 1,
    Sports = 2,
    Politics = 3,
    Finance = 4,
    Tech = 5,
    Culture = 6,
    Weather = 7,
    Economics = 8,
    Crypto = 9,
    Other = 10,
}

impl From<MarketCategory> for ChMarketCategory {
    fn from(value: MarketCategory) -> Self {
        match value {
            MarketCategory::Geopolitics => Self::Geopolitics,
            MarketCategory::Sports => Self::Sports,
            MarketCategory::Politics => Self::Politics,
            MarketCategory::Finance => Self::Finance,
            MarketCategory::Tech => Self::Tech,
            MarketCategory::Culture => Self::Culture,
            MarketCategory::Weather => Self::Weather,
            MarketCategory::Economics => Self::Economics,
            MarketCategory::Crypto => Self::Crypto,
            MarketCategory::Other => Self::Other,
        }
    }
}

impl From<ChMarketCategory> for MarketCategory {
    fn from(value: ChMarketCategory) -> Self {
        match value {
            ChMarketCategory::Geopolitics => Self::Geopolitics,
            ChMarketCategory::Sports => Self::Sports,
            ChMarketCategory::Politics => Self::Politics,
            ChMarketCategory::Finance => Self::Finance,
            ChMarketCategory::Tech => Self::Tech,
            ChMarketCategory::Culture => Self::Culture,
            ChMarketCategory::Weather => Self::Weather,
            ChMarketCategory::Economics => Self::Economics,
            ChMarketCategory::Crypto => Self::Crypto,
            ChMarketCategory::Other => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChOpportunityAuditStage {
    Detected = 1,
    ValidationRejected = 2,
    RiskRejected = 3,
    SizingRejected = 4,
    Filled = 5,
    Missed = 6,
    Failed = 7,
    Settled = 8,
    FactorValidationRejected = 9,
}

impl From<OpportunityAuditStage> for ChOpportunityAuditStage {
    fn from(value: OpportunityAuditStage) -> Self {
        match value {
            OpportunityAuditStage::Detected => Self::Detected,
            OpportunityAuditStage::ValidationRejected => Self::ValidationRejected,
            OpportunityAuditStage::FactorValidationRejected => Self::FactorValidationRejected,
            OpportunityAuditStage::RiskRejected => Self::RiskRejected,
            OpportunityAuditStage::SizingRejected => Self::SizingRejected,
            OpportunityAuditStage::Filled => Self::Filled,
            OpportunityAuditStage::Missed => Self::Missed,
            OpportunityAuditStage::Failed => Self::Failed,
            OpportunityAuditStage::Settled => Self::Settled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSettlementOutcome {
    Won = 1,
    Lost = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSettlementTrigger {
    Ws = 1,
    PeriodicRetry = 2,
    Manual = 3,
}

impl From<SettlementTrigger> for ChSettlementTrigger {
    fn from(value: SettlementTrigger) -> Self {
        match value {
            SettlementTrigger::Ws => Self::Ws,
            SettlementTrigger::PeriodicRetry => Self::PeriodicRetry,
            SettlementTrigger::Manual => Self::Manual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChSettlementAccountingStatus {
    Pending = 1,
    Redeemed = 2,
    Accounted = 3,
    Failed = 4,
}

impl From<SettlementAccountingStatus> for ChSettlementAccountingStatus {
    fn from(value: SettlementAccountingStatus) -> Self {
        match value {
            SettlementAccountingStatus::Pending => Self::Pending,
            SettlementAccountingStatus::Redeemed => Self::Redeemed,
            SettlementAccountingStatus::Accounted => Self::Accounted,
            SettlementAccountingStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChTradeBusinessOutcome {
    Success = 1,
    Miss = 2,
    Failed = 3,
}

impl From<TradeBusinessOutcome> for ChTradeBusinessOutcome {
    fn from(value: TradeBusinessOutcome) -> Self {
        match value {
            TradeBusinessOutcome::Success => Self::Success,
            TradeBusinessOutcome::Miss => Self::Miss,
            TradeBusinessOutcome::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChAuditOutcome {
    Rejected = 1,
    Settled = 2,
    Success = 3,
    Miss = 4,
    Failed = 5,
}

impl From<TradeBusinessOutcome> for ChAuditOutcome {
    fn from(value: TradeBusinessOutcome) -> Self {
        match value {
            TradeBusinessOutcome::Success => Self::Success,
            TradeBusinessOutcome::Miss => Self::Miss,
            TradeBusinessOutcome::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum ChRejectionStage {
    Validation = 1,
    Risk = 2,
    Sizing = 3,
    SubmitPersist = 4,
    FactorValidation = 5,
    Other = 127,
}

impl ChRejectionStage {
    #[must_use]
    pub fn from_stage(stage: &str) -> Self {
        match stage {
            "validation" => Self::Validation,
            "risk" => Self::Risk,
            "sizing" => Self::Sizing,
            "submit_persist" => Self::SubmitPersist,
            "factor_validation" => Self::FactorValidation,
            _ => Self::Other,
        }
    }
}
