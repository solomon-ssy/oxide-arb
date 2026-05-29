//! Transactional outbox enums — aggregate roots and event classifications.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

/// Aggregate root referenced by an outbox event.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum OutboxAggregateType {
    #[sea_orm(string_value = "opportunity")]
    Opportunity,
    #[sea_orm(string_value = "trade")]
    Trade,
    #[sea_orm(string_value = "execution")]
    Execution,
    #[sea_orm(string_value = "risk_engine")]
    RiskEngine,
    #[sea_orm(string_value = "system")]
    System,
}

impl Display for OutboxAggregateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Opportunity => f.write_str("opportunity"),
            Self::Trade => f.write_str("trade"),
            Self::Execution => f.write_str("execution"),
            Self::RiskEngine => f.write_str("risk_engine"),
            Self::System => f.write_str("system"),
        }
    }
}

/// High-level outbox event classification for downstream routing.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum OutboxEventType {
    #[sea_orm(string_value = "lifecycle")]
    Lifecycle,
    #[sea_orm(string_value = "trade_outcome_observed")]
    TradeOutcomeObserved,
    #[sea_orm(string_value = "audit")]
    Audit,
    #[sea_orm(string_value = "alert")]
    Alert,
    #[sea_orm(string_value = "reconciliation")]
    Reconciliation,
    #[sea_orm(string_value = "notification")]
    Notification,
}

impl Display for OutboxEventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lifecycle => f.write_str("lifecycle"),
            Self::TradeOutcomeObserved => f.write_str("trade_outcome_observed"),
            Self::Audit => f.write_str("audit"),
            Self::Alert => f.write_str("alert"),
            Self::Reconciliation => f.write_str("reconciliation"),
            Self::Notification => f.write_str("notification"),
        }
    }
}
