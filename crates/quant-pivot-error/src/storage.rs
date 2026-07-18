//! Persistence layer errors — `PostgreSQL`, `ClickHouse`, and cache.

use sea_orm::DbErr;
use std::{fmt::Display, time::Duration};
use thiserror::Error;

/// Logical persistence entity names for typed [`StorageError`] variants.
///
/// Names align with schema-catalog table / domain identifiers used in repository
/// error messages and HTTP mapping.
pub mod entity {
    /// RBAC `user` table.
    pub const USER: &str = "user";
    /// RBAC `role` table.
    pub const ROLE: &str = "role";
    /// RBAC `menu` table.
    pub const MENU: &str = "menu";

    /// `quant_recommendation`.
    pub const QUANT_RECOMMENDATION: &str = "quant_recommendation";
    /// `quant_recommendation_attribution`.
    pub const QUANT_RECOMMENDATION_ATTRIBUTION: &str = "quant_recommendation_attribution";
    /// `quant_recommendation_report`.
    pub const QUANT_RECOMMENDATION_REPORT: &str = "quant_recommendation_report";
    /// `quant_report_run`.
    pub const QUANT_REPORT_RUN: &str = "quant_report_run";
    /// `quant_report_schedule_state`.
    pub const QUANT_REPORT_SCHEDULE_STATE: &str = "quant_report_schedule_state";
    /// `quant_report_schedule_gap`.
    pub const QUANT_REPORT_SCHEDULE_GAP: &str = "quant_report_schedule_gap";
    /// `quant_order_intent`.
    pub const QUANT_ORDER_INTENT: &str = "quant_order_intent";
    /// `quant_execution_order`.
    pub const QUANT_EXECUTION_ORDER: &str = "quant_execution_order";
    /// `quant_capital_allocation`.
    pub const QUANT_CAPITAL_ALLOCATION: &str = "quant_capital_allocation";
    /// `quant_position`.
    pub const QUANT_POSITION: &str = "quant_position";
    /// `quant_reconciliation`.
    pub const QUANT_RECONCILIATION: &str = "quant_reconciliation";
    /// `quant_settlement_redeem`.
    pub const QUANT_SETTLEMENT_REDEEM: &str = "quant_settlement_redeem";
    /// `quant_settlement_redeem_lot`.
    pub const QUANT_SETTLEMENT_REDEEM_LOT: &str = "quant_settlement_redeem_lot";
    /// `quant_training_dataset`.
    pub const QUANT_TRAINING_DATASET: &str = "quant_training_dataset";
    /// `quant_source_slice`.
    pub const QUANT_SOURCE_SLICE: &str = "quant_source_slice";
    /// `quant_research_readiness_evidence`.
    pub const QUANT_RESEARCH_READINESS_EVIDENCE: &str = "quant_research_readiness_evidence";
    /// `quant_calibration_artifact`.
    pub const QUANT_CALIBRATION_ARTIFACT: &str = "quant_calibration_artifact";
    /// `quant_trade_policy_artifact`.
    pub const QUANT_TRADE_POLICY_ARTIFACT: &str = "quant_trade_policy_artifact";
    /// `quant_domain_source_cursor`.
    pub const QUANT_DOMAIN_SOURCE_CURSOR: &str = "quant_domain_source_cursor";
    /// `quant_domain_source_expectation`.
    pub const QUANT_DOMAIN_SOURCE_EXPECTATION: &str = "quant_domain_source_expectation";
    /// `quant_domain_event_outbox`.
    pub const QUANT_DOMAIN_EVENT_OUTBOX: &str = "quant_domain_event_outbox";
    /// `quant_research_job`.
    pub const QUANT_RESEARCH_JOB: &str = "quant_research_job";
    /// `quant_feature_parity_run`.
    pub const QUANT_FEATURE_PARITY_RUN: &str = "quant_feature_parity_run";
    /// `quant_feature_parity_state`.
    pub const QUANT_FEATURE_PARITY_STATE: &str = "quant_feature_parity_state";
    /// `quant_model_spec`.
    pub const QUANT_MODEL_SPEC: &str = "quant_model_spec";
    /// `quant_model_version`.
    pub const QUANT_MODEL_VERSION: &str = "quant_model_version";
    /// `quant_model_run`.
    pub const QUANT_MODEL_RUN: &str = "quant_model_run";
    /// `runtime_config_version`.
    pub const RUNTIME_CONFIG_VERSION: &str = "runtime_config_version";
    /// `runtime_config_activation`.
    pub const RUNTIME_CONFIG_ACTIVATION: &str = "runtime_config_activation";
    /// `quant_factor`.
    pub const QUANT_FACTOR: &str = "quant_factor";
    /// `quant_kill_switch`.
    pub const QUANT_KILL_SWITCH: &str = "quant_kill_switch";
    /// Catalog `market`.
    pub const MARKET: &str = "market";
}

/// Errors from the storage subsystem (DB, analytics, cache).
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    #[error("Database transaction failed: {0}")]
    Transaction(String),

    #[cfg(feature = "storage")]
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[cfg(not(feature = "storage"))]
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[cfg(feature = "storage")]
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[cfg(feature = "storage")]
    #[error("Redis pool error: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),

    #[cfg(not(feature = "storage"))]
    #[error("Cache error: {0}")]
    Cache(String),

    #[cfg(feature = "storage")]
    #[error("Serialization error: {0}")]
    Serialization(#[from] bitcode::Error),

    #[error("Codec error: {0}")]
    Codec(String),

    #[cfg(not(feature = "storage"))]
    #[error("Serialization error: {0}")]
    SerializationStr(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("Stale data: {0}")]
    StaleData(String),

    /// Postgres unique / primary key violation (via `map_unique` or explicit check).
    #[error("{entity} already exists: {key}")]
    Duplicate { entity: &'static str, key: String },

    /// Explicit FSM transition rejected (order intent, execution order, dataset, …).
    #[error(
        "illegal transition for {entity}{id}: {from} -> {to}",
        id = display_optional_id(.id.as_ref())
    )]
    IllegalTransition {
        entity: &'static str,
        id: Option<String>,
        from: String,
        to: String,
    },

    /// Entity exists but current lifecycle/state forbids the operation.
    #[error(
        "state conflict for {entity}{id}: {detail}",
        id = display_optional_id(.id.as_ref())
    )]
    StateConflict {
        entity: &'static str,
        id: Option<String>,
        detail: String,
    },

    /// Caller payload violates domain invariants at the repository boundary.
    #[error(
        "invariant violation{entity}: {detail}",
        entity = display_optional_entity(*.entity)
    )]
    InvariantViolation {
        entity: Option<&'static str>,
        detail: String,
    },

    /// A bounded durable queue cannot accept more work.
    #[error("capacity exceeded for {entity}: limit {limit}")]
    CapacityExceeded { entity: &'static str, limit: u64 },

    #[error("Operation `{operation}` timed out after {duration:?}")]
    Timeout {
        operation: String,
        duration: Duration,
    },

    #[error("ClickHouse write semaphore closed (system shutting down)")]
    ClickHouseWriteSemaphoreClosed,
}

fn display_optional_id(id: Option<&String>) -> String {
    id.map_or(String::new(), |value| format!(" `{value}`"))
}

fn display_optional_entity(entity: Option<&'static str>) -> String {
    entity.map_or(String::new(), |value| format!(" for {value}"))
}

impl StorageError {
    /// Construct an explicit duplicate-key error.
    pub fn duplicate(entity: &'static str, key: impl Display) -> Self {
        Self::Duplicate {
            entity,
            key: key.to_string(),
        }
    }

    /// Construct a not-found error.
    pub fn not_found(entity: &'static str, id: impl Display) -> Self {
        Self::NotFound {
            entity,
            id: id.to_string(),
        }
    }

    /// Construct an illegal FSM transition error.
    pub fn illegal_transition(
        entity: &'static str,
        id: Option<impl Display>,
        from: impl Display,
        to: impl Display,
    ) -> Self {
        Self::IllegalTransition {
            entity,
            id: id.map(|value| value.to_string()),
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    /// Construct a lifecycle/state conflict error.
    pub fn state_conflict(
        entity: &'static str,
        id: Option<impl Display>,
        detail: impl Display,
    ) -> Self {
        Self::StateConflict {
            entity,
            id: id.map(|value| value.to_string()),
            detail: detail.to_string(),
        }
    }

    /// Construct an invariant violation error.
    pub fn invariant_violation(entity: Option<&'static str>, detail: impl Display) -> Self {
        Self::InvariantViolation {
            entity,
            detail: detail.to_string(),
        }
    }

    /// Construct a bounded-capacity error.
    pub const fn capacity_exceeded(entity: &'static str, limit: u64) -> Self {
        Self::CapacityExceeded { entity, limit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_display_matches_legacy_format() {
        let error = StorageError::Duplicate {
            entity: entity::USER,
            key: "alice".to_owned(),
        };
        assert_eq!(error.to_string(), "user already exists: alice");
    }

    #[test]
    fn illegal_transition_display_includes_states() {
        let error = StorageError::IllegalTransition {
            entity: entity::QUANT_ORDER_INTENT,
            id: Some("intent-1".to_owned()),
            from: "submitted".to_owned(),
            to: "draft".to_owned(),
        };
        assert!(error.to_string().contains("submitted -> draft"));
    }
}
