//! Persistence layer errors — `PostgreSQL`, `ClickHouse`, and cache.

use std::{fmt::Display, time::Duration};

#[cfg(feature = "storage")]
use bitcode::Error as BitcodeError;
#[cfg(feature = "storage")]
use clickhouse::error::Error as ClickhouseError;
#[cfg(feature = "storage")]
use deadpool_redis::PoolError;
#[cfg(feature = "storage")]
use redis::RedisError;
use sea_orm::{DbErr, RuntimeErr, SqlxError};
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
    /// `quant_execution_attempt_outcome`.
    pub const QUANT_EXECUTION_ATTEMPT_OUTCOME: &str = "quant_execution_attempt_outcome";
    /// `quant_execution_attempt_reconciliation_task`.
    pub const QUANT_EXECUTION_ATTEMPT_RECONCILIATION_TASK: &str =
        "quant_execution_attempt_reconciliation_task";
    /// `quant_execution_rollup_reconciliation_task`.
    pub const QUANT_EXECUTION_ROLLUP_RECONCILIATION_TASK: &str =
        "quant_execution_rollup_reconciliation_task";
    /// `quant_resolution_outcome_reconciliation_task`.
    pub const QUANT_RESOLUTION_OUTCOME_RECONCILIATION_TASK: &str =
        "quant_resolution_outcome_reconciliation_task";
    /// `quant_recommendation_execution_rollup`.
    pub const QUANT_RECOMMENDATION_EXECUTION_ROLLUP: &str = "quant_recommendation_execution_rollup";
    /// `quant_recommendation_resolution_outcome`.
    pub const QUANT_RECOMMENDATION_RESOLUTION_OUTCOME: &str =
        "quant_recommendation_resolution_outcome";
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
    /// `quant_execution_trade_ref`.
    pub const QUANT_EXECUTION_TRADE_REF: &str = "quant_execution_trade_ref";
    /// `quant_clob_trade_observation`.
    pub const QUANT_CLOB_TRADE_OBSERVATION: &str = "quant_clob_trade_observation";
    /// `quant_execution_transaction_ref`.
    pub const QUANT_EXECUTION_TRANSACTION_REF: &str = "quant_execution_transaction_ref";
    /// `quant_execution_account`.
    pub const QUANT_EXECUTION_ACCOUNT: &str = "quant_execution_account";
    /// `quant_account_chain_execution`.
    pub const QUANT_ACCOUNT_CHAIN_EXECUTION: &str = "quant_account_chain_execution";
    /// `quant_account_recovery_incident`.
    pub const QUANT_ACCOUNT_RECOVERY_INCIDENT: &str = "quant_account_recovery_incident";
    /// `quant_account_execution_association`.
    pub const QUANT_ACCOUNT_EXECUTION_ASSOCIATION: &str = "quant_account_execution_association";
    /// `quant_account_pause_submission`.
    pub const QUANT_ACCOUNT_PAUSE_SUBMISSION: &str = "quant_account_pause_submission";
    /// `quant_venue_incentive_event`.
    pub const QUANT_VENUE_INCENTIVE_EVENT: &str = "quant_venue_incentive_event";
    pub const QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN: &str =
        "quant_venue_incentive_reconciliation_scan";
    /// `quant_capital_allocation`.
    pub const QUANT_CAPITAL_ALLOCATION: &str = "quant_capital_allocation";
    /// `quant_strategy_position_lot`.
    pub const QUANT_STRATEGY_POSITION_LOT: &str = "quant_strategy_position_lot";
    /// `quant_reconciliation`.
    pub const QUANT_RECONCILIATION: &str = "quant_reconciliation";
    /// `quant_settlement_redeem`.
    pub const QUANT_SETTLEMENT_REDEEM: &str = "quant_settlement_redeem";
    /// `quant_settlement_chain_submission`.
    pub const QUANT_SETTLEMENT_CHAIN_SUBMISSION: &str = "quant_settlement_chain_submission";
    /// `quant_settlement_authorization`.
    pub const QUANT_SETTLEMENT_AUTHORIZATION: &str = "quant_settlement_authorization";
    /// `quant_settlement_governed_action`.
    pub const QUANT_SETTLEMENT_GOVERNED_ACTION: &str = "quant_settlement_governed_action";
    /// `quant_settlement_external_cursor`.
    pub const QUANT_SETTLEMENT_EXTERNAL_CURSOR: &str = "quant_settlement_external_cursor";
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
    /// `quant_attribution_artifact`.
    pub const QUANT_ATTRIBUTION_ARTIFACT: &str = "quant_attribution_artifact";
    /// `quant_model_candidate_manifest`.
    pub const QUANT_MODEL_CANDIDATE_MANIFEST: &str = "quant_model_candidate_manifest";
    /// `quant_trade_policy_artifact`.
    pub const QUANT_TRADE_POLICY_ARTIFACT: &str = "quant_trade_policy_artifact";
    /// `quant_domain_source_cursor`.
    pub const QUANT_DOMAIN_SOURCE_CURSOR: &str = "quant_domain_source_cursor";
    /// `quant_domain_source_expectation`.
    pub const QUANT_DOMAIN_SOURCE_EXPECTATION: &str = "quant_domain_source_expectation";
    /// `quant_domain_event_outbox`.
    pub const QUANT_DOMAIN_EVENT_OUTBOX: &str = "quant_domain_event_outbox";
    /// `quant_resolution_observation_inbox`.
    pub const QUANT_RESOLUTION_OBSERVATION_INBOX: &str = "quant_resolution_observation_inbox";
    /// `quant_resolution_observation_projection`.
    pub const QUANT_RESOLUTION_OBSERVATION_PROJECTION: &str =
        "quant_resolution_observation_projection";
    /// `quant_research_job`.
    pub const QUANT_RESEARCH_JOB: &str = "quant_research_job";
    /// `quant_feedback_cycle`.
    pub const QUANT_FEEDBACK_CYCLE: &str = "quant_feedback_cycle";
    /// `quant_feedback_coordinator_fault`.
    pub const QUANT_FEEDBACK_COORDINATOR_FAULT: &str = "quant_feedback_coordinator_fault";
    /// `quant_feedback_scheduler_state`.
    pub const QUANT_FEEDBACK_SCHEDULER_STATE: &str = "quant_feedback_scheduler_state";
    /// `quant_feedback_stage_event`.
    pub const QUANT_FEEDBACK_STAGE_EVENT: &str = "quant_feedback_stage_event";
    /// `quant_feedback_trigger_event`.
    pub const QUANT_FEEDBACK_TRIGGER_EVENT: &str = "quant_feedback_trigger_event";
    /// `quant_feedback_event_outbox`.
    pub const QUANT_FEEDBACK_EVENT_OUTBOX: &str = "quant_feedback_event_outbox";
    /// `quant_drift_report`.
    pub const QUANT_DRIFT_REPORT: &str = "quant_drift_report";
    /// `quant_feedback_evaluation_use`.
    pub const QUANT_FEEDBACK_EVALUATION_USE: &str = "quant_feedback_evaluation_use";
    /// `quant_feedback_promotion_permit`.
    pub const QUANT_FEEDBACK_PROMOTION_PERMIT: &str = "quant_feedback_promotion_permit";
    /// `quant_model_route_shadow_binding`.
    pub const QUANT_MODEL_ROUTE_SHADOW_BINDING: &str = "quant_model_route_shadow_binding";
    /// `quant_feature_parity_run`.
    pub const QUANT_FEATURE_PARITY_RUN: &str = "quant_feature_parity_run";
    /// `quant_feature_parity_state`.
    pub const QUANT_FEATURE_PARITY_STATE: &str = "quant_feature_parity_state";
    /// `ClickHouse` `market_resolution_event`.
    pub const MARKET_RESOLUTION_EVENT: &str = "market_resolution_event";
    /// `quant_model_spec`.
    pub const QUANT_MODEL_SPEC: &str = "quant_model_spec";
    /// `quant_model_version`.
    pub const QUANT_MODEL_VERSION: &str = "quant_model_version";
    /// `quant_model_run`.
    pub const QUANT_MODEL_RUN: &str = "quant_model_run";
    /// `quant_backtest_report`.
    pub const QUANT_BACKTEST_REPORT: &str = "quant_backtest_report";
    /// `quant_backtest_path_set`.
    pub const QUANT_BACKTEST_PATH_SET: &str = "quant_backtest_path_set";
    /// `decision_policy_snapshot`.
    pub const DECISION_POLICY_SNAPSHOT: &str = "decision_policy_snapshot";
    /// `policy_activation`.
    pub const POLICY_ACTIVATION: &str = "policy_activation";
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
    ClickHouse(#[from] ClickhouseError),

    #[cfg(not(feature = "storage"))]
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[cfg(feature = "storage")]
    #[error("Redis error: {0}")]
    Redis(#[from] RedisError),

    #[cfg(feature = "storage")]
    #[error("Redis pool error: {0}")]
    RedisPool(#[from] PoolError),

    #[cfg(not(feature = "storage"))]
    #[error("Cache error: {0}")]
    Cache(String),

    #[cfg(feature = "storage")]
    #[error("Serialization error: {0}")]
    Serialization(#[from] BitcodeError),

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
    /// Whether this error proves a transient transport/capacity failure that a
    /// durable worker may retry without masking schema or business defects.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connection(_) | Self::Timeout { .. } => true,
            Self::Database(error) => match error {
                DbErr::ConnectionAcquire(_) | DbErr::Conn(_) => true,
                DbErr::Exec(RuntimeErr::SqlxError(error))
                | DbErr::Query(RuntimeErr::SqlxError(error)) => matches!(
                    error.as_ref(),
                    SqlxError::Io(_)
                        | SqlxError::Tls(_)
                        | SqlxError::PoolTimedOut
                        | SqlxError::PoolClosed
                        | SqlxError::WorkerCrashed
                ),
                _ => false,
            },
            _ => false,
        }
    }

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
    use sea_orm::ConnAcquireErr;

    use entity::{QUANT_ORDER_INTENT, USER};

    use super::*;

    #[test]
    fn duplicate_display_names_key() {
        let error = StorageError::Duplicate {
            entity: USER,
            key: "alice".to_owned(),
        };
        assert_eq!(error.to_string(), "user already exists: alice");
    }

    #[test]
    fn illegal_transition_display_states() {
        let error = StorageError::IllegalTransition {
            entity: QUANT_ORDER_INTENT,
            id: Some("intent-1".to_owned()),
            from: "submitted".to_owned(),
            to: "draft".to_owned(),
        };
        assert!(error.to_string().contains("submitted -> draft"));
    }

    #[test]
    fn transient_taxonomy_fails_closed() {
        assert!(StorageError::Connection("connection reset".to_owned()).is_transient());
        assert!(
            StorageError::Database(DbErr::ConnectionAcquire(ConnAcquireErr::Timeout))
                .is_transient()
        );
        assert!(!StorageError::invariant_violation(None, "bad contract").is_transient());
        assert!(!StorageError::Database(DbErr::Type("bad type".to_owned())).is_transient());
    }
}
