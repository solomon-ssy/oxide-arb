use sea_orm_migration::{
    prelude::*,
    sea_query::{ConditionalStatement, IndexOrder, IndexType},
};

use super::v1;

pub const SOURCE: &[u8] = include_bytes!("query_indexes.rs");

#[derive(Debug, Clone, Copy)]
enum IndexMethod {
    BTree,
    Gin,
}

#[derive(Debug, Clone, Copy)]
enum IndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy)]
struct IndexColumnSpec {
    name: &'static str,
    direction: IndexDirection,
}

#[derive(Debug, Clone, Copy)]
struct IndexSpec {
    name: &'static str,
    table: &'static str,
    method: IndexMethod,
    unique: bool,
    columns: &'static [IndexColumnSpec],
    predicate: Option<&'static str>,
}

const INDEXES: &[IndexSpec] = &[
    IndexSpec {
        name: "uq_research_profile_artifact_name_version",
        table: "research_profile_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "version",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_research_profile_artifact_content_hash",
        table: "research_profile_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_profile_artifact_kind_created",
        table: "policy_profile_artifact",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_policy_profile_artifact_kind_hash",
        table: "policy_profile_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "content_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_activation_resource_latest",
        table: "policy_activation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "resource_kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "activated_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "policy_activation_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_revision_resource_created",
        table: "policy_revision",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "resource_kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "policy_revision_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_approval_revision_created",
        table: "policy_approval",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "policy_revision_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_domain_source_expectation_family_status",
        table: "quant_domain_source_expectation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "family",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "source_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "instrument_key",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_casbin_ptype",
        table: "casbin_rule",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "ptype",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_casbin_v0",
        table: "casbin_rule",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "v0",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_casbin_rule",
        table: "casbin_rule",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "ptype",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v0",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v1",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v2",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v3",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v4",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "v5",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_event_change_pit",
        table: "catalog_event_change",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "event_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "source_effective_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "event_change_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_catalog_event_object_content_hash",
        table: "catalog_event_object",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_market_change_event_pit",
        table: "catalog_market_change",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "event_change_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_market_change_pit",
        table: "catalog_market_change",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "source_effective_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "market_change_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_catalog_market_object_content_hash",
        table: "catalog_market_object",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_sync_batch_baseline_committed",
        table: "catalog_sync_batch",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "committed_at",
            direction: IndexDirection::Desc,
        }],
        predicate: Some(
            "((status = 'committed'::qp_catalog_sync_status) AND (sync_kind = 'baseline'::qp_catalog_sync_kind))",
        ),
    },
    IndexSpec {
        name: "idx_catalog_sync_batch_committed",
        table: "catalog_sync_batch",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "committed_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_sync_batch_hash",
        table: "catalog_sync_batch",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "batch_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_sync_batch_started",
        table: "catalog_sync_batch",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "started_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_catalog_sync_rejection_batch_reason",
        table: "catalog_sync_rejection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "catalog_sync_batch_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "reason_code",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_clob_market_info_version_pit",
        table: "clob_market_info_version",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "effective_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_clob_market_info_version_payload",
        table: "clob_market_info_version",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "payload_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_events_status",
        table: "event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "status",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_events_tags",
        table: "event",
        method: IndexMethod::Gin,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "tags",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_markets_active_endgame",
        table: "market",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "end_date",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("((status = 'active'::qp_market_status) AND (end_date IS NOT NULL))"),
    },
    IndexSpec {
        name: "idx_markets_categories",
        table: "market",
        method: IndexMethod::Gin,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "categories",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_markets_event_id",
        table: "market",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "event_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_markets_no_token",
        table: "market",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "no_token_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_markets_status",
        table: "market",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "status",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_markets_yes_token",
        table: "market",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "yes_token_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_menu_parent",
        table: "menu",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "parent_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "sort",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_oplog_actor",
        table: "operation_log",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "actor_user_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "occurred_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_oplog_category",
        table: "operation_log",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "category",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "occurred_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_oplog_occurred",
        table: "operation_log",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "occurred_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_oplog_request",
        table: "operation_log",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "request_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_oplog_resource",
        table: "operation_log",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "resource_type",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "resource_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_account_chain_execution_source",
        table: "quant_account_chain_execution",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "source_event_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_account_pause_incident_exchange",
        table: "quant_account_pause_submission",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "recovery_incident_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "exchange_address",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_account_recovery_incident_active",
        table: "quant_account_recovery_incident",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "execution_account_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("status <> 'sealed'"),
    },
    IndexSpec {
        name: "idx_quant_account_chain_execution_cursor",
        table: "quant_account_chain_execution",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "block_number",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "transaction_index",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "log_index",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_account_snapshot_as_of",
        table: "quant_account_snapshot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "as_of",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_backtest_path_set_version_created",
        table: "quant_backtest_path_set",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_backtest_report_version_created",
        table: "quant_backtest_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_backtest_report_hash",
        table: "quant_backtest_report",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "report_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_backtest_report_run",
        table: "quant_backtest_report",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "model_run_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_backtest_report_dataset_created",
        table: "quant_backtest_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "evaluation_dataset_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_basis_alert_as_of",
        table: "quant_basis_alert",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "as_of",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_basis_alert_market_as_of",
        table: "quant_basis_alert",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "as_of",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_basis_alert_open",
        table: "quant_basis_alert",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "acknowledged",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "as_of",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_calibration_artifact_kind_created",
        table: "quant_calibration_artifact",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_calibration_artifact_hash",
        table: "quant_calibration_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_calibration_publication_pit",
        table: "quant_calibration_artifact_publication",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "published_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "publication_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_capital_allocation_state",
        table: "quant_capital_allocation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "state",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_capital_allocation_intent",
        table: "quant_capital_allocation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "order_intent_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_crypto_price_projection_health",
        table: "quant_crypto_price_projection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "source_healthy",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "event_time",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_domain_event_outbox_pending",
        table: "quant_domain_event_outbox",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "published_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_domain_source_cursor_status",
        table: "quant_domain_source_cursor",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "status",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_entry_condition_artifact_hash",
        table: "quant_entry_condition_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_entry_condition_audit_timeline",
        table: "quant_entry_condition_audit",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "condition_instance_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "revision",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_entry_condition_evaluation_outbox_pending",
        table: "quant_entry_condition_evaluation_outbox",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "published_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_entry_condition_evaluation_outbox_evaluation",
        table: "quant_entry_condition_evaluation_outbox",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "evaluation_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_entry_condition_instance_due",
        table: "quant_entry_condition_instance",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "state",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_evaluation_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "expires_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_entry_condition_instance_lease",
        table: "quant_entry_condition_instance",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "lease_expires_at",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_entry_condition_instance_recommendation",
        table: "quant_entry_condition_instance",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "recommendation_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_equity_snapshot_as_of",
        table: "quant_equity_snapshot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "as_of",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_equity_snapshot_created_at",
        table: "quant_equity_snapshot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "created_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_order_intent_created",
        table: "quant_execution_order",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "order_intent_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_order_state",
        table: "quant_execution_order",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "state",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_trade_ref_order",
        table: "quant_execution_trade_ref",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "execution_order_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_trade_ref_transaction",
        table: "quant_execution_trade_ref",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "transaction_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("transaction_hash IS NOT NULL"),
    },
    IndexSpec {
        name: "idx_quant_clob_trade_observation_order",
        table: "quant_clob_trade_observation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_order_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "matched_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_clob_trade_observation_account",
        table: "quant_clob_trade_observation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "matched_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_venue_incentive_account_date",
        table: "quant_venue_incentive_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "program_date",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "stage",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_venue_incentive_market_date",
        table: "quant_venue_incentive_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "program_date",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: Some("market_id IS NOT NULL"),
    },
    IndexSpec {
        name: "idx_quant_venue_incentive_latest",
        table: "quant_venue_incentive_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "source_partition",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_venue_incentive_scan_health",
        table: "quant_venue_incentive_reconciliation_scan",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "program_date",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "stage",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "completed_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_execution_transaction_ref_order_hash",
        table: "quant_execution_transaction_ref",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "execution_order_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "transaction_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_transaction_ref_hash",
        table: "quant_execution_transaction_ref",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "transaction_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_factor_definition_family",
        table: "quant_factor_definition",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "factor_family",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_factor_definition_definition_hash",
        table: "quant_factor_definition",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "definition_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_factor_value_definition_decision_at",
        table: "quant_factor_value",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "factor_definition_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_factor_value_market_decision_at",
        table: "quant_factor_value",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_factor_value_run",
        table: "quant_factor_value",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_attribution_artifact_hash",
        table: "quant_attribution_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "artifact_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_attribution_available",
        table: "quant_attribution_artifact",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "source_feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "artifact_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_attribution_intent_kind",
        table: "quant_attribution_artifact",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "order_intent_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "artifact_kind",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(order_intent_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_model_candidate_manifest_hash",
        table: "quant_model_candidate_manifest",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "manifest_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_model_candidate_cycle_recipe",
        table: "quant_model_candidate_manifest",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "candidate_recipe_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_candidate_version",
        table: "quant_model_candidate_manifest",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_cycle_idempotency",
        table: "quant_feedback_cycle",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "idempotency_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_cycle_profile_status",
        table: "quant_feedback_cycle",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "label_cutoff",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_cycle_profile_cutoff",
        table: "quant_feedback_cycle",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "label_cutoff",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(evaluation_mode = 'conditional')"),
    },
    IndexSpec {
        name: "idx_quant_feedback_cycle_claim",
        table: "quant_feedback_cycle",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "stage_resume_after",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(status IN ('queued', 'running'))"),
    },
    IndexSpec {
        name: "idx_quant_feedback_scheduler_due",
        table: "quant_feedback_scheduler_state",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "paused",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_due_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "retry_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "cooldown_until",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "research_profile_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(paused = false)"),
    },
    IndexSpec {
        name: "idx_quant_feedback_scheduler_pending_age",
        table: "quant_feedback_scheduler_state",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "pending_cutoff",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "pending_started_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "research_profile_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(pending_cutoff IS NOT NULL)"),
    },
    IndexSpec {
        name: "idx_quant_feedback_recipe_catalog",
        table: "quant_feedback_recipe_template",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "route",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "model_family",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "catalog_priority",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recipe_template_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "revision",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: Some("(status = 'approved')"),
    },
    IndexSpec {
        name: "uq_quant_model_route_active_shadow",
        table: "quant_model_route_shadow_binding",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "route",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(status = 'active')"),
    },
    IndexSpec {
        name: "uq_quant_model_route_shadow_termination_activation",
        table: "quant_model_route_shadow_binding",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "termination_policy_activation_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(termination_policy_activation_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_policy_activation_shadow_termination_key",
        table: "policy_activation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "idempotency_key",
            direction: IndexDirection::Asc,
        }],
        predicate: Some(
            "(activation_kind IN ('model_shadow_cancellation', 'model_shadow_rejection'))",
        ),
    },
    IndexSpec {
        name: "idx_quant_model_route_shadow_status_age",
        table: "quant_model_route_shadow_binding",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "bound_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "binding_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_event_outbox_pending",
        table: "quant_feedback_event_outbox",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "revision",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(published_at IS NULL)"),
    },
    IndexSpec {
        name: "uq_quant_feedback_permit_idempotency",
        table: "quant_feedback_promotion_permit",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "idempotency_key",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_permit_scope",
        table: "quant_feedback_promotion_permit",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "scope_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_permit_issuance",
        table: "quant_feedback_promotion_permit",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "issuance_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_permit_active_scope",
        table: "quant_feedback_promotion_permit",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "category",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "promotion_permit_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(revoked_at IS NULL)"),
    },
    IndexSpec {
        name: "idx_quant_feedback_permit_cycle",
        table: "quant_feedback_promotion_permit",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "promotion_permit_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_stage_sequence",
        table: "quant_feedback_stage_event",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "event_sequence",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_stage_timeline",
        table: "quant_feedback_stage_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "occurred_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "feedback_stage_event_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_stage_job",
        table: "quant_feedback_stage_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "research_job_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(research_job_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_feedback_trigger_hash",
        table: "quant_feedback_trigger_event",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "event_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_trigger_timeline",
        table: "quant_feedback_trigger_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "occurred_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "feedback_trigger_event_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_drift_report_cycle_metric",
        table: "quant_drift_report",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "metric",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_drift_report_latest",
        table: "quant_drift_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "observed_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "drift_report_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_evaluation_dataset",
        table: "quant_feedback_evaluation_use",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "evaluation_dataset_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_evaluation_semantics",
        table: "quant_feedback_evaluation_use",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "evaluation_dataset_hash",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "evaluation_artifact_bytes_hash",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "cohort_manifest_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feedback_evaluation_use_hash",
        table: "quant_feedback_evaluation_use",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "semantic_use_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feedback_evaluation_cycle",
        table: "quant_feedback_evaluation_use",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "reserved_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_parity_candidate_subject_ordinal",
        table: "quant_feature_parity_candidate",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "parity_subject_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "ordinal",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_parity_run_kind_created",
        table: "quant_feature_parity_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_parity_run_status_created",
        table: "quant_feature_parity_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feature_parity_run_full_window",
        table: "quant_feature_parity_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "window_start",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "window_end",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some(
            "((kind = 'full'::qp_feature_parity_run_kind) AND (report_id IS NULL) AND (model_version_id IS NULL) AND (training_dataset_id IS NULL) AND (status = ANY (ARRAY['queued'::qp_feature_parity_run_status, 'running'::qp_feature_parity_run_status, 'pending_materialization'::qp_feature_parity_run_status])))",
        ),
    },
    IndexSpec {
        name: "uq_quant_feature_parity_run_sampled_report",
        table: "quant_feature_parity_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "report_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some(
            "((kind = 'sampled'::qp_feature_parity_run_kind) AND (report_id IS NOT NULL))",
        ),
    },
    IndexSpec {
        name: "idx_quant_feature_parity_state_created",
        table: "quant_feature_parity_state",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "state_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_parity_subject_run",
        table: "quant_feature_parity_subject",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "subject_kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_feature_parity_subject_run_model",
        table: "quant_feature_parity_subject",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "model_run_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(model_run_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_feature_parity_subject_run_model_version",
        table: "quant_feature_parity_subject",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "training_dataset_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(model_version_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_feature_parity_subject_run_report",
        table: "quant_feature_parity_subject",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_report_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(recommendation_report_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "idx_quant_feature_vector_hash",
        table: "quant_feature_vector",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "feature_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_vector_market_decision_at",
        table: "quant_feature_vector",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_feature_vector_schema_decision_at",
        table: "quant_feature_vector",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feature_schema_version",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_linkage_market_derived",
        table: "quant_market_linkage",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "derived_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_linkage_status_derived",
        table: "quant_market_linkage",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "derived_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_market_linkage_content_hash",
        table: "quant_market_linkage",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_linkage_source_discovery",
        table: "quant_market_linkage_source",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "source_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "instrument_key",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "role",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_selection_decision_at",
        table: "quant_market_selection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "decision_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_selection_runtime_decision_at",
        table: "quant_market_selection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "decision_policy_snapshot_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_selection_selector_hash",
        table: "quant_market_selection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "selector_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_selection_member_event",
        table: "quant_market_selection_member",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "event_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_market_selection_member_market",
        table: "quant_market_selection_member",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "market_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_comparison_report_candidate_created",
        table: "quant_model_comparison_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "candidate_model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_model_comparison_report_hash",
        table: "quant_model_comparison_report",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "comparison_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_governance_audit_version_created",
        table: "quant_model_governance_audit",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_run_kind_started",
        table: "quant_model_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "run_kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "started_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_run_status_started",
        table: "quant_model_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "started_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_spec_family",
        table: "quant_model_spec",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "model_family",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_version_spec_created",
        table: "quant_model_version",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_spec_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_model_version_parent",
        table: "quant_model_version",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "parent_model_version_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(parent_model_version_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "idx_quant_model_version_calibration_artifact",
        table: "quant_model_version",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "calibration_artifact_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(calibration_artifact_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_model_version_spec_version",
        table: "quant_model_version",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "model_spec_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "version",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_order_intent_recommendation",
        table: "quant_order_intent",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "recommendation_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_order_intent_status_expires",
        table: "quant_order_intent",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "expires_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_portfolio_plan_decision_at",
        table: "quant_portfolio_plan",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "decision_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_strategy_position_lot_account_market_state",
        table: "quant_strategy_position_lot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "state",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_strategy_position_lot_state",
        table: "quant_strategy_position_lot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "state",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_strategy_position_lot_token",
        table: "quant_strategy_position_lot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "token_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_strategy_position_lot_intent_account",
        table: "quant_strategy_position_lot",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "order_intent_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("origin_kind = 'system_intent'"),
    },
    IndexSpec {
        name: "uq_quant_strategy_position_lot_recovery_token",
        table: "quant_strategy_position_lot",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "recovery_incident_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "token_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("origin_kind IN ('account_recovery_incident', 'opening_inventory')"),
    },
    IndexSpec {
        name: "idx_quant_recommendation_market_status",
        table: "quant_recommendation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_route_rank",
        table: "quant_recommendation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "report_route_run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "rank",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_attempt_outcome_recommendation",
        table: "quant_execution_attempt_outcome",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_attempt_task_due",
        table: "quant_execution_attempt_reconciliation_task",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "ready_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "order_intent_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("status <> 'completed'::qp_outcome_reconciliation_task_status"),
    },
    IndexSpec {
        name: "idx_quant_execution_rollup_task_due",
        table: "quant_execution_rollup_reconciliation_task",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "ready_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("status <> 'completed'::qp_outcome_reconciliation_task_status"),
    },
    IndexSpec {
        name: "idx_quant_resolution_outcome_task_due",
        table: "quant_resolution_outcome_reconciliation_task",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "ready_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("status <> 'completed'::qp_outcome_reconciliation_task_status"),
    },
    IndexSpec {
        name: "idx_quant_execution_rollup_available",
        table: "quant_recommendation_execution_rollup",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_status_valid_until",
        table: "quant_recommendation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "valid_until",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_recommendation_report_market_token",
        table: "quant_recommendation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "recommendation_report_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "token_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_recommendation_report_rank",
        table: "quant_recommendation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "recommendation_report_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "rank",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_attempt_outcome_available",
        table: "quant_execution_attempt_outcome",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_resolution_outcome_available",
        table: "quant_recommendation_resolution_outcome",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_resolution_inbox_available",
        table: "quant_resolution_observation_inbox",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "available_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_resolution_projection_due",
        table: "quant_resolution_observation_projection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_resolution_remediation_observation_created",
        table: "quant_resolution_projection_remediation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "resolution_observation_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "remediation_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_report_decision_at_id",
        table: "quant_recommendation_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "recommendation_report_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_report_status_decision_at",
        table: "quant_recommendation_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_recommendation_report_valid_until",
        table: "quant_recommendation_report",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "valid_until",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_recommendation_report_current_scope",
        table: "quant_recommendation_report",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "report_kind",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(status = 'published'::qp_recommendation_report_status)"),
    },
    IndexSpec {
        name: "idx_quant_report_route_run_report",
        table: "quant_report_route_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "report_run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "finished_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_route_run_model",
        table: "quant_report_route_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "model_run_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_reconciliation_result",
        table: "quant_reconciliation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "result",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_reconciliation_execution_order",
        table: "quant_reconciliation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "execution_order_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_dq_snapshot_decision_at",
        table: "quant_report_data_quality_snapshot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "decision_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_fact_delivery_pending",
        table: "quant_report_fact_delivery",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_run_claim",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "requested_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "report_run_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_run_lease_recovery",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_report_run_output_report",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "output_report_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_report_run_queued_schedule",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "schedule_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some(
            "((status = 'queued'::qp_report_run_status) AND (trigger_kind = 'scheduled'::qp_report_trigger_kind))",
        ),
    },
    IndexSpec {
        name: "uq_quant_report_run_single_running",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "status",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(status = 'running'::qp_report_run_status)"),
    },
    IndexSpec {
        name: "uq_quant_report_run_trigger_key",
        table: "quant_report_run",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "trigger_key",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_schedule_gap_detected",
        table: "quant_report_schedule_gap",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "schedule_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "detected_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "gap_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_report_schedule_state_due",
        table: "quant_report_schedule_state",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "enabled",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_scheduled_for",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_research_job_kind_status",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_research_job_lease",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_research_job_parent",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "parent_job_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_research_job_feedback_root",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "feedback_stage",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(feedback_cycle_id IS NOT NULL AND parent_job_id IS NULL)"),
    },
    IndexSpec {
        name: "uq_quant_research_job_feedback_retry",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "parent_job_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(feedback_cycle_id IS NOT NULL AND parent_job_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "idx_quant_research_job_feedback_stage",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "feedback_cycle_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "feedback_stage",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some("(feedback_cycle_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "idx_quant_research_job_status_created",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_research_job_due",
        table: "quant_research_job",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some(
            "(status = ANY (ARRAY['awaiting_evidence'::qp_research_job_status, \
             'retry_scheduled'::qp_research_job_status]))",
        ),
    },
    IndexSpec {
        name: "idx_quant_research_readiness_evidence_latest",
        table: "quant_research_readiness_evidence",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "observed_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_research_readiness_evidence_payload",
        table: "quant_research_readiness_evidence",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "scope_hash",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "payload_hash",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_execution_account_identity_digest",
        table: "quant_execution_account",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "identity_digest",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_execution_account_funder",
        table: "quant_execution_account",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "chain_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "funder_address",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_settlement_redeem_state_next_attempt",
        table: "quant_settlement_redeem",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "state",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_redeem_market_account",
        table: "quant_settlement_redeem",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "market_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_inventory_digest_position",
        table: "quant_settlement_inventory_lot",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "settlement_redeem_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "inventory_digest",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "strategy_position_lot_id",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_settlement_inventory_current",
        table: "quant_settlement_inventory_lot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "settlement_redeem_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "inventory_digest",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_authorization_attempt",
        table: "quant_settlement_authorization",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "settlement_redeem_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "attempt_ordinal",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_settlement_authorization_expiry",
        table: "quant_settlement_authorization",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "state",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "expires_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some(
            "state = ANY (ARRAY['pending'::qp_settlement_authorization_state, 'approved'::qp_settlement_authorization_state])",
        ),
    },
    IndexSpec {
        name: "uq_quant_settlement_governed_action_idempotency",
        table: "quant_settlement_governed_action",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "idempotency_key",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_active_canary",
        table: "quant_settlement_governed_action",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "settlement_redeem_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "authorization_digest",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: Some(
            "kind = 'canary_grant'::qp_settlement_governed_action_kind AND state = 'authorized'::qp_settlement_governed_action_state",
        ),
    },
    IndexSpec {
        name: "idx_quant_settlement_governed_action_work",
        table: "quant_settlement_governed_action",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "state",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_external_cursor_scope",
        table: "quant_settlement_external_cursor",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "execution_account_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "target_adapter",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "deployment_digest",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_settlement_chain_submission_case_created",
        table: "quant_settlement_chain_submission",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "settlement_redeem_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_chain_submission_active_redeem",
        table: "quant_settlement_chain_submission",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "settlement_redeem_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some(
            "purpose = 'redeem'::qp_settlement_submission_purpose AND state = ANY (ARRAY['prepared'::qp_settlement_submission_state, 'dispatching'::qp_settlement_submission_state, 'awaiting_chain_hash'::qp_settlement_submission_state, 'awaiting_finality'::qp_settlement_submission_state])",
        ),
    },
    IndexSpec {
        name: "uq_quant_settlement_chain_submission_active_action",
        table: "quant_settlement_chain_submission",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "settlement_governed_action_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some(
            "settlement_governed_action_id IS NOT NULL AND state = ANY (ARRAY['prepared'::qp_settlement_submission_state, 'dispatching'::qp_settlement_submission_state, 'awaiting_chain_hash'::qp_settlement_submission_state, 'awaiting_finality'::qp_settlement_submission_state])",
        ),
    },
    IndexSpec {
        name: "uq_quant_settlement_chain_submission_relayer_id",
        table: "quant_settlement_chain_submission",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "relayer_transaction_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("relayer_transaction_id IS NOT NULL"),
    },
    IndexSpec {
        name: "uq_quant_settlement_chain_submission_tx_hash",
        table: "quant_settlement_chain_submission",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "transaction_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("transaction_hash IS NOT NULL"),
    },
    IndexSpec {
        name: "idx_quant_settlement_redeem_lot_redeem",
        table: "quant_settlement_redeem_lot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "settlement_redeem_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_settlement_redeem_lot_position",
        table: "quant_settlement_redeem_lot",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "strategy_position_lot_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_shadow_comparison_candidate_version_decision_at",
        table: "quant_shadow_comparison",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "candidate_model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_shadow_comparison_hash",
        table: "quant_shadow_comparison",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "comparison_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_shadow_observation_contract_window",
        table: "quant_shadow_comparison",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "candidate_model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "champion_model_version_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "policy_bundle_generation",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_policy_snapshot_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "champion_serving_contract_hash",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "candidate_serving_contract_hash",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decision_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_source_slice_status_created",
        table: "quant_source_slice",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_source_slice_identity",
        table: "quant_source_slice",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "identity_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_trade_policy_status_created",
        table: "quant_trade_policy_artifact",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_trade_policy_artifact_hash",
        table: "quant_trade_policy_artifact",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "content_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_trade_policy_audit_artifact_created",
        table: "quant_trade_policy_governance_audit",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_trade_policy_trial_attempt_drilldown",
        table: "quant_trade_policy_trial_attempt",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "fit_job_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "scope",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "attempt_ordinal",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_trade_policy_trial_attempt_ordinal",
        table: "quant_trade_policy_trial_attempt",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[
            IndexColumnSpec {
                name: "fit_job_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "attempt_ordinal",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_trade_policy_validation_artifact_created",
        table: "quant_trade_policy_validation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_trade_policy_validation_running_artifact",
        table: "quant_trade_policy_validation",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "artifact_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(status = 'running'::qp_trade_policy_validation_status)"),
    },
    IndexSpec {
        name: "idx_quant_trade_policy_validation_row_result",
        table: "quant_trade_policy_validation_row",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "validation_run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "passed",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "row_ordinal",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_exchange_history_chunk_range",
        table: "quant_exchange_history_chunk",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "frontier",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "from_block",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "to_block",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_exchange_history_chunk_frontier",
        table: "quant_exchange_history_chunk",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "frontier",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "to_block",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_history_fit_seal_plan_window",
        table: "quant_history_fit_seal",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "plan_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "window_from_block",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "window_to_block",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_history_fit_seal_chunk_chunk",
        table: "quant_history_fit_seal_chunk",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "chunk_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "state_revision",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_history_serving_head_lookup",
        table: "quant_history_serving_head_seal",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "frontier",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
            IndexColumnSpec {
                name: "serving_head_seal_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_history_serving_head_predecessor",
        table: "quant_history_serving_head_seal",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "previous_seal_id",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(previous_seal_id IS NOT NULL)"),
    },
    IndexSpec {
        name: "uq_quant_history_serving_head_root",
        table: "quant_history_serving_head_seal",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "frontier",
            direction: IndexDirection::Asc,
        }],
        predicate: Some("(previous_seal_id IS NULL)"),
    },
    IndexSpec {
        name: "idx_quant_history_serving_head_chunk_chunk",
        table: "quant_history_serving_head_seal_chunk",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "chunk_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "state_revision",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_exchange_history_quarantine_chunk",
        table: "quant_exchange_history_quarantine",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "chunk_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "quarantined_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_exchange_history_quarantine_kind_page",
        table: "quant_exchange_history_quarantine",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "kind",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "quarantine_id",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_exchange_history_quarantine_resolution_chunk",
        table: "quant_exchange_history_quarantine_resolution",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "replacement_chunk_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "resolved_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_fresh_boot_run_due",
        table: "quant_fresh_boot_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "status",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "next_attempt_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "lease_expires_at",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_fresh_boot_run_profile_created",
        table: "quant_fresh_boot_run",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "research_profile_artifact_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_fresh_boot_run_event_timeline",
        table: "quant_fresh_boot_run_event",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "run_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "event_sequence",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_training_dataset_spec_created",
        table: "quant_training_dataset",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "model_spec_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "created_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_training_dataset_status",
        table: "quant_training_dataset",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "status",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_quant_training_dataset_hash",
        table: "quant_training_dataset",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "dataset_hash",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_weather_daily_temperature_open",
        table: "quant_weather_daily_temperature_projection",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "day_closed",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "local_date",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "station",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_quant_weather_observation_current_daily_high",
        table: "quant_weather_observation_current",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "station",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "local_date",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "temperature_celsius",
                direction: IndexDirection::Asc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "uq_role_code",
        table: "role",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "code",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_role_menu_menu",
        table: "role_menu",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "menu_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_activation_activated_at",
        table: "policy_activation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "activated_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_activation_snapshot",
        table: "policy_activation",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "decision_policy_snapshot_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_policy_approval_revision_decided",
        table: "policy_approval",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[
            IndexColumnSpec {
                name: "policy_revision_id",
                direction: IndexDirection::Asc,
            },
            IndexColumnSpec {
                name: "decided_at",
                direction: IndexDirection::Desc,
            },
        ],
        predicate: None,
    },
    IndexSpec {
        name: "idx_decision_policy_snapshot_created_at",
        table: "decision_policy_snapshot",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "created_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_system_runtime_control_transition_occurred_at",
        table: "system_runtime_control_transition",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "occurred_at",
            direction: IndexDirection::Desc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "uq_user_username",
        table: "user",
        method: IndexMethod::BTree,
        unique: true,
        columns: &[IndexColumnSpec {
            name: "username",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
    IndexSpec {
        name: "idx_user_role_role",
        table: "user_role",
        method: IndexMethod::BTree,
        unique: false,
        columns: &[IndexColumnSpec {
            name: "role_id",
            direction: IndexDirection::Asc,
        }],
        predicate: None,
    },
];

pub async fn apply(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for spec in INDEXES {
        let mut statement = Index::create();
        statement
            .name(spec.name)
            .table((Alias::new("public"), Alias::new(spec.table)));
        if spec.unique {
            statement.unique();
        }
        if matches!(spec.method, IndexMethod::Gin) {
            statement.index_type(IndexType::Custom(Alias::new("GIN").into_iden()));
        }
        for column in spec.columns {
            match column.direction {
                IndexDirection::Asc => {
                    statement.col(Alias::new(column.name));
                }
                IndexDirection::Desc => {
                    statement.col((Alias::new(column.name), IndexOrder::Desc));
                }
            }
        }
        if let Some(predicate) = spec.predicate {
            statement.and_where(v1::index_predicate(predicate)?);
        }
        manager
            .create_index(statement.clone())
            .await
            .map_err(|error| {
                DbErr::Custom(format!(
                    "create index `{}` on `{}` failed: {error}",
                    spec.name, spec.table
                ))
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{INDEXES, IndexSpec};

    fn index(name: &str) -> &'static IndexSpec {
        let Some(index) = INDEXES.iter().find(|index| index.name == name) else {
            panic!("missing query index {name}");
        };
        index
    }

    #[test]
    fn feedback_indexes_are_semantic() {
        assert!(index("uq_quant_feedback_cycle_idempotency").unique);
        assert!(index("uq_quant_feedback_stage_sequence").unique);
        assert!(index("uq_quant_feedback_evaluation_dataset").unique);
        let semantics = index("uq_quant_feedback_evaluation_semantics");
        assert!(semantics.unique);
        assert_eq!(semantics.columns.len(), 3);
        assert_eq!(semantics.columns[0].name, "evaluation_dataset_hash");
        assert_eq!(semantics.columns[1].name, "evaluation_artifact_bytes_hash");
        assert_eq!(semantics.columns[2].name, "cohort_manifest_hash");
        assert!(index("uq_quant_feedback_evaluation_use_hash").unique);
    }

    #[test]
    fn promotion_permit_indexes_semantic() {
        assert!(index("uq_quant_feedback_permit_idempotency").unique);
        assert!(index("uq_quant_feedback_permit_scope").unique);
        assert!(index("uq_quant_feedback_permit_issuance").unique);
        let active = index("idx_quant_feedback_permit_active_scope");
        assert!(!active.unique);
        assert_eq!(active.predicate, Some("(revoked_at IS NULL)"));
        assert_eq!(active.columns.len(), 4);
    }
}
