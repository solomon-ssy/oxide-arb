//! Append-only row diagnostics for one trade-policy validation run.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_trade_policy_validation;
use crate::types::{
    ContentHash, DiagnosticCode, MarketId, TokenId, TradePolicyValidationRunId, TrainingExampleId,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_policy_validation_row")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub validation_run_id: TradePolicyValidationRunId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub row_ordinal: i64,
    pub evidence_kind: String,
    pub record_key: String,
    pub example_id: Option<TrainingExampleId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub decision_at: Option<DateTime<Utc>>,
    pub expected_row_hash: Option<ContentHash>,
    pub actual_row_hash: Option<ContentHash>,
    pub passed: bool,
    pub diagnostic_kind: Option<DiagnosticCode>,
    #[sea_orm(column_type = "Text", nullable)]
    pub detail: Option<String>,
    pub row_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ValidationRun",
        from = "validation_run_id",
        to = "validation_run_id"
    )]
    pub validation_run: BelongsTo<quant_trade_policy_validation::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
