//! `quant_portfolio_plan` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, quant_account_snapshot, quant_market_selection};
use crate::{
    domain::quant::{
        ExistingPortfolioState, PortfolioDecisionResult, PortfolioScenarioArtifact,
        RepresentedRouteSet,
    },
    runtime_config::PortfolioConfig,
    types::{
        AccountSnapshotId, ContentHash, DecisionPolicySnapshotId, MarketSelectionId,
        PortfolioPlanId, PortfolioScenarioArtifactId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_portfolio_plan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub portfolio_plan_id: PortfolioPlanId,
    pub account_snapshot_id: AccountSnapshotId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub decision_at: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub represented_routes_json: RepresentedRouteSet,
    pub scenario_artifact_id: Option<PortfolioScenarioArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub scenario_artifact_json: Option<PortfolioScenarioArtifact>,
    #[sea_orm(column_type = "JsonBinary")]
    pub portfolio_policy_json: PortfolioConfig,
    #[sea_orm(column_type = "JsonBinary")]
    pub existing_state_json: ExistingPortfolioState,
    #[sea_orm(column_type = "JsonBinary")]
    pub decision_json: PortfolioDecisionResult,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "AccountSnapshot",
        from = "account_snapshot_id",
        to = "account_snapshot_id"
    )]
    pub account_snapshot: BelongsTo<quant_account_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<quant_market_selection::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
