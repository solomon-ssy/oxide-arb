//! Portfolio plan persistence DTOs.
//!
//! The JSONB columns are strong-typed (`types::portfolio_plan`) — never a bare
//! `serde_json::Value`.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    runtime_config::PortfolioConfig,
    types::{
        AccountSnapshotId, ContentHash, DecisionPolicySnapshotId, MarketSelectionId,
        PortfolioPlanId, PortfolioScenarioArtifactId,
    },
};

use super::{
    ExistingPortfolioState, PortfolioDecisionResult, PortfolioScenarioArtifact, RepresentedRouteSet,
};

/// Persisted portfolio pruning and allocation result.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_portfolio_plan::Entity")]
pub struct PortfolioPlanInfo {
    pub portfolio_plan_id: PortfolioPlanId,
    pub account_snapshot_id: AccountSnapshotId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub decision_at: DateTime<Utc>,
    pub represented_routes_json: RepresentedRouteSet,
    pub scenario_artifact_id: Option<PortfolioScenarioArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    pub scenario_artifact_json: Option<PortfolioScenarioArtifact>,
    pub portfolio_policy_json: PortfolioConfig,
    pub existing_state_json: ExistingPortfolioState,
    pub decision_json: PortfolioDecisionResult,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PortfolioPlanInfo, crate::entities::quant_portfolio_plan::Model, {
    portfolio_plan_id, account_snapshot_id, decision_policy_snapshot_id, market_selection_id,
    decision_at, represented_routes_json, scenario_artifact_id, scenario_artifact_hash,
    scenario_artifact_json, portfolio_policy_json, existing_state_json, decision_json,
    created_at,
});

/// Insert payload for `quant_portfolio_plan`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_portfolio_plan::ActiveModel")]
pub struct NewPortfolioPlan {
    pub portfolio_plan_id: PortfolioPlanId,
    pub account_snapshot_id: AccountSnapshotId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub decision_at: DateTime<Utc>,
    pub represented_routes_json: RepresentedRouteSet,
    pub scenario_artifact_id: Option<PortfolioScenarioArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    pub scenario_artifact_json: Option<PortfolioScenarioArtifact>,
    pub portfolio_policy_json: PortfolioConfig,
    pub existing_state_json: ExistingPortfolioState,
    pub decision_json: PortfolioDecisionResult,
}
