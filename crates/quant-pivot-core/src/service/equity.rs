//! Equity-history snapshot assembly for drawdown-aware report sizing.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{
        EquitySnapshotInfo, NewAccountSnapshot, NewEquitySnapshot, PositionInfo, capital_drawdown,
        capital_hwm,
    },
    types::{AccountPositions, AccountSnapshotId, EquitySnapshotId, ExecutionAccountId, Usd},
};
use quant_pivot_repository::traits::{EquitySnapshotRepository, PositionRepository};
use quant_pivot_research::portfolio::{AccountSnapshot, DrawdownState};

/// Drawdown resolution used for sizing and persisted equity snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizingDrawdownResolution {
    pub drawdown_state: DrawdownState,
    pub high_water_mark_usd: Usd,
}

/// Rows and sizing state derived from one decision-time account snapshot.
#[derive(Debug, Clone)]
pub struct ReportEquitySnapshot {
    pub account_snapshot: NewAccountSnapshot,
    pub equity_snapshot: NewEquitySnapshot,
    pub drawdown_state: DrawdownState,
}

/// Drawdown provider consumed by the report builder.
#[async_trait]
pub trait DrawdownProvider: Send + Sync {
    async fn snapshot_for_report(
        &self,
        account: &AccountSnapshot,
    ) -> QuantResult<ReportEquitySnapshot>;

    /// Re-read the equity ledger immediately before sizing and return the
    /// conservative drawdown estimate (`max(floor, fresh)`).
    ///
    /// This closes the TOCTOU window between the initial account read and the
    /// planner while keeping fail-safe semantics: drawdown never shrinks below
    /// the earlier estimate within one report build.
    async fn resolve_drawdown_for_sizing(
        &self,
        account: &AccountSnapshot,
        floor: DrawdownState,
    ) -> QuantResult<SizingDrawdownResolution>;
}

/// Repository-backed equity snapshot service.
pub struct EquitySnapshotService {
    equity_snapshots: Arc<dyn EquitySnapshotRepository>,
    positions: Arc<dyn PositionRepository>,
    execution_account_id: ExecutionAccountId,
}

impl EquitySnapshotService {
    #[must_use]
    pub const fn new(
        equity_snapshots: Arc<dyn EquitySnapshotRepository>,
        positions: Arc<dyn PositionRepository>,
        execution_account_id: ExecutionAccountId,
    ) -> Self {
        Self {
            equity_snapshots,
            positions,
            execution_account_id,
        }
    }

    pub async fn record_history_snapshot(
        &self,
        account: &AccountSnapshot,
    ) -> QuantResult<EquitySnapshotInfo> {
        let built = self.build(account, None).await?;
        self.equity_snapshots
            .create(built.equity_snapshot)
            .await
            .map_err(Into::into)
    }

    async fn build(
        &self,
        account: &AccountSnapshot,
        account_snapshot_id: Option<AccountSnapshotId>,
    ) -> QuantResult<ReportEquitySnapshot> {
        let resolution = self
            .resolve_drawdown_from_ledger(account, DrawdownState::neutral())
            .await?;
        let realized_pnl_cumulative_usd = self.positions.realized_pnl_cumulative_usd().await?;
        let open_lots = self.positions.find_open_lots().await?;
        let unrealized_pnl_usd = unrealized_pnl_usd(&open_lots, account)?;

        let account_snapshot_ref = account_snapshot_id;
        let account_snapshot_id = account_snapshot_id.unwrap_or_else(AccountSnapshotId::from_v7);

        let account_snapshot = NewAccountSnapshot {
            account_snapshot_id,
            execution_account_id: self.execution_account_id,
            as_of: account.as_of,
            source: account.source,
            venue_net_liquidation_usd: account.venue_net_liquidation_usd,
            capital_base_usd: account.capital_base_usd,
            available_usd: account.available_usd,
            reserved_usd: account.reserved_usd,
            positions_json: AccountPositions(account.positions.clone()),
            exposures_json: account.exposures.clone(),
        };
        let equity_snapshot = NewEquitySnapshot {
            equity_snapshot_id: EquitySnapshotId::from_v7(),
            as_of: account.as_of,
            source: account.source,
            venue_net_liquidation_usd: account.venue_net_liquidation_usd,
            capital_base_usd: account.capital_base_usd,
            available_usd: account.available_usd,
            reserved_usd: account.reserved_usd,
            realized_pnl_cumulative_usd,
            unrealized_pnl_usd,
            high_water_mark_usd: resolution.high_water_mark_usd,
            drawdown_pct: resolution.drawdown_state.current_drawdown,
            account_snapshot_ref,
        };

        Ok(ReportEquitySnapshot {
            account_snapshot,
            equity_snapshot,
            drawdown_state: resolution.drawdown_state,
        })
    }

    async fn resolve_drawdown_from_ledger(
        &self,
        account: &AccountSnapshot,
        floor: DrawdownState,
    ) -> QuantResult<SizingDrawdownResolution> {
        let previous = self
            .equity_snapshots
            .latest_at_or_before(account.as_of)
            .await?;
        let high_water_mark_usd = capital_hwm(
            previous.map(|snapshot| snapshot.high_water_mark_usd),
            account.capital_base_usd,
        );
        let fresh_drawdown = capital_drawdown(account.capital_base_usd, high_water_mark_usd);
        Ok(SizingDrawdownResolution {
            drawdown_state: floor.conservative_max(DrawdownState {
                current_drawdown: fresh_drawdown,
            }),
            high_water_mark_usd,
        })
    }
}

#[async_trait]
impl DrawdownProvider for EquitySnapshotService {
    async fn snapshot_for_report(
        &self,
        account: &AccountSnapshot,
    ) -> QuantResult<ReportEquitySnapshot> {
        self.build(account, Some(AccountSnapshotId::from_v7()))
            .await
    }

    async fn resolve_drawdown_for_sizing(
        &self,
        account: &AccountSnapshot,
        floor: DrawdownState,
    ) -> QuantResult<SizingDrawdownResolution> {
        self.resolve_drawdown_from_ledger(account, floor).await
    }
}

fn unrealized_pnl_usd(open_lots: &[PositionInfo], account: &AccountSnapshot) -> QuantResult<Usd> {
    let marks = account
        .positions
        .iter()
        .map(|position| (position.token_id.as_str(), position.cur_price))
        .collect::<HashMap<_, _>>();
    let mut total = Usd::ZERO;
    for lot in open_lots {
        let mark = marks.get(lot.token_id.as_str()).copied().ok_or_else(|| {
            ReportError::InvariantViolation {
                stage: "equity_snapshot",
                detail: format!(
                    "open strategy lot {} has no current venue mark for token {}",
                    lot.position_id, lot.token_id
                ),
            }
        })?;
        total += lot.shares * mark - lot.cost_usd;
    }
    Ok(total)
}
