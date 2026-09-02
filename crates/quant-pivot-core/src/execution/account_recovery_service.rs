//! Stable venue capture, finalized-chain comparison, and recovery manifest materialization.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_api::{
    clob::{ClobClient, OpenOrder},
    data_api::{DataApiClient, VenuePosition},
    exchange::account_recovery::AlloyAccountRecoveryReader,
};
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::OnchainConfig,
    domain::{
        api::{
            AccountPauseOperationView, AccountRecoveryIncidentView, AccountRecoveryManifestView,
        },
        ports::AccountRecoveryControlPort,
        quant::{
            AccountChainExecutionInfo, AccountCleanFunderBlockerEvidence,
            AccountRecoveryAssessmentInput, AccountRecoveryCreatedLot,
            AccountRecoveryExecutionDelta, AccountRecoveryIncidentInfo, AccountRecoveryLotBalance,
            AccountRecoveryManifestDraft, AccountRecoveryManifestInfo,
            AccountRecoverySellAllocation, AccountRecoveryTokenBalance,
            FinalizeAccountRecoveryIncident, NewStrategyPositionLot, SealAccountRecoveryIncident,
            StrategyPositionLot, settlement::SettlementRecoveryBlocker,
        },
    },
    enums::{
        common::Side,
        execution::{
            AccountChainExecutionRole, AccountPauseOperationKind, PositionLedgerState,
            StrategyPositionOriginKind,
        },
        quant::{AccountSource, OutcomeSide},
    },
    hashing::CanonicalDigest,
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, AccountRecoveryManifestId, ContentHash,
        EvmAddress, ExecutionAccountId, OrderId, Price, Shares, TokenId, Usd, UserId,
    },
};
use quant_pivot_repository::traits::{
    AccountPauseOperationRepository, AccountRecoveryRepository, ReservedCapitalRepository,
    StrategyPositionLotRepository, quant::settlement_redeem::SettlementRedeemRepository,
};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{
    account_pause::AccountPauseCoordinator, account_recovery_reconciler::AccountRecoveryReconciler,
};
use crate::ingest::market_registry::MarketRegistry;

const CLOB_SNAPSHOT_DOMAIN: &str = "quant-pivot/account-recovery-clob-snapshot";
const DATA_SNAPSHOT_DOMAIN: &str = "quant-pivot/account-recovery-data-snapshot";
const SETTLEMENT_SNAPSHOT_DOMAIN: &str = "quant-pivot/account-recovery-settlement-snapshot";
const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DataPositionEvidence {
    token_id: TokenId,
    condition_id: String,
    outcome: String,
    shares: Shares,
}

#[derive(Serialize)]
struct ClobSnapshotPreimage<'a> {
    collateral_usd: Usd,
    open_orders: &'a [OpenOrder],
}

struct VenueCapture {
    collateral_usd: Usd,
    open_order_ids: Vec<OrderId>,
    positions: Vec<AccountRecoveryTokenBalance>,
    clob_hash: ContentHash,
    data_hash: ContentHash,
}

pub struct AccountRecoveryServiceDeps {
    pub onchain: OnchainConfig,
    pub execution_account_id: ExecutionAccountId,
    pub funder: EvmAddress,
    pub clob: Arc<ClobClient>,
    pub data_api: Arc<DataApiClient>,
    pub market_registry: Arc<MarketRegistry>,
    pub pause: Arc<AccountPauseCoordinator>,
    pub pause_operations: Arc<dyn AccountPauseOperationRepository>,
    pub recovery: Arc<dyn AccountRecoveryRepository>,
    pub positions: Arc<dyn StrategyPositionLotRepository>,
    pub reserved: Arc<dyn ReservedCapitalRepository>,
    pub settlement: Arc<dyn SettlementRedeemRepository>,
}

pub struct AccountRecoveryService {
    chain: AlloyAccountRecoveryReader,
    execution_account_id: ExecutionAccountId,
    funder: EvmAddress,
    clob: Arc<ClobClient>,
    data_api: Arc<DataApiClient>,
    market_registry: Arc<MarketRegistry>,
    pause: Arc<AccountPauseCoordinator>,
    pause_operations: Arc<dyn AccountPauseOperationRepository>,
    recovery: Arc<dyn AccountRecoveryRepository>,
    positions: Arc<dyn StrategyPositionLotRepository>,
    reserved: Arc<dyn ReservedCapitalRepository>,
    settlement: Arc<dyn SettlementRedeemRepository>,
}

impl AccountRecoveryService {
    pub fn connect(deps: AccountRecoveryServiceDeps) -> QuantResult<Self> {
        let chain = AlloyAccountRecoveryReader::connect(&deps.onchain)
            .map_err(|error| recovery_error(&error.to_string()))?;
        Ok(Self {
            chain,
            execution_account_id: deps.execution_account_id,
            funder: deps.funder,
            clob: deps.clob,
            data_api: deps.data_api,
            market_registry: deps.market_registry,
            pause: deps.pause,
            pause_operations: deps.pause_operations,
            recovery: deps.recovery,
            positions: deps.positions,
            reserved: deps.reserved,
            settlement: deps.settlement,
        })
    }

    pub async fn reconcile_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        explicit_sell_allocations: Vec<AccountRecoverySellAllocation>,
    ) -> QuantResult<AccountRecoveryManifestInfo> {
        if incident.execution_account_id != self.execution_account_id {
            return Err(recovery_error(
                "incident execution account differs from the runtime account",
            )
            .into());
        }
        let pause_confirmed = self.pause.confirm_pause(incident, &self.funder).await?;
        let first = self.capture_venue().await?;
        let second = self.capture_venue().await?;
        let stable = first.clob_hash == second.clob_hash && first.data_hash == second.data_hash;
        let open_lots = self
            .positions
            .find_account_open_lots(&self.execution_account_id)
            .await?;
        let executions = self
            .recovery
            .incident_executions(&incident.account_recovery_incident_id)
            .await?;
        let (deltas, invalid_execution_ids) = execution_deltas(&executions);
        let token_ids = recovery_tokens(&second.positions, &open_lots, &deltas);
        let unmapped_token_ids = token_ids
            .iter()
            .filter(|token_id| self.market_registry.market_for_token(token_id).is_none())
            .cloned()
            .collect::<Vec<_>>();
        let chain = self
            .chain
            .snapshot(&self.funder, &token_ids)
            .await
            .map_err(|error| recovery_error(&error.to_string()))?;
        let blockers = self
            .settlement
            .recovery_blockers(&self.execution_account_id)
            .await?;
        let clean_funder_blocker = self
            .recovery
            .clean_funder_blocker(&incident.account_recovery_incident_id)
            .await?
            .map(|blocker| AccountCleanFunderBlockerEvidence {
                account_chain_execution_id: blocker.account_chain_execution_id,
                role: blocker.role,
                evidence_hash: blocker.evidence_hash,
            });
        let input = AccountRecoveryAssessmentInput {
            recovery_incident_id: incident.account_recovery_incident_id,
            execution_account_id: self.execution_account_id,
            observed_at: Utc::now(),
            finalized_block_number: i64::try_from(chain.block_number)
                .map_err(|error| recovery_error(&error.to_string()))?,
            finalized_block_hash: chain.block_hash,
            clob_snapshot_hash: second.clob_hash,
            data_api_snapshot_hash: second.data_hash,
            chain_snapshot_hash: chain.source_hash,
            settlement_snapshot_hash: settlement_hash(&blockers)?,
            pause_confirmed,
            venue_snapshot_stable: stable,
            clob_collateral_usd: second.collateral_usd,
            chain_collateral_usd: chain.collateral_usd,
            reserved_usd: self.reserved.sum_reserved_usd().await?,
            open_order_ids: second.open_order_ids,
            unmapped_token_ids,
            invalid_execution_ids,
            clean_funder_blocker,
            data_api_positions: second.positions,
            chain_positions: chain.positions,
            open_lots: open_lots.iter().map(lot_balance).collect(),
            incident_executions: deltas,
            explicit_sell_allocations,
            pending_settlement_count: u64::try_from(blockers.len())
                .map_err(|error| recovery_error(&error.to_string()))?,
        };
        let assessment = AccountRecoveryReconciler::assess(input.clone())?;
        let created_lots = if assessment.converged() {
            self.materialize_lots(
                incident.account_recovery_incident_id,
                &assessment.created_lots,
                &executions,
            )?
        } else {
            Vec::new()
        };
        self.recovery
            .append_manifest(AccountRecoveryManifestDraft {
                recovery_incident_id: incident.account_recovery_incident_id,
                input,
                assessment,
                created_lots,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn seal_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        command: SealAccountRecoveryIncident,
    ) -> QuantResult<AccountRecoveryIncidentInfo> {
        if incident.account_recovery_incident_id != command.recovery_incident_id {
            return Err(recovery_error("seal command targets a different incident").into());
        }
        let latest_manifest = self
            .recovery
            .latest_manifest(&incident.account_recovery_incident_id)
            .await?
            .ok_or_else(|| recovery_error("seal requires a recovery manifest"))?;
        let latest = self
            .reconcile_incident(
                incident,
                latest_manifest.input_json.explicit_sell_allocations,
            )
            .await?;
        if latest.account_recovery_manifest_id != command.account_recovery_manifest_id
            || !latest.converged
        {
            return Err(recovery_error(
                "seal command does not reference the latest converged manifest",
            )
            .into());
        }
        let sealed = self.recovery.seal_incident(command).await?;
        self.pause.unpause_incident(&sealed, &self.funder).await?;
        Ok(sealed)
    }

    pub async fn advance_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
    ) -> QuantResult<()> {
        if incident.seal_hash.is_none() {
            self.reconcile_incident(incident, Vec::new()).await?;
            return Ok(());
        }
        self.pause.unpause_incident(incident, &self.funder).await?;
        if self.pause.confirm_unpause(incident, &self.funder).await? {
            self.recovery
                .finalize_incident(FinalizeAccountRecoveryIncident {
                    recovery_incident_id: incident.account_recovery_incident_id,
                    expected_revision: incident.revision,
                    finalized_at: Utc::now(),
                })
                .await?;
        }
        Ok(())
    }

    async fn incident_view(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> QuantResult<Option<AccountRecoveryIncidentView>> {
        let Some(incident) = self.recovery.find_incident(incident_id).await? else {
            return Ok(None);
        };
        let latest_manifest = self
            .recovery
            .latest_manifest(incident_id)
            .await?
            .map(AccountRecoveryManifestView::from);
        let (pause, unpause) = tokio::try_join!(
            self.pause_operations
                .for_incident(incident_id, AccountPauseOperationKind::Pause,),
            self.pause_operations
                .for_incident(incident_id, AccountPauseOperationKind::Unpause,),
        )?;
        let mut pause_operations = pause
            .into_iter()
            .chain(unpause)
            .map(AccountPauseOperationView::from)
            .collect::<Vec<_>>();
        pause_operations.sort_by(|left, right| {
            left.created_at.cmp(&right.created_at).then_with(|| {
                left.account_pause_operation_id
                    .as_uuid()
                    .cmp(&right.account_pause_operation_id.as_uuid())
            })
        });
        Ok(Some(AccountRecoveryIncidentView {
            incident,
            latest_manifest,
            pause_operations,
        }))
    }

    async fn capture_venue(&self) -> QuantResult<VenueCapture> {
        let (collateral_usd, mut open_orders, positions) = tokio::try_join!(
            self.clob.collateral_balance(),
            self.clob.get_open_orders(),
            self.data_api.positions(self.funder.as_str()),
        )
        .map_err(|error| recovery_error(&error.to_string()))?;
        open_orders.sort_by(|left, right| left.order_id.as_str().cmp(right.order_id.as_str()));
        let clob_hash = CanonicalDigest::content_hash_typed(
            CLOB_SNAPSHOT_DOMAIN,
            SNAPSHOT_VERSION,
            &ClobSnapshotPreimage {
                collateral_usd,
                open_orders: &open_orders,
            },
        )
        .map_err(|error| recovery_error(&error.to_string()))?;
        let mut data_evidence = positions.iter().map(position_evidence).collect::<Vec<_>>();
        data_evidence.sort_by(|left, right| {
            left.token_id
                .as_str()
                .cmp(right.token_id.as_str())
                .then_with(|| left.condition_id.cmp(&right.condition_id))
        });
        let data_hash = CanonicalDigest::content_hash_typed(
            DATA_SNAPSHOT_DOMAIN,
            SNAPSHOT_VERSION,
            &data_evidence,
        )
        .map_err(|error| recovery_error(&error.to_string()))?;
        Ok(VenueCapture {
            collateral_usd,
            open_order_ids: open_orders
                .into_iter()
                .map(|order| order.order_id)
                .collect(),
            positions: aggregate_positions(&data_evidence),
            clob_hash,
            data_hash,
        })
    }

    fn materialize_lots(
        &self,
        incident_id: AccountRecoveryIncidentId,
        created: &[AccountRecoveryCreatedLot],
        executions: &[AccountChainExecutionInfo],
    ) -> QuantResult<Vec<NewStrategyPositionLot>> {
        let executions = executions
            .iter()
            .map(|execution| (execution.account_chain_execution_id, execution))
            .collect::<HashMap<_, _>>();
        let mut lots = Vec::with_capacity(created.len());
        for created in created {
            let execution = executions
                .get(&created.account_chain_execution_id)
                .ok_or_else(|| recovery_error("recovery lot has no chain execution"))?;
            let market_id = self
                .market_registry
                .market_for_token(&created.token_id)
                .ok_or_else(|| recovery_error("recovery lot token is not catalogued"))?;
            let market = self
                .market_registry
                .get_market(&market_id)
                .ok_or_else(|| recovery_error("recovery lot market is not catalogued"))?;
            let (yes_token, no_token) = self
                .market_registry
                .token_pair(&market_id)
                .ok_or_else(|| recovery_error("recovery lot market has no token pair"))?;
            let side = outcome_side(&created.token_id, &yes_token, &no_token)?;
            let state = if created.remaining_shares.is_zero() {
                PositionLedgerState::Closed
            } else {
                PositionLedgerState::Open
            };
            lots.push(NewStrategyPositionLot {
                strategy_position_lot_id: created.strategy_position_lot_id,
                origin_kind: StrategyPositionOriginKind::AccountRecoveryIncident,
                order_intent_id: None,
                recovery_incident_id: Some(incident_id),
                execution_account_id: self.execution_account_id,
                token_id: created.token_id.clone(),
                market_id,
                event_id: Some(market.event_id.clone()),
                category: market.categories.primary_category(),
                side,
                state,
                shares: created.remaining_shares,
                avg_price: Price::new(
                    created.acquired_cost_usd.inner() / created.acquired_shares.inner(),
                ),
                cost_usd: created.remaining_cost_usd,
                realized_pnl_usd: created.realized_pnl_delta_usd,
                source: AccountSource::Polymarket,
                opened_at: execution.available_at,
                closed_at: created.closed_at,
            });
        }
        Ok(lots)
    }
}

#[async_trait]
impl AccountRecoveryControlPort for AccountRecoveryService {
    async fn active_incident(&self) -> QuantResult<Option<AccountRecoveryIncidentView>> {
        let Some(incident) = self
            .recovery
            .active_incident(&self.execution_account_id)
            .await?
        else {
            return Ok(None);
        };
        self.incident_view(&incident.account_recovery_incident_id)
            .await
    }

    async fn incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> QuantResult<Option<AccountRecoveryIncidentView>> {
        self.incident_view(incident_id).await
    }

    async fn pause_and_reconcile(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        expected_revision: i64,
        allocations: Vec<AccountRecoverySellAllocation>,
    ) -> QuantResult<AccountRecoveryIncidentView> {
        let incident = self
            .recovery
            .find_incident(incident_id)
            .await?
            .ok_or_else(|| recovery_error("account recovery incident was not found"))?;
        if incident.revision != expected_revision {
            return Err(recovery_error("account recovery incident revision changed").into());
        }
        self.reconcile_incident(&incident, allocations).await?;
        self.incident_view(incident_id)
            .await?
            .ok_or_else(|| recovery_error("reconciled incident disappeared").into())
    }

    async fn seal(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        manifest_id: AccountRecoveryManifestId,
        expected_revision: i64,
        actor_id: UserId,
    ) -> QuantResult<AccountRecoveryIncidentView> {
        let incident = self
            .recovery
            .find_incident(incident_id)
            .await?
            .ok_or_else(|| recovery_error("account recovery incident was not found"))?;
        if incident.revision != expected_revision {
            return Err(recovery_error("account recovery incident revision changed").into());
        }
        self.seal_incident(
            &incident,
            SealAccountRecoveryIncident {
                recovery_incident_id: *incident_id,
                account_recovery_manifest_id: manifest_id,
                expected_revision,
                actor: actor_id,
                sealed_at: Utc::now(),
            },
        )
        .await?;
        self.incident_view(incident_id)
            .await?
            .ok_or_else(|| recovery_error("sealed incident disappeared").into())
    }

    async fn unpause_and_finalize(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        expected_revision: i64,
    ) -> QuantResult<AccountRecoveryIncidentView> {
        let incident = self
            .recovery
            .find_incident(incident_id)
            .await?
            .ok_or_else(|| recovery_error("account recovery incident was not found"))?;
        if incident.revision != expected_revision {
            return Err(recovery_error("account recovery incident revision changed").into());
        }
        if incident.seal_hash.is_none() {
            return Err(recovery_error("incident must be sealed before unpause").into());
        }
        self.advance_incident(&incident).await?;
        self.incident_view(incident_id)
            .await?
            .ok_or_else(|| recovery_error("advanced incident disappeared").into())
    }
}

fn position_evidence(position: &VenuePosition) -> DataPositionEvidence {
    DataPositionEvidence {
        token_id: TokenId::new(&position.asset),
        condition_id: position.condition_id.clone(),
        outcome: position.outcome.clone(),
        shares: Shares::new(position.size),
    }
}

fn aggregate_positions(evidence: &[DataPositionEvidence]) -> Vec<AccountRecoveryTokenBalance> {
    let mut balances = BTreeMap::<TokenId, Decimal>::new();
    for position in evidence {
        *balances.entry(position.token_id.clone()).or_default() += position.shares.inner();
    }
    balances
        .into_iter()
        .filter_map(|(token_id, shares)| {
            (shares > Decimal::ZERO).then_some(AccountRecoveryTokenBalance {
                token_id,
                shares: Shares::new(shares),
            })
        })
        .collect()
}

fn lot_balance(lot: &StrategyPositionLot) -> AccountRecoveryLotBalance {
    AccountRecoveryLotBalance {
        strategy_position_lot_id: lot.strategy_position_lot_id,
        token_id: lot.token_id.clone(),
        shares: lot.shares,
        cost_usd: lot.cost_usd,
        opened_at: lot.opened_at,
    }
}

fn execution_deltas(
    executions: &[AccountChainExecutionInfo],
) -> (
    Vec<AccountRecoveryExecutionDelta>,
    Vec<AccountChainExecutionId>,
) {
    let mut deltas = Vec::new();
    let mut invalid = Vec::new();
    for execution in executions {
        let (Some(side), Some(token_id), Some(shares), Some(principal_usd), Some(exact_fee_usd)) = (
            execution.account_side,
            execution.account_token_id.clone(),
            execution.shares,
            execution.principal_usd,
            execution.exact_fee_usd,
        ) else {
            invalid.push(execution.account_chain_execution_id);
            continue;
        };
        if execution.role == AccountChainExecutionRole::SelfMatch {
            invalid.push(execution.account_chain_execution_id);
            continue;
        }
        deltas.push(AccountRecoveryExecutionDelta {
            account_chain_execution_id: execution.account_chain_execution_id,
            token_id,
            shares_delta: match side {
                Side::Buy => shares.inner(),
                Side::Sell => -shares.inner(),
            },
            principal_usd,
            exact_fee_usd,
            available_at: execution.available_at,
        });
    }
    (deltas, invalid)
}

fn recovery_tokens(
    venue: &[AccountRecoveryTokenBalance],
    lots: &[StrategyPositionLot],
    executions: &[AccountRecoveryExecutionDelta],
) -> Vec<TokenId> {
    venue
        .iter()
        .map(|balance| balance.token_id.clone())
        .chain(lots.iter().map(|lot| lot.token_id.clone()))
        .chain(
            executions
                .iter()
                .map(|execution| execution.token_id.clone()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn outcome_side(token_id: &TokenId, yes: &TokenId, no: &TokenId) -> QuantResult<OutcomeSide> {
    if token_id == yes {
        Ok(OutcomeSide::Yes)
    } else if token_id == no {
        Ok(OutcomeSide::No)
    } else {
        Err(recovery_error("recovery lot token differs from the market pair").into())
    }
}

fn settlement_hash(blockers: &[SettlementRecoveryBlocker]) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_typed(SETTLEMENT_SNAPSHOT_DOMAIN, SNAPSHOT_VERSION, blockers)
        .map_err(|error| recovery_error(&error.to_string()).into())
}

fn recovery_error(reason: &str) -> ExecutionError {
    ExecutionError::AccountRecovery {
        reason: reason.to_owned(),
    }
}
