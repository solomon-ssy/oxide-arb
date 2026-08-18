//! Finalized V2 exchange-event projection into account execution truth.

use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use quant_pivot_api::exchange::constants::EXCHANGE_CONTRACTS;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    clickhouse::ExchangeEventRow,
    domain::{
        ports::{KillSwitchPort, SetKillSwitchCommand},
        quant::{AccountRecoveryIncidentInfo, NewAccountChainExecution},
    },
    enums::{
        clickhouse::{ChExchangeEventKind, ChExchangeSide, ChExchangeVersion},
        common::{AlertCategory, AlertLevel, AlertSource, Side},
        execution::{AccountChainExecutionRole, KillSwitchState},
    },
    types::{
        AccountChainExecutionId, ContentHash, EvmAddress, EvmBlockHash, EvmTransactionHash,
        ExecutionAccountId, OrderId, Price, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{
    AccountChainExecutionRepository, AccountRecoveryRepository, QuantFactReadRepository,
};
use rust_decimal::Decimal;

use crate::{
    execution::account_pause::AccountPauseCoordinator,
    observability::alert_dispatcher::{Alert, AlertDispatcher},
};

const ASSET_SCALE: u64 = 1_000_000;
const PROJECTION_BATCH: u64 = 512;

pub struct AccountChainExecutionProjector {
    execution_account_id: ExecutionAccountId,
    funder: EvmAddress,
    facts: Arc<dyn QuantFactReadRepository>,
    executions: Arc<dyn AccountChainExecutionRepository>,
    recovery: Arc<dyn AccountRecoveryRepository>,
    kill_switch: Arc<dyn KillSwitchPort>,
    alerts: Arc<AlertDispatcher>,
    pause: Arc<AccountPauseCoordinator>,
}

pub struct AccountChainExecutionProjectorDeps {
    pub execution_account_id: ExecutionAccountId,
    pub funder: EvmAddress,
    pub facts: Arc<dyn QuantFactReadRepository>,
    pub executions: Arc<dyn AccountChainExecutionRepository>,
    pub recovery: Arc<dyn AccountRecoveryRepository>,
    pub kill_switch: Arc<dyn KillSwitchPort>,
    pub alerts: Arc<AlertDispatcher>,
    pub pause: Arc<AccountPauseCoordinator>,
}

impl AccountChainExecutionProjector {
    #[must_use]
    pub fn new(deps: AccountChainExecutionProjectorDeps) -> Self {
        Self {
            execution_account_id: deps.execution_account_id,
            funder: deps.funder,
            facts: deps.facts,
            executions: deps.executions,
            recovery: deps.recovery,
            kill_switch: deps.kill_switch,
            alerts: deps.alerts,
            pause: deps.pause,
        }
    }

    pub async fn project_pass(&self) -> QuantResult<u64> {
        if let Some(incident) = self
            .recovery
            .active_incident(&self.execution_account_id)
            .await?
        {
            latch_exit_only(self.kill_switch.as_ref(), &incident).await?;
            self.pause.pause_incident(&incident, &self.funder).await?;
            let _ = self.pause.confirm_incident(&incident, &self.funder).await?;
        }
        let cursor = self
            .executions
            .latest_cursor(&self.execution_account_id)
            .await?;
        let events = self
            .facts
            .account_order_filled_events(&self.funder, cursor, PROJECTION_BATCH)
            .await?;
        if events.is_empty() {
            return Ok(0);
        }
        let taker_orders = self
            .facts
            .matches_for_taker_orders(
                events
                    .iter()
                    .map(|event| OrderId::new(event.order_hash.clone()))
                    .collect(),
            )
            .await?
            .into_iter()
            .map(|matched| matched.taker_order_hash)
            .collect::<BTreeSet<_>>();
        let projected = events
            .iter()
            .map(|event| {
                AccountChainProjectionInput {
                    execution_account_id: self.execution_account_id,
                    funder: &self.funder,
                    event,
                    taker_orders: &taker_orders,
                }
                .project()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let execution_ids = projected
            .iter()
            .map(|execution| execution.account_chain_execution_id)
            .collect::<Vec<_>>();
        let outcome = self.executions.append(projected).await?;
        for execution_id in execution_ids {
            let association = self
                .recovery
                .associate_execution(&execution_id, Utc::now())
                .await?;
            if let Some(incident) = association.incident.as_ref() {
                latch_exit_only(self.kill_switch.as_ref(), incident).await?;
                self.pause.pause_incident(incident, &self.funder).await?;
                if association.incident_created {
                    self.alerts.dispatch_background(
                        Alert::new(
                            format!("account-recovery:{}", incident.account_recovery_incident_id),
                            AlertLevel::Critical,
                            AlertCategory::TradingSafety,
                            AlertSource::Execution,
                            "Unknown external account execution",
                            incident.reason.clone(),
                            Utc::now(),
                        )
                        .with_affects_trading(true),
                    );
                }
            }
        }
        Ok(outcome.inserted)
    }

    pub async fn active_recovery(&self) -> QuantResult<Option<AccountRecoveryIncidentInfo>> {
        self.recovery
            .active_incident(&self.execution_account_id)
            .await
            .map_err(Into::into)
    }
}

async fn latch_exit_only(
    kill_switch: &dyn KillSwitchPort,
    incident: &AccountRecoveryIncidentInfo,
) -> QuantResult<()> {
    let current = kill_switch.current();
    if current.restriction_rank() >= KillSwitchState::ExitOnly.restriction_rank() {
        return Ok(());
    }
    let view = kill_switch.view();
    kill_switch
        .set(SetKillSwitchCommand {
            expected_revision: view.revision,
            target: KillSwitchState::ExitOnly,
            actor: "account-chain-projector".to_owned(),
            reason: format!(
                "account recovery incident {} requires exit-only",
                incident.account_recovery_incident_id
            ),
            ack: false,
            latch: true,
        })
        .await?;
    Ok(())
}

struct AccountChainProjectionInput<'a> {
    execution_account_id: ExecutionAccountId,
    funder: &'a EvmAddress,
    event: &'a ExchangeEventRow,
    taker_orders: &'a BTreeSet<String>,
}

impl AccountChainProjectionInput<'_> {
    fn project(&self) -> Result<NewAccountChainExecution, ExecutionError> {
        let event = self.event;
        if event.chain_id != 137
            || event.event_kind != ChExchangeEventKind::OrderFilled
            || event.exchange_version != ChExchangeVersion::V2
            || event.schema_version != ExchangeEventRow::SCHEMA_VERSION
            || !EXCHANGE_CONTRACTS.iter().any(|contract| {
                format!("{:#x}", contract.address).eq_ignore_ascii_case(&event.contract_address)
            })
        {
            return Err(projection_error(
                "event is not an accepted registered V2 OrderFilled",
            ));
        }
        let maker = parse_address(&event.maker, "maker")?;
        let taker = parse_address(
            event
                .taker
                .as_deref()
                .ok_or_else(|| projection_error("V2 OrderFilled has no taker"))?,
            "taker",
        )?;
        if &maker != self.funder {
            return Err(projection_error(
                "account-scoped query returned an order not owned by the funder",
            ));
        }
        let role = if &taker == self.funder {
            AccountChainExecutionRole::SelfMatch
        } else if self.taker_orders.contains(&event.order_hash) {
            AccountChainExecutionRole::Taker
        } else {
            AccountChainExecutionRole::Maker
        };
        let side = match event.side {
            ChExchangeSide::Buy => Side::Buy,
            ChExchangeSide::Sell => Side::Sell,
            ChExchangeSide::Unknown => {
                return Err(projection_error("V2 OrderFilled has unknown side"));
            }
        };
        let token_id = TokenId::new(
            event
                .token_id
                .as_deref()
                .ok_or_else(|| projection_error("V2 OrderFilled has no token id"))?,
        );
        let maker_amount = decimal_asset(&event.maker_amount, "maker_amount")?;
        let taker_amount = decimal_asset(&event.taker_amount, "taker_amount")?;
        let (principal, share_count) = match side {
            Side::Buy => (maker_amount, taker_amount),
            Side::Sell => (taker_amount, maker_amount),
        };
        if share_count <= Decimal::ZERO || principal < Decimal::ZERO {
            return Err(projection_error(
                "account execution amounts are non-positive",
            ));
        }
        let price = Price::new(principal / share_count);
        if price > Price::ONE {
            return Err(projection_error("account execution price exceeds one"));
        }
        let fee = event
            .fee_amount
            .as_deref()
            .map(|raw| decimal_asset(raw, "fee_amount"))
            .transpose()?
            .ok_or_else(|| projection_error("V2 OrderFilled has no exact fee"))?;
        let source_event_hash = ContentHash::from(event.event_id);
        Ok(NewAccountChainExecution {
            account_chain_execution_id: AccountChainExecutionId::from_content_hash(
                &source_event_hash,
            ),
            execution_account_id: self.execution_account_id,
            role,
            chain_id: i64::try_from(event.chain_id)
                .map_err(|error| projection_error(&format!("chain id overflow: {error}")))?,
            protocol_version: 2,
            exchange_address: parse_address(&event.contract_address, "exchange")?,
            block_number: i64::try_from(event.block_number)
                .map_err(|error| projection_error(&format!("block number overflow: {error}")))?,
            block_hash: EvmBlockHash::parse(event.block_hash.to_ascii_lowercase())
                .map_err(|error| projection_error(&format!("invalid block hash: {error}")))?,
            transaction_hash: EvmTransactionHash::parse(
                event.transaction_hash.to_ascii_lowercase(),
            )
            .map_err(|error| projection_error(&format!("invalid transaction hash: {error}")))?,
            transaction_index: i64::try_from(event.transaction_index).map_err(|error| {
                projection_error(&format!("transaction index overflow: {error}"))
            })?,
            log_index: i64::try_from(event.log_index)
                .map_err(|error| projection_error(&format!("log index overflow: {error}")))?,
            order_id: OrderId::new(event.order_hash.clone()),
            maker_address: maker,
            taker_address: taker,
            order_side: side,
            order_token_id: token_id.clone(),
            maker_amount_raw: event.maker_amount.clone(),
            taker_amount_raw: event.taker_amount.clone(),
            account_side: Some(side),
            account_token_id: Some(token_id.clone()),
            shares: Some(Shares::new(share_count)),
            principal_usd: Some(Usd::new(principal)),
            exact_fee_usd: Some(Usd::new(fee)),
            builder_code: event.builder.clone(),
            metadata: event.metadata.clone(),
            source_event_hash,
            availability_policy_hash: ContentHash::from(event.availability_policy_hash),
            observed_at: event_time(event.block_timestamp, "block_timestamp")?,
            available_at: event_time(event.model_available_at, "model_available_at")?,
        })
    }
}

fn parse_address(raw: &str, field: &str) -> Result<EvmAddress, ExecutionError> {
    EvmAddress::parse(raw.to_ascii_lowercase())
        .map_err(|error| projection_error(&format!("invalid {field} address: {error}")))
}

fn decimal_asset(raw: &str, field: &str) -> Result<Decimal, ExecutionError> {
    Decimal::from_str(raw)
        .map(|value| value / Decimal::from(ASSET_SCALE))
        .map_err(|error| projection_error(&format!("invalid {field}: {error}")))
}

fn event_time(value: i64, field: &str) -> Result<DateTime<Utc>, ExecutionError> {
    DateTime::from_timestamp_millis(value)
        .ok_or_else(|| projection_error(&format!("invalid {field}: {value}")))
}

fn projection_error(reason: &str) -> ExecutionError {
    ExecutionError::AccountChainProjection {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_api::exchange::constants::CTF_EXCHANGE_V2;
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        clickhouse::{ChDigest, ExchangeEventRow},
        domain::{
            governance::KillSwitchView,
            ports::{KillSwitchPort, SetKillSwitchCommand},
            quant::{AccountRecoveryIncidentInfo, NewAccountChainExecution},
        },
        enums::{
            clickhouse::{ChExchangeEventKind, ChExchangeSide, ChExchangeVersion},
            common::Side,
            execution::{
                AccountChainExecutionRole, AccountRecoveryIncidentKind,
                AccountRecoveryIncidentStatus, KillSwitchState,
            },
        },
        types::{AccountRecoveryIncidentId, EvmAddress, ExecutionAccountId, Shares, Usd},
    };
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{AccountChainProjectionInput, latch_exit_only};

    struct RecordingKillSwitch {
        state: Mutex<KillSwitchState>,
        sets: AtomicUsize,
    }

    impl RecordingKillSwitch {
        fn snapshot(&self) -> KillSwitchView {
            KillSwitchView {
                state: *self.state.lock().expect("kill state"),
                requires_operator_ack: self.sets.load(Ordering::Relaxed) > 0,
                revision: i64::try_from(self.sets.load(Ordering::Relaxed)).expect("revision"),
                last_reason: "test".to_owned(),
                changed_by: "test".to_owned(),
                changed_at: Utc::now(),
            }
        }
    }

    #[async_trait]
    impl KillSwitchPort for RecordingKillSwitch {
        fn current(&self) -> KillSwitchState {
            *self.state.lock().expect("kill state")
        }

        fn view(&self) -> KillSwitchView {
            self.snapshot()
        }

        async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
            *self.state.lock().expect("kill state") = command.target;
            self.sets.fetch_add(1, Ordering::Relaxed);
            Ok(self.snapshot())
        }
    }

    fn event(side: ChExchangeSide) -> ExchangeEventRow {
        let (maker_amount, taker_amount) = match side {
            ChExchangeSide::Buy => ("600000", "1000000"),
            ChExchangeSide::Sell => ("1000000", "400000"),
            ChExchangeSide::Unknown => ("0", "0"),
        };
        ExchangeEventRow {
            event_id: ChDigest::new([1; 32]),
            raw_log_hash: ChDigest::new([2; 32]),
            chain_id: 137,
            event_kind: ChExchangeEventKind::OrderFilled,
            contract_key: "ctf-v2".to_owned(),
            exchange_version: ChExchangeVersion::V2,
            contract_address: format!("{:#x}", CTF_EXCHANGE_V2.address),
            block_number: 1,
            block_hash: format!("0x{}", "3".repeat(64)),
            block_timestamp: 1_700_000_000_000,
            transaction_hash: format!("0x{}", "4".repeat(64)),
            transaction_index: 2,
            log_index: 3,
            order_hash: format!("0x{}", "5".repeat(64)),
            maker: "0x1111111111111111111111111111111111111111".to_owned(),
            taker: Some("0x2222222222222222222222222222222222222222".to_owned()),
            side,
            token_id: Some("12345".to_owned()),
            maker_asset_id: None,
            taker_asset_id: None,
            maker_amount: maker_amount.to_owned(),
            taker_amount: taker_amount.to_owned(),
            fee_amount: Some("10000".to_owned()),
            builder: Some(format!("0x{}", "6".repeat(64))),
            metadata: Some(format!("0x{}", "7".repeat(64))),
            observed_at: 1_700_000_000_100,
            model_available_at: 1_700_000_000_200,
            availability_policy_hash: ChDigest::new([8; 32]),
            chunk_id: Uuid::now_v7(),
            schema_version: ExchangeEventRow::SCHEMA_VERSION,
        }
    }

    fn project(
        event: &ExchangeEventRow,
        taker_orders: &BTreeSet<String>,
    ) -> NewAccountChainExecution {
        AccountChainProjectionInput {
            execution_account_id: ExecutionAccountId::from_v7(),
            funder: &EvmAddress::parse("0x1111111111111111111111111111111111111111")
                .expect("funder"),
            event,
            taker_orders,
        }
        .project()
        .expect("account execution")
    }

    #[test]
    fn projects_maker_buy() {
        let projected = project(&event(ChExchangeSide::Buy), &BTreeSet::new());
        assert_eq!(projected.role, AccountChainExecutionRole::Maker);
        assert_eq!(projected.account_side, Some(Side::Buy));
        assert_eq!(projected.shares, Some(Shares::new(dec!(1))));
        assert_eq!(projected.principal_usd, Some(Usd::new(dec!(0.6))));
        assert_eq!(projected.exact_fee_usd, Some(Usd::new(dec!(0.01))));
    }

    #[test]
    fn projects_taker_sell() {
        let event = event(ChExchangeSide::Sell);
        let taker_orders = BTreeSet::from([event.order_hash.clone()]);
        let projected = project(&event, &taker_orders);
        assert_eq!(projected.role, AccountChainExecutionRole::Taker);
        assert_eq!(projected.account_side, Some(Side::Sell));
        assert_eq!(projected.shares, Some(Shares::new(dec!(1))));
        assert_eq!(projected.principal_usd, Some(Usd::new(dec!(0.4))));
        assert_eq!(projected.exact_fee_usd, Some(Usd::new(dec!(0.01))));
    }

    #[test]
    fn projects_self_match() {
        let mut event = event(ChExchangeSide::Buy);
        event.taker = Some(event.maker.clone());
        let projected = project(&event, &BTreeSet::new());
        assert_eq!(projected.role, AccountChainExecutionRole::SelfMatch);
    }

    #[tokio::test]
    async fn incident_latches_exit_only() {
        let execution_account_id = ExecutionAccountId::from_v7();
        let incident = AccountRecoveryIncidentInfo {
            account_recovery_incident_id: AccountRecoveryIncidentId::from_v7(),
            execution_account_id,
            kind: AccountRecoveryIncidentKind::UnknownExternalExecution,
            status: AccountRecoveryIncidentStatus::Open,
            trigger_chain_execution_id: None,
            reason: "unknown external execution".to_owned(),
            opened_at: Utc::now(),
            seal_hash: None,
            sealed_by: None,
            sealed_at: None,
            revision: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let kill_switch = Arc::new(RecordingKillSwitch {
            state: Mutex::new(KillSwitchState::Closed),
            sets: AtomicUsize::new(0),
        });
        latch_exit_only(kill_switch.as_ref(), &incident)
            .await
            .expect("first latch");
        assert_eq!(kill_switch.current(), KillSwitchState::ExitOnly);
        assert_eq!(kill_switch.sets.load(Ordering::Relaxed), 1);
        latch_exit_only(kill_switch.as_ref(), &incident)
            .await
            .expect("replay latch");
        assert_eq!(kill_switch.sets.load(Ordering::Relaxed), 1);
    }
}
