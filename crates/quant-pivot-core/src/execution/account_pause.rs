//! Durable V2 account pause orchestration across every registered exchange.

use std::{collections::BTreeSet, sync::Arc};

use chrono::Utc;
use quant_pivot_api::{
    exchange::{
        constants::EXCHANGE_CONTRACTS,
        user_pause::{AlloyUserPauseReader, UserPauseError},
    },
    settlement::{eoa::EoaPreparedBlock, wallet_call::PreparedWalletCall},
};
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::OnchainConfig,
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseOperationInfo,
        AccountRecoveryIncidentInfo, NewAccountPauseOperation,
    },
    enums::execution::{AccountPauseOperationKind, AccountPauseOperationState},
    hashing::CanonicalDigest,
    types::{AccountPauseOperationId, EvmAddress},
};
use quant_pivot_repository::traits::AccountPauseOperationRepository;

use super::settlement_executor::{
    EnvelopeFields, ProductionSettlementExecutor, WalletEnvelopeDispatch,
};

const PAUSE_ID_DOMAIN: &str = "quant-pivot/account-pause-operation";
const PAUSE_ID_VERSION: u32 = 1;

pub struct AccountPauseCoordinator {
    reader: AlloyUserPauseReader,
    executor: Arc<ProductionSettlementExecutor>,
    repository: Arc<dyn AccountPauseOperationRepository>,
    exchanges: Vec<EvmAddress>,
}

struct PrepareOperationInput<'a, C> {
    incident: &'a AccountRecoveryIncidentInfo,
    exchange: &'a EvmAddress,
    operation_kind: AccountPauseOperationKind,
    requested_block: u64,
    interval_blocks: Option<u64>,
    effective_block: Option<u64>,
    call: &'a C,
}

impl AccountPauseCoordinator {
    pub fn connect(
        config: &OnchainConfig,
        executor: Arc<ProductionSettlementExecutor>,
        repository: Arc<dyn AccountPauseOperationRepository>,
    ) -> QuantResult<Self> {
        let reader = AlloyUserPauseReader::connect(config)
            .map_err(|error| pause_error(&error.to_string()))?;
        let exchanges = EXCHANGE_CONTRACTS
            .iter()
            .map(|contract| EvmAddress::parse(format!("{:#x}", contract.address)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| pause_error(&error.to_string()))?;
        Ok(Self {
            reader,
            executor,
            repository,
            exchanges,
        })
    }

    pub async fn pause_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        funder: &EvmAddress,
    ) -> QuantResult<()> {
        for submission in self
            .repository
            .recoverable(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Pause,
            )
            .await?
        {
            self.dispatch(&submission).await?;
        }
        let existing = self
            .repository
            .for_incident(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Pause,
            )
            .await?
            .into_iter()
            .map(|submission| submission.exchange_address)
            .collect::<BTreeSet<_>>();
        for exchange in &self.exchanges {
            if existing.contains(exchange) {
                continue;
            }
            let call = match self.reader.prepare_pause(exchange, funder).await {
                Ok(call) => call,
                Err(UserPauseError::AlreadyPaused { .. }) => continue,
                Err(error) => return Err(pause_error(&error.to_string()).into()),
            };
            let stored = self
                .prepare_operation(PrepareOperationInput {
                    incident,
                    exchange,
                    operation_kind: AccountPauseOperationKind::Pause,
                    requested_block: call.requested_block,
                    interval_blocks: Some(call.interval_blocks),
                    effective_block: Some(call.effective_block),
                    call: &call,
                })
                .await?;
            self.dispatch(&stored).await?;
        }
        Ok(())
    }

    pub async fn confirm_pause(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        funder: &EvmAddress,
    ) -> QuantResult<bool> {
        let submissions = self
            .repository
            .for_incident(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Pause,
            )
            .await?;
        if submissions.len() != self.exchanges.len() {
            return Ok(false);
        }
        for submission in submissions {
            if submission.state == AccountPauseOperationState::Confirmed {
                continue;
            }
            let state = self
                .reader
                .state(&submission.exchange_address, funder)
                .await
                .map_err(|error| pause_error(&error.to_string()))?;
            if !state.active
                || state.current_block
                    < u64::try_from(
                        submission
                            .effective_block
                            .ok_or_else(|| pause_error("pause operation has no effective block"))?,
                    )
                    .map_err(|error| pause_error(&error.to_string()))?
            {
                return Ok(false);
            }
            let Some(event) = self
                .reader
                .pause_event(
                    &submission.exchange_address,
                    funder,
                    u64::try_from(submission.requested_block)
                        .map_err(|error| pause_error(&error.to_string()))?,
                )
                .await
                .map_err(|error| pause_error(&error.to_string()))?
            else {
                return Ok(false);
            };
            if event.effective_block
                != u64::try_from(
                    submission
                        .effective_block
                        .ok_or_else(|| pause_error("pause operation has no effective block"))?,
                )
                .map_err(|error| pause_error(&error.to_string()))?
            {
                return Err(pause_error(
                    "UserPaused event effective block differs from prepared call",
                )
                .into());
            }
            self.repository
                .confirm(
                    &submission.account_pause_operation_id,
                    AccountPauseConfirmation {
                        block_number: to_i64(event.block_number, "confirmation_block_number")?,
                        block_hash: event.block_hash,
                        transaction_hash: event.transaction_hash,
                        log_index: to_i64(event.log_index, "confirmation_log_index")?,
                        confirmed_at: Utc::now(),
                    },
                )
                .await?;
        }
        Ok(true)
    }

    pub async fn unpause_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        funder: &EvmAddress,
    ) -> QuantResult<()> {
        if incident.seal_hash.is_none() {
            return Err(pause_error("incident must be sealed before unpause").into());
        }
        for operation in self
            .repository
            .recoverable(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Unpause,
            )
            .await?
        {
            self.dispatch(&operation).await?;
        }
        let existing = self
            .repository
            .for_incident(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Unpause,
            )
            .await?
            .into_iter()
            .map(|operation| operation.exchange_address)
            .collect::<BTreeSet<_>>();
        for exchange in &self.exchanges {
            if existing.contains(exchange) {
                continue;
            }
            let call = self
                .reader
                .prepare_unpause(exchange, funder)
                .await
                .map_err(|error| pause_error(&error.to_string()))?;
            let stored = self
                .prepare_operation(PrepareOperationInput {
                    incident,
                    exchange,
                    operation_kind: AccountPauseOperationKind::Unpause,
                    requested_block: call.requested_block,
                    interval_blocks: None,
                    effective_block: None,
                    call: &call,
                })
                .await?;
            self.dispatch(&stored).await?;
        }
        Ok(())
    }

    pub async fn confirm_unpause(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        funder: &EvmAddress,
    ) -> QuantResult<bool> {
        let operations = self
            .repository
            .for_incident(
                &incident.account_recovery_incident_id,
                AccountPauseOperationKind::Unpause,
            )
            .await?;
        if operations.len() != self.exchanges.len() {
            return Ok(false);
        }
        for operation in operations {
            if operation.state == AccountPauseOperationState::Confirmed {
                continue;
            }
            let state = self
                .reader
                .state(&operation.exchange_address, funder)
                .await
                .map_err(|error| pause_error(&error.to_string()))?;
            if state.active || state.effective_block.is_some() {
                return Ok(false);
            }
            let Some(event) = self
                .reader
                .unpause_event(
                    &operation.exchange_address,
                    funder,
                    u64::try_from(operation.requested_block)
                        .map_err(|error| pause_error(&error.to_string()))?,
                )
                .await
                .map_err(|error| pause_error(&error.to_string()))?
            else {
                return Ok(false);
            };
            self.repository
                .confirm(
                    &operation.account_pause_operation_id,
                    AccountPauseConfirmation {
                        block_number: to_i64(event.block_number, "confirmation_block_number")?,
                        block_hash: event.block_hash,
                        transaction_hash: event.transaction_hash,
                        log_index: to_i64(event.log_index, "confirmation_log_index")?,
                        confirmed_at: Utc::now(),
                    },
                )
                .await?;
        }
        Ok(true)
    }

    async fn prepare_operation<C: PreparedWalletCall>(
        &self,
        input: PrepareOperationInput<'_, C>,
    ) -> QuantResult<AccountPauseOperationInfo> {
        let PrepareOperationInput {
            incident,
            exchange,
            operation_kind,
            requested_block,
            interval_blocks,
            effective_block,
            call,
        } = input;
        let envelope = self
            .executor
            .prepare_envelope(call)
            .await
            .map_err(|error| pause_error(&error.to_string()))?;
        let identity_hash = CanonicalDigest::content_hash_typed(
            PAUSE_ID_DOMAIN,
            PAUSE_ID_VERSION,
            &(
                incident.account_recovery_incident_id,
                exchange,
                operation_kind,
                envelope.envelope_hash,
            ),
        )
        .map_err(|error| pause_error(&error.to_string()))?;
        self.repository
            .insert_prepared(NewAccountPauseOperation {
                account_pause_operation_id: AccountPauseOperationId::from_content_hash(
                    &identity_hash,
                ),
                recovery_incident_id: incident.account_recovery_incident_id,
                exchange_address: exchange.clone(),
                operation_kind,
                state: AccountPauseOperationState::Prepared,
                submission_kind: envelope.kind,
                requested_block: to_i64(requested_block, "requested_block")?,
                interval_blocks: interval_blocks
                    .map(|value| to_i64(value, "interval_blocks"))
                    .transpose()?,
                effective_block: effective_block
                    .map(|value| to_i64(value, "effective_block"))
                    .transpose()?,
                prepared_block_number: to_i64(
                    envelope.prepared_block.number,
                    "prepared_block_number",
                )?,
                prepared_block_hash: envelope.prepared_block.hash.clone(),
                prepared_nonce: envelope.nonce.clone(),
                gas_limit: envelope.gas_limit.clone(),
                calldata_hash: call.calldata_hash().clone(),
                deployment_digest: call.deployment_digest(),
                signed_envelope: envelope.envelope.clone(),
                signed_envelope_hash: envelope.envelope_hash,
                transaction_hash: envelope.transaction_hash.clone(),
            })
            .await
            .map_err(Into::into)
    }

    async fn dispatch(&self, submission: &AccountPauseOperationInfo) -> QuantResult<()> {
        let envelope = EnvelopeFields {
            kind: submission.submission_kind,
            prepared_block: EoaPreparedBlock {
                number: u64::try_from(submission.prepared_block_number)
                    .map_err(|error| pause_error(&error.to_string()))?,
                hash: submission.prepared_block_hash.clone(),
            },
            nonce: submission.prepared_nonce.clone(),
            gas_limit: submission.gas_limit.clone(),
            envelope: submission.signed_envelope.clone(),
            envelope_hash: submission.signed_envelope_hash,
            transaction_hash: submission.transaction_hash.clone(),
        };
        let dispatch = match self
            .executor
            .dispatch_envelope(&envelope)
            .await
            .map_err(|error| pause_error(&error.to_string()))?
        {
            WalletEnvelopeDispatch::EoaAccepted => AccountPauseDispatch::EoaAccepted,
            WalletEnvelopeDispatch::RelayerAccepted(id) => {
                AccountPauseDispatch::RelayerAccepted(id)
            }
            WalletEnvelopeDispatch::Ambiguous => AccountPauseDispatch::Ambiguous,
        };
        self.repository
            .record_dispatch(&submission.account_pause_operation_id, dispatch, Utc::now())
            .await?;
        Ok(())
    }
}

fn to_i64(value: u64, field: &'static str) -> QuantResult<i64> {
    i64::try_from(value).map_err(|error| pause_error(&format!("{field} overflow: {error}")).into())
}

fn pause_error(reason: &str) -> ExecutionError {
    ExecutionError::AccountChainProjection {
        reason: format!("account pause: {reason}"),
    }
}
