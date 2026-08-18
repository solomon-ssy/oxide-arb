//! Durable V2 account pause orchestration across every registered exchange.

use std::{collections::BTreeSet, sync::Arc};

use chrono::Utc;
use quant_pivot_api::{
    exchange::{
        constants::EXCHANGE_CONTRACTS,
        user_pause::{AlloyUserPauseReader, UserPauseError},
    },
    settlement::eoa::EoaPreparedBlock,
};
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::OnchainConfig,
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseSubmissionInfo,
        AccountRecoveryIncidentInfo, NewAccountPauseSubmission,
    },
    enums::execution::AccountPauseSubmissionState,
    hashing::CanonicalDigest,
    types::{AccountPauseSubmissionId, EvmAddress},
};
use quant_pivot_repository::traits::AccountPauseRepository;

use super::settlement_executor::{
    EnvelopeFields, ProductionSettlementExecutor, WalletEnvelopeDispatch,
};

const PAUSE_ID_DOMAIN: &str = "quant-pivot/account-pause-submission";
const PAUSE_ID_VERSION: u32 = 1;

pub struct AccountPauseCoordinator {
    reader: AlloyUserPauseReader,
    executor: Arc<ProductionSettlementExecutor>,
    repository: Arc<dyn AccountPauseRepository>,
    exchanges: Vec<EvmAddress>,
}

impl AccountPauseCoordinator {
    pub fn connect(
        config: &OnchainConfig,
        executor: Arc<ProductionSettlementExecutor>,
        repository: Arc<dyn AccountPauseRepository>,
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
            .recoverable(&incident.account_recovery_incident_id)
            .await?
        {
            self.dispatch(&submission).await?;
        }
        let existing = self
            .repository
            .for_incident(&incident.account_recovery_incident_id)
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
            let envelope = self
                .executor
                .prepare_envelope(&call)
                .await
                .map_err(|error| pause_error(&error.to_string()))?;
            let identity_hash = CanonicalDigest::content_hash_typed(
                PAUSE_ID_DOMAIN,
                PAUSE_ID_VERSION,
                &(
                    incident.account_recovery_incident_id,
                    exchange,
                    envelope.envelope_hash,
                ),
            )
            .map_err(|error| pause_error(&error.to_string()))?;
            let stored = self
                .repository
                .insert_prepared(NewAccountPauseSubmission {
                    account_pause_submission_id: AccountPauseSubmissionId::from_content_hash(
                        &identity_hash,
                    ),
                    recovery_incident_id: incident.account_recovery_incident_id,
                    exchange_address: exchange.clone(),
                    state: AccountPauseSubmissionState::Prepared,
                    kind: envelope.kind,
                    requested_block: to_i64(call.requested_block, "requested_block")?,
                    interval_blocks: to_i64(call.interval_blocks, "interval_blocks")?,
                    effective_block: to_i64(call.effective_block, "effective_block")?,
                    prepared_block_number: to_i64(
                        envelope.prepared_block.number,
                        "prepared_block_number",
                    )?,
                    prepared_block_hash: envelope.prepared_block.hash.clone(),
                    prepared_nonce: envelope.nonce.clone(),
                    gas_limit: envelope.gas_limit.clone(),
                    calldata_hash: call.calldata_hash,
                    deployment_digest: call.deployment_digest,
                    signed_envelope: envelope.envelope.clone(),
                    signed_envelope_hash: envelope.envelope_hash,
                    transaction_hash: envelope.transaction_hash.clone(),
                })
                .await?;
            self.dispatch(&stored).await?;
        }
        Ok(())
    }

    pub async fn confirm_incident(
        &self,
        incident: &AccountRecoveryIncidentInfo,
        funder: &EvmAddress,
    ) -> QuantResult<bool> {
        let submissions = self
            .repository
            .for_incident(&incident.account_recovery_incident_id)
            .await?;
        if submissions.len() != self.exchanges.len() {
            return Ok(false);
        }
        for submission in submissions {
            if submission.state == AccountPauseSubmissionState::Confirmed {
                continue;
            }
            let state = self
                .reader
                .state(&submission.exchange_address, funder)
                .await
                .map_err(|error| pause_error(&error.to_string()))?;
            if !state.active
                || state.current_block
                    < u64::try_from(submission.effective_block)
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
                != u64::try_from(submission.effective_block)
                    .map_err(|error| pause_error(&error.to_string()))?
            {
                return Err(pause_error(
                    "UserPaused event effective block differs from prepared call",
                )
                .into());
            }
            self.repository
                .confirm(
                    &submission.account_pause_submission_id,
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

    async fn dispatch(&self, submission: &AccountPauseSubmissionInfo) -> QuantResult<()> {
        let envelope = EnvelopeFields {
            kind: submission.kind,
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
            .record_dispatch(
                &submission.account_pause_submission_id,
                dispatch,
                Utc::now(),
            )
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
