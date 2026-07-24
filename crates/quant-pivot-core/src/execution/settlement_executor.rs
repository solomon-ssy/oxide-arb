//! Production settlement capability, envelope, transport, and confirmation boundary.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_api::{
    keystore::OrderSigner,
    settlement::{
        adapter::{
            AlloySettlementAdapterReader, PreparedSettlementCall, SettlementAdapterError,
            SettlementAdapterGateway, SettlementBinaryTokenPair,
        },
        confirmation::{
            AlloySettlementConfirmationReader, SettlementConfirmationPollOutcome,
            SettlementConfirmationReadError, SettlementOperatorApprovalPollOutcome,
            poll_operator_approval_confirmation, poll_settlement_confirmation,
        },
        contracts::{
            AlloySettlementChainReader, ContractDeploymentVerifier,
            SettlementCredentialAvailability, VerifiedSettlementDeployment,
        },
        eoa::{
            AlloyEoaSettlementRpc, DurableEoaEnvelope, EoaPreparedBlock,
            EoaSettlementEnvelopeBuilder, EoaSettlementError, PreparedEoaEnvelope,
        },
        relayer::{
            AlloyRelayerPreparationRpc, DurableRelayerEnvelope, PreparedRelayerEnvelope,
            RelayerError, RelayerPollOutcome, RelayerRequestBuilder, RelayerTransport,
        },
    },
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::PolymarketConfig,
    domain::quant::{
        settlement::{
            NewSettlementChainSubmission, SettlementChainSubmissionInfo, SettlementRedeemInfo,
        },
        settlement_governance::SettlementGovernedActionInfo,
    },
    enums::{
        quant::ExecutionWalletKind,
        settlement::{
            SettlementFailureCode, SettlementSubmissionKind, SettlementSubmissionPurpose,
            SettlementSubmissionState,
        },
    },
    types::{
        ContentHash, EvmAddress, EvmTransactionHash, EvmUint256, SettlementChainSubmissionId,
        settlement_payload::{SettlementChainReceiptEvidence, SettlementFailureHistory},
    },
};

use super::{
    settlement_governed_action_service::{
        SettlementGovernedActionExecutor, SettlementGovernedActionTrackingResult,
    },
    settlement_service::{
        SettlementDispatchResult, SettlementExecutorError, SettlementSubmissionExecutor,
        SettlementTrackingResult,
    },
};

/// The sole production implementation of [`SettlementSubmissionExecutor`].
///
/// New envelopes require a fresh current-deployment capability and same-block
/// simulation; recovery can only replay or poll exact durable bytes.
pub struct ProductionSettlementExecutor {
    verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    adapter_reader: AlloySettlementAdapterReader,
    eoa_rpc: AlloyEoaSettlementRpc,
    relayer_rpc: AlloyRelayerPreparationRpc,
    relayer: Option<RelayerTransport>,
    confirmation_reader: AlloySettlementConfirmationReader,
    signer: Arc<OrderSigner>,
    topology: WalletTopology,
    credentials: SettlementCredentialAvailability,
}

impl ProductionSettlementExecutor {
    /// Build every bounded client at boot. Contract-wallet topology without
    /// valid relayer credentials fails startup instead of silently degrading.
    pub fn connect(
        config: &PolymarketConfig,
        verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
        signer: Arc<OrderSigner>,
        topology: WalletTopology,
        credentials: SettlementCredentialAvailability,
    ) -> QuantResult<Self> {
        let adapter_reader = AlloySettlementAdapterReader::connect(&config.onchain)
            .map_err(|source| QuantError::config(source.to_string()))?;
        let eoa_rpc = AlloyEoaSettlementRpc::connect(&config.onchain)
            .map_err(|source| QuantError::config(source.to_string()))?;
        let relayer_rpc = AlloyRelayerPreparationRpc::connect(&config.onchain)
            .map_err(|source| QuantError::config(source.to_string()))?;
        let confirmation_reader = AlloySettlementConfirmationReader::connect(&config.onchain)
            .map_err(|source| QuantError::config(source.to_string()))?;
        let relayer = if topology.kind == ExecutionWalletKind::Eoa {
            None
        } else {
            Some(
                RelayerTransport::connect(&config.relayer, &topology)
                    .map_err(|source| QuantError::config(source.to_string()))?,
            )
        };
        Ok(Self {
            verifier,
            adapter_reader,
            eoa_rpc,
            relayer_rpc,
            relayer,
            confirmation_reader,
            signer,
            topology,
            credentials,
        })
    }

    async fn prepare_call(
        &self,
        redeem: &SettlementRedeemInfo,
    ) -> Result<PreparedSettlementCall, SettlementExecutorError> {
        let deployment = self
            .verifier
            .verify(redeem.route, &self.topology, self.credentials, Utc::now())
            .await
            .map_err(|readiness| SettlementExecutorError::Terminal {
                failure_code: SettlementFailureCode::RouteNotReady,
                detail: format!("fresh deployment verification blocked: {readiness:?}"),
            })?;
        verify_deployment_scope(redeem, &deployment)?;
        let route = self
            .adapter_reader
            .verify_redeem_route(
                &deployment,
                &redeem.market_id,
                &SettlementBinaryTokenPair {
                    yes: redeem.yes_token_id.clone(),
                    no: redeem.no_token_id.clone(),
                },
            )
            .await
            .map_err(classify_adapter_error)?;
        if redeem.payout_vector_json != route.preflight().payout_vector
            || redeem.balance_before_json.as_ref() != Some(&route.preflight().balances)
        {
            return Err(SettlementExecutorError::Terminal {
                failure_code: SettlementFailureCode::BalanceMismatch,
                detail: "fresh payout or balance preflight differs from the authorized case"
                    .to_owned(),
            });
        }
        Ok(SettlementAdapterGateway.prepare_redeem(&route))
    }

    async fn prepare_action_call(
        &self,
        action: &SettlementGovernedActionInfo,
    ) -> Result<PreparedSettlementCall, SettlementExecutorError> {
        let route = action
            .route
            .ok_or_else(|| corrupt("governed action has no settlement route"))?;
        let deployment = self
            .verifier
            .verify(route, &self.topology, self.credentials, Utc::now())
            .await
            .map_err(|readiness| SettlementExecutorError::Terminal {
                failure_code: SettlementFailureCode::RouteNotReady,
                detail: format!(
                    "fresh governed-action deployment verification blocked: {readiness:?}"
                ),
            })?;
        verify_action_scope(action, &deployment)?;
        match action.desired_approval {
            Some(true) => SettlementAdapterGateway
                .prepare_operator_approval(&deployment)
                .map_err(classify_adapter_error),
            Some(false) => SettlementAdapterGateway
                .prepare_operator_revocation(&deployment)
                .map_err(classify_adapter_error),
            None => Err(corrupt(
                "transport governed action has no desired operator-approval state",
            )),
        }
    }

    async fn prepare_envelope(
        &self,
        call: &PreparedSettlementCall,
    ) -> Result<EnvelopeFields, SettlementExecutorError> {
        match self.topology.kind {
            ExecutionWalletKind::Eoa => {
                let prepared = EoaSettlementEnvelopeBuilder
                    .prepare(&self.eoa_rpc, &self.signer, &self.topology, call)
                    .await
                    .map_err(classify_eoa_prepare_error)?;
                Ok(EnvelopeFields {
                    kind: SettlementSubmissionKind::DirectEoa,
                    prepared_block: prepared.prepared_block().clone(),
                    nonce: prepared.nonce().clone(),
                    gas_limit: Some(prepared.gas_limit().clone()),
                    envelope: prepared.signed_envelope().to_vec(),
                    envelope_hash: prepared.signed_envelope_hash(),
                    transaction_hash: Some(prepared.transaction_hash().clone()),
                })
            }
            ExecutionWalletKind::Proxy
            | ExecutionWalletKind::GnosisSafe
            | ExecutionWalletKind::DepositWallet => {
                let prepared = RelayerRequestBuilder
                    .prepare(
                        self.relayer()?,
                        &self.relayer_rpc,
                        &self.signer,
                        &self.topology,
                        call,
                    )
                    .await
                    .map_err(classify_relayer_prepare_error)?;
                Ok(EnvelopeFields {
                    kind: SettlementSubmissionKind::Relayer,
                    prepared_block: prepared.prepared_block().clone(),
                    nonce: prepared.nonce().clone(),
                    gas_limit: prepared.gas_limit().cloned(),
                    envelope: prepared.signed_envelope().to_vec(),
                    envelope_hash: prepared.signed_envelope_hash(),
                    transaction_hash: None,
                })
            }
        }
    }

    fn relayer(&self) -> Result<&RelayerTransport, SettlementExecutorError> {
        self.relayer
            .as_ref()
            .ok_or_else(|| corrupt("relayer topology has no boot-validated transport"))
    }

    fn restore_eoa(
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<PreparedEoaEnvelope, SettlementExecutorError> {
        PreparedEoaEnvelope::restore_durable(DurableEoaEnvelope {
            target_adapter: submission.target_adapter.clone(),
            call_target: submission.call_target.clone(),
            deployment_digest: submission.deployment_digest,
            calldata_hash: submission.calldata_hash.clone(),
            prepared_block: durable_prepared_block(submission)?,
            nonce: required_nonce(submission)?,
            gas_limit: submission
                .gas_limit
                .clone()
                .ok_or_else(|| corrupt("direct EOA submission has no gas limit"))?,
            signed_envelope: required_envelope(submission)?,
            signed_envelope_hash: required_envelope_hash(submission)?,
            transaction_hash: submission
                .transaction_hash
                .clone()
                .ok_or_else(|| corrupt("direct EOA submission has no local transaction hash"))?,
        })
        .map_err(|source| corrupt(source.to_string()))
    }

    fn restore_relayer(
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<PreparedRelayerEnvelope, SettlementExecutorError> {
        Self::restore_relayer_scope(
            redeem.wallet_kind,
            redeem.funder_address.clone(),
            submission,
        )
    }

    fn restore_relayer_scope(
        wallet_kind: ExecutionWalletKind,
        funder: EvmAddress,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<PreparedRelayerEnvelope, SettlementExecutorError> {
        PreparedRelayerEnvelope::restore_durable(DurableRelayerEnvelope {
            wallet_kind,
            funder,
            target_adapter: submission.target_adapter.clone(),
            call_target: submission.call_target.clone(),
            deployment_digest: submission.deployment_digest,
            calldata_hash: submission.calldata_hash.clone(),
            prepared_block: durable_prepared_block(submission)?,
            nonce: required_nonce(submission)?,
            gas_limit: submission.gas_limit.clone(),
            signed_envelope: required_envelope(submission)?,
            signed_envelope_hash: required_envelope_hash(submission)?,
        })
        .map_err(|source| corrupt(source.to_string()))
    }

    fn typed_funder(&self) -> Result<EvmAddress, SettlementExecutorError> {
        EvmAddress::parse(format!("{:#x}", self.topology.funder))
            .map_err(|source| corrupt(format!("invalid boot-verified funder: {source}")))
    }
}

#[async_trait::async_trait]
impl SettlementSubmissionExecutor for ProductionSettlementExecutor {
    async fn prepare(
        &self,
        redeem: &SettlementRedeemInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
        let call = self.prepare_call(redeem).await?;
        let attempt_ordinal = redeem
            .attempt_count
            .checked_add(1)
            .ok_or_else(|| corrupt("settlement attempt ordinal overflow"))?;
        let envelope = self.prepare_envelope(&call).await?;
        new_submission(redeem, &call, attempt_ordinal, envelope)
    }

    async fn dispatch(
        &self,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError> {
        match submission.kind {
            SettlementSubmissionKind::DirectEoa => {
                let prepared = Self::restore_eoa(submission)?;
                match EoaSettlementEnvelopeBuilder
                    .broadcast(&self.eoa_rpc, &prepared)
                    .await
                {
                    Ok(_) => Ok(SettlementDispatchResult::EoaAccepted),
                    Err(EoaSettlementError::AmbiguousBroadcast { .. }) => {
                        Ok(SettlementDispatchResult::Ambiguous)
                    }
                    Err(source) => Err(corrupt(source.to_string())),
                }
            }
            SettlementSubmissionKind::Relayer => {
                let envelope = required_envelope(submission)?;
                let envelope_hash = required_envelope_hash(submission)?;
                match self
                    .relayer()?
                    .submit_durable(&envelope, envelope_hash)
                    .await
                {
                    Ok(accepted) => Ok(SettlementDispatchResult::RelayerAccepted(
                        accepted.transaction_id,
                    )),
                    Err(RelayerError::AmbiguousSubmission { .. }) => {
                        Ok(SettlementDispatchResult::Ambiguous)
                    }
                    Err(RelayerError::SubmissionRejected { .. }) => {
                        Err(SettlementExecutorError::Terminal {
                            failure_code: SettlementFailureCode::SubmissionRejected,
                            detail: "relayer rejected the exact durable body".to_owned(),
                        })
                    }
                    Err(source) => Err(corrupt(source.to_string())),
                }
            }
            SettlementSubmissionKind::ExternallyObserved => Err(corrupt(
                "externally observed submission cannot be dispatched",
            )),
        }
    }

    async fn track(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementTrackingResult, SettlementExecutorError> {
        if submission.state == SettlementSubmissionState::AwaitingChainHash {
            let transaction_id = submission.relayer_transaction_id.as_ref().ok_or_else(|| {
                corrupt("AwaitingChainHash submission has no relayer transaction ID")
            })?;
            let prepared = Self::restore_relayer(redeem, submission)?;
            return match self
                .relayer()?
                .poll(transaction_id, &prepared)
                .await
                .map_err(classify_relayer_poll_error)?
            {
                RelayerPollOutcome::Pending { .. } => Ok(SettlementTrackingResult::Pending),
                RelayerPollOutcome::ChainHashObserved {
                    transaction_hash, ..
                } => Ok(SettlementTrackingResult::ChainHashObserved(
                    transaction_hash,
                )),
                RelayerPollOutcome::TerminalFailure { state } => {
                    Ok(SettlementTrackingResult::ReconciliationRequired {
                        failure_code: SettlementFailureCode::RelayerTerminalFailure,
                        detail: format!("relayer entered terminal state {state:?}"),
                    })
                }
            };
        }
        match poll_settlement_confirmation(
            &self.confirmation_reader,
            &self.topology,
            redeem,
            submission,
        )
        .await
        .map_err(classify_confirmation_read_error)?
        {
            SettlementConfirmationPollOutcome::PendingReceipt
            | SettlementConfirmationPollOutcome::PendingFinality => {
                Ok(SettlementTrackingResult::Pending)
            }
            SettlementConfirmationPollOutcome::Confirmed(confirmation) => {
                Ok(SettlementTrackingResult::Confirmed(confirmation))
            }
            SettlementConfirmationPollOutcome::ReconciliationRequired(error) => {
                Ok(SettlementTrackingResult::ReconciliationRequired {
                    failure_code: error.failure_code(),
                    detail: error.to_string(),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl SettlementGovernedActionExecutor for ProductionSettlementExecutor {
    async fn prepare_action(
        &self,
        action: &SettlementGovernedActionInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
        let call = self.prepare_action_call(action).await?;
        let envelope = self.prepare_envelope(&call).await?;
        new_governed_action_submission(action, &call, envelope)
    }

    async fn dispatch_action(
        &self,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError> {
        <Self as SettlementSubmissionExecutor>::dispatch(self, submission).await
    }

    async fn track_action(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementGovernedActionTrackingResult, SettlementExecutorError> {
        if submission.state == SettlementSubmissionState::AwaitingChainHash {
            let transaction_id = submission.relayer_transaction_id.as_ref().ok_or_else(|| {
                corrupt("governed action AwaitingChainHash has no relayer transaction ID")
            })?;
            let prepared =
                Self::restore_relayer_scope(self.topology.kind, self.typed_funder()?, submission)?;
            return match self
                .relayer()?
                .poll(transaction_id, &prepared)
                .await
                .map_err(classify_relayer_poll_error)?
            {
                RelayerPollOutcome::Pending { .. } => {
                    Ok(SettlementGovernedActionTrackingResult::Pending)
                }
                RelayerPollOutcome::ChainHashObserved {
                    transaction_hash, ..
                } => Ok(SettlementGovernedActionTrackingResult::ChainHashObserved(
                    transaction_hash,
                )),
                RelayerPollOutcome::TerminalFailure { state } => Ok(
                    SettlementGovernedActionTrackingResult::ReconciliationRequired {
                        failure_code: SettlementFailureCode::RelayerTerminalFailure,
                        detail: format!("relayer entered terminal state {state:?}"),
                    },
                ),
            };
        }
        match poll_operator_approval_confirmation(
            &self.confirmation_reader,
            &self.topology,
            action,
            submission,
        )
        .await
        .map_err(classify_confirmation_read_error)?
        {
            SettlementOperatorApprovalPollOutcome::PendingReceipt
            | SettlementOperatorApprovalPollOutcome::PendingFinality => {
                Ok(SettlementGovernedActionTrackingResult::Pending)
            }
            SettlementOperatorApprovalPollOutcome::Confirmed(evidence) => {
                Ok(SettlementGovernedActionTrackingResult::Confirmed(Box::new(
                    SettlementChainReceiptEvidence::OperatorApproval(evidence),
                )))
            }
            SettlementOperatorApprovalPollOutcome::ReconciliationRequired(error) => Ok(
                SettlementGovernedActionTrackingResult::ReconciliationRequired {
                    failure_code: error.failure_code(),
                    detail: error.to_string(),
                },
            ),
        }
    }
}

struct EnvelopeFields {
    kind: SettlementSubmissionKind,
    prepared_block: EoaPreparedBlock,
    nonce: EvmUint256,
    gas_limit: Option<EvmUint256>,
    envelope: Vec<u8>,
    envelope_hash: ContentHash,
    transaction_hash: Option<EvmTransactionHash>,
}

fn new_submission(
    redeem: &SettlementRedeemInfo,
    call: &PreparedSettlementCall,
    attempt_ordinal: i32,
    envelope: EnvelopeFields,
) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
    Ok(NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: Some(redeem.settlement_redeem_id),
        settlement_governed_action_id: None,
        canary_action_id: None,
        purpose: SettlementSubmissionPurpose::Redeem,
        kind: envelope.kind,
        state: SettlementSubmissionState::Prepared,
        route: call.route(),
        target_adapter: call.target_adapter().clone(),
        target_code_hash: call.target_code_hash().clone(),
        conditional_tokens: call.conditional_tokens().clone(),
        collateral_token: call.collateral_token().clone(),
        usdce: call.usdce().clone(),
        call_target: call.call_target().clone(),
        deployment_digest: call.deployment_digest(),
        deployment_evidence_version: call.deployment_evidence_version().clone(),
        verified_block_number: i64_value(call.verified_block_number(), "verified_block_number")?,
        verified_block_hash: call.verified_block_hash().clone(),
        prepared_block_number: Some(i64_value(
            envelope.prepared_block.number,
            "prepared_block_number",
        )?),
        prepared_block_hash: Some(envelope.prepared_block.hash),
        calldata_hash: call.calldata_hash().clone(),
        calldata: call.calldata().to_vec(),
        signed_envelope: Some(envelope.envelope),
        signed_envelope_hash: Some(envelope.envelope_hash),
        prepared_nonce: Some(envelope.nonce),
        gas_limit: envelope.gas_limit,
        relayer_transaction_id: None,
        transaction_hash: envelope.transaction_hash,
        failure_code: None,
        failure_history_json: SettlementFailureHistory::default(),
        receipt_evidence_json: None,
        attempt_ordinal,
        last_error: None,
        dispatched_at: None,
        chain_hash_observed_at: None,
        confirmed_at: None,
    })
}

fn new_governed_action_submission(
    action: &SettlementGovernedActionInfo,
    call: &PreparedSettlementCall,
    envelope: EnvelopeFields,
) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
    Ok(NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: None,
        settlement_governed_action_id: Some(action.settlement_governed_action_id),
        canary_action_id: None,
        purpose: call.purpose(),
        kind: envelope.kind,
        state: SettlementSubmissionState::Prepared,
        route: call.route(),
        target_adapter: call.target_adapter().clone(),
        target_code_hash: call.target_code_hash().clone(),
        conditional_tokens: call.conditional_tokens().clone(),
        collateral_token: call.collateral_token().clone(),
        usdce: call.usdce().clone(),
        call_target: call.call_target().clone(),
        deployment_digest: call.deployment_digest(),
        deployment_evidence_version: call.deployment_evidence_version().clone(),
        verified_block_number: i64_value(call.verified_block_number(), "verified_block_number")?,
        verified_block_hash: call.verified_block_hash().clone(),
        prepared_block_number: Some(i64_value(
            envelope.prepared_block.number,
            "prepared_block_number",
        )?),
        prepared_block_hash: Some(envelope.prepared_block.hash),
        calldata_hash: call.calldata_hash().clone(),
        calldata: call.calldata().to_vec(),
        signed_envelope: Some(envelope.envelope),
        signed_envelope_hash: Some(envelope.envelope_hash),
        prepared_nonce: Some(envelope.nonce),
        gas_limit: envelope.gas_limit,
        relayer_transaction_id: None,
        transaction_hash: envelope.transaction_hash,
        failure_code: None,
        failure_history_json: SettlementFailureHistory::default(),
        receipt_evidence_json: None,
        attempt_ordinal: 1,
        last_error: None,
        dispatched_at: None,
        chain_hash_observed_at: None,
        confirmed_at: None,
    })
}

fn verify_deployment_scope(
    redeem: &SettlementRedeemInfo,
    deployment: &VerifiedSettlementDeployment,
) -> Result<(), SettlementExecutorError> {
    if redeem.route != deployment.route()
        || redeem.wallet_kind != deployment.wallet_kind()
        || &redeem.funder_address != deployment.funder()
        || redeem.target_adapter.as_ref() != Some(deployment.target())
        || redeem.target_code_hash.as_ref() != Some(deployment.target_code_hash())
        || redeem.deployment_digest != Some(deployment.deployment_digest())
        || redeem.deployment_evidence_version.as_ref() != Some(deployment.evidence_version())
    {
        return Err(SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::DeploymentChanged,
            detail: "fresh deployment capability differs from the frozen case scope".to_owned(),
        });
    }
    Ok(())
}

fn verify_action_scope(
    action: &SettlementGovernedActionInfo,
    deployment: &VerifiedSettlementDeployment,
) -> Result<(), SettlementExecutorError> {
    let verified_block = i64::try_from(deployment.verified_block()).map_err(|source| {
        corrupt(format!(
            "verified deployment block exceeds bigint: {source}"
        ))
    })?;
    if action.route != Some(deployment.route())
        || action.target_adapter.as_ref() != Some(deployment.target())
        || action.deployment_digest != Some(deployment.deployment_digest())
        || action.deployment_evidence_version.as_ref() != Some(deployment.evidence_version())
        || action
            .verified_block_number
            .is_none_or(|block| verified_block < block)
    {
        return Err(SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::DeploymentChanged,
            detail: "fresh deployment capability differs from the governed action scope".to_owned(),
        });
    }
    Ok(())
}

fn durable_prepared_block(
    submission: &SettlementChainSubmissionInfo,
) -> Result<EoaPreparedBlock, SettlementExecutorError> {
    let number = submission
        .prepared_block_number
        .ok_or_else(|| corrupt("durable submission has no prepared block number"))?;
    Ok(EoaPreparedBlock {
        number: u64::try_from(number)
            .map_err(|source| corrupt(format!("invalid prepared block number: {source}")))?,
        hash: submission
            .prepared_block_hash
            .clone()
            .ok_or_else(|| corrupt("durable submission has no prepared block hash"))?,
    })
}

fn required_nonce(
    submission: &SettlementChainSubmissionInfo,
) -> Result<EvmUint256, SettlementExecutorError> {
    submission
        .prepared_nonce
        .clone()
        .ok_or_else(|| corrupt("durable submission has no prepared nonce"))
}

fn required_envelope(
    submission: &SettlementChainSubmissionInfo,
) -> Result<Vec<u8>, SettlementExecutorError> {
    submission
        .signed_envelope
        .clone()
        .ok_or_else(|| corrupt("durable submission has no signed envelope"))
}

fn required_envelope_hash(
    submission: &SettlementChainSubmissionInfo,
) -> Result<ContentHash, SettlementExecutorError> {
    submission
        .signed_envelope_hash
        .ok_or_else(|| corrupt("durable submission has no envelope hash"))
}

fn i64_value(value: u64, field: &'static str) -> Result<i64, SettlementExecutorError> {
    i64::try_from(value).map_err(|source| corrupt(format!("{field} exceeds bigint: {source}")))
}

fn classify_adapter_error(source: SettlementAdapterError) -> SettlementExecutorError {
    match source {
        SettlementAdapterError::RpcConnection { .. }
        | SettlementAdapterError::RpcCall { .. }
        | SettlementAdapterError::CanonicalBlockChanged { .. } => {
            SettlementExecutorError::Transient {
                stage: "redeem_preflight",
                detail: source.to_string(),
            }
        }
        SettlementAdapterError::SimulationReverted { .. } => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::SimulationReverted,
            detail: source.to_string(),
        },
        SettlementAdapterError::EmptyOutcomeBalances => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::BalanceMismatch,
            detail: source.to_string(),
        },
        SettlementAdapterError::MissingOperatorApproval
        | SettlementAdapterError::AdapterPaused
        | SettlementAdapterError::AdapterResidualUsdce { .. }
        | SettlementAdapterError::ConditionNotResolved
        | SettlementAdapterError::InvalidPayoutVector { .. } => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::RouteNotReady,
            detail: source.to_string(),
        },
        other => corrupt(other.to_string()),
    }
}

fn classify_eoa_prepare_error(source: EoaSettlementError) -> SettlementExecutorError {
    match source {
        EoaSettlementError::RpcCall { .. }
        | EoaSettlementError::MissingCanonicalHead
        | EoaSettlementError::CanonicalBlockChanged { .. } => SettlementExecutorError::Transient {
            stage: "eoa_prepare",
            detail: source.to_string(),
        },
        other => corrupt(other.to_string()),
    }
}

fn classify_relayer_prepare_error(source: RelayerError) -> SettlementExecutorError {
    match source {
        RelayerError::PreparationCall { .. }
        | RelayerError::CanonicalBlockChanged { .. }
        | RelayerError::TransportCall { .. } => SettlementExecutorError::Transient {
            stage: "relayer_prepare",
            detail: source.to_string(),
        },
        RelayerError::WalletNotReady { .. } => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::RouteNotReady,
            detail: source.to_string(),
        },
        other => corrupt(other.to_string()),
    }
}

fn classify_relayer_poll_error(source: RelayerError) -> SettlementExecutorError {
    match source {
        RelayerError::TransportCall { .. } => SettlementExecutorError::Transient {
            stage: "relayer_poll",
            detail: source.to_string(),
        },
        other => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::RelayerTerminalFailure,
            detail: other.to_string(),
        },
    }
}

fn classify_confirmation_read_error(
    source: SettlementConfirmationReadError,
) -> SettlementExecutorError {
    match source {
        SettlementConfirmationReadError::RpcCall { .. } => SettlementExecutorError::Transient {
            stage: "confirmation_poll",
            detail: source.to_string(),
        },
        other => SettlementExecutorError::Terminal {
            failure_code: SettlementFailureCode::ReceiptEvidenceMismatch,
            detail: other.to_string(),
        },
    }
}

fn corrupt(detail: impl Into<String>) -> SettlementExecutorError {
    SettlementExecutorError::Invariant {
        detail: detail.into(),
    }
}
