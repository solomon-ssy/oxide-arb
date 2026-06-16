//! CTF redemption client (Polygon on-chain).
//!
//! Contract addresses are compiled-in chain facts (`oxide_arb_models::constants`);
//! execution uses the immutable [`ResolvedRedeemPlan`] carried on each
//! [`RedeemRequest`] (snapshotted at fill time). The routing policy itself is
//! hot-reloadable through [`CtfRedeemClient::stage_reload`] for pre-trade
//! gates and new position snapshots.

use crate::{
    ctf::types::{RedeemOutcome, RedeemRequest},
    keystore::OrderSigner,
};
use alloy::{
    network::EthereumWallet,
    primitives::{Address, FixedBytes, U256},
    providers::ProviderBuilder,
    signers::Signer,
    sol,
};
use arc_swap::ArcSwap;
use oxide_arb_error::redeem::{RedeemError, RedeemSendError};
use oxide_arb_models::{
    constants::{
        CTF_ADDRESS, CTF_COLLATERAL_ADAPTER_ADDRESS, NEG_RISK_ADAPTER_ADDRESS,
        NEG_RISK_COLLATERAL_ADAPTER_ADDRESS, POLYGON_CHAIN_ID, USDC_E_ADDRESS, USDC_SCALE,
    },
    enums::common::{ExecutionMode, ResolvedRedeemRoute},
    runtime_config::{RedeemRoutingPolicy, ResolvedRedeemPlan},
    types::{Shares, TokenId},
};
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc};

sol! {
    #[sol(rpc)]
    interface IConditionalTokensRedeemer {
        function balanceOf(address owner, uint256 id) external view returns (uint256);
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;
    }

    #[sol(rpc)]
    interface INegRiskAdapter {
        function redeemPositions(bytes32 conditionId, uint256[] amounts) external;
    }
}

struct RedeemState {
    policy: RedeemRoutingPolicy,
}

impl RedeemState {
    fn validate_policy(policy: RedeemRoutingPolicy) -> Result<Self, RedeemError> {
        if let Some(standard) = &policy.standard {
            validate_optional_holder(standard.holder_address.as_deref())?;
        }
        if let Some(neg_risk) = &policy.neg_risk {
            validate_optional_holder(neg_risk.holder_address.as_deref())?;
        }
        for override_policy in policy.overrides.values() {
            validate_optional_holder(override_policy.holder_address())?;
        }
        Ok(Self { policy })
    }
}

fn validate_optional_holder(holder: Option<&str>) -> Result<(), RedeemError> {
    if let Some(address) = holder {
        Address::from_str(address).map_err(|e| RedeemError::InvalidAddress {
            value: address.to_owned(),
            reason: e.to_string(),
        })?;
    }
    Ok(())
}

fn resolve_holder(signer: &OrderSigner, plan: &ResolvedRedeemPlan) -> Result<Address, RedeemError> {
    plan.holder_address.as_deref().map_or_else(
        || Ok(signer.address()),
        |address| {
            Address::from_str(address).map_err(|e| RedeemError::InvalidAddress {
                value: address.to_owned(),
                reason: e.to_string(),
            })
        },
    )
}

pub struct CtfRedeemClient {
    signer: Arc<OrderSigner>,
    rpc_url: String,
    redeem: ArcSwap<RedeemState>,
    chain_id: u64,
}

/// A staged routing-policy reload, published atomically on commit.
#[must_use = "a staged reload has no effect until committed"]
pub struct StagedRedeemReload<'a> {
    client: &'a CtfRedeemClient,
    state: Arc<RedeemState>,
}

impl StagedRedeemReload<'_> {
    /// Publish the staged policy (infallible).
    pub fn commit(self) {
        self.client.redeem.store(self.state);
    }
}

impl CtfRedeemClient {
    pub fn new(
        signer: Arc<OrderSigner>,
        rpc_url: String,
        policy: RedeemRoutingPolicy,
        chain_id: u64,
    ) -> Result<Self, RedeemError> {
        let state = RedeemState::validate_policy(policy)?;
        Ok(Self {
            signer,
            rpc_url,
            redeem: ArcSwap::from_pointee(state),
            chain_id,
        })
    }

    /// Current routing policy (for pre-trade gates and diagnostics).
    #[must_use]
    pub fn policy(&self) -> RedeemRoutingPolicy {
        self.redeem.load_full().policy.clone()
    }

    /// Stage a hot-reload of the redeem routing policy.
    pub fn stage_reload(
        &self,
        policy: RedeemRoutingPolicy,
    ) -> Result<StagedRedeemReload<'_>, RedeemError> {
        let state = RedeemState::validate_policy(policy)?;
        Ok(StagedRedeemReload {
            client: self,
            state: Arc::new(state),
        })
    }

    pub async fn redeem(&self, req: &RedeemRequest) -> Result<RedeemOutcome, RedeemError> {
        match req.execution_mode {
            ExecutionMode::DryRun => {
                tracing::info!(
                    condition_id = %req.condition_id,
                    route = %req.plan.route,
                    "dry-run CTF redeem skipped"
                );
                Ok(RedeemOutcome::dry_run())
            }
            ExecutionMode::Paper => {
                tracing::info!(
                    condition_id = %req.condition_id,
                    route = %req.plan.route,
                    "paper CTF redeem simulated"
                );
                Ok(RedeemOutcome::paper(&req.condition_id))
            }
            ExecutionMode::Live => self.redeem_live(req).await,
        }
    }

    /// Query CTF ERC-1155 token balance for a holder and convert base units to shares.
    pub async fn position_balance(
        &self,
        holder_address: &str,
        token_id: &TokenId,
    ) -> Result<Shares, RedeemError> {
        let holder =
            Address::from_str(holder_address).map_err(|e| RedeemError::InvalidAddress {
                value: holder_address.to_owned(),
                reason: e.to_string(),
            })?;
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let ctf_address = parse_constant_address(CTF_ADDRESS)?;
        let token =
            U256::from_str(token_id.as_str()).map_err(|e| RedeemError::InvalidConditionId {
                value: token_id.to_string(),
                reason: e.to_string(),
            })?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);
        let ctf = IConditionalTokensRedeemer::new(ctf_address, &provider);
        let raw = ctf
            .balanceOf(holder, token)
            .call()
            .await
            .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?;
        let value = Decimal::from_str(&raw.to_string())
            .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?
            / Decimal::from(USDC_SCALE);
        Ok(Shares::new(value))
    }

    async fn redeem_live(&self, req: &RedeemRequest) -> Result<RedeemOutcome, RedeemError> {
        let plan = &req.plan;
        if plan.route.expects_neg_risk() != plan.neg_risk {
            return Err(RedeemError::UnsupportedRoute {
                route: plan.route.to_string(),
                reason: format!(
                    "snapshotted route does not match neg_risk={}",
                    plan.neg_risk
                ),
            });
        }

        let holder_address = resolve_holder(&self.signer, plan)?;
        let signer_address = self.signer.address();
        if signer_address != holder_address {
            return Err(RedeemError::WrongHolder {
                holder: holder_address.to_checksum(None),
                signer: signer_address.to_checksum(None),
            });
        }

        match plan.route {
            ResolvedRedeemRoute::StandardCtf
            | ResolvedRedeemRoute::CtfCollateralAdapter
            | ResolvedRedeemRoute::NegRiskCollateralAdapter => {
                self.redeem_standard(req, plan, holder_address).await
            }
            ResolvedRedeemRoute::NegRiskLegacyAdapter => {
                self.redeem_neg_risk_legacy(req, plan, holder_address).await
            }
        }
    }

    async fn redeem_standard(
        &self,
        req: &RedeemRequest,
        plan: &ResolvedRedeemPlan,
        holder_address: Address,
    ) -> Result<RedeemOutcome, RedeemError> {
        let _ = holder_address;
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let target = match plan.route {
            ResolvedRedeemRoute::CtfCollateralAdapter => CTF_COLLATERAL_ADAPTER_ADDRESS,
            ResolvedRedeemRoute::NegRiskCollateralAdapter => NEG_RISK_COLLATERAL_ADAPTER_ADDRESS,
            _ => CTF_ADDRESS,
        };
        let ctf_address = parse_constant_address(target)?;
        let collateral = parse_constant_address(USDC_E_ADDRESS)?;
        let condition_id = FixedBytes::<32>::from_str(req.condition_id.as_str()).map_err(|e| {
            RedeemError::InvalidConditionId {
                value: req.condition_id.to_string(),
                reason: e.to_string(),
            }
        })?;

        let mut signer = self.signer.inner().clone();
        signer.set_chain_id(Some(self.chain_id.max(POLYGON_CHAIN_ID)));
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);
        let ctf = IConditionalTokensRedeemer::new(ctf_address, &provider);
        let index_sets = vec![U256::from(1_u64), U256::from(2_u64)];

        let pending = ctf
            .redeemPositions(collateral, FixedBytes::<32>::ZERO, condition_id, index_sets)
            .gas(plan.gas_limit)
            .send()
            .await
            .map_err(|e| RedeemError::from(RedeemSendError::from_display(e)))?;
        let tx_hash = pending.tx_hash().to_string();
        let receipt = pending
            .get_receipt()
            .await
            .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?;

        if !receipt.status() {
            return Err(RedeemError::TransactionFailed {
                tx_hash: Some(tx_hash),
                reason: "receipt status false".into(),
            });
        }

        Ok(RedeemOutcome::live(tx_hash))
    }

    async fn redeem_neg_risk_legacy(
        &self,
        req: &RedeemRequest,
        plan: &ResolvedRedeemPlan,
        holder_address: Address,
    ) -> Result<RedeemOutcome, RedeemError> {
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let ctf_address = parse_constant_address(CTF_ADDRESS)?;
        let adapter_address = parse_constant_address(NEG_RISK_ADAPTER_ADDRESS)?;
        let condition_id = FixedBytes::<32>::from_str(req.condition_id.as_str()).map_err(|e| {
            RedeemError::InvalidConditionId {
                value: req.condition_id.to_string(),
                reason: e.to_string(),
            }
        })?;

        let mut signer = self.signer.inner().clone();
        signer.set_chain_id(Some(self.chain_id.max(POLYGON_CHAIN_ID)));
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);
        let ctf = IConditionalTokensRedeemer::new(ctf_address, &provider);
        let yes = U256::from_str(req.yes_token_id.as_str()).map_err(|e| {
            RedeemError::InvalidConditionId {
                value: req.yes_token_id.to_string(),
                reason: e.to_string(),
            }
        })?;
        let no = U256::from_str(req.no_token_id.as_str()).map_err(|e| {
            RedeemError::InvalidConditionId {
                value: req.no_token_id.to_string(),
                reason: e.to_string(),
            }
        })?;
        let amounts = vec![
            ctf.balanceOf(holder_address, yes)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
            ctf.balanceOf(holder_address, no)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
        ];
        let adapter = INegRiskAdapter::new(adapter_address, &provider);
        let pending = adapter
            .redeemPositions(condition_id, amounts)
            .gas(plan.gas_limit)
            .send()
            .await
            .map_err(|e| RedeemError::from(RedeemSendError::from_display(e)))?;
        let tx_hash = pending.tx_hash().to_string();
        let receipt = pending
            .get_receipt()
            .await
            .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?;
        if !receipt.status() {
            return Err(RedeemError::TransactionFailed {
                tx_hash: Some(tx_hash),
                reason: "receipt status false".into(),
            });
        }
        Ok(RedeemOutcome::live(tx_hash))
    }
}

fn parse_constant_address(value: &str) -> Result<Address, RedeemError> {
    Address::from_str(value).map_err(|e| RedeemError::InvalidAddress {
        value: value.to_owned(),
        reason: e.to_string(),
    })
}
