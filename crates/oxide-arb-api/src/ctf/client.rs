//! CTF redemption client (Polygon on-chain).
//!
//! Contract addresses are compiled-in chain facts (`oxide_arb_models::constants`);
//! only the redeem route and its parameters are configuration, and they are
//! hot-reloadable through [`CtfRedeemClient::stage_reload`] +
//! [`StagedRedeemReload::commit`] so an operator can enable or switch the
//! redeem route without a restart (guarded by the runtime-config activation
//! preflight).

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
        NEG_RISK_COLLATERAL_ADAPTER_ADDRESS, POLYGON_CHAIN_ID, USDC_E_ADDRESS,
    },
    enums::common::{ExecutionMode, RedeemRoute},
    runtime_config::SettlementRedeemConfig,
};
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

/// Redeem route configuration + the resolved holder address, swapped as one
/// unit so a reload can never expose a route with a stale holder.
struct RedeemState {
    config: SettlementRedeemConfig,
    holder_address: Address,
}

impl RedeemState {
    fn resolve(signer: &OrderSigner, config: SettlementRedeemConfig) -> Result<Self, RedeemError> {
        let holder_address = match config.holder_address.clone() {
            Some(address) => {
                Address::from_str(&address).map_err(|e| RedeemError::InvalidAddress {
                    value: address,
                    reason: e.to_string(),
                })?
            }
            None => signer.address(),
        };
        Ok(Self {
            config,
            holder_address,
        })
    }
}

pub struct CtfRedeemClient {
    signer: Arc<OrderSigner>,
    rpc_url: String,
    redeem: ArcSwap<RedeemState>,
    chain_id: u64,
}

/// A resolved next redeem route, staged but not yet visible.
///
/// Produced by [`CtfRedeemClient::stage_reload`]. Splitting the fallible
/// holder resolution from the infallible publish lets the runtime-config
/// applicator stage every fallible subscriber first and commit only when all
/// of them succeeded — an aborted activation never leaves the redeem route
/// partially reloaded.
#[must_use = "a staged reload has no effect until committed"]
pub struct StagedRedeemReload<'a> {
    client: &'a CtfRedeemClient,
    state: Arc<RedeemState>,
}

impl StagedRedeemReload<'_> {
    /// Publish the staged route + holder as one unit (infallible).
    pub fn commit(self) {
        self.client.redeem.store(self.state);
    }
}

impl CtfRedeemClient {
    pub fn new(
        signer: Arc<OrderSigner>,
        rpc_url: String,
        redeem: SettlementRedeemConfig,
        chain_id: u64,
    ) -> Result<Self, RedeemError> {
        let state = RedeemState::resolve(&signer, redeem)?;
        Ok(Self {
            signer,
            rpc_url,
            redeem: ArcSwap::from_pointee(state),
            chain_id,
        })
    }

    /// Stage a hot-reload of the redeem route (runtime-config activation).
    ///
    /// Re-resolves the holder address against the new config; an invalid
    /// address rejects the staging, leaving the previous route active.
    /// Nothing becomes visible until [`StagedRedeemReload::commit`].
    pub fn stage_reload(
        &self,
        redeem: SettlementRedeemConfig,
    ) -> Result<StagedRedeemReload<'_>, RedeemError> {
        let state = RedeemState::resolve(&self.signer, redeem)?;
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
                    "dry-run CTF redeem skipped"
                );
                Ok(RedeemOutcome::dry_run())
            }
            ExecutionMode::Paper => {
                tracing::info!(
                    condition_id = %req.condition_id,
                    "paper CTF redeem simulated"
                );
                Ok(RedeemOutcome::paper(&req.condition_id))
            }
            ExecutionMode::Live => self.redeem_live(req).await,
        }
    }

    async fn redeem_live(&self, req: &RedeemRequest) -> Result<RedeemOutcome, RedeemError> {
        let state = self.redeem.load_full();
        let signer_address = self.signer.address();
        if signer_address != state.holder_address && state.config.route != RedeemRoute::ProxySafe {
            return Err(RedeemError::WrongHolder {
                holder: state.holder_address.to_checksum(None),
                signer: signer_address.to_checksum(None),
            });
        }

        match state.config.route {
            RedeemRoute::Disabled => Err(RedeemError::UnsupportedRoute {
                route: state.config.route.to_string(),
                reason: "Live redeem route is disabled".into(),
            }),
            RedeemRoute::StandardCtf | RedeemRoute::CtfCollateralAdapter => {
                if req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: state.config.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_standard(req, &state).await
            }
            RedeemRoute::NegRiskLegacyAdapter => {
                if !req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: state.config.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_neg_risk_legacy(req, &state).await
            }
            RedeemRoute::NegRiskCollateralAdapter => {
                if !req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: state.config.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_standard(req, &state).await
            }
            RedeemRoute::ProxySafe => Err(RedeemError::UnsupportedRoute {
                route: state.config.route.to_string(),
                reason: "Proxy Safe execution requires Safe transaction support".into(),
            }),
        }
    }

    async fn redeem_standard(
        &self,
        req: &RedeemRequest,
        state: &RedeemState,
    ) -> Result<RedeemOutcome, RedeemError> {
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let target = match state.config.route {
            RedeemRoute::CtfCollateralAdapter => CTF_COLLATERAL_ADAPTER_ADDRESS,
            RedeemRoute::NegRiskCollateralAdapter => NEG_RISK_COLLATERAL_ADAPTER_ADDRESS,
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
            .gas(state.config.gas_limit)
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
        state: &RedeemState,
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
            ctf.balanceOf(state.holder_address, yes)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
            ctf.balanceOf(state.holder_address, no)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
        ];
        let adapter = INegRiskAdapter::new(adapter_address, &provider);
        let pending = adapter
            .redeemPositions(condition_id, amounts)
            .gas(state.config.gas_limit)
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

/// Parse a compiled-in contract address. The constants are verified Polygon
/// deployments, so a failure here indicates a build-time defect; it is still
/// surfaced as an error to keep the redeem path panic-free.
fn parse_constant_address(value: &str) -> Result<Address, RedeemError> {
    Address::from_str(value).map_err(|e| RedeemError::InvalidAddress {
        value: value.to_owned(),
        reason: e.to_string(),
    })
}
