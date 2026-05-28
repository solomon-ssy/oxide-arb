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
use oxide_arb_error::redeem::{RedeemError, RedeemSendError};
use oxide_arb_models::{
    config::settlement::{SettlementContractsSection, SettlementRedeemSection},
    constants::POLYGON_CHAIN_ID,
    enums::common::{ExecutionMode, RedeemRoute},
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

pub struct CtfRedeemClient {
    signer: Arc<OrderSigner>,
    rpc_url: String,
    contracts: SettlementContractsSection,
    redeem: SettlementRedeemSection,
    holder_address: Address,
    chain_id: u64,
}

impl CtfRedeemClient {
    pub fn new(
        signer: Arc<OrderSigner>,
        rpc_url: String,
        contracts: SettlementContractsSection,
        redeem: SettlementRedeemSection,
        chain_id: u64,
    ) -> Result<Self, RedeemError> {
        let holder_address = match redeem.holder_address.clone() {
            Some(address) => {
                Address::from_str(&address).map_err(|e| RedeemError::InvalidAddress {
                    value: address,
                    reason: e.to_string(),
                })?
            }
            None => signer.address(),
        };

        Ok(Self {
            signer,
            rpc_url,
            contracts,
            redeem,
            holder_address,
            chain_id,
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
        let signer_address = self.signer.address();
        if signer_address != self.holder_address && self.redeem.route != RedeemRoute::ProxySafe {
            return Err(RedeemError::WrongHolder {
                holder: self.holder_address.to_checksum(None),
                signer: signer_address.to_checksum(None),
            });
        }

        match self.redeem.route {
            RedeemRoute::Disabled => Err(RedeemError::UnsupportedRoute {
                route: self.redeem.route.to_string(),
                reason: "Live redeem route is disabled".into(),
            }),
            RedeemRoute::StandardCtf | RedeemRoute::CtfCollateralAdapter => {
                if req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: self.redeem.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_standard(req).await
            }
            RedeemRoute::NegRiskLegacyAdapter => {
                if !req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: self.redeem.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_neg_risk_legacy(req).await
            }
            RedeemRoute::NegRiskCollateralAdapter => {
                if !req.neg_risk {
                    return Err(RedeemError::UnsupportedRoute {
                        route: self.redeem.route.to_string(),
                        reason: format!("route does not match market.neg_risk={}", req.neg_risk),
                    });
                }
                self.redeem_standard(req).await
            }
            RedeemRoute::ProxySafe => Err(RedeemError::UnsupportedRoute {
                route: self.redeem.route.to_string(),
                reason: "Proxy Safe execution requires Safe transaction support".into(),
            }),
        }
    }

    async fn redeem_standard(&self, req: &RedeemRequest) -> Result<RedeemOutcome, RedeemError> {
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let target = match self.redeem.route {
            RedeemRoute::CtfCollateralAdapter => self
                .contracts
                .ctf_collateral_adapter
                .as_deref()
                .ok_or_else(|| RedeemError::UnsupportedRoute {
                route: "ctf_collateral_adapter".into(),
                reason: "required settlement contract address is not configured".into(),
            })?,
            RedeemRoute::NegRiskCollateralAdapter => self
                .contracts
                .neg_risk_collateral_adapter
                .as_deref()
                .ok_or_else(|| RedeemError::UnsupportedRoute {
                    route: "neg_risk_collateral_adapter".into(),
                    reason: "required settlement contract address is not configured".into(),
                })?,
            _ => self.contracts.ctf_address.as_str(),
        };
        let ctf_address = Address::from_str(target).map_err(|e| RedeemError::InvalidAddress {
            value: target.to_owned(),
            reason: e.to_string(),
        })?;
        let collateral = Address::from_str(&self.contracts.usdc_e_address).map_err(|e| {
            RedeemError::InvalidAddress {
                value: self.contracts.usdc_e_address.clone(),
                reason: e.to_string(),
            }
        })?;
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
            .gas(self.redeem.gas_limit)
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
    ) -> Result<RedeemOutcome, RedeemError> {
        let rpc_url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RedeemError::RpcTimeout(e.to_string()))?;
        let ctf_address = Address::from_str(&self.contracts.ctf_address).map_err(|e| {
            RedeemError::InvalidAddress {
                value: self.contracts.ctf_address.clone(),
                reason: e.to_string(),
            }
        })?;
        let adapter = self.contracts.neg_risk_adapter.as_deref().ok_or_else(|| {
            RedeemError::UnsupportedRoute {
                route: "neg_risk_adapter".into(),
                reason: "required settlement contract address is not configured".into(),
            }
        })?;
        let adapter_address =
            Address::from_str(adapter).map_err(|e| RedeemError::InvalidAddress {
                value: adapter.to_owned(),
                reason: e.to_string(),
            })?;
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
            ctf.balanceOf(self.holder_address, yes)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
            ctf.balanceOf(self.holder_address, no)
                .call()
                .await
                .map_err(|e| RedeemError::RpcTimeout(e.to_string()))?,
        ];
        let adapter = INegRiskAdapter::new(adapter_address, &provider);
        let pending = adapter
            .redeemPositions(condition_id, amounts)
            .gas(self.redeem.gas_limit)
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
