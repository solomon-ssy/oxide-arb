//! On-chain wallet-ownership verification for Polymarket funders.
//!
//! Polymarket derives Proxy / Gnosis Safe funder addresses via CREATE2, but the
//! factory + init-code constants pinned in the SDK cannot reproduce every wallet
//! generation Polymarket has deployed (factories are versioned). When the
//! deterministic derivation does not match the configured funder, this client is
//! the authoritative fallback: read the funder's on-chain controller and confirm
//! it is the bot signer.
//!
//! - **Proxy** exposes `owner` — the controlling EOA.
//! - **Gnosis Safe** exposes `isOwner(address)` — its (1-of-1) owner set.

use std::time::Duration;

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    sol,
    transports::http::Http,
};
use async_trait::async_trait;
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::{config::PolymarketConfig, enums::quant::ExecutionWalletKind};
use reqwest::{Client, Url};

sol! {
    #[sol(rpc)]
    interface PolymarketProxyWallet {
        function owner() external view returns (address);
    }
}

sol! {
    #[sol(rpc)]
    interface DepositWalletView {
        function owner() external view returns (address);
        function sessionSignerAuthorizedUntil(address signer) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface GnosisSafeWallet {
        function isOwner(address owner) external view returns (bool);
    }
}

/// Confirms that a Polymarket funder wallet is controlled by the bot signer.
///
/// Abstracted as a trait so [`super::WalletTopology::resolve_verified`] can be
/// unit-tested with a stub, independent of a live Polygon node.
#[async_trait]
pub trait WalletOwnershipVerifier: Send + Sync {
    /// Returns owner and controller authorization observed on chain.
    async fn control_evidence(
        &self,
        kind: ExecutionWalletKind,
        funder: Address,
        signer: Address,
    ) -> Result<WalletControlEvidence, RpcError>;
}

/// On-chain control evidence used to retain owner/session-signer lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletControlEvidence {
    pub owner: Address,
    pub controller_authorized: bool,
}

/// Read-only Polygon client that resolves wallet controllers on-chain.
pub struct WalletOwnershipClient {
    provider: DynProvider,
}

impl WalletOwnershipClient {
    /// Build a read-only client from the Polymarket on-chain RPC config.
    ///
    /// No network I/O happens here; the first request is issued lazily when a
    /// verification actually runs (i.e. only when CREATE2 derivation missed).
    pub fn connect(config: &PolymarketConfig) -> Result<Self, RpcError> {
        let rpc_url = Url::parse(config.onchain.rpc_url()).map_err(|error| {
            RpcError::ConnectionFailed(format!(
                "configured Polygon RPC endpoint is invalid: {error}"
            ))
        })?;
        // reqwest defaults to *no* timeout; bound each ownership read so a stalled
        // provider cannot hang boot indefinitely.
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.onchain.rpc_timeout_ms))
            .build()
            .map_err(|e| {
                RpcError::ConnectionFailed(format!("failed to build Polygon RPC HTTP client: {e}"))
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        let provider = ProviderBuilder::new().connect_client(rpc_client).erased();
        Ok(Self { provider })
    }
}

#[async_trait]
impl WalletOwnershipVerifier for WalletOwnershipClient {
    async fn control_evidence(
        &self,
        kind: ExecutionWalletKind,
        funder: Address,
        signer: Address,
    ) -> Result<WalletControlEvidence, RpcError> {
        match kind {
            ExecutionWalletKind::Eoa => Ok(WalletControlEvidence {
                owner: funder,
                controller_authorized: funder == signer,
            }),
            ExecutionWalletKind::Proxy => {
                let owner = PolymarketProxyWallet::new(funder, &self.provider)
                    .owner()
                    .call()
                    .await
                    .map_err(|e| RpcError::CallFailed {
                        method: "proxy.owner".into(),
                        reason: e.to_string(),
                    })?;
                Ok(WalletControlEvidence {
                    owner,
                    controller_authorized: owner == signer,
                })
            }
            ExecutionWalletKind::GnosisSafe => {
                let is_owner = GnosisSafeWallet::new(funder, &self.provider)
                    .isOwner(signer)
                    .call()
                    .await
                    .map_err(|e| RpcError::CallFailed {
                        method: "safe.isOwner".into(),
                        reason: e.to_string(),
                    })?;
                Ok(WalletControlEvidence {
                    owner: signer,
                    controller_authorized: is_owner,
                })
            }
            ExecutionWalletKind::DepositWallet => {
                let wallet = DepositWalletView::new(funder, &self.provider);
                let owner = wallet
                    .owner()
                    .call()
                    .await
                    .map_err(|error| RpcError::CallFailed {
                        method: "deposit_wallet.owner".into(),
                        reason: error.to_string(),
                    })?;
                super::verify_deposit_wallet_derivation(owner, funder, 137).map_err(|error| {
                    RpcError::CallFailed {
                        method: "deposit_wallet.factory_derivation".into(),
                        reason: error.to_string(),
                    }
                })?;
                if owner == signer {
                    return Ok(WalletControlEvidence {
                        owner,
                        controller_authorized: true,
                    });
                }
                let valid_until = wallet
                    .sessionSignerAuthorizedUntil(signer)
                    .call()
                    .await
                    .map_err(|error| RpcError::CallFailed {
                        method: "deposit_wallet.sessionSignerAuthorizedUntil".into(),
                        reason: error.to_string(),
                    })?;
                let head = self
                    .provider
                    .get_block_by_number(BlockNumberOrTag::Latest)
                    .await
                    .map_err(|error| RpcError::CallFailed {
                        method: "eth_getBlockByNumber(latest)".into(),
                        reason: error.to_string(),
                    })?
                    .ok_or_else(|| RpcError::CallFailed {
                        method: "eth_getBlockByNumber(latest)".into(),
                        reason: "latest block was absent".to_owned(),
                    })?;
                Ok(WalletControlEvidence {
                    owner,
                    controller_authorized: valid_until >= U256::from(head.header.timestamp),
                })
            }
        }
    }
}
