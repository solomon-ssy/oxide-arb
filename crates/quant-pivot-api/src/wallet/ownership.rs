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
    primitives::Address,
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
    /// Returns `true` when `funder` is controlled by `signer` under `kind`.
    async fn is_controlled_by(
        &self,
        kind: ExecutionWalletKind,
        funder: Address,
        signer: Address,
    ) -> Result<bool, RpcError>;
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
    async fn is_controlled_by(
        &self,
        kind: ExecutionWalletKind,
        funder: Address,
        signer: Address,
    ) -> Result<bool, RpcError> {
        match kind {
            ExecutionWalletKind::Eoa => Ok(funder == signer),
            ExecutionWalletKind::Proxy => {
                let owner = PolymarketProxyWallet::new(funder, &self.provider)
                    .owner()
                    .call()
                    .await
                    .map_err(|e| RpcError::CallFailed {
                        method: "proxy.owner".into(),
                        reason: e.to_string(),
                    })?;
                Ok(owner == signer)
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
                Ok(is_owner)
            }
        }
    }
}
