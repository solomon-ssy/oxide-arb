//! Polymarket gasless relayer client for Proxy / Gnosis Safe settlement.
//!
//! Proxy and Gnosis Safe wallets hold the collateral/positions but cannot be
//! driven by a direct EOA transaction. The signer EOA instead authorizes a
//! wallet-native call (Safe `execTransaction` `SafeTx` / Proxy GSN relay) and the
//! Polymarket relayer broadcasts it on-chain and pays the gas.
//!
//! Wire format mirrors the official `@polymarket/builder-relayer-client`
//! (`relayer-v2`): `POST /submit` with a wallet-typed `{from,to,proxyWallet,
//! data,nonce,signature,signatureParams,type}` body, polled via
//! `GET /transaction?id=`.

use std::{str::FromStr, time::Duration};

use alloy::{
    primitives::{Address, B256, Bytes, U256, keccak256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol,
    sol_types::{SolCall, SolStruct, eip712_domain},
};
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::{
    config::RelayerConfig,
    constants::{CTF_ADDRESS, PUSD_ADDRESS},
    enums::quant::ExecutionWalletKind,
};
use reqwest::{Client, Error, Response, Url};
use serde::{Deserialize, Serialize};

use crate::{keystore::OrderSigner, wallet::WalletTopology};

/// Polygon Polymarket Proxy factory (EIP-1167 minimal proxy / GSN entry).
const PROXY_FACTORY: &str = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052";
/// Polygon GSN `RelayHub` used by the Proxy wallet relay flow.
const RELAY_HUB: &str = "0xD216153c06E857cD7f72665E0aF1d7D82172F494";
/// Standard binary YES/NO partition for `redeemPositions`.
const STANDARD_BINARY_INDEX_SETS: [u64; 2] = [1, 2];
/// GSN call type for a plain `CALL` (Proxy `proxy(calls)` tuple `typeCode`).
const PROXY_CALL_TYPE: u8 = 1;
/// Gnosis Safe `Operation::Call`.
const SAFE_OPERATION_CALL: u8 = 0;
/// Fallback proxy gas limit (mirrors the reference client default).
const PROXY_DEFAULT_GAS_LIMIT: u64 = 10_000_000;

sol! {
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );

    struct SafeTx {
        address to;
        uint256 value;
        bytes data;
        uint8 operation;
        uint256 safeTxGas;
        uint256 baseGas;
        uint256 gasPrice;
        address gasToken;
        address refundReceiver;
        uint256 nonce;
    }

    struct ProxyCall {
        uint8 typeCode;
        address to;
        uint256 value;
        bytes data;
    }
    function proxy(ProxyCall[] calls);
}

/// Relayer transaction wallet type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayerTxType {
    Safe,
    Proxy,
}

impl RelayerTxType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "SAFE",
            Self::Proxy => "PROXY",
        }
    }
}

/// Submitted relayer transaction handle (relayer-side id + optional on-chain hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayerSubmission {
    pub transaction_id: String,
    pub tx_hash: Option<String>,
}

/// Confirmation outcome of a relayer transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayerTxOutcome {
    /// Not yet mined/confirmed; keep polling.
    Pending,
    /// Mined and confirmed; carries the on-chain transaction hash.
    Confirmed { tx_hash: String },
    /// Relayer reported a terminal failure.
    Failed { detail: String },
}

/// Gasless relayer client scoped to standard binary CTF redemption.
#[derive(Clone)]
pub struct RelayerClient {
    http: Client,
    base_url: Url,
    api_key: String,
    api_key_address: String,
    chain_id: u64,
    signer: PrivateKeySigner,
    signer_address: Address,
    funder: Address,
    tx_type: RelayerTxType,
}

impl RelayerClient {
    /// Build a relayer client for a Proxy / Gnosis Safe topology.
    ///
    /// Returns an error for EOA topologies (which settle on-chain directly) or
    /// when the relayer API credentials are absent.
    pub fn connect(
        signer: &OrderSigner,
        config: &RelayerConfig,
        topology: &WalletTopology,
        chain_id: u64,
    ) -> Result<Self, RpcError> {
        let tx_type = match topology.kind {
            ExecutionWalletKind::Proxy => RelayerTxType::Proxy,
            ExecutionWalletKind::GnosisSafe => RelayerTxType::Safe,
            ExecutionWalletKind::Eoa => {
                return Err(RpcError::ConnectionFailed(
                    "relayer client is not applicable to eoa wallets".to_owned(),
                ));
            }
        };
        let api_key = config
            .api_key()
            .ok_or_else(|| {
                RpcError::ConnectionFailed("polymarket.relayer.api_key is required".to_owned())
            })?
            .to_owned();
        let api_key_address = config
            .api_key_address()
            .ok_or_else(|| {
                RpcError::ConnectionFailed(
                    "polymarket.relayer.api_key_address is required".to_owned(),
                )
            })?
            .to_owned();
        let base_url = Url::parse(&ensure_trailing_slash(&config.base_url)).map_err(|e| {
            RpcError::ConnectionFailed(format!(
                "invalid relayer base_url '{}': {e}",
                config.base_url
            ))
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|e| RpcError::ConnectionFailed(format!("relayer http client: {e}")))?;
        Ok(Self {
            http,
            base_url,
            api_key,
            api_key_address,
            chain_id,
            signer: signer.inner().clone(),
            signer_address: topology.signer,
            funder: topology.funder,
            tx_type,
        })
    }

    /// Build, sign, and submit a standard binary `redeemPositions` for the
    /// condition id (the market id, a 0x-prefixed 32-byte hex string).
    pub async fn submit_standard_binary_redeem(
        &self,
        condition_id: &str,
    ) -> Result<RelayerSubmission, RpcError> {
        let condition_id =
            B256::from_str(condition_id.trim()).map_err(|e| RpcError::CallFailed {
                method: "relayer.parse_condition_id".into(),
                reason: format!("invalid condition id '{condition_id}': {e}"),
            })?;
        let redeem_calldata = redeem_calldata(condition_id);
        let body = match self.tx_type {
            RelayerTxType::Safe => self.build_safe_request(redeem_calldata).await?,
            RelayerTxType::Proxy => self.build_proxy_request(redeem_calldata).await?,
        };
        let response: SubmitResponse = self.post_json("submit", &body).await?;
        Ok(RelayerSubmission {
            transaction_id: response.transaction_id,
            tx_hash: response.transaction_hash.filter(|hash| !hash.is_empty()),
        })
    }

    /// Poll the terminal/intermediate outcome of a submitted relayer transaction.
    pub async fn transaction_outcome(
        &self,
        transaction_id: &str,
    ) -> Result<RelayerTxOutcome, RpcError> {
        let txns: Vec<RelayerTransaction> = self
            .get_json("transaction", &[("id", transaction_id)])
            .await?;
        let Some(txn) = txns.into_iter().next() else {
            return Ok(RelayerTxOutcome::Pending);
        };
        Ok(match txn.state.as_str() {
            "STATE_MINED" | "STATE_CONFIRMED" => match txn.transaction_hash {
                Some(hash) if !hash.is_empty() => RelayerTxOutcome::Confirmed { tx_hash: hash },
                _ => RelayerTxOutcome::Pending,
            },
            "STATE_FAILED" | "STATE_INVALID" => RelayerTxOutcome::Failed {
                detail: format!(
                    "relayer transaction {transaction_id} terminal state {}",
                    txn.state
                ),
            },
            _ => RelayerTxOutcome::Pending,
        })
    }

    async fn build_safe_request(&self, redeem_calldata: Bytes) -> Result<SubmitBody, RpcError> {
        let nonce: NonceResponse = self
            .get_json(
                "nonce",
                &[
                    ("address", self.signer_address.to_checksum(None).as_str()),
                    ("type", RelayerTxType::Safe.as_str()),
                ],
            )
            .await?;
        let nonce = parse_u256("nonce", &nonce.nonce)?;
        let ctf = parse_address(CTF_ADDRESS, "CTF_ADDRESS")?;

        let safe_tx = SafeTx {
            to: ctf,
            value: U256::ZERO,
            data: redeem_calldata.clone(),
            operation: SAFE_OPERATION_CALL,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            nonce,
        };
        let domain = eip712_domain! {
            chain_id: self.chain_id,
            verifying_contract: self.funder,
        };
        let digest = safe_tx.eip712_signing_hash(&domain);
        // Gnosis `checkSignatures` eth_sign path expects v += 4 over the 27/28 base.
        let signature = self.sign_packed(&digest, 31)?;

        Ok(SubmitBody {
            tx_type: RelayerTxType::Safe.as_str().to_owned(),
            from: self.signer_address.to_checksum(None),
            to: ctf.to_checksum(None),
            proxy_wallet: self.funder.to_checksum(None),
            data: hex_prefixed(&redeem_calldata),
            nonce: nonce.to_string(),
            signature,
            signature_params: SignatureParamsBody {
                gas_price: Some("0".to_owned()),
                operation: Some(SAFE_OPERATION_CALL.to_string()),
                safe_txn_gas: Some("0".to_owned()),
                base_gas: Some("0".to_owned()),
                gas_token: Some(Address::ZERO.to_checksum(None)),
                refund_receiver: Some(Address::ZERO.to_checksum(None)),
                ..SignatureParamsBody::default()
            },
            metadata: String::new(),
        })
    }

    async fn build_proxy_request(&self, redeem_calldata: Bytes) -> Result<SubmitBody, RpcError> {
        let relay: RelayPayloadResponse = self
            .get_json(
                "relay-payload",
                &[
                    ("address", self.signer_address.to_checksum(None).as_str()),
                    ("type", RelayerTxType::Proxy.as_str()),
                ],
            )
            .await?;
        let nonce = parse_u256("relay nonce", &relay.nonce)?;
        let relay_address = parse_address(&relay.address, "relay address")?;
        let proxy_factory = parse_address(PROXY_FACTORY, "PROXY_FACTORY")?;
        let relay_hub = parse_address(RELAY_HUB, "RELAY_HUB")?;
        let ctf = parse_address(CTF_ADDRESS, "CTF_ADDRESS")?;

        // Wrap the CTF redeem in the proxy's `proxy([{typeCode,to,value,data}])`.
        let proxy_calldata = proxyCall {
            calls: vec![ProxyCall {
                typeCode: PROXY_CALL_TYPE,
                to: ctf,
                value: U256::ZERO,
                data: redeem_calldata,
            }],
        }
        .abi_encode();
        let proxy_calldata = Bytes::from(proxy_calldata);

        let gas_limit = U256::from(PROXY_DEFAULT_GAS_LIMIT);
        let digest = proxy_relay_hash(&ProxyRelayHashArgs {
            from: self.signer_address,
            to: proxy_factory,
            data: &proxy_calldata,
            tx_fee: U256::ZERO,
            gas_price: U256::ZERO,
            gas_limit,
            nonce,
            relay_hub,
            relay: relay_address,
        });
        // GSN RelayHub recovers a standard eth_sign signature (v = 27/28).
        let signature = self.sign_packed(&digest, 27)?;

        Ok(SubmitBody {
            tx_type: RelayerTxType::Proxy.as_str().to_owned(),
            from: self.signer_address.to_checksum(None),
            to: proxy_factory.to_checksum(None),
            proxy_wallet: self.funder.to_checksum(None),
            data: hex_prefixed(&proxy_calldata),
            nonce: nonce.to_string(),
            signature,
            signature_params: SignatureParamsBody {
                gas_price: Some("0".to_owned()),
                gas_limit: Some(gas_limit.to_string()),
                relayer_fee: Some("0".to_owned()),
                relay_hub: Some(relay_hub.to_checksum(None)),
                relay: Some(relay_address.to_checksum(None)),
                ..SignatureParamsBody::default()
            },
            metadata: String::new(),
        })
    }

    /// Sign a 32-byte digest as an Ethereum personal message and pack it into the
    /// `r||s||v` form the relayer expects, with `base_v` (27 standard, 31 Safe).
    fn sign_packed(&self, digest: &B256, base_v: u8) -> Result<String, RpcError> {
        let signature = self
            .signer
            .sign_message_sync(digest.as_slice())
            .map_err(|e| RpcError::CallFailed {
                method: "relayer.sign".into(),
                reason: e.to_string(),
            })?;
        let parity = u8::from(signature.v());
        let mut bytes = [0_u8; 65];
        bytes[..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
        bytes[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());
        bytes[64] = base_v + parity;
        Ok(format!("0x{}", hex::encode(bytes)))
    }

    async fn post_json<B: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<R, RpcError> {
        let url = self.endpoint(path)?;
        let response = self
            .http
            .post(url)
            .header("RELAYER_API_KEY", &self.api_key)
            .header("RELAYER_API_KEY_ADDRESS", &self.api_key_address)
            .json(body)
            .send()
            .await
            .map_err(|e| relayer_call_failed(path, &e))?;
        Self::parse_response(path, response).await
    }

    async fn get_json<R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<R, RpcError> {
        let url = self.endpoint(path)?;
        let response = self
            .http
            .get(url)
            .query(params)
            .header("RELAYER_API_KEY", &self.api_key)
            .header("RELAYER_API_KEY_ADDRESS", &self.api_key_address)
            .send()
            .await
            .map_err(|e| relayer_call_failed(path, &e))?;
        Self::parse_response(path, response).await
    }

    async fn parse_response<R: for<'de> Deserialize<'de>>(
        path: &str,
        response: Response,
    ) -> Result<R, RpcError> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| relayer_call_failed(path, &e))?;
        if !status.is_success() {
            return Err(RpcError::CallFailed {
                method: format!("relayer.{path}"),
                reason: format!("status {status}: {body}"),
            });
        }
        serde_json::from_str(&body).map_err(|e| RpcError::CallFailed {
            method: format!("relayer.{path}"),
            reason: format!("decode failed: {e}; body={body}"),
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, RpcError> {
        self.base_url.join(path).map_err(|e| RpcError::CallFailed {
            method: format!("relayer.{path}"),
            reason: format!("invalid endpoint: {e}"),
        })
    }
}

struct ProxyRelayHashArgs<'a> {
    from: Address,
    to: Address,
    data: &'a Bytes,
    tx_fee: U256,
    gas_price: U256,
    gas_limit: U256,
    nonce: U256,
    relay_hub: Address,
    relay: Address,
}

/// GSN `RelayHub` `keccak256("rlx:" || from || to || data || txFee || gasPrice ||
/// gasLimit || nonce || relayHub || relay)` challenge hash.
fn proxy_relay_hash(args: &ProxyRelayHashArgs<'_>) -> B256 {
    let mut buf = Vec::with_capacity(4 + 20 * 4 + args.data.len() + 32 * 4);
    buf.extend_from_slice(b"rlx:");
    buf.extend_from_slice(args.from.as_slice());
    buf.extend_from_slice(args.to.as_slice());
    buf.extend_from_slice(args.data);
    buf.extend_from_slice(&args.tx_fee.to_be_bytes::<32>());
    buf.extend_from_slice(&args.gas_price.to_be_bytes::<32>());
    buf.extend_from_slice(&args.gas_limit.to_be_bytes::<32>());
    buf.extend_from_slice(&args.nonce.to_be_bytes::<32>());
    buf.extend_from_slice(args.relay_hub.as_slice());
    buf.extend_from_slice(args.relay.as_slice());
    keccak256(&buf)
}

fn redeem_calldata(condition_id: B256) -> Bytes {
    let collateral = Address::from_str(PUSD_ADDRESS).unwrap_or(Address::ZERO);
    let calldata = redeemPositionsCall {
        collateralToken: collateral,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        indexSets: STANDARD_BINARY_INDEX_SETS
            .into_iter()
            .map(U256::from)
            .collect(),
    }
    .abi_encode();
    Bytes::from(calldata)
}

fn ensure_trailing_slash(url: &str) -> String {
    if url.ends_with('/') {
        url.to_owned()
    } else {
        format!("{url}/")
    }
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn parse_address(value: &str, label: &str) -> Result<Address, RpcError> {
    Address::from_str(value.trim()).map_err(|e| RpcError::CallFailed {
        method: "relayer.parse_address".into(),
        reason: format!("{label}: {e}"),
    })
}

fn parse_u256(label: &str, value: &str) -> Result<U256, RpcError> {
    U256::from_str(value.trim()).map_err(|e| RpcError::CallFailed {
        method: "relayer.parse_u256".into(),
        reason: format!("invalid {label} '{value}': {e}"),
    })
}

fn relayer_call_failed(path: &str, err: &Error) -> RpcError {
    if err.is_timeout() {
        RpcError::Timeout {
            method: format!("relayer.{path}"),
            elapsed_ms: 0,
        }
    } else {
        RpcError::CallFailed {
            method: format!("relayer.{path}"),
            reason: err.to_string(),
        }
    }
}

#[derive(Serialize)]
struct SubmitBody {
    #[serde(rename = "type")]
    tx_type: String,
    from: String,
    to: String,
    #[serde(rename = "proxyWallet")]
    proxy_wallet: String,
    data: String,
    nonce: String,
    signature: String,
    #[serde(rename = "signatureParams")]
    signature_params: SignatureParamsBody,
    metadata: String,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct SignatureParamsBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    safe_txn_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gas_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refund_receiver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gas_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relayer_fee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_hub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay: Option<String>,
}

#[derive(Deserialize)]
struct NonceResponse {
    nonce: String,
}

#[derive(Deserialize)]
struct RelayPayloadResponse {
    address: String,
    nonce: String,
}

#[derive(Deserialize)]
struct SubmitResponse {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
}

#[derive(Deserialize)]
struct RelayerTransaction {
    state: String,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redeem_calldata_has_selector_and_index_sets() {
        let condition =
            B256::from_str("0x0102030405060708091011121314151617181920212223242526272829303132")
                .expect("condition id");
        let calldata = redeem_calldata(condition);
        // 4-byte selector + 5 abi words (collateral, parent, condition, offset, len)
        // + 2 index-set words.
        assert_eq!(calldata.len(), 4 + 32 * 7);
    }

    #[test]
    fn proxy_relay_hash_is_deterministic() {
        let args = ProxyRelayHashArgs {
            from: Address::ZERO,
            to: Address::ZERO,
            data: &Bytes::from_static(b"\x01\x02"),
            tx_fee: U256::ZERO,
            gas_price: U256::ZERO,
            gas_limit: U256::from(10_000_000_u64),
            nonce: U256::from(7_u64),
            relay_hub: Address::ZERO,
            relay: Address::ZERO,
        };
        let a = proxy_relay_hash(&args);
        let args2 = ProxyRelayHashArgs {
            from: Address::ZERO,
            to: Address::ZERO,
            data: &Bytes::from_static(b"\x01\x02"),
            tx_fee: U256::ZERO,
            gas_price: U256::ZERO,
            gas_limit: U256::from(10_000_000_u64),
            nonce: U256::from(7_u64),
            relay_hub: Address::ZERO,
            relay: Address::ZERO,
        };
        assert_eq!(a, proxy_relay_hash(&args2));
    }
}
