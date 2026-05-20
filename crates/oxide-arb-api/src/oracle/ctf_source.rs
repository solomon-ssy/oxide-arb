//! On-chain CTF contract oracle source via alloy.
//!
//! Queries the Conditional Tokens Framework contract to determine
//! whether a market has been resolved and what the payout is.

use alloy::primitives::{Address, FixedBytes, U256};
use alloy::providers::ProviderBuilder;
use alloy::sol;
use async_trait::async_trait;
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::types::MarketId;
use std::str::FromStr;

use super::source::OracleSource;
use super::types::SourceVote;

sol! {
    #[sol(rpc)]
    interface IConditionalTokens {
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
    }
}

/// Oracle source that queries the CTF contract's payoutNumerators on-chain.
///
/// Resolution logic:
/// - If `payoutDenominator == 0` → market not yet resolved → return `None`
/// - If `payoutNumerators[0] > 0` → YES outcome won
/// - If `payoutNumerators[1] > 0` → NO outcome won
pub struct CtfOracleSource {
    rpc_url: String,
    ctf_address: Address,
}

impl CtfOracleSource {
    pub fn new(rpc_url: String, ctf_address: &str) -> Result<Self, RpcError> {
        let addr = Address::from_str(ctf_address).map_err(|e| RpcError::CallFailed {
            method: "parse_ctf_address".into(),
            reason: e.to_string(),
        })?;
        Ok(Self {
            rpc_url,
            ctf_address: addr,
        })
    }
}

#[async_trait]
impl OracleSource for CtfOracleSource {
    fn source_id(&self) -> &'static str {
        "ctf_onchain"
    }

    async fn query_resolution(
        &self,
        _market_id: &MarketId,
        condition_id: &str,
    ) -> Result<Option<SourceVote>, RpcError> {
        let rpc_url: url::Url = self
            .rpc_url
            .parse()
            .map_err(|e: url::ParseError| RpcError::ConnectionFailed(e.to_string()))?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        let condition_bytes: FixedBytes<32> =
            FixedBytes::from_str(condition_id).map_err(|e| RpcError::AbiDecode {
                contract: "CTF".into(),
                reason: format!("Invalid conditionId hex: {e}"),
            })?;

        let ctf = IConditionalTokens::new(self.ctf_address, &provider);

        let denominator: U256 = ctf
            .payoutDenominator(condition_bytes)
            .call()
            .await
            .map_err(|e| RpcError::CallFailed {
                method: "payoutDenominator".into(),
                reason: e.to_string(),
            })?;

        if denominator.is_zero() {
            return Ok(None);
        }

        let yes_payout: U256 = ctf
            .payoutNumerators(condition_bytes, U256::ZERO)
            .call()
            .await
            .map_err(|e| RpcError::CallFailed {
                method: "payoutNumerators[0]".into(),
                reason: e.to_string(),
            })?;

        let actual_yes = !yes_payout.is_zero();

        Ok(Some(SourceVote {
            source_id: "ctf_onchain".into(),
            actual_yes,
            confidence: rust_decimal::Decimal::ONE,
            reported_at: chrono::Utc::now(),
        }))
    }
}
