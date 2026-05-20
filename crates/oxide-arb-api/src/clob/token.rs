//! Token ID parsing for CLOB wire types.

use oxide_arb_error::api::ApiError;
use oxide_arb_models::types::TokenId;
use polymarket_client_sdk_v2::types::U256;
use std::str::FromStr;

/// CLOB wire-format token identifier (SDK `U256`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WireTokenId(pub U256);

impl TryFrom<&TokenId> for WireTokenId {
    type Error = ApiError;

    fn try_from(token_id: &TokenId) -> Result<Self, ApiError> {
        U256::from_str(token_id.as_str())
            .map(Self)
            .map_err(|e| ApiError::Deserialize {
                context: "token_id to U256".into(),
                detail: e.to_string(),
            })
    }
}

impl From<WireTokenId> for U256 {
    fn from(id: WireTokenId) -> Self {
        id.0
    }
}
