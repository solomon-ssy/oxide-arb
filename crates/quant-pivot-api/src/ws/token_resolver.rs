//! Lock-free token-key resolution injected by the owning data plane.

use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

use polymarket_client_sdk_v2::types::U256;
use quant_pivot_models::types::TokenKey;

/// Resolves venue token values into stable process-local keys.
pub trait TokenKeyResolver: Send + Sync {
    fn resolve(&self, token: U256) -> Option<TokenKey>;
}

impl<F> TokenKeyResolver for F
where
    F: Fn(U256) -> Option<TokenKey> + Send + Sync,
{
    fn resolve(&self, token: U256) -> Option<TokenKey> {
        self(token)
    }
}

/// A venue event referenced a token absent from the immutable catalog index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnregisteredToken(pub U256);

impl Display for UnregisteredToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "unregistered CLOB token {}", self.0)
    }
}

impl Error for UnregisteredToken {}
