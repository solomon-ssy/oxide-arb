use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedeemError {
    #[error("invalid address `{value}`: {reason}")]
    InvalidAddress { value: String, reason: String },
    #[error("invalid condition id `{value}`: {reason}")]
    InvalidConditionId { value: String, reason: String },
    #[error("unsupported redeem route `{route}`: {reason}")]
    UnsupportedRoute { route: String, reason: String },
    #[error("redeem holder {holder} differs from signer {signer}; proxy execution is required")]
    WrongHolder { holder: String, signer: String },
    #[error("signer {signer} is not an owner of Safe {safe}")]
    ProxySafeOwnerMismatch { safe: String, signer: String },
    #[error("RPC timeout or transport failure: {0}")]
    RpcTimeout(String),
    #[error("insufficient gas or gas estimation failure: {0}")]
    InsufficientGas(String),
    #[error("contract reverted: {0}")]
    ContractRevert(String),
    #[error("position already redeemed: {0}")]
    AlreadyRedeemed(String),
    #[error("redeem transaction failed: tx_hash={tx_hash:?}, reason={reason}")]
    TransactionFailed {
        tx_hash: Option<String>,
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct RedeemSendError {
    message: String,
}

impl RedeemSendError {
    #[must_use]
    pub fn from_display(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}

impl From<RedeemSendError> for RedeemError {
    fn from(error: RedeemSendError) -> Self {
        let lower = error.message.to_ascii_lowercase();
        if lower.contains("already") {
            Self::AlreadyRedeemed(error.message)
        } else if lower.contains("gas") {
            Self::InsufficientGas(error.message)
        } else if lower.contains("revert") {
            Self::ContractRevert(error.message)
        } else {
            Self::RpcTimeout(error.message)
        }
    }
}

impl RedeemError {
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::RpcTimeout(_) | Self::InsufficientGas(_))
    }

    #[must_use]
    pub const fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            Self::InvalidAddress { .. }
                | Self::UnsupportedRoute { .. }
                | Self::WrongHolder { .. }
                | Self::ProxySafeOwnerMismatch { .. }
        )
    }

    #[must_use]
    pub const fn is_terminal_success_equivalent(&self) -> bool {
        matches!(self, Self::AlreadyRedeemed(_))
    }
}
