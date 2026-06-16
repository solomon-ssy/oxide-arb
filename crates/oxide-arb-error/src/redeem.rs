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
            Self::InvalidAddress { .. } | Self::UnsupportedRoute { .. } | Self::WrongHolder { .. }
        )
    }

    #[must_use]
    pub const fn is_terminal_success_equivalent(&self) -> bool {
        matches!(self, Self::AlreadyRedeemed(_))
    }

    /// Stable Prometheus label for [`settlement_redeem_failure_total`].
    #[must_use]
    pub const fn metrics_error_class(&self) -> &'static str {
        match self {
            Self::InvalidAddress { .. } => "invalid_address",
            Self::InvalidConditionId { .. } => "invalid_condition_id",
            Self::UnsupportedRoute { .. } => "unsupported_route",
            Self::WrongHolder { .. } => "wrong_holder",
            Self::RpcTimeout(_) => "rpc_timeout",
            Self::InsufficientGas(_) => "insufficient_gas",
            Self::ContractRevert(_) => "contract_revert",
            Self::AlreadyRedeemed(_) => "already_redeemed",
            Self::TransactionFailed { .. } => "transaction_failed",
        }
    }
}
