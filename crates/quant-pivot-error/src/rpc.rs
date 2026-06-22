//! Blockchain RPC errors — Polygon JSON-RPC and contract interactions.

use thiserror::Error;

/// Errors from on-chain RPC calls (Polygon node, CTF contract, etc.).
#[derive(Debug, Error)]
pub enum RpcError {
    #[error("RPC call {method} failed: {reason}")]
    CallFailed { method: String, reason: String },

    #[error("RPC connection failed: {0}")]
    ConnectionFailed(String),

    #[error("RPC timeout after {elapsed_ms}ms calling {method}")]
    Timeout { method: String, elapsed_ms: u64 },

    #[error("Contract ABI decode error on {contract}: {reason}")]
    AbiDecode { contract: String, reason: String },

    #[error("Insufficient gas or funds: {0}")]
    InsufficientFunds(String),
}
