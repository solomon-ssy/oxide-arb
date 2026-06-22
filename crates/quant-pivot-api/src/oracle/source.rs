//! `OracleSource` trait definition.

use super::types::SourceVote;
use async_trait::async_trait;
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::types::MarketId;

/// Trait for a single oracle data source.
///
/// Implementations include Gamma API and CTF on-chain contract.
/// Kept as a trait for testability (mock injection) per ADR-001.
#[async_trait]
pub trait OracleSource: Send + Sync + 'static {
    fn source_id(&self) -> &'static str;

    async fn query_resolution(
        &self,
        market_id: &MarketId,
        condition_id: &str,
    ) -> Result<Option<SourceVote>, RpcError>;
}
