//! `OracleSource` trait definition.

use async_trait::async_trait;
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::types::MarketId;

use super::types::SourceVote;

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
