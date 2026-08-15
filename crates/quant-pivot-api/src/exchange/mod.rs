//! Polymarket CTF/NegRisk exchange `OrderFilled` log ingestion helpers.

pub mod constants;
pub mod execution_projector;
pub mod history_client;
pub mod order_filled_v1;
pub mod order_filled_v2;
pub mod orders_matched_v1;
pub mod orders_matched_v2;

pub use constants::{EXCHANGE_CONTRACTS, ExchangeContract, ExchangeVersion};
pub use execution_projector::{
    ExchangeHistoryProjection, ExecutionProjectionError, history_token_ids, project_history,
};
pub use history_client::{
    ArchiveProbe, AttestedHistoryChunk, CanonicalBlockHeader, CanonicalExchangeLog,
    ExchangeHistoryAttestor, ExchangeHistoryExtractor, ExtractedHistoryChunk, HistoryClientError,
    HistoryContinuityProof, HistoryContinuityProofBasis, HistoryDigest, canonical_digest,
    chunks_agree, polygon_chain_id,
};
