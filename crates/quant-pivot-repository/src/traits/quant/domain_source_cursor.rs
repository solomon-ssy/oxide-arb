//! Domain-source ingest cursor repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        DomainSourceCursorCasOutcome, DomainSourceCursorInfo, UpsertDomainSourceCursor,
    },
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};

/// Durable checkpoint repository for external domain-source ingestion.
#[async_trait::async_trait]
pub trait DomainSourceCursorRepository: Send + Sync {
    async fn find(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> Result<Option<DomainSourceCursorInfo>, StorageError>;

    async fn upsert(
        &self,
        cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorInfo, StorageError>;

    /// Atomically initialize or replace a cursor only when its current content
    /// hash equals `expected_checkpoint_hash`.
    async fn compare_and_set(
        &self,
        expected_checkpoint_hash: Option<ContentHash>,
        cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorCasOutcome, StorageError>;

    /// Every cursor, ordered by source then instrument (ingest health surface).
    async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError>;
}
