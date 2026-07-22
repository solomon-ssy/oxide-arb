//! Bounded ownership envelope for one normalized WebSocket message.

use std::{mem::size_of, sync::Arc};

use quant_pivot_models::{
    domain::{
        data_plane::pipeline::{PipelineEvent, PriceLevelDelta},
        market::book::BookLevel,
    },
    types::{TokenId, TokenKey},
};
use tokio::sync::OwnedSemaphorePermit;

/// One permit accounts for one KiB of retained ingress memory.
pub const INGRESS_PERMIT_BYTES: usize = 1_024;
/// Global memory retained between WS normalization and partition completion.
pub const INGRESS_MEMORY_BUDGET_BYTES: usize = 256 * 1_024 * 1_024;
/// Number of batches waiting between all WS shards and the partition router.
pub const INGRESS_MAILBOX_CAPACITY: usize = 256;

/// One normalized WS message and its shared byte-budget ownership.
///
/// The permit is reference counted only because the partition router can split
/// one source message into several token-affine batches. Memory is released
/// after the final partition finishes the source message, never at router time.
pub struct NormalizedIngressBatch {
    pub events: Vec<PipelineEvent>,
    pub memory_permit: Arc<OwnedSemaphorePermit>,
}

impl NormalizedIngressBatch {
    #[must_use]
    pub fn new(events: Vec<PipelineEvent>, memory_permit: OwnedSemaphorePermit) -> Self {
        Self {
            events,
            memory_permit: Arc::new(memory_permit),
        }
    }
}

/// Conservatively account retained event memory without serializing the event.
#[must_use]
pub fn estimated_event_bytes(event: &PipelineEvent) -> usize {
    let dynamic = match event {
        PipelineEvent::BookSnapshot(command) => command
            .bids
            .levels
            .len()
            .saturating_add(command.asks.levels.len())
            .saturating_mul(size_of::<BookLevel>()),
        PipelineEvent::PriceDelta(command) => command
            .changes
            .len()
            .saturating_mul(size_of::<PriceLevelDelta>()),
        PipelineEvent::LastTradePrice { market_id, .. } => market_id.as_str().len(),
        PipelineEvent::MarketResolved {
            market_id,
            winning_outcome,
            tokens,
            ..
        } => market_id
            .as_str()
            .len()
            .saturating_add(winning_outcome.len())
            .saturating_add(tokens.len().saturating_mul(size_of::<TokenKey>())),
        PipelineEvent::StreamSessionOpened {
            subscription_tokens,
            ..
        } => subscription_tokens.iter().fold(0_usize, |bytes, token| {
            bytes
                .saturating_add(size_of::<TokenId>())
                .saturating_add(token.as_str().len())
        }),
        PipelineEvent::StreamSessionClosed {
            received_sequences, ..
        } => received_sequences
            .len()
            .saturating_mul(size_of::<(TokenKey, u64)>()),
        PipelineEvent::TickSizeChange { .. }
        | PipelineEvent::ShardStatus { .. }
        | PipelineEvent::StreamGap { .. } => 0,
    };
    size_of::<PipelineEvent>().saturating_add(dynamic)
}

#[must_use]
pub fn ingress_permits(events: &[PipelineEvent]) -> u32 {
    let bytes = size_of::<Vec<PipelineEvent>>().saturating_add(
        events
            .iter()
            .map(estimated_event_bytes)
            .fold(0_usize, usize::saturating_add),
    );
    let permits = bytes.max(1).div_ceil(INGRESS_PERMIT_BYTES);
    u32::try_from(permits).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, sync::Arc};

    use quant_pivot_models::{
        domain::data_plane::pipeline::PipelineEvent, enums::system::ShardConnectionStatus,
    };

    use tokio::sync::Semaphore;

    use super::{INGRESS_PERMIT_BYTES, NormalizedIngressBatch, ingress_permits};

    #[test]
    fn accounting_rounds_up_to_kib_permits() {
        let event = PipelineEvent::ShardStatus {
            shard_id: 0,
            status: ShardConnectionStatus::Connected,
        };
        let permits = ingress_permits(&[event]);
        let retained = size_of::<Vec<PipelineEvent>>() + size_of::<PipelineEvent>();

        assert!(permits >= 1);
        assert!(usize::try_from(permits).expect("u32 fits") * INGRESS_PERMIT_BYTES >= retained);
    }

    #[test]
    fn split_batches_hold_budget_until_the_last_partition_finishes() {
        let budget = Arc::new(Semaphore::new(1));
        let source = NormalizedIngressBatch::new(
            vec![PipelineEvent::ShardStatus {
                shard_id: 0,
                status: ShardConnectionStatus::Connected,
            }],
            Arc::clone(&budget)
                .try_acquire_owned()
                .expect("one permit available"),
        );
        let partition_permit = Arc::clone(&source.memory_permit);

        drop(source);
        assert_eq!(budget.available_permits(), 0);
        drop(partition_permit);
        assert_eq!(budget.available_permits(), 1);
    }
}
