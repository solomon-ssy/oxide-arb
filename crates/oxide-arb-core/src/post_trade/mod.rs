//! Durable, idempotent post-trade processing.
//!
//! The execution hot path records the venue outcome on the `trade` row
//! (`*_observed`) and rings [`relay::PostTradeRelay`]. The relay claims
//! unprocessed rows (notify-woken, with a periodic poll as the crash-recovery
//! safety net) and drives each through the idempotent [`consumer::PostTradeConsumer`]:
//! risk Fill accounting → position create (idempotent) → terminal state advance.
//!
//! Replaces the former in-memory channel + spill + dead transactional outbox:
//! the trade row is the single durable source of truth, replayed on restart.

pub mod consumer;
pub mod relay;
