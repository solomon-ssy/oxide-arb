//! WebSocket wire protocol — the strongly-typed contract for real-time push.
//!
//! Lives beside [`crate::domain::event::CoreEvent`] because the protocol and the
//! event-to-wire projection are one cohesive real-time contract, shared by the
//! `oxide-arb-web` session loop (server) and the `oxide-arb-core` book-update
//! coalescer (which keys fan-out off [`channel::SubscriptionKey`]).
//!
//! - [`channel`] — the [`channel::WsChannel`] taxonomy + [`channel::SubscriptionKey`];
//! - [`envelope`] — the server-push [`envelope::WsEnvelope`] + message kind;
//! - [`command`] — the client command grammar;
//! - [`sync`] — the typed `sync` full-state snapshot;
//! - [`mapping`] — [`mapping::event_envelope`], `CoreEvent` → wire projection.

pub mod channel;
pub mod command;
pub mod envelope;
pub mod mapping;
pub mod sync;

pub use channel::{ChannelScope, SubscriptionKey, UnknownChannel, WsChannel};
pub use command::ClientCommand;
pub use envelope::{ServerMessageKind, WsEnvelope};
pub use mapping::event_envelope;
pub use sync::SyncSnapshot;
