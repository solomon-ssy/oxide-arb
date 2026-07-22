//! Process-wide WebSocket session continuity fence.

use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use quant_pivot_models::{domain::data_plane::pipeline::StreamSessionTicket, types::TokenId};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionStatus {
    Active,
    Poisoned,
    Closed,
}

#[derive(Clone)]
struct SessionRecord {
    ticket: StreamSessionTicket,
    tokens: Arc<[TokenId]>,
    status: SessionStatus,
}

/// Cold-path writer plus a lock-free active-session snapshot shared by every
/// partition and semantic book reader.
pub struct SessionDirectory {
    snapshot: ArcSwap<SessionSnapshot>,
    writer: Mutex<AHashMap<Uuid, SessionRecord>>,
}

#[derive(Default)]
struct SessionSnapshot {
    records: AHashMap<Uuid, SessionRecord>,
    active_epochs: AHashSet<u64>,
}

impl Default for SessionDirectory {
    fn default() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(SessionSnapshot::default()),
            writer: Mutex::new(AHashMap::new()),
        }
    }
}

impl SessionDirectory {
    /// Register one newly established physical stream. A UUID may never be
    /// rebound to another epoch or token scope inside the process.
    pub fn open(&self, ticket: StreamSessionTicket, tokens: Arc<[TokenId]>) -> bool {
        if !ticket.is_valid() || tokens.is_empty() {
            return false;
        }
        let mut writer = self.writer.lock();
        if let Some(existing) = writer.get(&ticket.stream_session_id) {
            return existing.ticket == ticket
                && existing.status == SessionStatus::Active
                && existing.tokens.as_ref() == tokens.as_ref();
        }
        if writer
            .values()
            .any(|record| record.ticket.epoch == ticket.epoch)
        {
            return false;
        }
        writer.insert(
            ticket.stream_session_id,
            SessionRecord {
                ticket,
                tokens,
                status: SessionStatus::Active,
            },
        );
        self.publish_snapshot(&writer);
        drop(writer);
        true
    }

    #[must_use]
    #[inline]
    pub fn is_active(&self, ticket: StreamSessionTicket) -> bool {
        let snapshot = self.snapshot.load();
        snapshot
            .records
            .get(&ticket.stream_session_id)
            .is_some_and(|record| record.ticket == ticket && record.status == SessionStatus::Active)
    }

    #[must_use]
    #[inline]
    pub fn is_epoch_active(&self, epoch: u64) -> bool {
        epoch != 0 && self.snapshot.load().active_epochs.contains(&epoch)
    }

    /// Atomically poison the matching epoch and return its complete transport
    /// subscription scope for invalidation/restart.
    pub fn poison(&self, ticket: StreamSessionTicket) -> Option<Arc<[TokenId]>> {
        let mut writer = self.writer.lock();
        let record = writer.get_mut(&ticket.stream_session_id)?;
        if record.ticket != ticket || record.status != SessionStatus::Active {
            return None;
        }
        record.status = SessionStatus::Poisoned;
        let tokens = Arc::clone(&record.tokens);
        self.publish_snapshot(&writer);
        drop(writer);
        Some(tokens)
    }

    #[must_use]
    pub fn tokens(&self, ticket: StreamSessionTicket) -> Option<Arc<[TokenId]>> {
        let snapshot = self.snapshot.load();
        let record = snapshot.records.get(&ticket.stream_session_id)?;
        (record.ticket == ticket && record.status != SessionStatus::Closed)
            .then(|| Arc::clone(&record.tokens))
    }

    /// Permanently close a fully drained session. The tombstone prevents the
    /// UUID from ever being rebound to another epoch or scope in this process.
    pub fn close(&self, ticket: StreamSessionTicket) -> bool {
        let mut writer = self.writer.lock();
        let Some(record) = writer.get_mut(&ticket.stream_session_id) else {
            return false;
        };
        if record.ticket != ticket {
            return false;
        }
        record.status = SessionStatus::Closed;
        record.tokens = Arc::from([]);
        self.publish_snapshot(&writer);
        drop(writer);
        true
    }

    fn publish_snapshot(&self, writer: &AHashMap<Uuid, SessionRecord>) {
        let active_epochs = writer
            .values()
            .filter(|record| record.status == SessionStatus::Active)
            .map(|record| record.ticket.epoch)
            .collect();
        self.snapshot.store(Arc::new(SessionSnapshot {
            records: writer.clone(),
            active_epochs,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket(epoch: u64) -> StreamSessionTicket {
        StreamSessionTicket::new(Uuid::from_u128(1), epoch).expect("valid ticket")
    }

    #[test]
    fn poison_is_sticky_and_epoch_scoped() {
        let sessions = SessionDirectory::default();
        let active = ticket(1);
        let tokens: Arc<[TokenId]> = Arc::from([TokenId::new("1"), TokenId::new("2")]);
        assert!(sessions.open(active, Arc::clone(&tokens)));
        assert!(sessions.is_active(active));
        assert!(sessions.is_epoch_active(active.epoch));

        assert_eq!(sessions.poison(active).as_deref(), Some(tokens.as_ref()));
        assert!(!sessions.is_active(active));
        assert!(!sessions.is_epoch_active(active.epoch));
        assert!(sessions.poison(ticket(2)).is_none());
        assert!(!sessions.open(ticket(2), tokens));
    }

    #[test]
    fn close_makes_queued_ticket_unavailable() {
        let sessions = SessionDirectory::default();
        let active = ticket(1);
        assert!(sessions.open(active, Arc::from([TokenId::new("1")])));
        assert!(sessions.close(active));
        assert!(!sessions.is_active(active));
        assert!(sessions.tokens(active).is_none());
        assert!(!sessions.open(active, Arc::from([TokenId::new("1")])));
    }

    #[test]
    fn epoch_cannot_be_rebound_to_another_uuid() {
        let sessions = SessionDirectory::default();
        let active = ticket(1);
        assert!(sessions.open(active, Arc::from([TokenId::new("1")])));
        let duplicate_epoch =
            StreamSessionTicket::new(Uuid::from_u128(2), 1).expect("valid duplicate-epoch ticket");
        assert!(!sessions.open(duplicate_epoch, Arc::from([TokenId::new("2")])));
    }
}
