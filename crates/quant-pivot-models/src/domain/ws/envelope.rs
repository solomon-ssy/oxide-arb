//! Server-to-client message envelope.
//!
//! Every server push is a JSON object `{ "type", "timestamp", "data" }`. The
//! `type` is a [`ServerMessageKind`] — either a subscribable [`WsChannel`] or a
//! control reply (`sync` / `pong` / `error`) — and never a bare string. The
//! timestamp is a strongly-typed [`DateTime<Utc>`] serialized as an RFC3339
//! millisecond instant, so the wire format is fixed by the serializer rather
//! than re-derived at every call site.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use crate::domain::ws::channel::WsChannel;

/// The `type` discriminator of a server message.
///
/// A [`Self::Channel`] carries the wire channel name; the control variants are
/// the fixed replies the session loop emits outside the fan-out path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMessageKind {
    /// A fan-out push on a subscribable channel.
    Channel(WsChannel),
    /// Reply to a `sync` command (full-state snapshot).
    Sync,
    /// Reply to a `ping` command (application keepalive).
    Pong,
    /// A command error (e.g. forbidden / unknown channel).
    Error,
}

impl ServerMessageKind {
    /// The on-the-wire `type` string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Channel(channel) => channel.as_str(),
            Self::Sync => "sync",
            Self::Pong => "pong",
            Self::Error => "error",
        }
    }
}

impl Serialize for ServerMessageKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// A server-to-client message envelope.
#[derive(Debug, Clone, Serialize)]
pub struct WsEnvelope {
    /// Message type: a channel name or a control reply.
    #[serde(rename = "type")]
    pub kind: ServerMessageKind,
    /// Emission instant, serialized as an RFC3339 millisecond UTC timestamp.
    #[serde(serialize_with = "serialize_rfc3339_millis")]
    pub timestamp: DateTime<Utc>,
    /// Type-specific payload.
    pub data: Value,
}

/// Serialize an instant as `YYYY-MM-DDThh:mm:ss.SSSZ` (RFC3339, millis, `Z`).
fn serialize_rfc3339_millis<S: Serializer>(
    timestamp: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

impl WsEnvelope {
    /// Build an envelope of `kind` stamped with the current time.
    #[must_use]
    fn now(kind: ServerMessageKind, data: Value) -> Self {
        Self {
            kind,
            timestamp: Utc::now(),
            data,
        }
    }

    /// A fan-out push on `channel`.
    #[must_use]
    pub fn channel(channel: WsChannel, data: Value) -> Self {
        Self::now(ServerMessageKind::Channel(channel), data)
    }

    /// A `sync` full-state snapshot reply.
    #[must_use]
    pub fn sync(data: Value) -> Self {
        Self::now(ServerMessageKind::Sync, data)
    }

    /// A `pong` keepalive reply.
    #[must_use]
    pub fn pong() -> Self {
        Self::now(ServerMessageKind::Pong, Value::Object(Map::new()))
    }

    /// A command `error` reply.
    #[must_use]
    pub fn error(data: Value) -> Self {
        Self::now(ServerMessageKind::Error, data)
    }

    /// Serialize to a JSON string for transmission (best-effort; never panics).
    #[must_use]
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{ServerMessageKind, WsEnvelope};
    use crate::domain::ws::channel::WsChannel;

    #[test]
    fn channel_envelope_serializes_timestamp() {
        let envelope = WsEnvelope::channel(WsChannel::MarketBookUpdate, serde_json::json!({}));
        let json: Value = serde_json::from_str(&envelope.to_text()).expect("valid json");
        assert_eq!(json["type"], "market.book_update");
        let ts = json["timestamp"].as_str().expect("timestamp string");
        assert!(ts.ends_with('Z'), "trailing Z: {ts}");
        // `…ss.SSSZ`: exactly three fractional-second digits.
        let frac = ts.split('.').nth(1).expect("fractional part");
        assert_eq!(frac.len(), 4, "three millis digits + Z: {ts}");
    }

    #[test]
    fn control_replies_serialize_strings() {
        assert_eq!(ServerMessageKind::Sync.as_str(), "sync");
        assert_eq!(ServerMessageKind::Pong.as_str(), "pong");
        assert_eq!(ServerMessageKind::Error.as_str(), "error");
        let pong: Value = serde_json::from_str(&WsEnvelope::pong().to_text()).expect("valid json");
        assert_eq!(pong["type"], "pong");
        assert!(pong["data"].is_object());
    }
}
