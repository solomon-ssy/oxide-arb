//! Client-to-server command grammar.
//!
//! Commands are fully strongly typed: the `channel` deserializes straight into a
//! [`WsChannel`] (an unknown channel is rejected with a naming error) and the
//! `market_id` into a [`MarketId`]. The session loop turns any deserialization
//! failure into a structured `error` frame, so strong typing costs no client
//! feedback.
//!
//! [`WsChannel`]: crate::domain::ws::channel::WsChannel
//! [`MarketId`]: crate::types::MarketId

use serde::Deserialize;

use crate::{domain::ws::channel::WsChannel, types::MarketId};

/// A client-to-server command.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ClientCommand {
    /// Subscribe to a channel (optionally scoped to a market).
    Subscribe {
        /// The channel to subscribe to.
        channel: WsChannel,
        /// Optional market scope for market-scoped channels.
        #[serde(default)]
        market_id: Option<MarketId>,
    },
    /// Unsubscribe from a channel.
    Unsubscribe {
        /// The channel to unsubscribe from.
        channel: WsChannel,
        /// Optional market scope for market-scoped channels.
        #[serde(default)]
        market_id: Option<MarketId>,
    },
    /// Request a full state snapshot.
    Sync,
    /// Application-level keepalive.
    Ping,
}

#[cfg(test)]
mod tests {
    use super::ClientCommand;
    use crate::{domain::ws::channel::WsChannel, types::MarketId};

    #[test]
    fn subscribe_parses_into_market() {
        let cmd: ClientCommand = serde_json::from_str(
            r#"{ "action": "subscribe", "channel": "market.book_update", "market_id": "0xabc" }"#,
        )
        .expect("valid subscribe");
        match cmd {
            ClientCommand::Subscribe { channel, market_id } => {
                assert_eq!(channel, WsChannel::MarketBookUpdate);
                assert_eq!(market_id, Some(MarketId::new("0xabc")));
            }
            _ => panic!("expected subscribe"),
        }
    }

    #[test]
    fn subscribe_without_market_none() {
        let cmd: ClientCommand =
            serde_json::from_str(r#"{ "action": "subscribe", "channel": "quant.report" }"#)
                .expect("valid subscribe");
        match cmd {
            ClientCommand::Subscribe { channel, market_id } => {
                assert_eq!(channel, WsChannel::QuantReport);
                assert_eq!(market_id, None);
            }
            _ => panic!("expected subscribe"),
        }
    }

    #[test]
    fn unknown_channel_rejected_name() {
        let err = serde_json::from_str::<ClientCommand>(
            r#"{ "action": "subscribe", "channel": "market.bogus" }"#,
        )
        .expect_err("unknown channel must fail");
        assert!(
            err.to_string().contains("market.bogus"),
            "error should name the offending channel: {err}"
        );
    }
}
