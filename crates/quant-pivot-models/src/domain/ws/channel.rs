//! Strongly-typed WebSocket channel taxonomy.
//!
//! [`WsChannel`] is the closed set of server-push channels a client can
//! subscribe to. Each channel knows three things that previously lived as
//! scattered string magic values:
//!
//! 1. its on-the-wire name (`as_str` / [`FromStr`]),
//! 2. the RBAC [`ResourceType`] guarding it (`resource`), so a socket can never
//!    bypass the same `(resource, Read)` check as its HTTP counterpart, and
//! 3. whether it is fanned out globally or per-market (`scope`).
//!
//! [`SubscriptionKey`] pairs a channel with its optional market scope; it is the
//! single value stored in a session's subscription set and produced by the event
//! fan-out, so there is no longer any `"channel:market"` string parsing anywhere.

use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use serde_with::DeserializeFromStr;

use crate::{enums::rbac::ResourceType, types::MarketId};

/// Fan-out scope of a [`WsChannel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelScope {
    /// One stream for every subscriber (e.g. system status, `PnL` updates).
    Global,
    /// One stream per market; the [`MarketId`] is part of the fan-out key so
    /// only sessions watching that market receive the push.
    Market,
}

/// The closed set of server-to-client push channels.
///
/// The wire name is `"{namespace}.{leaf}"`; the namespace determines the RBAC
/// [`ResourceType`]. Adding a variant forces a compile error in [`Self::as_str`],
/// [`Self::resource`], and [`Self::scope`], keeping the taxonomy exhaustive.
///
/// `DeserializeFromStr` routes deserialization through [`FromStr`], so an
/// unknown channel is rejected with the precise [`UnknownChannel`] message and
/// the session loop can answer with an exact error frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, DeserializeFromStr)]
pub enum WsChannel {
    /// Connection/system status snapshot and subsequent status changes.
    SystemStatus,
    /// Operator-facing alerts (level + message).
    SystemAlert,
    /// Market resolution (the `market_id` rides in the payload, not the key).
    MarketResolved,
    /// Coalesced per-market order-book snapshots (market-scoped fan-out).
    MarketBookUpdate,
    /// A runtime-config version was activated.
    ConfigActivated,
    /// Durable recommendation-report artifact lifecycle revision hints.
    QuantReport,
    /// Durable report-run queue/lease lifecycle revision hints.
    QuantReportRun,
    /// Order-intent lifecycle events (created / approved / rejected / cancelled
    /// / expired / invalidated), discriminated by the payload's `event` field.
    QuantIntent,
    /// Recommendation-owned entry-condition instance revision hints.
    QuantCondition,
    /// Materialization / replay run lifecycle update for dashboard clients.
    MaterializationRunUpdate,
    /// Durable feedback-cycle stage-event revision hint.
    ResearchFeedback,
    /// Reconciliation row detect/update lifecycle (worker + operator resolve),
    /// discriminated by the payload; a revision hint for the reconciliation
    /// queue + recovery panel (the list is always re-fetched over REST).
    QuantReconciliation,
    /// Settlement-redeem state transition (submitted / confirmed / failed /
    /// `manual_required`); a revision hint for the settlement ledger.
    QuantSettlement,
}

impl WsChannel {
    /// Every channel, used by exhaustiveness tests and reverse lookup.
    pub const ALL: [Self; 13] = [
        Self::SystemStatus,
        Self::SystemAlert,
        Self::MarketResolved,
        Self::MarketBookUpdate,
        Self::ConfigActivated,
        Self::QuantReport,
        Self::QuantReportRun,
        Self::QuantIntent,
        Self::QuantCondition,
        Self::MaterializationRunUpdate,
        Self::ResearchFeedback,
        Self::QuantReconciliation,
        Self::QuantSettlement,
    ];

    /// The on-the-wire channel name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SystemStatus => "system.status",
            Self::SystemAlert => "system.alert",
            Self::MarketResolved => "market.resolved",
            Self::MarketBookUpdate => "market.book_update",
            Self::ConfigActivated => "config.activated",
            Self::QuantReport => "quant.report",
            Self::QuantReportRun => "quant.report_run",
            Self::QuantIntent => "quant.intent",
            Self::QuantCondition => "quant.condition",
            Self::MaterializationRunUpdate => "materialization.run_update",
            Self::ResearchFeedback => "research.feedback",
            Self::QuantReconciliation => "quant.reconciliation",
            Self::QuantSettlement => "quant.settlement",
        }
    }

    /// The RBAC resource a session must hold `Read` on to subscribe.
    ///
    /// This mirrors the HTTP route authorization for the same data, so a
    /// WebSocket subscription can never read data a REST call would deny.
    #[must_use]
    pub const fn resource(self) -> ResourceType {
        match self {
            Self::SystemStatus | Self::SystemAlert => ResourceType::System,
            Self::MarketResolved | Self::MarketBookUpdate => ResourceType::Market,
            Self::QuantReport | Self::QuantReportRun | Self::QuantCondition => {
                ResourceType::QuantReport
            }
            Self::QuantIntent => ResourceType::OrderIntent,
            Self::MaterializationRunUpdate | Self::ResearchFeedback => {
                ResourceType::Materialization
            }
            Self::ConfigActivated => ResourceType::DecisionPolicySnapshot,
            Self::QuantReconciliation => ResourceType::Reconciliation,
            Self::QuantSettlement => ResourceType::SettlementRedeem,
        }
    }

    /// Whether this channel fans out globally or per-market.
    #[must_use]
    pub const fn scope(self) -> ChannelScope {
        match self {
            Self::MarketBookUpdate => ChannelScope::Market,
            _ => ChannelScope::Global,
        }
    }
}

impl Display for WsChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}

/// Error returned when a client names a channel that does not exist.
///
/// The session loop turns this into an `error` envelope and refuses the
/// subscription (fail-closed): an unknown channel can never be silently retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownChannel(pub String);

impl Display for UnknownChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "unknown ws channel: {}", self.0)
    }
}

impl Error for UnknownChannel {}

impl FromStr for WsChannel {
    type Err = UnknownChannel;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|channel| channel.as_str() == s)
            .ok_or_else(|| UnknownChannel(s.to_owned()))
    }
}

/// A concrete fan-out / subscription target: a channel plus its market scope.
///
/// This is the typed value stored in a session's subscription set and produced
/// by the event fan-out. Equality + hashing make membership checks exact, so the
/// broadcaster never compares stringly-typed keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    /// The channel being subscribed to.
    pub channel: WsChannel,
    /// The market scope, present iff the channel is [`ChannelScope::Market`].
    pub market: Option<MarketId>,
}

impl SubscriptionKey {
    /// Build a key, normalizing the market against the channel's scope: a market
    /// supplied for a [`ChannelScope::Global`] channel is dropped (it would never
    /// match), so a client cannot fragment a global stream by passing a market.
    #[must_use]
    pub fn new(channel: WsChannel, market: Option<MarketId>) -> Self {
        let market = match channel.scope() {
            ChannelScope::Market => market,
            ChannelScope::Global => None,
        };
        Self { channel, market }
    }

    /// A global (unscoped) subscription key.
    #[must_use]
    pub const fn global(channel: WsChannel) -> Self {
        Self {
            channel,
            market: None,
        }
    }

    /// A market-scoped subscription key.
    #[must_use]
    pub const fn scoped(channel: WsChannel, market: MarketId) -> Self {
        Self {
            channel,
            market: Some(market),
        }
    }
}

impl Display for SubscriptionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match &self.market {
            Some(market) => write!(f, "{}:{}", self.channel.as_str(), market.as_str()),
            None => f.write_str(self.channel.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{ChannelScope, SubscriptionKey, WsChannel};
    use crate::{enums::rbac::ResourceType, types::MarketId};

    #[test]
    fn materialization_channel_reads_resource() {
        // The run-update channel is a materialization-run lifecycle stream, so it
        // gates on `materialization:read` — not `publication:read`.
        assert_eq!(
            WsChannel::MaterializationRunUpdate.resource(),
            ResourceType::Materialization
        );
    }

    #[test]
    fn research_feedback_channel_contract() {
        let channel = WsChannel::from_str("research.feedback").expect("canonical feedback channel");
        assert_eq!(channel.resource(), ResourceType::Materialization);
        assert_eq!(channel.scope(), ChannelScope::Global);
    }

    #[test]
    fn channel_round_trips_str() {
        for channel in WsChannel::ALL {
            let parsed = WsChannel::from_str(channel.as_str()).expect("known channel");
            assert_eq!(parsed, channel);
        }
    }

    #[test]
    fn str_rejects_unknown_channel() {
        assert!(WsChannel::from_str("does.not_exist").is_err());
    }

    #[test]
    fn only_book_update_scoped() {
        for channel in WsChannel::ALL {
            let expected = if channel == WsChannel::MarketBookUpdate {
                ChannelScope::Market
            } else {
                ChannelScope::Global
            };
            assert_eq!(channel.scope(), expected, "scope of {channel}");
        }
    }

    #[test]
    fn drops_market_global_channels() {
        let key = SubscriptionKey::new(WsChannel::SystemStatus, Some(MarketId::new("0xabc")));
        assert_eq!(key.market, None, "global channel must not carry a market");

        let scoped =
            SubscriptionKey::new(WsChannel::MarketBookUpdate, Some(MarketId::new("0xabc")));
        assert_eq!(scoped.market, Some(MarketId::new("0xabc")));
    }

    #[test]
    fn display_matches_wire_format() {
        assert_eq!(
            SubscriptionKey::global(WsChannel::QuantReport).to_string(),
            "quant.report"
        );
        assert_eq!(
            SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new("0xabc"))
                .to_string(),
            "market.book_update:0xabc"
        );
    }
}
