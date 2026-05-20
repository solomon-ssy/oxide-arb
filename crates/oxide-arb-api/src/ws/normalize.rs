//! Map Polymarket SDK WebSocket payloads into domain [`WsEvent`].

use oxide_arb_models::enums::common::TickSize;
use oxide_arb_models::types::{MarketId, Price, Shares, TokenId};
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BestBidAsk, BookUpdate, LastTradePrice, MarketResolved, PriceChange, TickSizeChange, WsMessage,
};

use super::event::{PriceLevel, PriceLevelDelta, WsEvent};

/// Normalized output of a single SDK WebSocket message (local newtype for [`From`]).
#[derive(Debug, Default)]
pub struct NormalizedWsEvents(pub Vec<WsEvent>);

impl From<WsMessage> for NormalizedWsEvents {
    fn from(msg: WsMessage) -> Self {
        let events = match msg {
            WsMessage::Book(book) => vec![WsEvent::from(&book)],
            WsMessage::PriceChange(pc) => price_change_events(&pc),
            WsMessage::BestBidAsk(bba) => vec![WsEvent::from(&bba)],
            WsMessage::TickSizeChange(tsc) => vec![WsEvent::from(&tsc)],
            WsMessage::LastTradePrice(ltp) => vec![WsEvent::from(&ltp)],
            WsMessage::MarketResolved(mr) => vec![WsEvent::from(&mr)],
            _ => Vec::new(),
        };
        Self(events)
    }
}

/// Convert a raw SDK market message into zero or more normalized events.
#[inline]
pub fn normalize_ws_message(msg: WsMessage) -> Vec<WsEvent> {
    NormalizedWsEvents::from(msg).0
}

impl From<&BookUpdate> for WsEvent {
    fn from(book: &BookUpdate) -> Self {
        let bids: Vec<PriceLevel> = book
            .bids
            .iter()
            .map(|l| PriceLevel {
                price: Price::new(l.price),
                size: Shares::new(l.size),
            })
            .collect();
        let asks: Vec<PriceLevel> = book
            .asks
            .iter()
            .map(|l| PriceLevel {
                price: Price::new(l.price),
                size: Shares::new(l.size),
            })
            .collect();

        Self::BookSnapshot {
            asset_id: TokenId::new(book.asset_id.to_string()),
            bids,
            asks,
            timestamp_ms: u64::try_from(book.timestamp).unwrap_or(0),
            hash: book.hash.clone().unwrap_or_default(),
        }
    }
}

fn price_change_events(pc: &PriceChange) -> Vec<WsEvent> {
    let timestamp_ms = u64::try_from(pc.timestamp).unwrap_or(0);
    pc.price_changes
        .iter()
        .map(|entry| WsEvent::PriceChange {
            asset_id: TokenId::new(entry.asset_id.to_string()),
            changes: vec![PriceLevelDelta {
                price: Price::new(entry.price),
                size: Shares::new(entry.size.unwrap_or_default()),
            }],
            timestamp_ms,
        })
        .collect()
}

impl From<&BestBidAsk> for WsEvent {
    fn from(bba: &BestBidAsk) -> Self {
        Self::BestBidAsk {
            asset_id: TokenId::new(bba.asset_id.to_string()),
            best_bid: Price::new(bba.best_bid),
            best_ask: Price::new(bba.best_ask),
            timestamp_ms: u64::try_from(bba.timestamp).unwrap_or(0),
        }
    }
}

impl From<&TickSizeChange> for WsEvent {
    fn from(tsc: &TickSizeChange) -> Self {
        Self::TickSizeChange {
            asset_id: TokenId::new(tsc.asset_id.to_string()),
            old_tick: TickSize::try_from(tsc.old_tick_size).unwrap_or(TickSize::Hundredth),
            new_tick: TickSize::try_from(tsc.new_tick_size).unwrap_or(TickSize::Hundredth),
        }
    }
}

impl From<&LastTradePrice> for WsEvent {
    fn from(ltp: &LastTradePrice) -> Self {
        Self::LastTradePrice {
            asset_id: TokenId::new(ltp.asset_id.to_string()),
            price: Price::new(ltp.price),
            timestamp_ms: u64::try_from(ltp.timestamp).unwrap_or(0),
        }
    }
}

impl From<&MarketResolved> for WsEvent {
    fn from(mr: &MarketResolved) -> Self {
        Self::MarketResolved {
            market_id: MarketId::new(format!("{:#x}", mr.market)),
            winning_token_id: TokenId::new(mr.winning_asset_id.to_string()),
            winning_outcome: mr.winning_outcome.clone(),
            asset_ids: mr
                .asset_ids
                .iter()
                .map(|id| TokenId::new(id.to_string()))
                .collect(),
            timestamp_ms: u64::try_from(mr.timestamp).unwrap_or(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::types::{B256, U256};
    use rust_decimal_macros::dec;

    #[test]
    fn maps_market_resolved_event() {
        let mr = MarketResolved::builder()
            .id("m1".into())
            .market(B256::ZERO)
            .asset_ids(vec![U256::from(1_u64), U256::from(2_u64)])
            .outcomes(vec!["Yes".into(), "No".into()])
            .winning_asset_id(U256::from(1_u64))
            .winning_outcome("Yes".into())
            .timestamp(1_700_000_000_000)
            .build();

        let events = normalize_ws_message(WsMessage::MarketResolved(mr));
        assert_eq!(events.len(), 1);
        match &events[0] {
            WsEvent::MarketResolved {
                winning_outcome,
                asset_ids,
                ..
            } => {
                assert_eq!(winning_outcome, "Yes");
                assert_eq!(asset_ids.len(), 2);
            }
            _ => panic!("expected MarketResolved"),
        }
    }

    #[test]
    fn maps_book_snapshot() {
        use polymarket_client_sdk_v2::clob::ws::types::response::OrderBookLevel;

        let book = BookUpdate::builder()
            .asset_id(U256::from(42_u64))
            .market(B256::ZERO)
            .timestamp(1000)
            .bids(vec![
                OrderBookLevel::builder()
                    .price(dec!(0.4))
                    .size(dec!(10))
                    .build(),
            ])
            .asks(vec![
                OrderBookLevel::builder()
                    .price(dec!(0.6))
                    .size(dec!(5))
                    .build(),
            ])
            .build();

        let events = normalize_ws_message(WsMessage::Book(book));
        assert!(matches!(events[0], WsEvent::BookSnapshot { .. }));
    }
}
