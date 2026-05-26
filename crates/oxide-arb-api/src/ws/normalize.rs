//! Map Polymarket SDK WebSocket payloads into domain [`PipelineEvent`].

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use num_traits::ToPrimitive;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::domain::pipeline::{
    BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
};
use oxide_arb_models::enums::common::TickSize;
use oxide_arb_models::types::{MarketId, Price, Shares, TokenId};
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BestBidAsk, BookUpdate, LastTradePrice, MarketResolved, PriceChange, TickSizeChange, WsMessage,
};

use super::token_intern::intern_u256;

thread_local! {
    static DELTA_GROUP: RefCell<AHashMap<TokenId, Vec<PriceLevelDelta>>> =
        RefCell::new(AHashMap::new());
}

/// Convert a raw SDK market message into zero or more normalized events.
///
/// `ws_ingress` must be captured before parsing (typically `Instant::now()` in the shard).
#[inline]
pub fn normalize_ws_message(msg: WsMessage, ws_ingress: Instant) -> Vec<PipelineEvent> {
    match msg {
        WsMessage::Book(book) => vec![book_update_to_event(&book, ws_ingress)],
        WsMessage::PriceChange(pc) => price_change_events(&pc, ws_ingress),
        WsMessage::BestBidAsk(bba) => vec![best_bid_ask_event(&bba, ws_ingress)],
        WsMessage::TickSizeChange(tsc) => vec![tick_size_event(&tsc, ws_ingress)],
        WsMessage::LastTradePrice(ltp) => vec![last_trade_event(&ltp, ws_ingress)],
        WsMessage::MarketResolved(mr) => vec![market_resolved_event(&mr, ws_ingress)],
        _ => Vec::new(),
    }
}

#[inline]
const fn ingress_trace(ws_ingress: Instant, ws_timestamp_ms: u64) -> IngressTrace {
    IngressTrace::new(ws_ingress, ws_timestamp_ms)
}

fn book_update_to_event(book: &BookUpdate, ws_ingress: Instant) -> PipelineEvent {
    let timestamp_ms = ToPrimitive::to_u64(&book.timestamp.max(0)).unwrap_or(0);
    let bid_cap = book.bids.len();
    let ask_cap = book.asks.len();
    let mut bids = Vec::with_capacity(bid_cap);
    let mut asks = Vec::with_capacity(ask_cap);
    for level in &book.bids {
        bids.push(BookLevel::from_decimal_unchecked(
            Price::new(level.price),
            Shares::new(level.size),
        ));
    }
    for level in &book.asks {
        asks.push(BookLevel::from_decimal_unchecked(
            Price::new(level.price),
            Shares::new(level.size),
        ));
    }

    PipelineEvent::BookSnapshot(BookSnapshotCmd {
        asset_id: intern_u256(book.asset_id),
        bids: BookSideData::from_levels(Arc::from(bids)),
        asks: BookSideData::from_levels(Arc::from(asks)),
        timestamp_ms,
        trace: ingress_trace(ws_ingress, timestamp_ms),
    })
}

fn price_change_events(pc: &PriceChange, ws_ingress: Instant) -> Vec<PipelineEvent> {
    let timestamp_ms = ToPrimitive::to_u64(&pc.timestamp.max(0)).unwrap_or(0);
    let trace = ingress_trace(ws_ingress, timestamp_ms);

    DELTA_GROUP.with(|group| {
        let mut grouped = group.borrow_mut();
        grouped.clear();

        for entry in &pc.price_changes {
            let asset_id = intern_u256(entry.asset_id);
            grouped.entry(asset_id).or_default().push(PriceLevelDelta {
                price: Price::new(entry.price),
                size: Shares::new(entry.size.unwrap_or_default()),
            });
        }

        grouped
            .drain()
            .map(|(asset_id, changes)| {
                PipelineEvent::PriceDelta(PriceDeltaCmd {
                    asset_id,
                    changes: Arc::from(changes),
                    timestamp_ms,
                    trace,
                })
            })
            .collect()
    })
}

fn best_bid_ask_event(bba: &BestBidAsk, ws_ingress: Instant) -> PipelineEvent {
    let timestamp_ms = ToPrimitive::to_u64(&bba.timestamp.max(0)).unwrap_or(0);
    PipelineEvent::BestBidAsk {
        asset_id: intern_u256(bba.asset_id),
        best_bid: Price::new(bba.best_bid),
        best_ask: Price::new(bba.best_ask),
        timestamp_ms,
        trace: ingress_trace(ws_ingress, timestamp_ms),
    }
}

fn tick_size_event(tsc: &TickSizeChange, ws_ingress: Instant) -> PipelineEvent {
    PipelineEvent::TickSizeChange {
        asset_id: intern_u256(tsc.asset_id),
        old_tick: TickSize::try_from(tsc.old_tick_size).unwrap_or(TickSize::Hundredth),
        new_tick: TickSize::try_from(tsc.new_tick_size).unwrap_or(TickSize::Hundredth),
        trace: ingress_trace(ws_ingress, 0),
    }
}

fn last_trade_event(ltp: &LastTradePrice, ws_ingress: Instant) -> PipelineEvent {
    let timestamp_ms = ToPrimitive::to_u64(&ltp.timestamp.max(0)).unwrap_or(0);
    PipelineEvent::LastTradePrice {
        asset_id: intern_u256(ltp.asset_id),
        price: Price::new(ltp.price),
        timestamp_ms,
        trace: ingress_trace(ws_ingress, timestamp_ms),
    }
}

fn market_resolved_event(mr: &MarketResolved, ws_ingress: Instant) -> PipelineEvent {
    let timestamp_ms = ToPrimitive::to_u64(&mr.timestamp.max(0)).unwrap_or(0);
    PipelineEvent::MarketResolved {
        market_id: MarketId::new(format!("{:#x}", mr.market)),
        winning_token_id: intern_u256(mr.winning_asset_id),
        winning_outcome: mr.winning_outcome.clone(),
        asset_ids: Arc::from(
            mr.asset_ids
                .iter()
                .copied()
                .map(intern_u256)
                .collect::<Vec<_>>(),
        ),
        timestamp_ms,
        trace: ingress_trace(ws_ingress, timestamp_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::domain::pipeline::PipelineEvent;
    use polymarket_client_sdk_v2::types::{B256, U256};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

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

        let events = normalize_ws_message(WsMessage::MarketResolved(mr), Instant::now());
        assert_eq!(events.len(), 1);
        match &events[0] {
            PipelineEvent::MarketResolved {
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
    fn maps_book_snapshot_with_arc_levels() {
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

        let events = normalize_ws_message(WsMessage::Book(book), Instant::now());
        match &events[0] {
            PipelineEvent::BookSnapshot(cmd) => {
                assert_eq!(cmd.bids.levels.len(), 1);
                assert_eq!(Arc::strong_count(&cmd.bids.levels), 1);
            }
            _ => panic!("expected BookSnapshot"),
        }
    }

    #[test]
    fn price_change_reuses_thread_local_buffer() {
        use polymarket_client_sdk_v2::clob::types::Side;
        use polymarket_client_sdk_v2::clob::ws::types::response::PriceChangeBatchEntry;

        let pc = PriceChange::builder()
            .market(B256::ZERO)
            .timestamp(1000)
            .price_changes(vec![
                PriceChangeBatchEntry::builder()
                    .asset_id(U256::from(1_u64))
                    .price(dec!(0.5))
                    .size(dec!(10))
                    .side(Side::Buy)
                    .build(),
                PriceChangeBatchEntry::builder()
                    .asset_id(U256::from(2_u64))
                    .price(dec!(0.6))
                    .size(dec!(5))
                    .side(Side::Sell)
                    .build(),
            ])
            .build();

        let events = normalize_ws_message(WsMessage::PriceChange(pc), Instant::now());
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], PipelineEvent::PriceDelta(_)));
    }
}
