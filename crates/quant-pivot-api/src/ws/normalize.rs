//! Map Polymarket SDK WebSocket payloads into domain [`PipelineEvent`].

use std::{cell::RefCell, cmp::Reverse, sync::Arc, time::Instant};

use ahash::AHashMap;
use num_traits::ToPrimitive;
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, LastTradePrice, MarketResolved, PriceChange, TickSizeChange, WsMessage,
};
use quant_pivot_models::{
    domain::{
        data_plane::pipeline::{
            BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent, PriceDeltaCmd,
            PriceLevelDelta,
        },
        market::book::BookLevel,
    },
    enums::common::TickSize,
    types::{MarketId, Price, Shares, TokenKey},
};

use super::{
    ingest_hooks::BookLevelRejectHook,
    token_resolver::{TokenKeyResolver, UnregisteredToken},
};
use crate::clob::ClobSide;

thread_local! {
    static DELTA_GROUP: RefCell<AHashMap<TokenKey, Vec<PriceLevelDelta>>> =
        RefCell::new(AHashMap::new());
}

/// Convert a raw SDK market message into zero or more normalized events.
///
/// `ws_ingress` must be captured before parsing (typically `Instant::now` in the shard).
#[inline]
pub fn normalize_ws_message(
    msg: WsMessage,
    ws_ingress: Instant,
    on_level_rejected: Option<&BookLevelRejectHook>,
    tokens: &dyn TokenKeyResolver,
) -> Result<Vec<PipelineEvent>, UnregisteredToken> {
    match msg {
        WsMessage::Book(book) => book_update_to_event(&book, ws_ingress, on_level_rejected, tokens)
            .map(|event| vec![event]),
        WsMessage::PriceChange(pc) => {
            price_change_events(&pc, ws_ingress, on_level_rejected, tokens)
        }
        WsMessage::TickSizeChange(tsc) => tick_size_events(&tsc, ws_ingress, tokens),
        WsMessage::LastTradePrice(ltp) => {
            last_trade_event(&ltp, ws_ingress, tokens).map(|event| vec![event])
        }
        WsMessage::MarketResolved(mr) => {
            market_resolved_event(&mr, ws_ingress, tokens).map(|event| vec![event])
        }
        _ => Ok(Vec::new()),
    }
}

fn push_level(
    levels: &mut Vec<BookLevel>,
    price: Price,
    size: Shares,
    on_level_rejected: Option<&BookLevelRejectHook>,
) {
    match BookLevel::from_decimal(price, size) {
        Ok(level) if level.size.is_positive() => levels.push(level),
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, ?price, ?size, "rejecting invalid WS book level");
            if let Some(hook) = on_level_rejected {
                hook();
            }
        }
    }
}

fn book_update_to_event(
    book: &BookUpdate,
    ws_ingress: Instant,
    on_level_rejected: Option<&BookLevelRejectHook>,
    tokens: &dyn TokenKeyResolver,
) -> Result<PipelineEvent, UnregisteredToken> {
    let timestamp_ms = ToPrimitive::to_u64(&book.timestamp.max(0)).unwrap_or(0);
    let mut bids = Vec::with_capacity(book.bids.len());
    let mut asks = Vec::with_capacity(book.asks.len());
    for level in &book.bids {
        push_level(
            &mut bids,
            Price::new(level.price),
            Shares::new(level.size),
            on_level_rejected,
        );
    }
    for level in &book.asks {
        push_level(
            &mut asks,
            Price::new(level.price),
            Shares::new(level.size),
            on_level_rejected,
        );
    }

    bids.sort_by_key(|b| Reverse(b.price));
    asks.sort_by_key(|a| a.price);

    Ok(PipelineEvent::BookSnapshot(BookSnapshotCmd {
        token: tokens
            .resolve(book.asset_id)
            .ok_or(UnregisteredToken(book.asset_id))?,
        bids: BookSideData::from_levels(Arc::from(bids)),
        asks: BookSideData::from_levels(Arc::from(asks)),
        timestamp_ms,
        trace: IngressTrace::new(ws_ingress, timestamp_ms),
    }))
}

fn price_change_events(
    pc: &PriceChange,
    ws_ingress: Instant,
    on_level_rejected: Option<&BookLevelRejectHook>,
    tokens: &dyn TokenKeyResolver,
) -> Result<Vec<PipelineEvent>, UnregisteredToken> {
    let timestamp_ms = ToPrimitive::to_u64(&pc.timestamp.max(0)).unwrap_or(0);
    let trace = IngressTrace::new(ws_ingress, timestamp_ms);

    DELTA_GROUP.with(|group| {
        let mut grouped = group.borrow_mut();
        grouped.clear();

        for entry in &pc.price_changes {
            let token = tokens
                .resolve(entry.asset_id)
                .ok_or(UnregisteredToken(entry.asset_id))?;
            let price = Price::new(entry.price);
            let share_qty = Shares::new(entry.size.unwrap_or_default());
            let book_side = match ClobSide::try_from(entry.side) {
                Ok(clob_side) => clob_side.0,
                Err(error) => {
                    tracing::warn!(%error, ?price, ?share_qty, side = ?entry.side, "rejecting WS price change with unknown side");
                    if let Some(hook) = on_level_rejected {
                        hook();
                    }
                    continue;
                }
            };
            match BookLevel::from_decimal(price, share_qty) {
                Ok(level) => {
                    grouped.entry(token).or_default().push(PriceLevelDelta {
                        price: level.price_decimal(),
                        size: level.size_decimal(),
                        side: book_side,
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, ?price, ?share_qty, "rejecting invalid WS price change");
                    if let Some(hook) = on_level_rejected {
                        hook();
                    }
                }
            }
        }

        Ok(grouped
            .drain()
            .map(|(token, changes)| {
                PipelineEvent::PriceDelta(PriceDeltaCmd {
                    token,
                    changes: Arc::from(changes),
                    timestamp_ms,
                    trace,
                })
            })
            .collect())
    })
}

fn tick_size_events(
    tsc: &TickSizeChange,
    ws_ingress: Instant,
    tokens: &dyn TokenKeyResolver,
) -> Result<Vec<PipelineEvent>, UnregisteredToken> {
    let Ok(old_tick) = TickSize::try_from(tsc.old_tick_size) else {
        tracing::warn!(
            asset_id = %tsc.asset_id,
            old_tick = %tsc.old_tick_size,
            new_tick = %tsc.new_tick_size,
            "dropping WS tick-size change with unsupported old tick"
        );
        return Ok(Vec::new());
    };
    let Ok(new_tick) = TickSize::try_from(tsc.new_tick_size) else {
        tracing::warn!(
            asset_id = %tsc.asset_id,
            old_tick = %tsc.old_tick_size,
            new_tick = %tsc.new_tick_size,
            "dropping WS tick-size change with unsupported new tick"
        );
        return Ok(Vec::new());
    };
    Ok(vec![PipelineEvent::TickSizeChange {
        token: tokens
            .resolve(tsc.asset_id)
            .ok_or(UnregisteredToken(tsc.asset_id))?,
        old_tick,
        new_tick,
        trace: IngressTrace::new(ws_ingress, 0),
    }])
}

fn last_trade_event(
    ltp: &LastTradePrice,
    ws_ingress: Instant,
    tokens: &dyn TokenKeyResolver,
) -> Result<PipelineEvent, UnregisteredToken> {
    let timestamp_ms = ToPrimitive::to_u64(&ltp.timestamp.max(0)).unwrap_or(0);
    let side = ltp
        .side
        .and_then(|side| ClobSide::try_from(side).ok().map(|side| side.0));
    Ok(PipelineEvent::LastTradePrice {
        market_id: MarketId::new(format!("{:#x}", ltp.market)),
        token: tokens
            .resolve(ltp.asset_id)
            .ok_or(UnregisteredToken(ltp.asset_id))?,
        price: Price::new(ltp.price),
        side,
        size: ltp.size.map(Shares::new),
        fee_rate_bps: ltp.fee_rate_bps,
        transaction_hash: None,
        timestamp_ms,
        trace: IngressTrace::new(ws_ingress, timestamp_ms),
    })
}

fn market_resolved_event(
    mr: &MarketResolved,
    ws_ingress: Instant,
    tokens: &dyn TokenKeyResolver,
) -> Result<PipelineEvent, UnregisteredToken> {
    let timestamp_ms = ToPrimitive::to_u64(&mr.timestamp.max(0)).unwrap_or(0);
    Ok(PipelineEvent::MarketResolved {
        market_id: MarketId::new(format!("{:#x}", mr.market)),
        winning_token: tokens
            .resolve(mr.winning_asset_id)
            .ok_or(UnregisteredToken(mr.winning_asset_id))?,
        winning_outcome: mr.winning_outcome.clone(),
        tokens: Arc::from(
            mr.asset_ids
                .iter()
                .copied()
                .map(|token| tokens.resolve(token).ok_or(UnregisteredToken(token)))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        timestamp_ms,
        trace: IngressTrace::new(ws_ingress, timestamp_ms),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use polymarket_client_sdk_v2::{
        clob::{
            types::Side,
            ws::types::response::{OrderBookLevel, PriceChangeBatchEntry},
        },
        types::{B256, U256},
    };
    use quant_pivot_models::domain::data_plane::pipeline::PipelineEvent;
    use rust_decimal_macros::dec;

    use super::*;

    fn test_token_resolver(token: U256) -> Option<TokenKey> {
        (token != U256::MAX).then(|| TokenKey::new(token.to::<u32>()))
    }

    #[test]
    fn maps_tick_size_cent() {
        let tsc = TickSizeChange::builder()
            .asset_id(U256::from(7_u64))
            .market(B256::ZERO)
            .old_tick_size(dec!(0.01))
            .new_tick_size(dec!(0.005))
            .timestamp(1_700_000_000_000)
            .build();
        let events = normalize_ws_message(
            WsMessage::TickSizeChange(tsc),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered token");
        match &events[..] {
            [
                PipelineEvent::TickSizeChange {
                    old_tick, new_tick, ..
                },
            ] => {
                assert_eq!(*old_tick, TickSize::Hundredth);
                assert_eq!(*new_tick, TickSize::HalfCent);
            }
            other => panic!("expected TickSizeChange, got {other:?}"),
        }

        let tsc = TickSizeChange::builder()
            .asset_id(U256::from(7_u64))
            .market(B256::ZERO)
            .old_tick_size(dec!(0.005))
            .new_tick_size(dec!(0.0025))
            .timestamp(1_700_000_000_000)
            .build();
        let events = normalize_ws_message(
            WsMessage::TickSizeChange(tsc),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered token");
        match &events[..] {
            [
                PipelineEvent::TickSizeChange {
                    old_tick, new_tick, ..
                },
            ] => {
                assert_eq!(*old_tick, TickSize::HalfCent);
                assert_eq!(*new_tick, TickSize::QuarterCent);
            }
            other => panic!("expected TickSizeChange, got {other:?}"),
        }
    }

    #[test]
    fn drops_tick_without_fallback() {
        let tsc = TickSizeChange::builder()
            .asset_id(U256::from(7_u64))
            .market(B256::ZERO)
            .old_tick_size(dec!(0.01))
            .new_tick_size(dec!(0.00001))
            .timestamp(1_700_000_000_000)
            .build();
        let events = normalize_ws_message(
            WsMessage::TickSizeChange(tsc),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered token");
        assert!(events.is_empty());
    }

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

        let events = normalize_ws_message(
            WsMessage::MarketResolved(mr),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered tokens");
        assert_eq!(events.len(), 1);
        match &events[0] {
            PipelineEvent::MarketResolved {
                winning_outcome,
                tokens,
                ..
            } => {
                assert_eq!(winning_outcome, "Yes");
                assert_eq!(tokens.len(), 2);
            }
            _ => panic!("expected MarketResolved"),
        }
    }

    #[test]
    fn maps_book_snapshot_levels() {
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

        let events = normalize_ws_message(
            WsMessage::Book(book),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered token");
        match &events[0] {
            PipelineEvent::BookSnapshot(cmd) => {
                assert_eq!(cmd.bids.levels.len(), 1);
                assert_eq!(Arc::strong_count(&cmd.bids.levels), 1);
            }
            _ => panic!("expected BookSnapshot"),
        }
    }

    #[test]
    fn rejects_invalid_book_level() {
        let rejects = Arc::new(AtomicU32::new(0));
        let hook: BookLevelRejectHook = {
            let rejects = Arc::clone(&rejects);
            Arc::new(move || {
                rejects.fetch_add(1, Ordering::Relaxed);
            })
        };

        let book = BookUpdate::builder()
            .asset_id(U256::from(42_u64))
            .market(B256::ZERO)
            .timestamp(1000)
            .bids(vec![
                OrderBookLevel::builder()
                    .price(dec!(1.5))
                    .size(dec!(10))
                    .build(),
            ])
            .asks(vec![])
            .build();

        let events = normalize_ws_message(
            WsMessage::Book(book),
            Instant::now(),
            Some(&hook),
            &test_token_resolver,
        )
        .expect("registered token");
        match &events[0] {
            PipelineEvent::BookSnapshot(cmd) => assert!(cmd.bids.levels.is_empty()),
            _ => panic!("expected BookSnapshot"),
        }
        assert_eq!(rejects.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn keeps_valid_invalid_present() {
        let book = BookUpdate::builder()
            .asset_id(U256::from(42_u64))
            .market(B256::ZERO)
            .timestamp(1000)
            .bids(vec![
                OrderBookLevel::builder()
                    .price(dec!(0.5))
                    .size(dec!(10))
                    .build(),
                OrderBookLevel::builder()
                    .price(dec!(1.5))
                    .size(dec!(10))
                    .build(),
            ])
            .asks(vec![])
            .build();

        let events = normalize_ws_message(
            WsMessage::Book(book),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered token");
        match &events[0] {
            PipelineEvent::BookSnapshot(cmd) => {
                assert_eq!(cmd.bids.levels.len(), 1);
                assert_eq!(cmd.bids.levels[0].price_decimal().inner(), dec!(0.5));
            }
            _ => panic!("expected BookSnapshot"),
        }
    }

    #[test]
    fn price_change_reuses_buffer() {
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

        let events = normalize_ws_message(
            WsMessage::PriceChange(pc),
            Instant::now(),
            None,
            &test_token_resolver,
        )
        .expect("registered tokens");
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], PipelineEvent::PriceDelta(_)));
    }

    #[test]
    fn unregistered_token_fails_message() {
        let book = BookUpdate::builder()
            .asset_id(U256::from(42_u64))
            .market(B256::ZERO)
            .timestamp(1000)
            .bids(Vec::new())
            .asks(Vec::new())
            .build();

        let error = normalize_ws_message(WsMessage::Book(book), Instant::now(), None, &|_| None)
            .expect_err("unknown token must fail closed");
        assert_eq!(error, UnregisteredToken(U256::from(42_u64)));
    }
}
