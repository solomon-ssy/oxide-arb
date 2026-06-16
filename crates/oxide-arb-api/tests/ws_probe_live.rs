//! Live CLOB WebSocket probes — isolate Polymarket rate limits vs local subscribe logic.
//!
//! ```bash
//! # Single-token SDK probe (mirrors production shard subscribe order)
//! cargo test -p oxide-arb-api probe_sdk_raw_single_token -- --ignored --nocapture
//!
//! # Single-token via ClobWsManager (production path)
//! cargo test -p oxide-arb-api probe_manager_single_token -- --ignored --nocapture
//!
//! # Scale probe (default 100 tokens on one SDK connection)
//! OXIDE_ARB_PROBE_TOKEN_COUNT=100 cargo test -p oxide-arb-api probe_sdk_scaled_tokens -- --ignored --nocapture
//! ```
//!
//! Optional env:
//! - `OXIDE_ARB_TEST_TOKEN_ID` — decimal CLOB token id (skips Gamma discovery)
//! - `OXIDE_ARB_PROBE_TOKEN_COUNT` — tokens for the scale probe (default `100`)
//! - `OXIDE_ARB_PROBE_TIMEOUT_SECS` — wait budget (default `45`)

use std::{
    collections::HashMap,
    env::var,
    slice::from_ref,
    str::FromStr,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use oxide_arb_api::{
    gamma::GammaClient,
    ws::{ClobWsManager, SubscriptionSource},
};
use oxide_arb_models::{
    config::{GammaConfig, PolymarketConfig, WebSocketConfig},
    domain::pipeline::PipelineEvent,
    types::TokenId,
};
use polymarket_client_sdk_v2::{
    clob::ws::Client as SdkWsClient, types::U256, ws::config::Config as SdkWsConfig,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_PROBE_TIMEOUT_SECS: u64 = 45;

/// Resolve one active decimal CLOB token id from Gamma or env override.
async fn resolve_token_id() -> TokenId {
    if let Ok(id) = var("OXIDE_ARB_TEST_TOKEN_ID") {
        return TokenId::new(id);
    }
    GammaClient::new(GammaConfig::default())
        .discover_active_token()
        .await
        .expect("discover active token from Gamma")
}

/// Collect up to `limit` active token ids from Gamma keyset pages.
async fn resolve_token_batch(limit: usize) -> Vec<TokenId> {
    GammaClient::new(GammaConfig::default())
        .discover_active_tokens(limit)
        .await
        .unwrap_or_else(|error| panic!("discover {limit} active tokens: {error}"))
}

fn probe_timeout() -> Duration {
    let secs = var("OXIDE_ARB_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

fn token_to_u256(token: &TokenId) -> U256 {
    U256::from_str(token.as_str()).expect("decimal token id")
}

/// Raw SDK path — identical subscribe order and multiplex loop as `ws/shard.rs`.
async fn run_sdk_multiplex_probe(
    label: &str,
    asset_ids: Vec<U256>,
    timeout: Duration,
) -> ProbeReport {
    let ws_url = PolymarketConfig::default().clob_ws_url;
    let client =
        SdkWsClient::new(&ws_url, SdkWsConfig::default()).expect("SDK WS client should construct");

    // Production order: resolutions first (enables custom_features / best_bid_ask).
    let mut resolution_stream = Box::pin(
        client
            .subscribe_market_resolutions(asset_ids.clone())
            .expect("subscribe_market_resolutions"),
    );
    let mut book_stream = Box::pin(
        client
            .subscribe_orderbook(asset_ids.clone())
            .expect("subscribe_orderbook"),
    );
    let mut price_stream = Box::pin(
        client
            .subscribe_prices(asset_ids.clone())
            .expect("subscribe_prices"),
    );
    let mut last_trade_stream = Box::pin(
        client
            .subscribe_last_trade_price(asset_ids.clone())
            .expect("subscribe_last_trade_price"),
    );
    let mut tick_size_stream = Box::pin(
        client
            .subscribe_tick_size_change(asset_ids.clone())
            .expect("subscribe_tick_size_change"),
    );
    let mut bbo_stream = Box::pin(
        client
            .subscribe_best_bid_ask(asset_ids)
            .expect("subscribe_best_bid_ask"),
    );

    let started = Instant::now();
    let deadline = started + timeout;
    let mut counts: HashMap<&'static str, u64> = HashMap::new();
    let mut stream_errors: HashMap<&'static str, u64> = HashMap::new();
    let mut stream_closed: HashMap<&'static str, bool> = HashMap::new();

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        tokio::select! {
            () = tokio::time::sleep(remaining) => break,
            item = book_stream.next() => {
                tally_sdk_item("book", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
            item = price_stream.next() => {
                tally_sdk_item("price", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
            item = resolution_stream.next() => {
                tally_sdk_item("resolution", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
            item = last_trade_stream.next() => {
                tally_sdk_item("last_trade", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
            item = tick_size_stream.next() => {
                tally_sdk_item("tick_size", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
            item = bbo_stream.next() => {
                tally_sdk_item("bbo", item, &mut counts, &mut stream_errors, &mut stream_closed);
            }
        }
    }

    ProbeReport {
        label: label.to_owned(),
        token_count: 0,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        sdk_message_counts: counts,
        sdk_stream_errors: stream_errors,
        sdk_streams_closed: stream_closed,
        manager_pipeline_counts: HashMap::new(),
        manager_got_book: false,
    }
}

fn tally_sdk_item<T, E: std::fmt::Display>(
    stream: &'static str,
    item: Option<Result<T, E>>,
    counts: &mut HashMap<&'static str, u64>,
    errors: &mut HashMap<&'static str, u64>,
    closed: &mut HashMap<&'static str, bool>,
) {
    match item {
        Some(Ok(_payload)) => {
            *counts.entry(stream).or_insert(0) += 1;
        }
        Some(Err(error)) => {
            *errors.entry(stream).or_insert(0) += 1;
            eprintln!("[sdk] {stream} error: {error}");
        }
        None => {
            closed.insert(stream, true);
            eprintln!("[sdk] {stream} stream closed");
        }
    }
}

#[derive(Debug)]
struct ProbeReport {
    label: String,
    token_count: u64,
    elapsed_ms: u64,
    sdk_message_counts: HashMap<&'static str, u64>,
    sdk_stream_errors: HashMap<&'static str, u64>,
    sdk_streams_closed: HashMap<&'static str, bool>,
    manager_pipeline_counts: HashMap<&'static str, u64>,
    manager_got_book: bool,
}

impl ProbeReport {
    fn sdk_total_messages(&self) -> u64 {
        self.sdk_message_counts.values().sum()
    }

    fn print_summary(&self) {
        println!("\n=== WS probe: {} ===", self.label);
        println!("tokens: {}", self.token_count);
        println!("elapsed_ms: {}", self.elapsed_ms);
        println!("sdk_messages_total: {}", self.sdk_total_messages());
        println!("sdk_by_stream: {:?}", self.sdk_message_counts);
        if !self.sdk_stream_errors.is_empty() {
            println!("sdk_stream_errors: {:?}", self.sdk_stream_errors);
        }
        if !self.sdk_streams_closed.is_empty() {
            println!("sdk_streams_closed: {:?}", self.sdk_streams_closed);
        }
        if !self.manager_pipeline_counts.is_empty() {
            println!("manager_pipeline: {:?}", self.manager_pipeline_counts);
            println!("manager_got_book: {}", self.manager_got_book);
        }
    }

    fn assert_sdk_received_market_data(&self) {
        let book = self.sdk_message_counts.get("book").copied().unwrap_or(0);
        let price = self.sdk_message_counts.get("price").copied().unwrap_or(0);
        let bbo = self.sdk_message_counts.get("bbo").copied().unwrap_or(0);
        assert!(
            book + price + bbo > 0,
            "expected book/price/bbo from SDK within {:?}; got {:?}",
            probe_timeout(),
            self.sdk_message_counts
        );
    }
}

#[tokio::test]
#[ignore = "live Polymarket CLOB WebSocket"]
async fn probe_sdk_raw_single_token() {
    let token = resolve_token_id().await;
    let asset = token_to_u256(&token);
    eprintln!("probe token: {}", token.as_str());

    let mut report = run_sdk_multiplex_probe("sdk_raw_1_token", vec![asset], probe_timeout()).await;
    report.token_count = 1;
    report.print_summary();
    report.assert_sdk_received_market_data();
}

#[tokio::test]
#[ignore = "live Polymarket CLOB WebSocket"]
async fn probe_sdk_orderbook_only() {
    let token = resolve_token_id().await;
    let asset = token_to_u256(&token);
    eprintln!("orderbook-only probe token: {}", token.as_str());

    let ws_url = PolymarketConfig::default().clob_ws_url;
    let client =
        SdkWsClient::new(&ws_url, SdkWsConfig::default()).expect("SDK WS client should construct");
    let mut book_stream = Box::pin(
        client
            .subscribe_orderbook(vec![asset])
            .expect("subscribe_orderbook"),
    );

    let timeout = probe_timeout();
    let started = Instant::now();
    let mut count = 0u64;
    let result = tokio::time::timeout(timeout, async {
        while let Some(item) = book_stream.next().await {
            match item {
                Ok(book) => {
                    count += 1;
                    eprintln!(
                        "book #{count}: asset={} bids={} asks={}",
                        book.asset_id,
                        book.bids.len(),
                        book.asks.len()
                    );
                    if !book.bids.is_empty() || !book.asks.is_empty() {
                        return;
                    }
                }
                Err(error) => eprintln!("book stream error: {error}"),
            }
        }
        panic!("book stream closed without snapshot");
    })
    .await;

    eprintln!(
        "orderbook-only elapsed_ms={} messages={count}",
        started.elapsed().as_millis()
    );
    result.expect("timed out on orderbook-only subscribe");
}

#[tokio::test]
#[ignore = "live Polymarket CLOB WebSocket"]
async fn probe_manager_single_token() {
    let token = resolve_token_id().await;
    eprintln!("probe token: {}", token.as_str());

    let shutdown = CancellationToken::new();
    let manager = ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        shutdown.clone(),
        None,
        None,
    );
    manager.subscribe_tokens(SubscriptionSource::Engine, from_ref(&token));
    let events = manager.events();

    let timeout = probe_timeout();
    let started = Instant::now();
    let mut counts: HashMap<&'static str, u64> = HashMap::new();
    let mut got_book = false;

    let result = tokio::time::timeout(timeout, async {
        loop {
            let event = events.recv_async().await.expect("pipeline channel open");
            match &event {
                PipelineEvent::BookSnapshot(cmd) if cmd.asset_id == token => {
                    *counts.entry("book_snapshot").or_insert(0) += 1;
                    if !cmd.bids.levels.is_empty() || !cmd.asks.levels.is_empty() {
                        got_book = true;
                    }
                }
                PipelineEvent::PriceDelta(cmd) if cmd.asset_id == token => {
                    *counts.entry("price_delta").or_insert(0) += 1;
                }
                PipelineEvent::BestBidAsk { asset_id, .. } if *asset_id == token => {
                    *counts.entry("bbo").or_insert(0) += 1;
                }
                PipelineEvent::ShardStatus { .. } => {
                    *counts.entry("shard_status").or_insert(0) += 1;
                }
                _ => {
                    *counts.entry("other").or_insert(0) += 1;
                }
            }
            if got_book {
                return;
            }
        }
    })
    .await;

    shutdown.cancel();

    let report = ProbeReport {
        label: "manager_1_token".into(),
        token_count: 1,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        sdk_message_counts: HashMap::new(),
        sdk_stream_errors: HashMap::new(),
        sdk_streams_closed: HashMap::new(),
        manager_pipeline_counts: counts,
        manager_got_book: got_book,
    };
    report.print_summary();
    result.expect("timed out waiting for BookSnapshot with depth via ClobWsManager");
    assert!(got_book, "manager path should deliver a non-empty book");
}

#[tokio::test]
#[ignore = "live Polymarket CLOB WebSocket"]
async fn probe_sdk_scaled_tokens() {
    let count: usize = var("OXIDE_ARB_PROBE_TOKEN_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let tokens = resolve_token_batch(count).await;
    assert!(
        !tokens.is_empty(),
        "need at least one token from Gamma for scale probe"
    );
    let asset_ids: Vec<U256> = tokens.iter().map(token_to_u256).collect();
    eprintln!(
        "scale probe: requested={count} resolved={} first={}",
        asset_ids.len(),
        tokens[0].as_str()
    );

    let mut report = run_sdk_multiplex_probe("sdk_raw_scaled", asset_ids, probe_timeout()).await;
    report.token_count = tokens.len() as u64;
    report.print_summary();

    // Scale probe is diagnostic: we print counts and only hard-fail on total silence.
    assert!(
        report.sdk_total_messages() > 0,
        "scaled subscribe produced zero SDK messages — likely Polymarket limit or silent drop"
    );
}
