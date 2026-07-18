//! Live Polymarket RTDS contract smoke.
//!
//! Run (public network, no credentials):
//! `cargo test -p quant-pivot-api --test integration rtds_live -- --ignored --nocapture`

use std::{collections::BTreeSet, slice, time::Duration};

use futures_util::future::join_all;
use quant_pivot_api::rtds::{PolymarketRtdsSource, PolymarketRtdsStream, RtdsCryptoSource};
use quant_pivot_models::{
    config::PolymarketRtdsSourceConfig,
    types::{BinanceSymbol, ChainlinkFeedKey, DomainInstrumentKey, DomainSourceId},
};
use rustls::crypto::aws_lc_rs;

#[tokio::test]
#[ignore = "requires public Polymarket RTDS network"]
async fn both_crypto_topics_emit_distinct_validated_reports() {
    let _ = aws_lc_rs::default_provider().install_default();
    let source = PolymarketRtdsSource::connect(PolymarketRtdsSourceConfig::default());
    let binance_instrument = DomainInstrumentKey::polymarket_rtds_binance(
        &BinanceSymbol::parse("BTCUSDT").expect("static Binance symbol"),
    );
    let chainlink_instrument = DomainInstrumentKey::polymarket_rtds_chainlink(
        &ChainlinkFeedKey::parse("BTC-USD").expect("static Chainlink feed"),
    );
    let binance_instruments = [binance_instrument.clone()];
    let chainlink_instruments = [chainlink_instrument.clone()];
    let (binance_stream, chainlink_stream) = tokio::join!(
        source.stream(RtdsCryptoSource::Binance, &binance_instruments),
        source.stream(RtdsCryptoSource::Chainlink, &chainlink_instruments),
    );
    let mut binance_stream = binance_stream.expect("Binance RTDS stream");
    let mut chainlink_stream = chainlink_stream.expect("Chainlink RTDS stream");
    let (binance, chainlink) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(30), binance_stream.next_report()),
        tokio::time::timeout(Duration::from_secs(30), chainlink_stream.next_report()),
    );
    let binance = binance
        .expect("Binance RTDS report within 30 seconds")
        .expect("valid Binance RTDS report");
    let chainlink = chainlink
        .expect("Chainlink RTDS report within 30 seconds")
        .expect("valid Chainlink RTDS report");

    assert_eq!(binance.source_id, DomainSourceId::polymarket_rtds_binance());
    assert_eq!(binance.instrument_key, binance_instrument);
    assert!(binance.observations_timestamp.is_none());
    assert_eq!(
        chainlink.source_id,
        DomainSourceId::polymarket_rtds_chainlink()
    );
    assert_eq!(chainlink.instrument_key, chainlink_instrument);
    assert_eq!(chainlink.observations_timestamp, Some(chainlink.event_time));
    assert_ne!(binance.report_hash, chainlink.report_hash);

    let (binance_close, chainlink_close) =
        tokio::join!(binance_stream.close(), chainlink_stream.close());
    binance_close.expect("close Binance RTDS stream");
    chainlink_close.expect("close Chainlink RTDS stream");
}

#[tokio::test]
#[ignore = "requires public Polymarket RTDS network"]
async fn both_crypto_topics_emit_every_official_public_symbol() {
    let _ = aws_lc_rs::default_provider().install_default();
    let source = PolymarketRtdsSource::connect(PolymarketRtdsSourceConfig::default());
    let binance_instruments = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
        .into_iter()
        .map(|symbol| {
            DomainInstrumentKey::polymarket_rtds_binance(
                &BinanceSymbol::parse(symbol).expect("official Binance symbol"),
            )
        })
        .collect::<Vec<_>>();
    let chainlink_instruments = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]
        .into_iter()
        .map(|feed| {
            DomainInstrumentKey::polymarket_rtds_chainlink(
                &ChainlinkFeedKey::parse(feed).expect("official Chainlink feed"),
            )
        })
        .collect::<Vec<_>>();
    let binance_streams =
        join_all(binance_instruments.iter().map(|instrument| {
            source.stream(RtdsCryptoSource::Binance, slice::from_ref(instrument))
        }))
        .await
        .into_iter()
        .map(|stream| stream.expect("Binance RTDS stream"))
        .collect::<Vec<_>>();
    let chainlink_streams =
        join_all(chainlink_instruments.iter().map(|instrument| {
            source.stream(RtdsCryptoSource::Chainlink, slice::from_ref(instrument))
        }))
        .await
        .into_iter()
        .map(|stream| stream.expect("Chainlink RTDS stream"))
        .collect::<Vec<_>>();
    let binance_collectors = binance_streams
        .into_iter()
        .zip(binance_instruments.iter().cloned())
        .map(|(stream, instrument)| async move {
            collect_all(
                stream,
                DomainSourceId::polymarket_rtds_binance(),
                BTreeSet::from([instrument]),
            )
            .await
        });
    let chainlink_collectors = chainlink_streams
        .into_iter()
        .zip(chainlink_instruments.iter().cloned())
        .map(|(stream, instrument)| async move {
            collect_all(
                stream,
                DomainSourceId::polymarket_rtds_chainlink(),
                BTreeSet::from([instrument]),
            )
            .await
        });
    let (binance_seen, chainlink_seen) = tokio::join!(
        async {
            join_all(binance_collectors)
                .await
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>()
        },
        async {
            join_all(chainlink_collectors)
                .await
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>()
        },
    );
    assert_eq!(binance_seen.len(), 4);
    assert_eq!(chainlink_seen.len(), 4);
}

async fn collect_all(
    mut stream: PolymarketRtdsStream,
    source_id: DomainSourceId,
    expected: BTreeSet<DomainInstrumentKey>,
) -> BTreeSet<DomainInstrumentKey> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let mut seen = BTreeSet::new();
    while seen != expected {
        let report = tokio::time::timeout_at(deadline, stream.next_report())
            .await
            .unwrap_or_else(|_| {
                panic!("RTDS source {source_id} timed out; seen={seen:?}; expected={expected:?}")
            })
            .expect("valid RTDS report");
        assert_eq!(report.source_id, source_id);
        assert!(expected.contains(&report.instrument_key));
        seen.insert(report.instrument_key);
    }
    stream.close().await.expect("close RTDS stream");
    seen
}
