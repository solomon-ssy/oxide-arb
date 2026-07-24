//! Credential-free, read-only probes for public upstream contracts.

use std::{slice::from_ref, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Error, Result, ensure};
use chrono::{Duration as ChronoDuration, NaiveDate, Timelike, Utc};
use chrono_tz::{America, Tz};
use polymarket_client_sdk_v2::types::U256;
use quant_pivot_api::{
    binance::{BinanceAggTradeSource, BinanceKlineSource, BinanceRequestBudget},
    domain::{DomainDataSource, DomainFetchRequest},
    gamma::GammaClient,
    rtds::{PolymarketRtdsSource, RtdsCryptoSource},
    weather::{
        airnow::AirNowSource,
        ghcnh::GhcnhSource,
        gistemp::NasaGistempSource,
        hko::HkoOpenDataSource,
        nhc::{NhcBasin, NhcSource},
        nsidc::{NsidcSeaIceSource, SeaIceHemisphere},
        nws::NwsObservationSource,
        tornado::TornadoSource,
    },
    ws::{ClobWsManager, ClobWsManagerHooks, SubscriptionSource, TokenKeyResolver},
};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_models::{
    config::{
        AirNowSourceConfig, BinanceSourceConfig, GammaConfig, GhcnhSourceConfig,
        HkoOpenDataSourceConfig, NasaGistempSourceConfig, NhcSourceConfig, NsidcSeaIceSourceConfig,
        NwsObservationSourceConfig, PolymarketConfig, PolymarketRtdsSourceConfig,
        TornadoSourceConfig, WeatherVerticalBindingsConfig, WebSocketConfig,
    },
    domain::data_plane::pipeline::PipelineEvent,
    enums::domain::{DomainFamily, DomainMetric, KlineInterval},
    types::{
        BinanceSymbol, ChainlinkFeedKey, DomainInstrumentKey, DomainMeasurementUnit,
        DomainSourceId, HkoStation, IcaoStation, TokenId, TokenKey, WeatherTemperatureStatistic,
        WeatherVariable,
    },
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

pub async fn run(
    include_binance: bool,
    include_polymarket: bool,
    include_weather: bool,
    stream_timeout: Duration,
) -> Result<()> {
    let compute = Arc::new(ComputeExecutor::new()?);
    if include_binance {
        println!("smoke public-read: Binance spot/USD-M REST + public archive");
        Box::pin(binance(Arc::clone(&compute)))
            .await
            .context("Binance public-read smoke")?;
        println!("smoke public-read: Binance passed");
    }
    if include_polymarket {
        println!("smoke public-read: polymarket catalog + CLOB WS + RTDS");
        Box::pin(polymarket(stream_timeout))
            .await
            .context("Polymarket public-read smoke")?;
        println!("smoke public-read: polymarket passed");
    }
    if include_weather {
        println!("smoke public-read: weather providers");
        Box::pin(weather(compute))
            .await
            .context("weather public-read smoke")?;
        println!("smoke public-read: weather passed");
    }
    Ok(())
}

async fn binance(compute: Arc<ComputeExecutor>) -> Result<()> {
    let spot_config = BinanceSourceConfig::default();
    let spot = BinanceKlineSource::connect(spot_config.clone(), Arc::clone(&compute))
        .context("connect Binance spot")?;
    let spot_symbol = BinanceSymbol::parse("BTCUSDT").context("parse Binance spot symbol")?;
    let spot_key = DomainInstrumentKey::binance_kline(&spot_symbol, KlineInterval::OneMinute);
    let to = Utc::now() - ChronoDuration::minutes(5);
    let observations = spot
        .fetch(DomainFetchRequest {
            instrument_key: spot_key.clone(),
            from_exclusive: to - ChronoDuration::hours(2),
            to_inclusive: to,
            bootstrap: true,
        })
        .await
        .context("read completed Binance spot klines")?;
    ensure!(
        observations.len() >= 4
            && observations
                .windows(2)
                .all(|pair| pair[0].observed_at <= pair[1].observed_at)
            && observations.iter().all(|item| {
                item.family == DomainFamily::Crypto
                    && item.source_id == DomainSourceId::binance()
                    && item.instrument_key == spot_key
                    && item.metric == DomainMetric::Close
                    && item.value > Decimal::ZERO
            }),
        "Binance spot kline source violated typed ordering or provenance"
    );

    let futures_config = BinanceSourceConfig::usdm_futures_default();
    let futures_budget =
        BinanceRequestBudget::new(&futures_config).context("build Binance USD-M request budget")?;
    let futures = BinanceKlineSource::connect_usdm_futures(
        futures_config.clone(),
        futures_budget,
        Arc::clone(&compute),
    )
    .context("connect Binance USD-M klines")?;
    let futures_symbol = BinanceSymbol::parse("HYPEUSDT").context("parse USD-M symbol")?;
    let futures_key =
        DomainInstrumentKey::binance_usdm_futures_kline(&futures_symbol, KlineInterval::OneHour);
    let futures_to = Utc::now() - ChronoDuration::hours(2);
    let futures_observations = futures
        .fetch(DomainFetchRequest {
            instrument_key: futures_key.clone(),
            from_exclusive: futures_to - ChronoDuration::hours(12),
            to_inclusive: futures_to,
            bootstrap: true,
        })
        .await
        .context("read Binance USD-M klines")?;
    ensure!(
        !futures_observations.is_empty()
            && futures_observations.iter().all(|item| {
                item.source_id == DomainSourceId::binance_usdm_futures()
                    && item.instrument_key == futures_key
                    && item.value > Decimal::ZERO
            }),
        "Binance USD-M kline source violated typed provenance"
    );

    let aggregate_trade_budget =
        BinanceRequestBudget::new(&futures_config).context("build Binance USD-M stream budget")?;
    let aggregate_trades = BinanceAggTradeSource::connect_usdm_futures(
        futures_config,
        aggregate_trade_budget,
        Arc::clone(&compute),
    )
    .context("connect Binance USD-M aggregate trades")?;
    let report = aggregate_trades
        .latest(&futures_symbol, Utc::now())
        .await
        .context("read Binance USD-M aggregate-trade frontier")?
        .context("Binance USD-M aggregate-trade frontier was empty")?;
    ensure!(
        report.source_id == DomainSourceId::binance_futures_trade()
            && report.instrument_key == DomainInstrumentKey::binance_futures_trade(&futures_symbol)
            && report.source_sequence > 0
            && report.price.inner() > Decimal::ZERO,
        "Binance USD-M aggregate-trade frontier violated typed provenance"
    );

    let archive_date =
        NaiveDate::from_ymd_opt(2025, 1, 1).context("construct Binance archive date")?;
    let available_at = Utc::now();
    let mut archive = spot
        .recover_archive_day(
            &spot_symbol,
            KlineInterval::OneMinute,
            archive_date,
            available_at,
        )
        .await
        .context("read checksum-verified Binance archive")?
        .context("Binance omitted published archive partition")?;
    let mut rows = Vec::new();
    while let Some(batch) = archive
        .next_batch()
        .await
        .context("decode Binance archive batch")?
    {
        rows.extend(batch);
    }
    ensure!(
        rows.len() == 1_440
            && rows
                .iter()
                .all(|row| row.available_at == Some(available_at))
            && rows.windows(2).all(|pair| {
                pair[1].observed_at - pair[0].observed_at == ChronoDuration::minutes(1)
            }),
        "Binance daily archive violated checksum-decoded continuity contract"
    );
    Ok(())
}

async fn polymarket(stream_timeout: Duration) -> Result<()> {
    let token = GammaClient::new(GammaConfig::default())
        .discover_active_token()
        .await
        .context("discover one active token through Gamma keyset")?;
    tokio::try_join!(
        clob_book(&token, stream_timeout),
        rtds_topics(stream_timeout),
    )?;
    Ok(())
}

async fn clob_book(token: &TokenId, timeout: Duration) -> Result<()> {
    let shutdown = CancellationToken::new();
    let token_value = U256::from_str(token.as_str()).context("parse Gamma token as U256")?;
    let token_key = TokenKey::new(0);
    let token_resolver: Arc<dyn TokenKeyResolver> =
        Arc::new(move |candidate| (candidate == token_value).then_some(token_key));
    let manager = ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        shutdown.clone(),
        token_resolver,
        ClobWsManagerHooks::default(),
    );
    manager.subscribe_tokens(SubscriptionSource::Engine, from_ref(token));
    let observed = tokio::time::timeout(timeout, async {
        loop {
            match manager.events().recv_async().await {
                Ok(batch)
                    if batch.events.iter().any(|event| {
                        matches!(
                            event,
                            PipelineEvent::BookSnapshot(book)
                                if book.token == token_key
                                    && (!book.bids.levels.is_empty()
                                        || !book.asks.levels.is_empty())
                        )
                    }) =>
                {
                    return Ok(());
                }
                Ok(_) => {}
                Err(error) => return Err(Error::new(error)),
            }
        }
    })
    .await;
    shutdown.cancel();
    drop(manager);
    observed
        .with_context(|| format!("CLOB book snapshot timeout after {timeout:?}"))?
        .context("receive CLOB book snapshot")
}

async fn rtds_topics(timeout: Duration) -> Result<()> {
    let source = PolymarketRtdsSource::connect(PolymarketRtdsSourceConfig::default());
    let binance_instrument = DomainInstrumentKey::polymarket_rtds_binance(
        &BinanceSymbol::parse("BTCUSDT").context("parse static Binance symbol")?,
    );
    let chainlink_instrument = DomainInstrumentKey::polymarket_rtds_chainlink(
        &ChainlinkFeedKey::parse("BTC-USD").context("parse static Chainlink feed")?,
    );
    let (binance_stream, chainlink_stream) = tokio::join!(
        source.stream(RtdsCryptoSource::Binance, from_ref(&binance_instrument)),
        source.stream(RtdsCryptoSource::Chainlink, from_ref(&chainlink_instrument)),
    );
    let mut binance_stream = binance_stream.context("connect Binance RTDS topic")?;
    let mut chainlink_stream = chainlink_stream.context("connect Chainlink RTDS topic")?;
    let (binance, chainlink) = tokio::join!(
        tokio::time::timeout(timeout, binance_stream.next_report()),
        tokio::time::timeout(timeout, chainlink_stream.next_report()),
    );
    let binance = binance
        .with_context(|| format!("Binance RTDS report timeout after {timeout:?}"))?
        .context("decode Binance RTDS report")?;
    let chainlink = chainlink
        .with_context(|| format!("Chainlink RTDS report timeout after {timeout:?}"))?
        .context("decode Chainlink RTDS report")?;
    ensure!(
        binance.source_id == DomainSourceId::polymarket_rtds_binance()
            && binance.instrument_key == binance_instrument
            && binance.observations_timestamp.is_none(),
        "Binance RTDS report violated typed provenance"
    );
    ensure!(
        chainlink.source_id == DomainSourceId::polymarket_rtds_chainlink()
            && chainlink.instrument_key == chainlink_instrument
            && chainlink.observations_timestamp == Some(chainlink.event_time),
        "Chainlink RTDS report violated typed provenance"
    );
    ensure!(
        binance.report_hash != chainlink.report_hash,
        "distinct RTDS topics produced the same report identity"
    );
    let (binance_close, chainlink_close) =
        tokio::join!(binance_stream.close(), chainlink_stream.close());
    binance_close.context("close Binance RTDS stream")?;
    chainlink_close.context("close Chainlink RTDS stream")?;
    Ok(())
}

async fn weather(compute: Arc<ComputeExecutor>) -> Result<()> {
    tokio::try_join!(
        hko(),
        ghcnh(),
        airnow(),
        tornado(compute),
        nhc(),
        gistemp(),
        nsidc(),
        nws(),
    )?;
    Ok(())
}

async fn hko() -> Result<()> {
    let source =
        HkoOpenDataSource::connect(HkoOpenDataSourceConfig::default()).context("connect HKO")?;
    let report = source
        .rainfall("North District", Utc::now())
        .await
        .context("read HKO rainfall")?
        .context("HKO omitted configured rainfall place")?;
    ensure!(
        report.source_id == DomainSourceId::hko_open_data()
            && report.variable == WeatherVariable::Precipitation
            && report.unit == DomainMeasurementUnit::Millimeter
            && report.value >= Decimal::ZERO,
        "HKO rainfall violated typed provenance"
    );
    ensure!(
        report
            .valid_from
            .is_some_and(|start| start < report.observed_at)
            && report.observed_at <= report.published_at
            && report.published_at <= report.available_at,
        "HKO rainfall timestamps violated publication ordering"
    );

    let station = HkoStation::parse("HKO").context("parse HKO station")?;
    for (statistic, variable) in [
        (
            WeatherTemperatureStatistic::Maximum,
            WeatherVariable::TemperatureMaximum,
        ),
        (
            WeatherTemperatureStatistic::Minimum,
            WeatherVariable::TemperatureMinimum,
        ),
    ] {
        let month = source
            .daily_temperatures(&station, statistic, 2025, 7, Utc::now())
            .await
            .context("read HKO daily temperature")?;
        ensure!(
            month.reports.len() == 31 && month.incomplete_rows == 0 && month.unavailable_rows == 0,
            "HKO July 2025 history was incomplete"
        );
        ensure!(
            month.reports.iter().all(|item| {
                item.source_id == DomainSourceId::hko_open_data()
                    && item.variable == variable
                    && item.unit == DomainMeasurementUnit::Celsius
                    && item.instrument_key
                        == DomainInstrumentKey::hko_daily_temperature(&station, statistic)
            }),
            "HKO temperature history violated typed provenance"
        );
    }
    Ok(())
}

async fn ghcnh() -> Result<()> {
    let source = GhcnhSource::connect(GhcnhSourceConfig::default()).context("connect GHCNh")?;
    let station = IcaoStation::parse("KLGA").context("parse GHCNh station")?;
    let year = source
        .yearly_station(&station, "USW00014732", 2025, Utc::now())
        .await
        .context("read GHCNh station year")?
        .context("GHCNh omitted published 2025 partition")?;
    ensure!(!year.reports.is_empty(), "GHCNh station year was empty");
    ensure!(
        year.reports.iter().all(|report| {
            report.source_id == DomainSourceId::ghcnh()
                && report.subject_key == "KLGA"
                && report.variable == WeatherVariable::Temperature
                && report.unit == DomainMeasurementUnit::Celsius
        }),
        "GHCNh station history violated typed provenance"
    );
    Ok(())
}

async fn airnow() -> Result<()> {
    let source = AirNowSource::connect(AirNowSourceConfig::default()).context("connect AirNow")?;
    let bindings = WeatherVerticalBindingsConfig::default();
    let area = bindings
        .airnow_pm25_reporting_areas
        .first()
        .context("AirNow reporting-area binding is missing")?;
    let timezone = area
        .timezone
        .parse::<Tz>()
        .context("parse AirNow reporting-area timezone")?;
    let snapshot = source
        .pm25_reporting_area(&area.area, &area.state, timezone, Utc::now())
        .await
        .context("read AirNow reporting area")?;
    ensure!(
        !snapshot.observations.is_empty(),
        "AirNow reporting area returned no observations"
    );
    let subject = format!("{}:{}", area.state, area.area);
    ensure!(
        snapshot.observations.iter().all(|report| {
            report.source_id == DomainSourceId::airnow()
                && report.instrument_key == DomainInstrumentKey::airnow_pm25_observation(&subject)
                && report.variable == WeatherVariable::Aqi
                && report.unit == DomainMeasurementUnit::Aqi
        }),
        "AirNow reporting-area facts violated typed provenance"
    );

    let now = Utc::now();
    let base_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .context("construct current UTC hour")?;
    let mut recent = None;
    for hours_ago in 1..=6 {
        recent = source
            .hourly_pm25_area_observation(
                &area.area,
                &area.state,
                base_hour - ChronoDuration::hours(hours_ago),
                now,
            )
            .await
            .context("read AirNow hourly partition")?;
        if recent.is_some() {
            break;
        }
    }
    let recent = recent.context("AirNow published no recent hourly AQI")?;
    ensure!(
        recent.source_id == DomainSourceId::airnow()
            && recent.variable == WeatherVariable::Aqi
            && recent.unit == DomainMeasurementUnit::Aqi,
        "AirNow hourly fact violated typed provenance"
    );
    Ok(())
}

async fn tornado(compute: Arc<ComputeExecutor>) -> Result<()> {
    let source = TornadoSource::connect(TornadoSourceConfig::default(), compute)
        .context("connect tornado sources")?;
    let report_date = (Utc::now() - ChronoDuration::days(1)).date_naive();
    let preliminary = source
        .spc_preliminary_day("oklahoma", "OK", report_date, Utc::now())
        .await
        .context("read SPC preliminary partition")?
        .context("SPC omitted yesterday's published partition")?;
    ensure!(
        preliminary.source_id == DomainSourceId::spc_storm_reports()
            && preliminary.variable == WeatherVariable::TornadoCount
            && preliminary.unit == DomainMeasurementUnit::Count,
        "SPC report violated typed provenance"
    );

    let historical_date =
        NaiveDate::from_ymd_opt(2013, 5, 20).context("construct historical date")?;
    let final_day = source
        .ncei_final_day(
            "oklahoma",
            "OKLAHOMA",
            America::Chicago,
            historical_date,
            Utc::now(),
        )
        .await
        .context("read NCEI final tornado day")?;
    ensure!(
        final_day.report.source_id == DomainSourceId::ncei_storm_events()
            && final_day.report.value > Decimal::ZERO,
        "NCEI final report violated typed provenance"
    );
    Ok(())
}

async fn nhc() -> Result<()> {
    let source = NhcSource::connect(NhcSourceConfig::default()).context("connect NHC")?;
    let advisories = source
        .active_advisories(Utc::now())
        .await
        .context("read NHC current advisories")?;
    ensure!(
        advisories.iter().all(|report| {
            report.source_id == DomainSourceId::nhc_advisory()
                && report.variable == WeatherVariable::CycloneIntensity
                && report.unit == DomainMeasurementUnit::Knot
                && report.valid_from == Some(report.observed_at)
                && report.published_at <= report.available_at
        }),
        "NHC current advisory violated typed provenance"
    );
    let track = source
        .hurdat2_storm(NhcBasin::Atlantic, "AL092021", Utc::now())
        .await
        .context("read NHC HURDAT2")?
        .context("NHC omitted Hurricane Ida best track")?;
    ensure!(!track.reports.is_empty(), "NHC HURDAT2 track was empty");
    ensure!(
        track
            .reports
            .iter()
            .all(|report| report.source_id == DomainSourceId::nhc_hurdat2()),
        "NHC HURDAT2 track violated typed provenance"
    );
    Ok(())
}

async fn gistemp() -> Result<()> {
    let source = NasaGistempSource::connect(NasaGistempSourceConfig::default())
        .context("connect NASA GISTEMP")?;
    let dataset = source
        .monthly_anomalies(Utc::now())
        .await
        .context("read NASA GISTEMP")?;
    ensure!(
        dataset.reports.len() > 1_000,
        "NASA GISTEMP history was unexpectedly short"
    );
    ensure!(
        dataset.reports.iter().all(|report| {
            report.source_id == DomainSourceId::nasa_gistemp()
                && report.variable == WeatherVariable::GlobalTemperatureAnomaly
                && report.unit == DomainMeasurementUnit::CelsiusAnomaly
        }),
        "NASA GISTEMP history violated typed provenance"
    );
    Ok(())
}

async fn nsidc() -> Result<()> {
    let source =
        NsidcSeaIceSource::connect(NsidcSeaIceSourceConfig::default()).context("connect NSIDC")?;
    for hemisphere in [SeaIceHemisphere::North, SeaIceHemisphere::South] {
        let dataset = source
            .daily_extent(hemisphere, Utc::now())
            .await
            .context("read NSIDC daily extent")?;
        ensure!(!dataset.reports.is_empty(), "NSIDC dataset was empty");
        ensure!(
            dataset.reports.iter().all(|report| {
                report.source_id == DomainSourceId::nsidc_sea_ice_index()
                    && report.variable == WeatherVariable::SeaIceExtent
                    && report.unit == DomainMeasurementUnit::MillionSquareKilometer
            }),
            "NSIDC dataset violated typed provenance"
        );
    }
    Ok(())
}

async fn nws() -> Result<()> {
    let source = NwsObservationSource::connect(NwsObservationSourceConfig::default())
        .context("connect NWS")?;
    let station = IcaoStation::parse("KMWN").context("parse NWS station")?;
    let reports = source
        .recent_wind(&station, Utc::now())
        .await
        .context("read NWS recent wind")?;
    ensure!(!reports.is_empty(), "NWS station returned no recent wind");
    ensure!(
        reports.iter().all(|report| {
            report.source_id == DomainSourceId::nws_observation()
                && matches!(
                    report.variable,
                    WeatherVariable::WindSpeed | WeatherVariable::WindGust
                )
                && report.unit == DomainMeasurementUnit::Knot
        }),
        "NWS wind reports violated typed provenance"
    );
    Ok(())
}
