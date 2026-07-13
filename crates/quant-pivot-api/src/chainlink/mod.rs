//! Chainlink on-chain aggregator source (`AggregatorV3` `eth_call`).
//!
//! Round ids are composite `uint80` values (`phase_id << 64 | aggregator_round_id`)
//! and are handled via [`round_id::RoundId`] — never narrowed to `u64`.
//!
//! **Current oracle plane (11.2.2):** this module reads **Chainlink Data Feeds**
//! on Polygon — on-chain `AggregatorV3` rounds pushed by deviation/heartbeat
//! rules. Polymarket crypto up/down markets settle against **Chainlink Data
//! Streams** (off-chain, sub-second signed reports; paid subscription). The two
//! sources can diverge by 0.3–0.8% for 10–30 seconds in volatile windows.
//!
//! Phase 11.2.2 mitigates (but does not eliminate) that gap via:
//! - PTB fail-closed: Chainlink-settled markets never silently fall back to
//!   Binance for price-to-beat;
//! - `domain.crypto.cross_check.max_oracle_staleness_secs`: reject stale Data
//!   Feed observations in basis/PTB features (`StaleBeyondPolicy`).
//!
//! True Data Streams ingest is deferred to Phase 11.2.3 (requires a paid
//! Chainlink subscription).

use std::{collections::BTreeMap, str::FromStr, sync::Mutex, time::Duration};

use alloy::{
    primitives::{Address, I256},
    providers::{Provider, ProviderBuilder},
    rpc::client::RpcClient,
    sol,
    transports::http::Http,
};

mod round_id;

use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, rpc::RpcError};
use quant_pivot_models::{
    config::{ChainlinkSourceConfig, OnchainConfig},
    domain::DomainObservation,
    enums::domain::{DomainFamily, DomainMetric},
    types::{ChainlinkFeedKey, DomainInstrumentKey, DomainSourceId},
};
use reqwest::Client as ReqwestClient;
use round_id::{backscan_start, gap_recovery_floor, is_missing_round_reason, prev};
use rust_decimal::Decimal;
use url::Url;

use crate::domain::{DomainDataSource, DomainFetchRequest};

type DynProvider = alloy::providers::DynProvider;

sol! {
    #[sol(rpc)]
    interface AggregatorV3Interface {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
        function getRoundData(uint80 roundId) external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
        function decimals() external view returns (uint8);
    }
}

/// Chainlink aggregator reader backed by Polygon `eth_call`.
pub struct ChainlinkAggregatorSource {
    config: ChainlinkSourceConfig,
    provider: DynProvider,
    feeds: BTreeMap<ChainlinkFeedKey, Address>,
    decimals_cache: Mutex<BTreeMap<ChainlinkFeedKey, u8>>,
}

impl ChainlinkAggregatorSource {
    /// Connect from deploy config (reuses the shared Polygon RPC endpoint).
    ///
    /// # Errors
    ///
    /// Returns an error when the RPC URL is invalid or feed addresses fail to parse.
    pub fn connect(
        onchain: &OnchainConfig,
        config: ChainlinkSourceConfig,
    ) -> Result<Self, RpcError> {
        let rpc_url = Url::parse(&onchain.rpc_url).map_err(|error| {
            RpcError::ConnectionFailed(format!(
                "invalid Polygon RPC URL '{}': {error}",
                onchain.rpc_url
            ))
        })?;
        let http_client = ReqwestClient::builder()
            .timeout(Duration::from_millis(onchain.rpc_timeout_ms))
            .build()
            .map_err(|error| {
                RpcError::ConnectionFailed(format!(
                    "failed to build Polygon RPC HTTP client: {error}"
                ))
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        let provider = ProviderBuilder::new().connect_client(rpc_client).erased();
        let feeds = config
            .feeds
            .iter()
            .map(|(feed, address)| {
                let key = ChainlinkFeedKey::parse(feed).map_err(|error| RpcError::CallFailed {
                    method: "chainlink_feed_key".into(),
                    reason: error.to_string(),
                })?;
                let address = Address::from_str(address).map_err(|error| RpcError::CallFailed {
                    method: "chainlink_feed_address".into(),
                    reason: format!("{feed}: {error}"),
                })?;
                Ok((key, address))
            })
            .collect::<Result<BTreeMap<_, _>, RpcError>>()?;
        Ok(Self {
            config,
            provider,
            feeds,
            decimals_cache: Mutex::new(BTreeMap::new()),
        })
    }

    fn parse_instrument(key: &DomainInstrumentKey) -> QuantResult<ChainlinkFeedKey> {
        let feed = key
            .as_str()
            .strip_prefix("CHAINLINK:")
            .ok_or_else(|| QuantError::config(format!("not a Chainlink instrument key: {key}")))?;
        ChainlinkFeedKey::parse(feed).map_err(|error| QuantError::config(error.to_string()))
    }

    async fn decimals(&self, feed: &ChainlinkFeedKey, address: Address) -> QuantResult<u8> {
        let cached = self.decimals_cache.lock().expect("lock").get(feed).copied();
        if let Some(decimals) = cached {
            return Ok(decimals);
        }
        let contract = AggregatorV3Interface::new(address, &self.provider);
        let decimals = contract.decimals().call().await.map_err(|error| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_decimals".into(),
                reason: error.to_string(),
            })
        })?;
        self.decimals_cache
            .lock()
            .expect("lock")
            .insert(feed.clone(), decimals);
        Ok(decimals)
    }

    async fn observation_from_round_fields(
        &self,
        feed: &ChainlinkFeedKey,
        address: Address,
        answer: I256,
        updated_at_raw: alloy::primitives::U256,
    ) -> QuantResult<Option<DomainObservation>> {
        if answer <= I256::ZERO {
            return Ok(None);
        }
        let updated_at = i64::try_from(updated_at_raw).map_err(|error| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_updated_at".into(),
                reason: error.to_string(),
            })
        })?;
        let observed_at = Utc.timestamp_opt(updated_at, 0).single().ok_or_else(|| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_updated_at".into(),
                reason: "timestamp out of range".into(),
            })
        })?;
        let decimals = self.decimals(feed, address).await?;
        let value = scale_answer(answer, decimals)?;
        Ok(Some(DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::chainlink(),
            instrument_key: DomainInstrumentKey::chainlink_feed(feed),
            metric: DomainMetric::OraclePrice,
            value,
            observed_at,
            publish_time: observed_at,
            available_at: None,
        }))
    }

    /// Steady-state fetch: normally just `latestRoundData`, but walks
    /// `getRoundData` backward — bounded by `max_incremental_gap_rounds` — to
    /// recover any rounds the aggregator advanced between polls (e.g. after a
    /// poller outage), rather than permanently losing them (R10 ingest
    /// hardening). The common case (no gap: the latest round is already
    /// `<= from_exclusive`'s successor) costs exactly one RPC call.
    async fn fetch_incremental(
        &self,
        feed: &ChainlinkFeedKey,
        address: Address,
        from_exclusive: DateTime<Utc>,
    ) -> QuantResult<Vec<DomainObservation>> {
        let contract = AggregatorV3Interface::new(address, &self.provider);
        let latest = contract.latestRoundData().call().await.map_err(|error| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_latestRoundData".into(),
                reason: error.to_string(),
            })
        })?;
        let latest_round = latest.roundId;
        let floor_round = gap_recovery_floor(latest_round, self.config.max_incremental_gap_rounds);

        let mut observations = Vec::new();
        let mut round_id = latest_round;
        let mut answer = latest.answer;
        let mut updated_at = latest.updatedAt;
        loop {
            let Some(observation) = self
                .observation_from_round_fields(feed, address, answer, updated_at)
                .await?
            else {
                // An unpriced round breaks the ascending-timestamp assumption
                // this walk relies on; stop rather than guess further back.
                break;
            };
            if observation.observed_at <= from_exclusive {
                break;
            }
            observations.push(observation);
            if round_id <= floor_round {
                break;
            }
            let Some(prev_round) = prev(round_id) else {
                break;
            };
            round_id = prev_round;
            let round = match contract.getRoundData(round_id).call().await {
                Ok(round) => round,
                Err(error) => {
                    let reason = error.to_string();
                    if is_missing_round_reason(&reason) {
                        tracing::debug!(
                            round_id = %round_id,
                            "chainlink incremental backscan stopped at missing round"
                        );
                        break;
                    }
                    return Err(QuantError::Rpc(RpcError::CallFailed {
                        method: "chainlink_getRoundData".into(),
                        reason,
                    }));
                }
            };
            answer = round.answer;
            updated_at = round.updatedAt;
        }
        observations.sort_by_key(|row| row.observed_at);
        Ok(observations)
    }

    async fn fetch_bootstrap(
        &self,
        feed: &ChainlinkFeedKey,
        address: Address,
        from_exclusive: DateTime<Utc>,
        to_inclusive: DateTime<Utc>,
    ) -> QuantResult<Vec<DomainObservation>> {
        let contract = AggregatorV3Interface::new(address, &self.provider);
        let latest = contract.latestRoundData().call().await.map_err(|error| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_latestRoundData".into(),
                reason: error.to_string(),
            })
        })?;
        let latest_round = latest.roundId;
        let floor_round = backscan_start(latest_round, self.config.max_round_backscan);
        let mut observations = Vec::new();
        let mut round_id = latest_round;
        loop {
            let round = match contract.getRoundData(round_id).call().await {
                Ok(round) => round,
                Err(error) => {
                    let reason = error.to_string();
                    if round_id == latest_round {
                        return Err(QuantError::Rpc(RpcError::CallFailed {
                            method: "chainlink_getRoundData".into(),
                            reason,
                        }));
                    }
                    if is_missing_round_reason(&reason) {
                        tracing::debug!(
                            round_id = %round_id,
                            "chainlink bootstrap backscan stopped at missing round"
                        );
                        break;
                    }
                    return Err(QuantError::Rpc(RpcError::CallFailed {
                        method: "chainlink_getRoundData".into(),
                        reason,
                    }));
                }
            };
            if let Some(observation) = self
                .observation_from_round_fields(feed, address, round.answer, round.updatedAt)
                .await?
                && observation.observed_at > from_exclusive
                && observation.observed_at <= to_inclusive
            {
                observations.push(observation);
            }
            if round_id <= floor_round {
                break;
            }
            let Some(prev_round) = prev(round_id) else {
                break;
            };
            round_id = prev_round;
        }
        observations.sort_by_key(|row| row.observed_at);
        Ok(observations)
    }
}

fn scale_answer(answer: I256, decimals: u8) -> QuantResult<Decimal> {
    let raw = answer
        .to_string()
        .parse::<Decimal>()
        .map_err(|error| QuantError::config(format!("chainlink answer parse: {error}")))?;
    let scale = Decimal::from(10_u64.pow(u32::from(decimals)));
    Ok(raw / scale)
}

#[async_trait::async_trait]
impl DomainDataSource for ChainlinkAggregatorSource {
    fn family(&self) -> DomainFamily {
        DomainFamily::Crypto
    }

    fn source_id(&self) -> DomainSourceId {
        DomainSourceId::chainlink()
    }

    async fn fetch(&self, request: DomainFetchRequest) -> QuantResult<Vec<DomainObservation>> {
        let feed = Self::parse_instrument(&request.instrument_key)?;
        let address = self.feeds.get(&feed).ok_or_else(|| {
            QuantError::config(format!("chainlink feed `{feed}` is not configured"))
        })?;
        if request.bootstrap {
            self.fetch_bootstrap(
                &feed,
                *address,
                request.from_exclusive,
                request.to_inclusive,
            )
            .await
        } else {
            self.fetch_incremental(&feed, *address, request.from_exclusive)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::round_id::{ONE, RoundId, gap_recovery_floor};

    fn phase_one_round(aggregator_round: u64) -> RoundId {
        (RoundId::from(1_u64) << 64) + RoundId::from(aggregator_round)
    }

    #[test]
    fn floor_is_bounded_by_gap_cap_in_phase_zero() {
        assert_eq!(
            gap_recovery_floor(RoundId::from(1_000_u64), 100),
            RoundId::from(900_u64)
        );
    }

    #[test]
    fn floor_supports_phase_one_round_ids() {
        let latest = phase_one_round(3_684_024);
        assert_eq!(gap_recovery_floor(latest, 100), phase_one_round(3_683_924));
    }

    #[test]
    fn floor_clamps_to_round_one_near_genesis() {
        assert_eq!(gap_recovery_floor(RoundId::from(50_u64), 100), ONE);
        assert_eq!(gap_recovery_floor(ONE, 100), ONE);
    }
}
