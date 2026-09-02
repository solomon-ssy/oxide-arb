//! Fully local finalized-exchange-history transport for the production-stack fixture.

use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant as StdInstant},
};

use alloy::{
    primitives::{Address, B256, U256},
    sol,
    sol_types::SolEvent,
};
use anyhow::{Context, Result, ensure};
use blake3::Hasher;
use chrono::Utc;
use hypersync_net_types::Query;
use quant_pivot_api::exchange::constants::{CTF_EXCHANGE_V2, EXCHANGE_CONTRACTS};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{header, method},
};

sol! {
    event OrderFilled(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        uint8 side,
        uint256 tokenId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee,
        bytes32 builder,
        bytes32 metadata
    );

    event OrdersMatched(
        bytes32 indexed takerOrderHash,
        address indexed takerOrderMaker,
        uint8 side,
        uint256 tokenId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled
    );
}

pub(crate) const HYPERSYNC_TOKEN: &str = "00000000-0000-0000-0000-000000000137";
pub(crate) const MODEL_CONFIRMATION_BLOCKS: u64 = 12;
pub(crate) const V2_PRODUCTION_BLOCK: u64 = CTF_EXCHANGE_V2.first_valid_block;
pub(crate) const V2_PRODUCTION_BLOCK_HASH: &str = CTF_EXCHANGE_V2.first_valid_block_hash;
const V2_PARENT_BLOCK_HASH: &str =
    "0x376951c9815fa5844193160377a833a20947e6d2293d50964335f38385fa7810";
const V2_PRODUCTION_TIMESTAMP: i64 = 1_777_379_340;

const RECENT_BLOCK_SECS: u64 = 2;
const MAX_RESPONSE_BLOCKS: u64 = 50_100;
// Freeze a four-hour, one-minute execution schedule before startup. Providers
// reveal only rows below the advancing chain head, so the production history
// worker remains the sole runtime writer while long closure runs retain fresh
// finalized executions without mutating an already-observed block.
const EXECUTION_CADENCE_BLOCKS: u64 = 30;
const EXECUTION_PAST_SAMPLES: u64 = 20;
const EXECUTION_FUTURE_SAMPLES: u64 = 240;
const EXECUTION_INITIAL_SPAN_BLOCKS: u64 = (EXECUTION_PAST_SAMPLES - 1) * EXECUTION_CADENCE_BLOCKS;

pub(crate) const DETERMINISTIC_POLYGON_BLOCK_SECS: i64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeterministicPolygonHead {
    pub(crate) block_number: u64,
    pub(crate) timestamp: i64,
}

/// Inclusive past-execution window bound to the exact registration head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredExecutionWindow {
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
}

/// Immutable local Polygon fork shared by settlement, attestor, and `HyperSync`.
pub(crate) struct DeterministicPolygonChain {
    anchor_block: u64,
    anchor_timestamp: i64,
    started_at: StdInstant,
    logs: RwLock<Vec<DeterministicExchangeLog>>,
    frozen: AtomicBool,
}

impl DeterministicPolygonChain {
    pub(crate) fn new() -> Self {
        Self::at(Utc::now().timestamp(), StdInstant::now())
    }

    pub(crate) fn at(timestamp: i64, started_at: StdInstant) -> Self {
        let elapsed = timestamp.saturating_sub(V2_PRODUCTION_TIMESTAMP);
        let elapsed_blocks = u64::try_from(elapsed)
            .unwrap_or_default()
            .checked_div(RECENT_BLOCK_SECS)
            .unwrap_or_default();
        let anchor_block = V2_PRODUCTION_BLOCK.saturating_add(elapsed_blocks);
        let anchor_timestamp = V2_PRODUCTION_TIMESTAMP.saturating_add(
            i64::try_from(elapsed_blocks.saturating_mul(RECENT_BLOCK_SECS)).unwrap_or(i64::MAX),
        );
        Self {
            anchor_block,
            anchor_timestamp,
            started_at,
            logs: RwLock::new(Vec::new()),
            frozen: AtomicBool::new(false),
        }
    }

    pub(crate) fn head(&self) -> DeterministicPolygonHead {
        self.head_after(self.started_at.elapsed())
    }

    pub(crate) fn head_after(&self, elapsed: Duration) -> DeterministicPolygonHead {
        let block_seconds = u64::try_from(DETERMINISTIC_POLYGON_BLOCK_SECS).unwrap_or(1);
        let elapsed_blocks = elapsed
            .as_secs()
            .checked_div(block_seconds)
            .unwrap_or_default();
        let elapsed_seconds = i64::try_from(elapsed_blocks)
            .unwrap_or(i64::MAX)
            .saturating_mul(DETERMINISTIC_POLYGON_BLOCK_SECS);
        DeterministicPolygonHead {
            block_number: self.anchor_block.saturating_add(elapsed_blocks),
            timestamp: self.anchor_timestamp.saturating_add(elapsed_seconds),
        }
    }

    pub(crate) fn block(
        block_number: u64,
        head: DeterministicPolygonHead,
    ) -> Option<DeterministicPolygonBlock> {
        if block_number > head.block_number {
            return None;
        }
        let timestamp = DeterministicPolygonBlock::timestamp(block_number);
        let parent_hash = if block_number == V2_PRODUCTION_BLOCK {
            V2_PARENT_BLOCK_HASH.to_owned()
        } else {
            polygon_block_hash(block_number.saturating_sub(1))
        };
        Some(DeterministicPolygonBlock {
            number: block_number,
            hash: polygon_block_hash(block_number),
            parent_hash,
            timestamp,
        })
    }

    pub(crate) fn block_at_or_before(&self, timestamp: i64) -> Option<DeterministicPolygonBlock> {
        let head = self.head();
        if timestamp < 0 {
            return None;
        }
        let mut lower = 0_u64;
        let mut upper = head.block_number;
        while lower < upper {
            let middle = lower + (upper - lower).div_ceil(2);
            let candidate = DeterministicPolygonBlock::timestamp(middle);
            if candidate <= timestamp {
                lower = middle;
            } else {
                upper = middle - 1;
            }
        }
        Self::block(lower, head).filter(|block| block.timestamp <= timestamp)
    }

    pub(crate) fn register_tokens(
        &self,
        token_ids: &[u64],
        registration_head: DeterministicPolygonHead,
    ) -> Result<RegisteredExecutionWindow> {
        ensure!(
            !self.frozen.load(Ordering::Acquire),
            "deterministic Polygon history is already frozen"
        );
        ensure!(!token_ids.is_empty(), "deterministic history has no tokens");
        ensure!(
            registration_head.timestamp
                == DeterministicPolygonBlock::timestamp(registration_head.block_number),
            "deterministic registration head has inconsistent block time"
        );
        let model_head = registration_head
            .block_number
            .checked_sub(MODEL_CONFIRMATION_BLOCKS)
            .context("deterministic head is below confirmation policy")?;
        let sample_count = EXECUTION_PAST_SAMPLES
            .checked_add(EXECUTION_FUTURE_SAMPLES)
            .context("deterministic history sample count overflowed")?;
        let first_block = model_head
            .checked_sub(EXECUTION_INITIAL_SPAN_BLOCKS)
            .context("deterministic history block range underflowed")?;
        ensure!(
            first_block >= V2_PRODUCTION_BLOCK,
            "deterministic report history predates V2"
        );
        let exchange = CTF_EXCHANGE_V2.address;
        let transaction_count = u64::try_from(token_ids.len())?
            .checked_mul(sample_count)
            .context("deterministic history transaction count overflowed")?;
        let mut rows = Vec::with_capacity(usize::try_from(transaction_count.saturating_mul(3))?);
        for (token_index, token_id) in token_ids.iter().copied().enumerate() {
            let transaction_index = u64::try_from(token_index)?;
            for sample in 0..sample_count {
                let ordinal = u64::try_from(token_index)?
                    .checked_mul(sample_count)
                    .and_then(|value| value.checked_add(sample))
                    .context("deterministic history ordinal overflowed")?;
                let block_number = first_block
                    .checked_add(sample.saturating_mul(EXECUTION_CADENCE_BLOCKS))
                    .context("deterministic history schedule overflowed")?;
                let maker = format!("0x{:040x}", ordinal.saturating_add(1)).parse::<Address>()?;
                let taker =
                    format!("0x{:040x}", ordinal.saturating_add(10_001)).parse::<Address>()?;
                let shares_raw = U256::from(100_000_000_u64);
                let phase = sample % EXECUTION_PAST_SAMPLES;
                let collateral_raw = U256::from(30_000_000_u64 + phase * 2_000_000);
                let maker_hash = seeded_hash((token_id, sample, 0));
                let taker_hash = seeded_hash((token_id, sample, 1));
                let transaction_hash = seeded_hash((token_id, sample, 2));
                let maker_fill = OrderFilled {
                    orderHash: maker_hash,
                    maker,
                    taker,
                    side: 1,
                    tokenId: U256::from(token_id),
                    makerAmountFilled: shares_raw,
                    takerAmountFilled: collateral_raw,
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data();
                let taker_fill = OrderFilled {
                    orderHash: taker_hash,
                    maker: taker,
                    taker: exchange,
                    side: 0,
                    tokenId: U256::from(token_id),
                    makerAmountFilled: collateral_raw,
                    takerAmountFilled: shares_raw,
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data();
                let matched = OrdersMatched {
                    takerOrderHash: taker_hash,
                    takerOrderMaker: taker,
                    side: 0,
                    tokenId: U256::from(token_id),
                    makerAmountFilled: collateral_raw,
                    takerAmountFilled: shares_raw,
                }
                .encode_log_data();
                for (log_index, data) in [maker_fill, taker_fill, matched].into_iter().enumerate() {
                    rows.push(DeterministicExchangeLog::new(
                        exchange,
                        block_number,
                        transaction_hash,
                        transaction_index,
                        u64::try_from(log_index)?,
                        data.topics().to_vec(),
                        data.data.to_vec(),
                    ));
                }
            }
        }
        rows.sort_unstable_by_key(|row| (row.block_number, row.transaction_index, row.log_index));
        ensure!(
            rows.windows(2).all(|pair| {
                (
                    pair[0].block_number,
                    pair[0].transaction_index,
                    pair[0].log_index,
                ) != (
                    pair[1].block_number,
                    pair[1].transaction_index,
                    pair[1].log_index,
                )
            }),
            "deterministic history contains duplicate log identities"
        );
        let mut stored = self
            .logs
            .write()
            .map_err(|_| anyhow::anyhow!("deterministic history lock is poisoned"))?;
        ensure!(
            stored.is_empty(),
            "deterministic report history was registered twice"
        );
        *stored = rows;
        drop(stored);
        Ok(RegisteredExecutionWindow {
            from_block: first_block,
            to_block: model_head,
        })
    }

    pub(crate) fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    pub(crate) fn logs_between(
        &self,
        from_block: u64,
        to_block_exclusive: u64,
    ) -> Result<Vec<DeterministicExchangeLog>> {
        ensure!(
            self.frozen.load(Ordering::Acquire),
            "deterministic Polygon history is not frozen"
        );
        let rows = self
            .logs
            .read()
            .map_err(|_| anyhow::anyhow!("deterministic history lock is poisoned"))?
            .iter()
            .filter(|row| row.block_number >= from_block && row.block_number < to_block_exclusive)
            .cloned()
            .collect();
        Ok(rows)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeterministicExchangeLog {
    pub(crate) address: Address,
    pub(crate) block_number: u64,
    pub(crate) transaction_hash: B256,
    pub(crate) transaction_index: u64,
    pub(crate) log_index: u64,
    pub(crate) topics: Vec<B256>,
    pub(crate) data: Vec<u8>,
}

impl DeterministicExchangeLog {
    const fn new(
        address: Address,
        block_number: u64,
        transaction_hash: B256,
        transaction_index: u64,
        log_index: u64,
        topics: Vec<B256>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            address,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            topics,
            data,
        }
    }
}

fn seeded_hash(input: (u64, u64, u64)) -> B256 {
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/production-history-fixture/v1\0");
    hasher.update(&input.0.to_be_bytes());
    hasher.update(&input.1.to_be_bytes());
    hasher.update(&input.2.to_be_bytes());
    B256::from(*hasher.finalize().as_bytes())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeterministicPolygonBlock {
    pub(crate) number: u64,
    pub(crate) hash: String,
    pub(crate) parent_hash: String,
    pub(crate) timestamp: i64,
}

impl DeterministicPolygonBlock {
    fn timestamp(block_number: u64) -> i64 {
        if block_number < V2_PRODUCTION_BLOCK {
            let timestamp = i128::from(block_number)
                .saturating_mul(i128::from(V2_PRODUCTION_TIMESTAMP))
                .checked_div(i128::from(V2_PRODUCTION_BLOCK))
                .unwrap_or_default();
            return i64::try_from(timestamp).unwrap_or(i64::MAX);
        }
        V2_PRODUCTION_TIMESTAMP.saturating_add(
            i64::try_from((block_number - V2_PRODUCTION_BLOCK).saturating_mul(RECENT_BLOCK_SECS))
                .unwrap_or(i64::MAX),
        )
    }
}

pub(crate) fn polygon_block_hash(block_number: u64) -> String {
    if block_number == V2_PRODUCTION_BLOCK {
        V2_PRODUCTION_BLOCK_HASH.to_owned()
    } else if block_number == V2_PRODUCTION_BLOCK.saturating_sub(1) {
        V2_PARENT_BLOCK_HASH.to_owned()
    } else {
        format!("0x{}", blake3::hash(&block_number.to_be_bytes()).to_hex())
    }
}

pub(crate) async fn start_hypersync(chain: Arc<DeterministicPolygonChain>) -> Result<MockServer> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", format!("Bearer {HYPERSYNC_TOKEN}")))
        .respond_with(HyperSyncResponder { chain })
        .mount(&server)
        .await;
    Ok(server)
}

struct HyperSyncResponder {
    chain: Arc<DeterministicPolygonChain>,
}

impl Respond for HyperSyncResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        match hypersync_response(&request.body, self.chain.as_ref()) {
            Ok(body) => ResponseTemplate::new(200).set_body_bytes(body),
            Err(error) => ResponseTemplate::new(400).set_body_string(error.to_string()),
        }
    }
}

fn hypersync_response(body: &[u8], chain: &DeterministicPolygonChain) -> Result<Vec<u8>> {
    let query = serde_json::from_slice(body).context("decode HyperSync JSON query")?;
    query_response(&query, chain)
}

fn query_response(query: &Query, chain: &DeterministicPolygonChain) -> Result<Vec<u8>> {
    let to_block = query
        .to_block
        .context("bounded HyperSync query has no to_block")?;
    ensure!(
        to_block > query.from_block,
        "HyperSync query range does not advance"
    );
    ensure!(
        to_block - query.from_block <= MAX_RESPONSE_BLOCKS,
        "HyperSync query exceeds deterministic response budget"
    );
    validate_query(query)?;
    let head = chain.head();
    ensure!(
        to_block.saturating_sub(1) <= head.block_number,
        "HyperSync query exceeds deterministic archive height"
    );
    let (blocks, logs) = if query.include_all_blocks {
        (json_blocks(head, query.from_block, to_block)?, Vec::new())
    } else {
        (
            Vec::new(),
            json_logs(&chain.logs_between(query.from_block, to_block)?),
        )
    };
    serde_json::to_vec(&json!({
        "archive_height": head.block_number,
        "next_block": to_block,
        "total_execution_time": 1,
        "data": {
            "blocks": blocks,
            "transactions": [],
            "logs": logs,
            "traces": []
        },
        "rollback_guard": null
    }))
    .context("encode HyperSync JSON response")
}

fn json_logs(rows: &[DeterministicExchangeLog]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let topics = row
                .topics
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>();
            json!({
                "removed": false,
                "log_index": format!("0x{:x}", row.log_index),
                "transaction_index": format!("0x{:x}", row.transaction_index),
                "transaction_hash": format!("{:#x}", row.transaction_hash),
                "block_hash": polygon_block_hash(row.block_number),
                "block_number": format!("0x{:x}", row.block_number),
                "address": format!("{:#x}", row.address),
                "data": format!("0x{}", hex::encode(&row.data)),
                "topic0": topics.first(),
                "topic1": topics.get(1),
                "topic2": topics.get(2),
                "topic3": topics.get(3)
            })
        })
        .collect()
}

fn validate_query(query: &Query) -> Result<()> {
    if query.include_all_blocks {
        ensure!(query.logs.is_empty(), "header query contains log filters");
        ensure!(
            query.field_selection.block.len() == 4,
            "header query changed its exact selected fields"
        );
        return Ok(());
    }
    ensure!(
        query.logs.len() == 1,
        "log query changed filter cardinality"
    );
    let selection = &query.logs[0];
    ensure!(selection.exclude.is_none(), "log query added exclusions");
    let filter = &selection.include;
    ensure!(
        filter.address_filter.is_none() && filter.topics.len() == 1,
        "log query changed its exact address/topic shape"
    );
    let addresses = filter
        .address
        .iter()
        .map(|address| hex::encode(address.as_ref()))
        .collect::<Vec<_>>();
    let expected_addresses = EXCHANGE_CONTRACTS
        .iter()
        .map(|contract| hex::encode(contract.address.as_slice()))
        .collect::<Vec<_>>();
    ensure!(
        addresses == expected_addresses,
        "log query changed V2 contract addresses"
    );
    let topics = filter.topics[0]
        .iter()
        .map(|topic| hex::encode(topic.as_ref()))
        .collect::<Vec<_>>();
    let expected_topics = EXCHANGE_CONTRACTS
        .iter()
        .flat_map(|contract| {
            [
                contract.order_filled_topic,
                contract.orders_matched_topic,
                contract.fee_charged_topic,
            ]
        })
        .map(|topic| hex::encode(topic.as_slice()))
        .collect::<Vec<_>>();
    ensure!(
        topics == expected_topics,
        "log query changed V2 event topics"
    );
    Ok(())
}

fn json_blocks(
    head: DeterministicPolygonHead,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    (from_block..to_block)
        .map(|number| {
            let block = DeterministicPolygonChain::block(number, head)
                .context("deterministic HyperSync block is above archive height")?;
            Ok(json!({
                "number": block.number,
                "hash": block.hash,
                "parent_hash": block.parent_hash,
                "timestamp": u64::try_from(block.timestamp)
                    .context("deterministic block predates Unix epoch")?
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::Arc,
        time::{Duration, Instant as StdInstant},
    };

    use anyhow::Result;
    use chrono::Utc;
    use quant_pivot_api::exchange::{
        constants::NEG_RISK_EXCHANGE_V2, history_client::ExchangeHistoryExtractor,
    };
    use quant_pivot_models::config::FinalizedExchangeHistoryConfig;

    use super::{
        DeterministicPolygonChain, EXECUTION_CADENCE_BLOCKS, EXECUTION_INITIAL_SPAN_BLOCKS,
        EXECUTION_PAST_SAMPLES, HYPERSYNC_TOKEN, MODEL_CONFIRMATION_BLOCKS, V2_PARENT_BLOCK_HASH,
        V2_PRODUCTION_BLOCK, V2_PRODUCTION_BLOCK_HASH, V2_PRODUCTION_TIMESTAMP, start_hypersync,
    };

    #[test]
    fn chain_preserves_boundary() {
        let chain = DeterministicPolygonChain::new();
        let head = chain.head_after(Duration::ZERO);
        let boundary =
            DeterministicPolygonChain::block(V2_PRODUCTION_BLOCK, head).expect("V2 boundary block");

        assert!(head.block_number > V2_PRODUCTION_BLOCK);
        assert_eq!(boundary.hash, V2_PRODUCTION_BLOCK_HASH);
        assert_eq!(boundary.parent_hash, V2_PARENT_BLOCK_HASH);
        assert_eq!(boundary.timestamp, V2_PRODUCTION_TIMESTAMP);
        assert_eq!(NEG_RISK_EXCHANGE_V2.first_valid_block, V2_PRODUCTION_BLOCK);
        assert_eq!(
            NEG_RISK_EXCHANGE_V2.first_valid_block_hash,
            V2_PRODUCTION_BLOCK_HASH
        );
        assert!(boundary.timestamp < Utc::now().timestamp());
    }

    #[test]
    fn history_is_immutable() {
        let chain = DeterministicPolygonChain::new();
        let first_head = chain.head_after(Duration::ZERO);
        let later_head = chain.head_after(Duration::from_secs(121));
        let fixed_number = first_head.block_number - 2_000;
        let first =
            DeterministicPolygonChain::block(fixed_number, first_head).expect("fixed block");
        let later =
            DeterministicPolygonChain::block(fixed_number, later_head).expect("fixed block");

        assert_eq!(first, later);
        let genesis = DeterministicPolygonChain::block(0, later_head).expect("genesis block");
        let middle = DeterministicPolygonChain::block(V2_PRODUCTION_BLOCK / 2, later_head)
            .expect("middle block");
        assert_eq!(genesis.timestamp, 0);
        assert!(middle.timestamp > genesis.timestamp);
        assert!(middle.timestamp < V2_PRODUCTION_TIMESTAMP);
        for (from, to) in [
            (V2_PRODUCTION_BLOCK - 2_000, V2_PRODUCTION_BLOCK + 2_000),
            (later_head.block_number - 2_000, later_head.block_number),
        ] {
            let mut previous =
                DeterministicPolygonChain::block(from, later_head).expect("history start");
            for number in (from + 1)..=to {
                let current =
                    DeterministicPolygonChain::block(number, later_head).expect("history block");
                assert!(current.timestamp > previous.timestamp);
                assert_eq!(current.parent_hash, previous.hash);
                previous = current;
            }
        }
    }

    #[test]
    fn execution_schedule_is_frozen() -> Result<()> {
        let chain = DeterministicPolygonChain::new();
        let registered = chain.register_tokens(&[750_001, 750_002], chain.head())?;
        chain.freeze();

        let initial = chain.logs_between(registered.from_block, registered.to_block + 1)?;
        let future = chain.logs_between(
            registered.to_block + 1,
            registered.to_block + 3 * EXECUTION_CADENCE_BLOCKS + 1,
        )?;

        assert_eq!(
            initial.len(),
            2 * usize::try_from(EXECUTION_PAST_SAMPLES)? * 3
        );
        assert_eq!(future.len(), 2 * 3 * 3);
        assert_eq!(
            initial
                .iter()
                .map(|row| row.transaction_index)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 1])
        );
        assert!(chain.register_tokens(&[750_003], chain.head()).is_err());
        Ok(())
    }

    #[test]
    fn registration_window_survives_advance() -> Result<()> {
        let chain =
            DeterministicPolygonChain::at(V2_PRODUCTION_TIMESTAMP + 10_000, StdInstant::now());
        let registration_head = chain.head_after(Duration::from_secs(57));
        let registered = chain.register_tokens(&[750_001], registration_head)?;
        chain.freeze();

        // Model an archive probe crossing one block without wall-clock sleeps.
        let query_head = chain.head_after(Duration::from_secs(59));
        assert_eq!(query_head.block_number, registration_head.block_number + 1);
        assert!(query_head.timestamp > registration_head.timestamp);
        let shifted_model_head = query_head.block_number - MODEL_CONFIRMATION_BLOCKS;
        let shifted = chain.logs_between(
            shifted_model_head - EXECUTION_INITIAL_SPAN_BLOCKS,
            shifted_model_head + 1,
        )?;
        let registered_rows = chain.logs_between(registered.from_block, registered.to_block + 1)?;
        assert_eq!(
            shifted.len(),
            19 * 3,
            "a new live-head window loses its first execution"
        );
        assert_eq!(registered_rows.len(), 20 * 3);
        assert_eq!(
            registered_rows.first().map(|row| row.block_number),
            Some(registered.from_block)
        );
        assert_eq!(
            registered_rows.last().map(|row| row.block_number),
            Some(registered.to_block)
        );
        Ok(())
    }

    #[test]
    fn frontier_search_is_valid() {
        let chain = DeterministicPolygonChain::new();
        let head = chain.head_after(Duration::ZERO);
        for days in [33_i64, 200] {
            let target = head.timestamp - days * 86_400;
            let mut lower = 0_u64;
            let mut upper = head.block_number;
            while lower < upper {
                let middle = lower + (upper - lower) / 2;
                let timestamp = DeterministicPolygonChain::block(middle, head)
                    .expect("search block")
                    .timestamp;
                if timestamp < target {
                    lower = middle + 1;
                } else {
                    upper = middle;
                }
            }
            let found = DeterministicPolygonChain::block(lower, head).expect("frontier block");
            assert!(found.timestamp >= target);
            if lower > 0 {
                assert!(
                    DeterministicPolygonChain::block(lower - 1, head)
                        .expect("frontier predecessor")
                        .timestamp
                        < target
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hypersync_serves_protocol() {
        let chain = Arc::new(DeterministicPolygonChain::new());
        let registered = chain
            .register_tokens(&[750_001], chain.head())
            .expect("register deterministic exchange logs");
        chain.freeze();
        let server = start_hypersync(Arc::clone(&chain))
            .await
            .expect("start HyperSync fixture");
        let mut config = FinalizedExchangeHistoryConfig::default();
        config.hypersync.endpoint = server.uri();
        config.hypersync.api_token = HYPERSYNC_TOKEN.into();
        let extractor = ExchangeHistoryExtractor::connect(&config).expect("connect extractor");
        let to_block = registered.to_block;
        let from_block = to_block - EXECUTION_CADENCE_BLOCKS;
        let chunk = extractor
            .fetch_chunk(from_block, to_block)
            .await
            .expect("fetch deterministic chunk");

        assert_eq!(chunk.from_block, from_block);
        assert_eq!(chunk.to_block, to_block);
        assert!(!chunk.logs.is_empty());
        assert!(chunk.logs.iter().all(|log| !log.topics.is_empty()));
        assert_eq!(chunk.first_block.number, from_block);
        assert_eq!(chunk.last_block.number, to_block);
        assert_eq!(
            chunk.confirmation_anchor.number,
            to_block + config.model_confirmation_blocks
        );
        extractor
            .shutdown()
            .await
            .expect("drain HyperSync extractor runtime");
    }
}
