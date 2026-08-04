//! Canonical fixed-width L2 session ledger.

use blake3::Hasher;
use quant_pivot_error::hashing::CanonicalDigestError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::{ChBps, ChDigest, ChPrice, ChSchemaVersion, ChShares},
    enums::clickhouse::{ChCanonicalBookEventType, ChLedgerTradeSide},
    types::{ContentHash, MarketId, TokenId},
};

const HASH_DOMAIN: &[u8] = b"quant-pivot/book-l2-ledger";

/// One canonical L2 stream event. Snapshot rows are also replay anchors.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookL2LedgerRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub stream_session_id: Uuid,
    pub shard_id: u32,
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub token_sequence: u64,
    pub event_type: ChCanonicalBookEventType,
    pub bid_prices: Vec<ChPrice>,
    pub bid_sizes: Vec<ChShares>,
    pub ask_prices: Vec<ChPrice>,
    pub ask_sizes: Vec<ChShares>,
    pub old_tick_size: Option<ChPrice>,
    pub new_tick_size: Option<ChPrice>,
    pub trade_price: Option<ChPrice>,
    pub trade_side: Option<ChLedgerTradeSide>,
    pub trade_size: Option<ChShares>,
    pub fee_rate_bps: Option<ChBps>,
    pub venue_event_time: i64,
    pub ingress_time: i64,
    pub persisted_time: i64,
    pub event_hash: ChDigest,
    pub schema_version: ChSchemaVersion,
}

/// Per-token lower bound for one point-in-time L2 replay batch.
///
/// A batch reader must match both the stream session and sequence for each
/// token. A global time or sequence lower bound is not equivalent because one
/// stale token could otherwise make the query scan unrelated history for every
/// active token in the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLedgerReplayAnchor {
    pub token_id: TokenId,
    pub stream_session_id: Uuid,
    pub from_sequence: u64,
}

impl From<&BookL2LedgerRow> for BookLedgerReplayAnchor {
    fn from(row: &BookL2LedgerRow) -> Self {
        Self {
            token_id: row.token_id.clone(),
            stream_session_id: row.stream_session_id,
            from_sequence: row.token_sequence,
        }
    }
}

impl BookL2LedgerRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion::FIRST;

    /// Compute the domain-separated canonical event digest without allocation.
    pub fn canonical_event_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        let mut hasher = Hasher::new();
        update_len_prefixed(&mut hasher, HASH_DOMAIN)?;
        hasher.update(&self.schema_version.0.to_be_bytes());
        hasher.update(self.stream_session_id.as_bytes());
        hasher.update(&self.shard_id.to_be_bytes());
        update_len_prefixed(&mut hasher, self.token_id.as_str().as_bytes())?;
        hasher.update(&self.token_sequence.to_be_bytes());
        hasher.update(&[self.event_type as u8]);
        hasher.update(&self.venue_event_time.to_be_bytes());
        match self.event_type {
            ChCanonicalBookEventType::Snapshot | ChCanonicalBookEventType::Delta => {
                update_prices(&mut hasher, &self.bid_prices)?;
                update_shares(&mut hasher, &self.bid_sizes)?;
                update_prices(&mut hasher, &self.ask_prices)?;
                update_shares(&mut hasher, &self.ask_sizes)?;
            }
            ChCanonicalBookEventType::TickSizeChange => {
                update_optional_i128(&mut hasher, self.old_tick_size.map(ChPrice::scaled_i128));
                update_optional_i128(&mut hasher, self.new_tick_size.map(ChPrice::scaled_i128));
            }
            ChCanonicalBookEventType::Gap => {}
            ChCanonicalBookEventType::LastTrade => {
                update_optional_i128(&mut hasher, self.trade_price.map(ChPrice::scaled_i128));
                update_optional_u8(&mut hasher, self.trade_side.map(|side| side as u8));
                update_optional_i128(&mut hasher, self.trade_size.map(ChShares::scaled_i128));
                update_optional_i128(&mut hasher, self.fee_rate_bps.map(ChBps::scaled_i128));
            }
        }
        Ok(ContentHash::from_bytes(*hasher.finalize().as_bytes()))
    }

    /// Seal a newly constructed row with its canonical binary digest.
    pub fn seal(mut self) -> Result<Self, CanonicalDigestError> {
        self.event_hash = ChDigest::from(self.canonical_event_hash()?);
        Ok(self)
    }
}

fn update_len_prefixed(hasher: &mut Hasher, value: &[u8]) -> Result<(), CanonicalDigestError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        CanonicalDigestError::Serialize("ledger field exceeds u32 length contract".to_owned())
    })?;
    hasher.update(&len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn update_prices(hasher: &mut Hasher, values: &[ChPrice]) -> Result<(), CanonicalDigestError> {
    update_array_len(hasher, values.len())?;
    for value in values {
        hasher.update(&value.scaled_i128().to_be_bytes());
    }
    Ok(())
}

fn update_shares(hasher: &mut Hasher, values: &[ChShares]) -> Result<(), CanonicalDigestError> {
    update_array_len(hasher, values.len())?;
    for value in values {
        hasher.update(&value.scaled_i128().to_be_bytes());
    }
    Ok(())
}

fn update_array_len(hasher: &mut Hasher, len: usize) -> Result<(), CanonicalDigestError> {
    let len = u32::try_from(len).map_err(|_| {
        CanonicalDigestError::Serialize("ledger array exceeds u32 length contract".to_owned())
    })?;
    hasher.update(&len.to_be_bytes());
    Ok(())
}

fn update_optional_i128(hasher: &mut Hasher, value: Option<i128>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn update_optional_u8(hasher: &mut Hasher, value: Option<u8>) {
    match value {
        Some(value) => {
            hasher.update(&[1, value]);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        enums::clickhouse::{ChCanonicalBookEventType, ChLedgerTradeSide},
        types::{Price, Shares},
    };

    impl ChCanonicalBookEventType {
        fn row(self) -> BookL2LedgerRow {
            BookL2LedgerRow {
                stream_session_id: Uuid::from_u128(0x018f_1234_5678_7000_8000_0000_0000_0001),
                shard_id: 7,
                token_id: TokenId::new("123456789"),
                market_id: Some(MarketId::new("market")),
                token_sequence: 42,
                event_type: self,
                bid_prices: vec![ChPrice::from(Price::new(Decimal::new(49, 2)))],
                bid_sizes: vec![ChShares::from(Shares::new(Decimal::new(100, 0)))],
                ask_prices: vec![ChPrice::from(Price::new(Decimal::new(51, 2)))],
                ask_sizes: vec![ChShares::from(Shares::new(Decimal::new(80, 0)))],
                old_tick_size: None,
                new_tick_size: None,
                trade_price: None,
                trade_side: None,
                trade_size: None,
                fee_rate_bps: None,
                venue_event_time: 1_718_000_000_123,
                ingress_time: 1_718_000_000_124,
                persisted_time: 1_718_000_000_125,
                event_hash: ChDigest::new([0; 32]),
                schema_version: BookL2LedgerRow::SCHEMA_VERSION,
            }
        }
    }

    #[test]
    fn snapshot_ignores_non_fields() {
        let mut first = (ChCanonicalBookEventType::Snapshot).row();
        let expected = first.canonical_event_hash().expect("hash");
        first.market_id = Some(MarketId::new("changed"));
        first.ingress_time += 100;
        first.persisted_time += 200;

        assert_eq!(first.canonical_event_hash().expect("hash"), expected);
        assert_eq!(
            expected.to_string(),
            "blake3:b97f5f46452d817119b4c4f8d993f675dac88a718e4ab0f5eb029b48df3831f6"
        );
    }

    #[test]
    fn variants_distinct_fixed_hashes() {
        let snapshot = (ChCanonicalBookEventType::Snapshot)
            .row()
            .canonical_event_hash()
            .expect("snapshot");
        let delta = (ChCanonicalBookEventType::Delta)
            .row()
            .canonical_event_hash()
            .expect("delta");
        let mut tick = (ChCanonicalBookEventType::TickSizeChange).row();
        tick.old_tick_size = Some(ChPrice::from(Price::new(Decimal::new(1, 2))));
        tick.new_tick_size = Some(ChPrice::from(Price::new(Decimal::new(1, 3))));
        let tick = tick.canonical_event_hash().expect("tick");
        let gap = (ChCanonicalBookEventType::Gap)
            .row()
            .canonical_event_hash()
            .expect("gap");
        let mut trade = (ChCanonicalBookEventType::LastTrade).row();
        trade.trade_price = Some(ChPrice::from(Price::new(Decimal::new(5, 1))));
        trade.trade_side = Some(ChLedgerTradeSide::Buy);
        trade.trade_size = Some(ChShares::from(Shares::new(Decimal::new(10, 0))));
        trade.fee_rate_bps = Some(ChBps::from(Decimal::new(25, 1)));
        let trade = trade.canonical_event_hash().expect("trade");

        let hashes = [snapshot, delta, tick, gap, trade];
        let expected = [
            "blake3:b97f5f46452d817119b4c4f8d993f675dac88a718e4ab0f5eb029b48df3831f6",
            "blake3:2c5e29f0b553d07bb2c3b72fe2989f2c11e5da648e4e121954df046597ad8464",
            "blake3:9abd9dffd384bc17377cf45c79f42b854e08e6c8d6cb6ce30b9f9c66137320f5",
            "blake3:2c2d79950633e289ad07e44d19745e4750f63f3ff580569d55ba7d095e9fb3f6",
            "blake3:38819b360bbc513a96be2a4a0e67ae388d2b6208f9051a4e75e62931d263f7b0",
        ];
        for (hash, expected) in hashes.iter().zip(expected) {
            assert_eq!(hash.to_string(), expected);
        }
        for (index, hash) in hashes.iter().enumerate() {
            assert!(!hashes[..index].contains(hash));
        }
    }
}
