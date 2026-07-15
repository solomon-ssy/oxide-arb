//! Point-in-time Polymarket CLOB market-info contracts.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::fee::MarketFeeSchedule,
    enums::common::TickSize,
    hashing::CanonicalDigest,
    jsonb_active,
    types::{ClobMarketInfoVersionId, ContentHash, MarketId, TokenId},
};

/// V2 platform-fee parameters returned in the CLOB `fd` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ClobFeeDetails {
    pub rate: Decimal,
    pub exponent: u32,
    pub taker_only: bool,
}

/// Outcome token carried by one market-info observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClobTokenDescriptor {
    pub token_id: TokenId,
    pub outcome: String,
}

/// Strongly typed JSONB token set for one market-info observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct ClobTokenSet(pub Vec<ClobTokenDescriptor>);

jsonb_active!(ClobFeeDetails, ClobTokenSet);

/// Append-only bitemporal CLOB truth used by research and live admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClobMarketInfoVersion {
    pub version_id: ClobMarketInfoVersionId,
    pub market_id: MarketId,
    pub tokens: Vec<ClobTokenDescriptor>,
    pub tick_size: TickSize,
    pub minimum_order_size: Decimal,
    pub neg_risk: bool,
    pub taker_order_delay_enabled: bool,
    pub minimum_order_age_secs: Option<u64>,
    pub blockaid_check_enabled: bool,
    pub fee_details: ClobFeeDetails,
    pub builder_maker_fee_rate_bps: u32,
    pub builder_taker_fee_rate_bps: u32,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub payload_hash: ContentHash,
    pub raw_payload: serde_json::Value,
}

impl ClobMarketInfoVersion {
    pub fn validate(&self) -> Result<(), String> {
        if self.tokens.len() < 2
            || self
                .tokens
                .iter()
                .any(|token| token.outcome.trim().is_empty())
            || self.minimum_order_size <= Decimal::ZERO
        {
            return Err("CLOB market info has invalid tokens or minimum order size".to_owned());
        }
        if self.effective_at > self.available_at {
            return Err("CLOB market info effective_at cannot exceed available_at".to_owned());
        }
        if self.fee_details.rate < Decimal::ZERO
            || self.fee_details.rate > Decimal::ONE
            || self.fee_details.exponent > 8
        {
            return Err("CLOB market info fee details are invalid".to_owned());
        }
        let expected = CanonicalDigest::content_hash_json(&self.raw_payload)
            .map_err(|error| format!("CLOB market info payload hash failed: {error}"))?;
        if expected != self.payload_hash {
            return Err("CLOB market info payload hash mismatch".to_owned());
        }
        Ok(())
    }

    #[must_use]
    pub fn fee_schedule(&self) -> MarketFeeSchedule {
        MarketFeeSchedule {
            market_id: self.market_id.clone(),
            fees_enabled: self.fee_details.rate > Decimal::ZERO,
            fee_rate: self.fee_details.rate,
            exponent: Decimal::from(self.fee_details.exponent),
            taker_only: self.fee_details.taker_only,
            rebate_rate: None,
            observed_at: self.available_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::{ClobFeeDetails, ClobMarketInfoVersion, ClobTokenDescriptor};
    use crate::{
        enums::common::TickSize,
        hashing::CanonicalDigest,
        types::{ClobMarketInfoVersionId, MarketId, TokenId},
    };

    #[test]
    fn market_info_requires_exact_payload_identity() {
        let raw_payload =
            serde_json::json!({"c": "0xmarket", "fd": {"r": "0.03", "e": 1, "to": true}});
        let payload_hash = CanonicalDigest::content_hash_json(&raw_payload).expect("hash");
        let now = Utc::now();
        let mut value = ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: TokenId::new("1"),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: TokenId::new("2"),
                    outcome: "No".to_owned(),
                },
            ],
            tick_size: TickSize::Hundredth,
            minimum_order_size: dec!(1),
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: dec!(0.03),
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at: now,
            available_at: now,
            payload_hash,
            raw_payload,
        };
        assert!(value.validate().is_ok());
        value.raw_payload = serde_json::json!({"changed": true});
        assert!(value.validate().is_err());
    }

    #[test]
    fn market_info_accepts_constant_fee_curve_exponent() {
        let raw_payload =
            serde_json::json!({"c": "0xmarket", "fd": {"r": "0.03", "e": 0, "to": true}});
        let payload_hash = CanonicalDigest::content_hash_json(&raw_payload).expect("hash");
        let now = Utc::now();
        let value = ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: TokenId::new("1"),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: TokenId::new("2"),
                    outcome: "No".to_owned(),
                },
            ],
            tick_size: TickSize::Hundredth,
            minimum_order_size: dec!(1),
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: dec!(0.03),
                exponent: 0,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at: now,
            available_at: now,
            payload_hash,
            raw_payload,
        };
        assert!(value.validate().is_ok());
    }
}
