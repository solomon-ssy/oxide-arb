//! Gamma HTTP wire DTOs — serde shapes matching `gamma-api.polymarket.com` keyset payloads.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};

/// Maximum `limit` accepted by `GET /events/keyset`.
pub const GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE: u32 = 500;

/// Gamma list fields (`clobTokenIds`, `outcomes`) arrive as JSON arrays or JSON-encoded strings.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(transparent)]
pub struct GammaStringList(Vec<String>);

impl GammaStringList {
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'de> Deserialize<'de> for GammaStringList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<serde_json::Value>::deserialize(deserializer)? {
            None | Some(serde_json::Value::Null) => Ok(Self(Vec::new())),
            Some(serde_json::Value::Array(items)) => {
                let strings = items
                    .into_iter()
                    .map(|item| {
                        item.as_str()
                            .ok_or_else(|| {
                                DeError::custom("gamma string list array elements must be strings")
                            })
                            .map(str::to_owned)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self(strings))
            }
            Some(serde_json::Value::String(encoded)) => {
                let parsed: Vec<String> = serde_json::from_str(&encoded).map_err(|err| {
                    DeError::custom(format!("gamma string list is not valid JSON array: {err}"))
                })?;
                Ok(Self(parsed))
            }
            Some(other) => Err(DeError::custom(format!(
                "gamma string list must be array or JSON string, got {other}"
            ))),
        }
    }
}

/// Gamma emits multiple optional end-date keys; prefer RFC3339 `endDate` over date-only `endDateIso`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct GammaEndDate {
    #[serde(default, rename = "endDate")]
    timestamp: Option<String>,
    #[serde(default, rename = "endDateIso")]
    date_only: Option<String>,
}

impl GammaEndDate {
    fn parse(&self) -> Option<DateTime<Utc>> {
        self.timestamp
            .as_deref()
            .or(self.date_only.as_deref())
            .and_then(parse_gamma_timestamp)
    }
}

fn parse_gamma_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<DateTime<Utc>>().ok()
}

/// Keyset page envelope from `GET /events/keyset`.
#[derive(Debug, Clone, Deserialize)]
pub struct KeysetEventsPage {
    pub events: Vec<WireEvent>,
    #[serde(default, rename = "next_cursor")]
    pub next_cursor: Option<String>,
}

/// Tag object on a Gamma event — the official categorization source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireTag {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
}

/// Event object embedded in keyset / incremental list responses.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireEvent {
    pub id: String,
    pub title: Option<String>,
    pub slug: Option<String>,
    /// Recurring-series slug (e.g. `btc-up-or-down-5m`) — the Tier-0 linkage
    /// anchor for clock-templated crypto markets.
    #[serde(default)]
    pub series_slug: Option<String>,
    #[serde(flatten)]
    end_date: GammaEndDate,
    #[serde(default)]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub closed: Option<bool>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub tags: Option<Vec<WireTag>>,
    #[serde(default)]
    pub markets: Option<Vec<WireMarket>>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl WireEvent {
    #[must_use]
    pub fn end_date(&self) -> Option<DateTime<Utc>> {
        self.end_date.parse()
    }

    /// Non-empty tag slugs in payload order.
    #[must_use]
    pub fn tag_slugs(&self) -> Vec<String> {
        self.tags
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|tag| tag.slug.as_deref())
            .map(str::trim)
            .filter(|slug| !slug.is_empty())
            .map(str::to_owned)
            .collect()
    }
}

/// Market object embedded under a Gamma event.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireMarket {
    pub condition_id: String,
    pub question: String,
    #[serde(default)]
    pub slug: Option<String>,
    /// Market rules text — carries the resolution-source sentence (Chainlink
    /// data stream / Binance candle) the linkage resolver grounds against.
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub clob_token_ids: GammaStringList,
    #[serde(default)]
    pub outcomes: GammaStringList,
    #[serde(default)]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub closed: Option<bool>,
    /// Per-outcome settlement prices; `"1"` marks the winning leg once
    /// `uma_resolution_status` is `resolved`.
    #[serde(default)]
    pub outcome_prices: GammaStringList,
    #[serde(default)]
    pub uma_resolution_status: Option<String>,
    /// Settlement close time, e.g. `2026-06-11 04:05:01+00` (not RFC3339).
    #[serde(default)]
    pub closed_time: Option<String>,
    #[serde(flatten)]
    end_date: GammaEndDate,
    #[serde(default)]
    pub fees_enabled: Option<bool>,
    #[serde(default)]
    pub fee_schedule: Option<WireFeeSchedule>,
    #[serde(default)]
    pub order_min_size: Option<Decimal>,
    #[serde(default)]
    pub order_price_min_tick_size: Option<Decimal>,
    /// Numeric liquidity (`liquidityNum`) in USD; absent on some payloads.
    #[serde(default)]
    pub liquidity_num: Option<Decimal>,
    /// Trailing 24h traded volume (`volume24hr`) in USD; absent on some payloads.
    #[serde(default)]
    pub volume_24hr: Option<Decimal>,
    /// Parent events embedded by `GET /markets?condition_ids=` responses
    /// (absent in keyset payloads where the market is nested under its event).
    #[serde(default)]
    pub events: Option<Vec<WireEvent>>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl WireMarket {
    #[must_use]
    pub fn end_date(&self) -> Option<DateTime<Utc>> {
        self.end_date.parse()
    }
}

/// Fee schedule block on a Gamma market.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireFeeSchedule {
    #[serde(default)]
    pub exponent: Option<Decimal>,
    #[serde(default)]
    pub rate: Option<Decimal>,
    #[serde(default)]
    pub taker_only: Option<bool>,
    #[serde(default)]
    pub rebate_rate: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::{GammaStringList, KeysetEventsPage, WireEvent, WireMarket};
    use rust_decimal::Decimal;

    #[test]
    fn string_list_deserializes_json_array() {
        let list: GammaStringList = serde_json::from_str(r#"["Yes", "No"]"#).expect("array form");
        assert_eq!(list.as_slice(), &["Yes", "No"]);
    }

    #[test]
    fn string_list_deserializes_encoded_json_string() {
        let list: GammaStringList =
            serde_json::from_str(r#""[\"111\", \"222\"]""#).expect("encoded form");
        assert_eq!(list.as_slice(), &["111", "222"]);
    }

    #[test]
    fn string_list_rejects_invalid_encoded_json() {
        let err =
            serde_json::from_str::<GammaStringList>(r#""not-json""#).expect_err("must fail closed");
        assert!(err.to_string().contains("not valid JSON array"));
    }

    #[test]
    fn market_deserializes_keyset_shape() {
        let market: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xabc",
                "question": "Q?",
                "clobTokenIds": "[\"111\", \"222\"]",
                "outcomes": "[\"Yes\", \"No\"]",
                "orderPriceMinTickSize": 0.001,
                "orderMinSize": 5,
                "endDate": "2021-12-04T00:00:00Z",
                "endDateIso": "2021-12-04"
            }"#,
        )
        .expect("market");
        assert_eq!(market.condition_id, "0xabc");
        assert_eq!(market.clob_token_ids.as_slice(), &["111", "222"]);
        assert_eq!(market.order_price_min_tick_size, Some(Decimal::new(1, 3)));
        assert!(market.end_date().is_some());
    }

    #[test]
    fn keyset_page_deserializes() {
        let page: KeysetEventsPage = serde_json::from_str(
            r#"{
                "events": [{
                    "id": "1",
                    "title": "Event",
                    "slug": "event",
                    "markets": [{
                        "conditionId": "0xabc",
                        "question": "Q?",
                        "clobTokenIds": ["1", "2"],
                        "outcomes": ["Yes", "No"]
                    }]
                }],
                "next_cursor": "opaque"
            }"#,
        )
        .expect("page");
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("opaque"));
    }

    #[test]
    fn event_list_deserializes_for_incremental_endpoint() {
        let events: Vec<WireEvent> = serde_json::from_str(
            r#"[{
                "id": "evt-1",
                "title": "Updated",
                "slug": "updated",
                "markets": [{
                    "conditionId": "0xdeadbeef",
                    "question": "Q?",
                    "clobTokenIds": ["111", "222"],
                    "outcomes": ["Yes", "No"]
                }]
            }]"#,
        )
        .expect("incremental events");
        assert_eq!(events.len(), 1);
    }
}
