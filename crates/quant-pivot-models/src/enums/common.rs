//! Common enums used across the quant-pivot platform.
//!
//! Postgres column enums use [`pg_enum!`]; wire-only enums use [`wire_enum!`].

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveValue, IntoActiveValue};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use thiserror::Error;

pg_enum! {
    type_name = "qp_side",
    pub enum Side {
        Buy => "BUY",
        Sell => "SELL",
    }
}

impl Side {
    #[must_use]
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// The stable `i8` code persisted to the `ClickHouse` `side` column of the
    /// execution fact (`quant_execution_event`). Append-only contract: never
    /// renumber an existing variant.
    #[must_use]
    #[inline]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Buy => 1,
            Self::Sell => 2,
        }
    }
}

/// Polymarket CLOB order time-in-force types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Fill-or-Kill: must fill entirely and immediately, or cancel. Never retried.
    Fok,
    /// Fill-and-Kill: fill immediately as much as possible and cancel the remainder.
    /// Never retried because a partial venue fill is a valid terminal outcome.
    Fak,
    /// Good-Till-Cancelled: rests on the book until filled or manually cancelled.
    Gtc,
    /// Good-Till-Date: rests on the book until `expiration` (unix timestamp).
    Gtd { expiration: u64 },
}

wire_enum! {
    /// Staleness classification for market-data snapshots.
    ///
    /// Variants are ordered from freshest to most stale. The derived `Ord`
    /// follows this ordering so `Fresh < Acceptable < Stale < Expired`.
    @derive(PartialOrd, Ord)
    pub enum StalenessLevel {
        Fresh => "fresh",
        Acceptable => "acceptable",
        Stale => "stale",
        Expired => "expired",
    }
}

impl StalenessLevel {
    #[must_use]
    #[inline]
    pub const fn worse(self, other: Self) -> Self {
        if (self as u8) > (other as u8) {
            self
        } else {
            other
        }
    }
}

/// Origin of a market-data update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    WebSocket,
    RestPoll,
}

/// Severity level for operational alerts.
///
/// Ordered from lowest to highest: `Info < Warning < Critical < Emergency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
    /// System-level fault: API fully down, DB corrupted, circuit breaker L4.
    Emergency,
}

/// Business category for an operational alert.
///
/// This separates money-critical trading state from informational operator
/// notices, so dashboards do not infer trading degradation from every warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertCategory {
    /// A trading-safety signal that can affect order admission or capital safety.
    TradingSafety,
    /// Infrastructure availability, storage, WebSocket, or service-health signal.
    Infrastructure,
    /// Scheduled materialization health for currently runnable cadences.
    SchedulerHealth,
    /// Operator-facing information that does not affect trading state.
    OperatorNotice,
}

impl AlertCategory {
    /// Whether alerts in this category affect trading by default.
    #[must_use]
    pub const fn default_affects_trading(self) -> bool {
        matches!(self, Self::TradingSafety)
    }

    /// Whether alerts in this category should show a UI toast by default.
    #[must_use]
    pub const fn default_visible_toast(self, severity: AlertLevel) -> bool {
        matches!(severity, AlertLevel::Critical | AlertLevel::Emergency)
            || matches!(self, Self::TradingSafety)
    }
}

/// Subsystem that produced an operational alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSource {
    Scheduler,
    HealthChecker,
    DataPipeline,
    Execution,
    Settlement,
    ReportGenerator,
    RiskEngine,
    System,
}

impl Display for AlertLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Critical => "CRITICAL",
            Self::Emergency => "EMERGENCY",
        };
        f.write_str(label)
    }
}

pg_enum! {
    type_name = "qp_market_category",
    /// Polymarket event category for business selection and model cohorts.
    @derive(PartialOrd, Ord, schemars::JsonSchema)
    @no_from_str
    pub enum MarketCategory {
        Geopolitics => "geopolitics",
        Sports => "sports",
        Politics => "politics",
        Finance => "finance",
        Tech => "tech",
        Culture => "culture",
        Weather => "weather",
        Economics => "economics",
        Crypto => "crypto",
        Other => "other",
    }
}

impl MarketCategory {
    /// Every persisted category variant — used for bulk cache invalidation.
    pub const ALL_VARIANTS: [Self; 10] = [
        Self::Geopolitics,
        Self::Sports,
        Self::Politics,
        Self::Finance,
        Self::Tech,
        Self::Culture,
        Self::Weather,
        Self::Economics,
        Self::Crypto,
        Self::Other,
    ];

    /// Index into fixed-size category weight tables (0..9).
    #[must_use]
    #[inline]
    pub const fn table_index(self) -> usize {
        match self {
            Self::Geopolitics => 0,
            Self::Sports => 1,
            Self::Politics => 2,
            Self::Finance => 3,
            Self::Tech => 4,
            Self::Culture => 5,
            Self::Weather => 6,
            Self::Economics => 7,
            Self::Crypto => 8,
            Self::Other => 9,
        }
    }

    /// Map a Gamma event tag slug to a governed business category.
    ///
    /// The Gamma API exposes no dedicated category field anymore: the official
    /// categorization is the event `tags[]` array, whose slugs correspond to
    /// the site navigation tag pages. This curated mapping covers the slugs
    /// that align with Polymarket's documented fee categories; every other
    /// slug (team names, people, sub-topics) returns `None`.
    #[must_use]
    pub fn from_gamma_tag_slug(slug: &str) -> Option<Self> {
        match slug.trim().to_ascii_lowercase().as_str() {
            "politics" | "elections" => Some(Self::Politics),
            "geopolitics" | "world" => Some(Self::Geopolitics),
            "crypto" => Some(Self::Crypto),
            "sports" => Some(Self::Sports),
            "tech" | "technology" | "ai" => Some(Self::Tech),
            "culture" | "pop-culture" => Some(Self::Culture),
            "weather" | "climate" => Some(Self::Weather),
            "economy" | "economics" => Some(Self::Economics),
            "finance" => Some(Self::Finance),
            _ => None,
        }
    }

    /// Stable precedence for consumers that require one primary cohort.
    ///
    /// Fee calculation must never use this projection. Venue fees come only
    /// from point-in-time CLOB market-info observations.
    #[must_use]
    #[inline]
    pub const fn primary_rank(self) -> u8 {
        match self {
            Self::Crypto => 9,
            Self::Economics => 8,
            Self::Culture => 7,
            Self::Weather => 6,
            Self::Politics => 5,
            Self::Finance => 4,
            Self::Tech => 3,
            Self::Sports => 2,
            Self::Geopolitics => 1,
            Self::Other => 0,
        }
    }
}

/// Bit set of [`MarketCategory`] memberships derived from Gamma event tags.
///
/// One event frequently carries several category tags (e.g. politics +
/// geopolitics + world). The set preserves every membership for selection
/// filtering, while [`Self::primary_category`] supplies a deterministic
/// single category only to consumers whose contract requires one cohort.
///
/// Serialized as an array of category names for registry snapshots and
/// debuggability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CategorySet(u16);

impl CategorySet {
    /// The empty set (no memberships).
    pub const EMPTY: Self = Self(0);

    /// Build a set from Gamma tag slugs; unknown slugs are ignored.
    pub fn from_slugs<'a>(slugs: impl IntoIterator<Item = &'a str>) -> Self {
        let mut set = Self::EMPTY;
        for slug in slugs {
            if let Some(category) = MarketCategory::from_gamma_tag_slug(slug) {
                set.insert(category);
            }
        }
        set
    }

    /// Add a category membership.
    #[inline]
    pub const fn insert(&mut self, category: MarketCategory) {
        self.0 |= 1 << category.table_index();
    }

    /// Whether `category` is a member.
    #[must_use]
    #[inline]
    pub const fn contains(self, category: MarketCategory) -> bool {
        self.0 & (1 << category.table_index()) != 0
    }

    /// Whether the two sets share at least one member.
    #[must_use]
    #[inline]
    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    /// Whether the set has no members.
    #[must_use]
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Iterate the member categories in `ALL_VARIANTS` order.
    pub fn iter(self) -> impl Iterator<Item = MarketCategory> {
        MarketCategory::ALL_VARIANTS
            .into_iter()
            .filter(move |category| self.contains(*category))
    }

    /// Deterministic single category for single-cohort consumers.
    ///
    /// Fee calculation must use PIT CLOB market info, never this category. The
    /// empty set collapses to [`MarketCategory::Other`].
    #[must_use]
    pub fn primary_category(self) -> MarketCategory {
        self.iter()
            .max_by_key(|category| category.primary_rank())
            .unwrap_or(MarketCategory::Other)
    }
}

impl From<MarketCategory> for CategorySet {
    fn from(category: MarketCategory) -> Self {
        let mut set = Self::EMPTY;
        set.insert(category);
        set
    }
}

impl IntoActiveValue<Vec<MarketCategory>> for CategorySet {
    fn into_active_value(self) -> ActiveValue<Vec<MarketCategory>> {
        ActiveValue::Set(self.iter().collect())
    }
}

impl From<&[MarketCategory]> for CategorySet {
    fn from(categories: &[MarketCategory]) -> Self {
        categories.iter().copied().collect()
    }
}

impl FromIterator<MarketCategory> for CategorySet {
    fn from_iter<I: IntoIterator<Item = MarketCategory>>(iter: I) -> Self {
        let mut set = Self::EMPTY;
        for category in iter {
            set.insert(category);
        }
        set
    }
}

impl Serialize for CategorySet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for CategorySet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let categories = Vec::<MarketCategory>::deserialize(deserializer)?;
        Ok(categories.into_iter().collect())
    }
}

/// Error returned when a category string cannot be parsed (config / DB keys).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown market category: {0}")]
pub struct MarketCategoryParseError(pub String);

impl FromStr for MarketCategory {
    type Err = MarketCategoryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "geopolitics" | "Geopolitics" => Ok(Self::Geopolitics),
            "sports" | "Sports" => Ok(Self::Sports),
            "politics" | "Politics" => Ok(Self::Politics),
            "finance" | "Finance" => Ok(Self::Finance),
            "tech" | "Tech" => Ok(Self::Tech),
            "culture" | "Culture" | "Pop Culture" => Ok(Self::Culture),
            "weather" | "Weather" | "Climate" => Ok(Self::Weather),
            "economics" | "Economics" => Ok(Self::Economics),
            "crypto" | "Crypto" => Ok(Self::Crypto),
            "other" | "Other" => Ok(Self::Other),
            other => Err(MarketCategoryParseError(other.to_owned())),
        }
    }
}

/// Invalid tick size string from Gamma / CLOB wire format.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid tick size: {0}")]
pub struct TickSizeParseError(pub String);

pg_enum! {
    type_name = "qp_tick_size",
    @from_str(trim)
    @from_str(err = TickSizeParseError)
    pub enum TickSize {
        Tenth => "0.1",
        Hundredth => "0.01",
        HalfCent => "0.005",
        QuarterCent => "0.0025",
        Thousandth => "0.001",
        TenThousandth => "0.0001",
    }
}

impl TickSize {
    #[must_use]
    #[inline]
    pub const fn as_decimal(&self) -> Decimal {
        match self {
            Self::Tenth => dec!(0.1),
            Self::Hundredth => dec!(0.01),
            Self::HalfCent => dec!(0.005),
            Self::QuarterCent => dec!(0.0025),
            Self::Thousandth => dec!(0.001),
            Self::TenThousandth => dec!(0.0001),
        }
    }
}

impl TryFrom<Decimal> for TickSize {
    type Error = TickSizeParseError;

    fn try_from(d: Decimal) -> Result<Self, TickSizeParseError> {
        Self::from_str(&d.normalize().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::sea_query::{ArrayType, Value};
    use std::str::FromStr;

    #[test]
    fn postgres_enum_array_value_preserves_native_type_identity() {
        let value = Value::from(vec![MarketCategory::Sports, MarketCategory::Crypto]);
        let Value::Array(ArrayType::Enum(type_name), Some(values)) = value else {
            panic!("MarketCategory array must bind as a PostgreSQL enum array");
        };
        assert_eq!(type_name.as_ref().as_ref(), "qp_market_category");
        assert!(values.iter().all(|value| matches!(value, Value::Enum(_))));
    }

    #[test]
    fn category_set_from_slugs_collects_every_match_and_ignores_unknowns() {
        let set = CategorySet::from_slugs(["politics", "geopolitics", "world", "trump", "earn-4"]);
        assert!(set.contains(MarketCategory::Politics));
        assert!(set.contains(MarketCategory::Geopolitics));
        assert!(!set.contains(MarketCategory::Other));
        assert_eq!(set.iter().count(), 2);
    }

    #[test]
    fn category_set_from_empty_slugs_is_empty() {
        let set = CategorySet::from_slugs([] as [&str; 0]);
        assert!(set.is_empty());
        assert_eq!(set.primary_category(), MarketCategory::Other);
    }

    #[test]
    fn primary_category_uses_stable_precedence() {
        let set = CategorySet::from_slugs(["politics", "crypto", "geopolitics"]);
        assert_eq!(set.primary_category(), MarketCategory::Crypto);

        let set = CategorySet::from_slugs(["sports", "geopolitics"]);
        assert_eq!(set.primary_category(), MarketCategory::Sports);
    }

    #[test]
    fn tick_size_parses_polymarket_labels_including_half_and_quarter_cent() {
        assert_eq!(TickSize::from_str("0.1").expect("tenth"), TickSize::Tenth);
        assert_eq!(
            TickSize::from_str("0.01").expect("hundredth"),
            TickSize::Hundredth
        );
        assert_eq!(
            TickSize::from_str("0.005").expect("half cent"),
            TickSize::HalfCent
        );
        assert_eq!(
            TickSize::from_str("0.0025").expect("quarter cent"),
            TickSize::QuarterCent
        );
        assert_eq!(
            TickSize::from_str("0.001").expect("thousandth"),
            TickSize::Thousandth
        );
        assert_eq!(
            TickSize::from_str("0.0001").expect("ten thousandth"),
            TickSize::TenThousandth
        );
        assert!(TickSize::from_str("0.00001").is_err());
    }

    #[test]
    fn tick_size_try_from_decimal_and_as_decimal_round_trip() {
        for tick in [
            TickSize::Tenth,
            TickSize::Hundredth,
            TickSize::HalfCent,
            TickSize::QuarterCent,
            TickSize::Thousandth,
            TickSize::TenThousandth,
        ] {
            let decimal = tick.as_decimal();
            assert_eq!(TickSize::try_from(decimal).expect("round-trip"), tick);
        }
        assert_eq!(
            TickSize::try_from(dec!(0.005)).expect("half cent"),
            TickSize::HalfCent
        );
        assert_eq!(
            TickSize::try_from(dec!(0.0025)).expect("quarter cent"),
            TickSize::QuarterCent
        );
    }

    #[test]
    fn primary_rank_is_a_total_order_over_all_variants() {
        let mut ranks: Vec<u8> = MarketCategory::ALL_VARIANTS
            .iter()
            .map(|category| category.primary_rank())
            .collect();
        ranks.sort_unstable();
        ranks.dedup();
        assert_eq!(ranks.len(), MarketCategory::ALL_VARIANTS.len());
    }

    #[test]
    fn category_set_intersects_any_membership() {
        let multi = CategorySet::from_slugs(["politics", "geopolitics"]);
        assert!(multi.intersects(CategorySet::from(MarketCategory::Geopolitics)));
        assert!(!multi.intersects(CategorySet::from(MarketCategory::Sports)));
        assert!(!multi.intersects(CategorySet::EMPTY));
    }

    #[test]
    fn category_set_serde_round_trips_as_name_array() {
        let set = CategorySet::from_slugs(["crypto", "sports"]);
        let json = serde_json::to_string(&set).expect("serialize");
        assert_eq!(json, r#"["sports","crypto"]"#);
        let parsed: CategorySet = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, set);
    }

    #[test]
    fn tag_slug_mapping_covers_aliases() {
        assert_eq!(
            MarketCategory::from_gamma_tag_slug("pop-culture"),
            Some(MarketCategory::Culture)
        );
        assert_eq!(
            MarketCategory::from_gamma_tag_slug("economy"),
            Some(MarketCategory::Economics)
        );
        assert_eq!(
            MarketCategory::from_gamma_tag_slug("AI"),
            Some(MarketCategory::Tech)
        );
        assert_eq!(MarketCategory::from_gamma_tag_slug("nba-finals"), None);
    }
}
