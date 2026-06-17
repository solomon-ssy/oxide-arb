//! Common enums used across the oxide-arb platform.
//!
//! Enums that appear as `SeaORM` entity columns use [`active_string_enum!`] so they
//! can be stored directly in the database without JSON serialization.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveValue, IntoActiveValue};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use thiserror::Error;

active_string_enum! {
    /// Trade direction.
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
}

/// Polymarket CLOB order time-in-force types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Fill-or-Kill: must fill entirely and immediately, or cancel. Never retried.
    Fok,
    /// Good-Till-Cancelled: rests on the book until filled or manually cancelled.
    Gtc,
    /// Good-Till-Date: rests on the book until `expiration` (unix timestamp).
    Gtd { expiration: u64 },
}

active_string_enum! {
    /// Trade execution mode.
    @derive(Default)
    pub enum ExecutionMode {
        #[default]
        DryRun => "dry_run",
        Paper => "paper",
        Live => "live",
    }
}

active_string_enum! {
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

active_string_enum! {
    /// Success/miss/failed bucket derived from [`TradeState`].
    ///
    /// This is not the trade's stored state: the `trade` row has exactly one state
    /// field, while this bucket is surfaced as a read-only PG generated column for
    /// risk accounting, reporting, and audit.
    pub enum TradeBusinessOutcome {
        /// Order filled, position opened.
        Success => "success",
        /// FOK not filled (book moved or insufficient depth).
        Miss => "miss",
        /// Order definitely failed/errored.
        Failed => "failed",
    }
}

active_string_enum! {
    /// Operator/worker conclusion for a trade that entered the reconciliation queue.
    ///
    /// This is intentionally separate from [`TradeBusinessOutcome`]: pending
    /// reconciliation is not a business outcome, and an unresolved ambiguity must
    /// never be counted as a failed trade.
    pub enum TradeReconcileResolution {
        /// External evidence proves the venue filled the order.
        Filled => "filled",
        /// External evidence proves the venue did not fill the order.
        Miss => "miss",
        /// External evidence could not resolve the order safely.
        Unresolvable => "unresolvable",
    }
}

/// Semantic kind of a trade row's `net_profit_usd` on the API wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetProfitKind {
    /// No fill-time EV is recorded.
    None,
    /// Fill-time expected value — not realized settlement `PnL`.
    FillEv,
}

impl NetProfitKind {
    /// Derive the wire kind from the persisted optional EV column.
    #[must_use]
    pub const fn for_net_profit(net_profit_usd: &Option<crate::types::Usd>) -> Self {
        if net_profit_usd.is_some() {
            Self::FillEv
        } else {
            Self::None
        }
    }
}

active_string_enum! {
    /// Durable trade lifecycle state machine — single source of truth on the `trade` row.
    ///
    /// Replaces the former `phase × outcome` two-column model: one field makes illegal
    /// states unrepresentable (e.g. "processed but no outcome" cannot occur). The
    /// business-outcome classification (success/miss/failed) is derived via
    /// [`TradeState::business_outcome`] in Rust and a PG generated column for reporting.
    ///
    /// Transitions:
    /// `Intent` → `Submitted` → (`FillObserved` | `MissObserved` | `FailObserved`)
    ///   → (`FillProcessing` | `MissProcessing` | `FailProcessing`)
    ///   → (`Settled` | `Missed` | `Failed`); stale `Submitted` → `Orphaned`.
    /// `Orphaned` means "unknown venue outcome, needs reconciliation"; it is
    /// deliberately not a failed trade until an explicit reconciliation terminal
    /// resolution says so.
    /// The `*Observed` states are unclaimed durable work. The `*Processing` states
    /// are lease-backed claims and may be reclaimed after lease expiry.
    pub enum TradeState {
        /// Row inserted, order not yet submitted to the venue.
        Intent => "intent",
        /// Order signed and sent, venue outcome not yet observed (crash-orphan window).
        Submitted => "submitted",
        /// Fill observed; position + risk Fill accounting not yet applied (relay queue).
        FillObserved => "fill_observed",
        /// FOK miss observed; finalization not yet applied (relay queue).
        MissObserved => "miss_observed",
        /// Venue error/timeout observed; finalization not yet applied (relay queue).
        FailObserved => "fail_observed",
        /// Fill relay side-effects are currently claimed by a worker.
        FillProcessing => "fill_processing",
        /// Miss relay finalization is currently claimed by a worker.
        MissProcessing => "miss_processing",
        /// Failure relay finalization is currently claimed by a worker.
        FailProcessing => "fail_processing",
        /// Fill fully processed (position created, risk Fill accounted). Terminal.
        Settled => "settled",
        /// Miss fully processed. Terminal.
        Missed => "missed",
        /// Failure fully processed. Terminal.
        Failed => "failed",
        /// Submitted but never confirmed past the timeout — needs reconciliation.
        Orphaned => "orphaned",
    }
}

impl TradeState {
    /// States awaiting relay processing (outcome observed, side-effects not applied).
    #[must_use]
    pub const fn is_unprocessed(self) -> bool {
        matches!(
            self,
            Self::FillObserved | Self::MissObserved | Self::FailObserved
        )
    }

    /// States held by a relay worker lease.
    #[must_use]
    pub const fn is_processing(self) -> bool {
        matches!(
            self,
            Self::FillProcessing | Self::MissProcessing | Self::FailProcessing
        )
    }

    /// Terminal state reached once the relay processes the matching observed state.
    #[must_use]
    pub const fn processed_terminal(self) -> Option<Self> {
        match self {
            Self::FillObserved | Self::FillProcessing => Some(Self::Settled),
            Self::MissObserved | Self::MissProcessing => Some(Self::Missed),
            Self::FailObserved | Self::FailProcessing => Some(Self::Failed),
            _ => None,
        }
    }

    /// Business-outcome classification, or `None` while still in-flight.
    #[must_use]
    pub const fn business_outcome(self) -> Option<TradeBusinessOutcome> {
        match self {
            Self::FillObserved | Self::FillProcessing | Self::Settled => {
                Some(TradeBusinessOutcome::Success)
            }
            Self::MissObserved | Self::MissProcessing | Self::Missed => {
                Some(TradeBusinessOutcome::Miss)
            }
            Self::FailObserved | Self::FailProcessing | Self::Failed => {
                Some(TradeBusinessOutcome::Failed)
            }
            Self::Intent | Self::Submitted | Self::Orphaned => None,
        }
    }

    /// True once a fill has been observed (whether or not post-trade is applied).
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(
            self,
            Self::FillObserved | Self::FillProcessing | Self::Settled
        )
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

active_string_enum! {
    /// Polymarket event category for fee-rate lookup and opportunity scoring.
    @derive(PartialOrd, Ord, schemars::JsonSchema)
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

    /// Map a Gamma event tag slug to a fee category.
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

    /// Fee-conservatism rank: higher rank means a higher documented fee rate.
    ///
    /// Used to break ties when an event maps to several categories — picking
    /// the highest-fee category overestimates (never underestimates) fees,
    /// which is the safe direction for net-profit gating.
    /// Order per Polymarket fee docs: crypto 0.072 > economics/culture/weather
    /// 0.05 > politics/finance/tech 0.04 > sports 0.03 > geopolitics 0.
    #[must_use]
    #[inline]
    pub const fn fee_rank(self) -> u8 {
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
/// geopolitics + world). The set preserves every membership for universe
/// filtering, while [`Self::fee_category`] collapses to a deterministic
/// single category for fee estimation.
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

    /// Deterministic single category for fee estimation.
    ///
    /// Picks the member with the highest [`MarketCategory::fee_rank`]
    /// (fee-conservative: overestimate, never underestimate). The empty set
    /// collapses to [`MarketCategory::Other`].
    #[must_use]
    pub fn fee_category(self) -> MarketCategory {
        self.iter()
            .max_by_key(|category| category.fee_rank())
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

active_string_enum! {
    /// Minimum price increment supported by a Polymarket CLOB market.
    pub enum TickSize {
        Tenth => "0.1",
        Hundredth => "0.01",
        Thousandth => "0.001",
        TenThousandth => "0.0001",
    }
}

/// Invalid tick size string from Gamma / CLOB wire format.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid tick size: {0}")]
pub struct TickSizeParseError(pub String);

impl TickSize {
    #[must_use]
    #[inline]
    pub const fn as_decimal(&self) -> Decimal {
        match self {
            Self::Tenth => dec!(0.1),
            Self::Hundredth => dec!(0.01),
            Self::Thousandth => dec!(0.001),
            Self::TenThousandth => dec!(0.0001),
        }
    }
}

impl FromStr for TickSize {
    type Err = TickSizeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "0.1" => Ok(Self::Tenth),
            "0.01" => Ok(Self::Hundredth),
            "0.001" => Ok(Self::Thousandth),
            "0.0001" => Ok(Self::TenThousandth),
            other => Err(TickSizeParseError(other.to_owned())),
        }
    }
}

impl TryFrom<Decimal> for TickSize {
    type Error = TickSizeParseError;

    fn try_from(d: Decimal) -> Result<Self, TickSizeParseError> {
        Self::from_str(&d.normalize().to_string())
    }
}

active_string_enum! {
    /// Lifecycle status of a position.
    pub enum PositionStatus {
        Open => "open",
        Closed => "closed",
        Settled => "settled",
    }
}

active_string_enum! {
    /// Lifecycle status of on-chain CTF redemption for a position.
    pub enum RedeemStatus {
        NotRequired => "not_required",
        Pending => "pending",
        Completed => "completed",
        Failed => "failed",
    }
}

impl RedeemStatus {
    #[must_use]
    pub const fn initial_for_mode(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => Self::NotRequired,
            ExecutionMode::Live => Self::Pending,
        }
    }

    #[must_use]
    pub const fn settled_for_mode(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => Self::NotRequired,
            ExecutionMode::Live => Self::Completed,
        }
    }
}

/// On-chain redemption route for standard (non-neg-risk) markets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandardRedeemRoute {
    #[default]
    StandardCtf,
    CtfCollateralAdapter,
}

impl StandardRedeemRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StandardCtf => "standard_ctf",
            Self::CtfCollateralAdapter => "ctf_collateral_adapter",
        }
    }
}

impl Display for StandardRedeemRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// On-chain redemption route for neg-risk markets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NegRiskRedeemRoute {
    #[default]
    NegRiskLegacyAdapter,
    NegRiskCollateralAdapter,
}

impl NegRiskRedeemRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NegRiskLegacyAdapter => "neg_risk_legacy_adapter",
            Self::NegRiskCollateralAdapter => "neg_risk_collateral_adapter",
        }
    }
}

impl Display for NegRiskRedeemRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolved on-chain redemption route (all four live redeem paths).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedRedeemRoute {
    StandardCtf,
    CtfCollateralAdapter,
    NegRiskLegacyAdapter,
    NegRiskCollateralAdapter,
}

impl ResolvedRedeemRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StandardCtf => "standard_ctf",
            Self::CtfCollateralAdapter => "ctf_collateral_adapter",
            Self::NegRiskLegacyAdapter => "neg_risk_legacy_adapter",
            Self::NegRiskCollateralAdapter => "neg_risk_collateral_adapter",
        }
    }

    #[must_use]
    pub const fn expects_neg_risk(self) -> bool {
        matches!(
            self,
            Self::NegRiskLegacyAdapter | Self::NegRiskCollateralAdapter
        )
    }
}

impl From<StandardRedeemRoute> for ResolvedRedeemRoute {
    fn from(route: StandardRedeemRoute) -> Self {
        match route {
            StandardRedeemRoute::StandardCtf => Self::StandardCtf,
            StandardRedeemRoute::CtfCollateralAdapter => Self::CtfCollateralAdapter,
        }
    }
}

impl From<NegRiskRedeemRoute> for ResolvedRedeemRoute {
    fn from(route: NegRiskRedeemRoute) -> Self {
        match route {
            NegRiskRedeemRoute::NegRiskLegacyAdapter => Self::NegRiskLegacyAdapter,
            NegRiskRedeemRoute::NegRiskCollateralAdapter => Self::NegRiskCollateralAdapter,
        }
    }
}

impl FromStr for ResolvedRedeemRoute {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "standard_ctf" => Ok(Self::StandardCtf),
            "ctf_collateral_adapter" => Ok(Self::CtfCollateralAdapter),
            "neg_risk_legacy_adapter" => Ok(Self::NegRiskLegacyAdapter),
            "neg_risk_collateral_adapter" => Ok(Self::NegRiskCollateralAdapter),
            _ => Err(()),
        }
    }
}

impl Display for ResolvedRedeemRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

active_string_enum! {
    /// How a position's redeem route was resolved at fill time.
    pub enum RedeemResolutionSource {
        Override => "override",
        ClassStandard => "class_standard",
        ClassNegRisk => "class_neg_risk",
    }
}

active_string_enum! {
    /// Lifecycle status for post-redeem accounting persistence.
    pub enum SettlementAccountingStatus {
        Pending => "pending",
        Redeemed => "redeemed",
        Accounted => "accounted",
        Failed => "failed",
    }
}

active_string_enum! {
    /// Source that triggered market settlement processing.
    pub enum SettlementTrigger {
        Ws => "ws",
        PeriodicRetry => "periodic_retry",
        Manual => "manual",
    }
}

active_string_enum! {
    /// Lifecycle status of a potential-loss ledger entry.
    pub enum LedgerStatus {
        Active => "active",
        Resolved => "resolved",
        Expired => "expired",
    }
}

active_string_enum! {
    /// Type of report snapshot.
    pub enum ReportType {
        Daily => "daily",
        Weekly => "weekly",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(set.fee_category(), MarketCategory::Other);
    }

    #[test]
    fn fee_category_picks_the_highest_fee_member() {
        // crypto 0.072 beats politics 0.04 beats geopolitics 0.
        let set = CategorySet::from_slugs(["politics", "crypto", "geopolitics"]);
        assert_eq!(set.fee_category(), MarketCategory::Crypto);

        let set = CategorySet::from_slugs(["sports", "geopolitics"]);
        assert_eq!(set.fee_category(), MarketCategory::Sports);
    }

    #[test]
    fn fee_rank_is_a_total_order_over_all_variants() {
        let mut ranks: Vec<u8> = MarketCategory::ALL_VARIANTS
            .iter()
            .map(|category| category.fee_rank())
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
