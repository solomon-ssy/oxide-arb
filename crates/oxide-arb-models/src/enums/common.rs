//! Common enums used across the oxide-arb platform.
//!
//! Enums that appear as `SeaORM` entity columns use [`active_string_enum!`] so they
//! can be stored directly in the database without JSON serialization.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
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
        /// Order failed/errored, or submitted-but-unconfirmed (orphaned).
        Failed => "failed",
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
        /// Submitted but never confirmed past the timeout — needs reconciliation. Terminal.
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
            Self::FailObserved | Self::FailProcessing | Self::Failed | Self::Orphaned => {
                Some(TradeBusinessOutcome::Failed)
            }
            Self::Intent | Self::Submitted => None,
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

active_string_enum! {
    /// Polymarket event category for fee-rate lookup and opportunity scoring.
    @derive(PartialOrd, Ord)
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

    /// Parse Gamma API category labels; unknown labels map to [`Self::Other`].
    #[must_use]
    pub fn from_gamma_label(label: Option<&str>) -> Self {
        label.and_then(|s| s.parse().ok()).unwrap_or(Self::Other)
    }
}

impl From<Option<&str>> for MarketCategory {
    fn from(label: Option<&str>) -> Self {
        Self::from_gamma_label(label)
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

/// Configured on-chain redemption route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedeemRoute {
    #[default]
    Disabled,
    StandardCtf,
    NegRiskLegacyAdapter,
    CtfCollateralAdapter,
    NegRiskCollateralAdapter,
    ProxySafe,
}

impl RedeemRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::StandardCtf => "standard_ctf",
            Self::NegRiskLegacyAdapter => "neg_risk_legacy_adapter",
            Self::CtfCollateralAdapter => "ctf_collateral_adapter",
            Self::NegRiskCollateralAdapter => "neg_risk_collateral_adapter",
            Self::ProxySafe => "proxy_safe",
        }
    }
}

impl Display for RedeemRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Settlement redeem output asset for adapter routes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedeemOutputAsset {
    #[default]
    UsdcE,
    Pusd,
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
