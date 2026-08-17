//! Canonical point-in-time Polymarket execution-fee schedule.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{Bps, ClobMarketInfoVersionId, ContentHash, MarketId, Usd};

/// Exact order-route attribution used when calculating builder fees.
///
/// deliberately supports only unattributed venue orders. Builder
/// rates remain part of the venue fact so a later builder-enabled artifact must
/// introduce a new route contract instead of silently changing historical `PnL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuilderFeeAttribution {
    NoBuilderCode,
}

/// Complete append-only fee fact resolved from one CLOB market-info revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketFeeSchedule {
    pub market_id: MarketId,
    pub market_info_version_id: ClobMarketInfoVersionId,
    pub market_info_payload_hash: ContentHash,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub builder_maker_fee_bps: Bps,
    pub builder_taker_fee_bps: Bps,
    pub builder_attribution: BuilderFeeAttribution,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

/// Canonical Gamma-sourced maker-rebate fact for one market revision.
///
/// This is intentionally separate from [`MarketFeeSchedule`]: CLOB market
/// metadata remains authoritative for immediate execution fees, while Gamma
/// is authoritative only for the delayed maker-incentive program. The copied
/// fee-curve fields exist solely for source-consistency validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketMakerRebateSchedule {
    pub market_id: MarketId,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub rebate_rate: Decimal,
    /// Identity of the normalized upstream terms. Local observation clocks are
    /// deliberately excluded so unchanged Gamma payloads remain content-stable.
    pub terms_hash: ContentHash,
}

/// Why Gamma did not provide an economically decidable maker-rebate program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateUnavailableReason {
    FeesFlagMissing,
    EnabledScheduleMissing,
    ScheduleIncomplete,
    InvalidSchedule,
    DisabledSchedulePresent,
}

/// Gamma field whose absence or invalid value made rebate terms undecidable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateField {
    FeesEnabled,
    FeeSchedule,
    PlatformRate,
    Exponent,
    TakerOnly,
    RebateRate,
}

/// Required maker-rebate truth carried by every catalog market object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MarketMakerRebateEvidence {
    NoProgram {
        terms_hash: ContentHash,
    },
    Available {
        schedule: MarketMakerRebateSchedule,
    },
    Unavailable {
        reason: MakerRebateUnavailableReason,
        missing_fields: Vec<MakerRebateField>,
        invalid_fields: Vec<MakerRebateField>,
        terms_hash: ContentHash,
    },
}

impl MarketMakerRebateEvidence {
    /// Explicit source-unavailable fixture for non-catalog construction paths.
    pub fn source_unavailable() -> Self {
        Self::Unavailable {
            reason: MakerRebateUnavailableReason::FeesFlagMissing,
            missing_fields: vec![MakerRebateField::FeesEnabled],
            invalid_fields: Vec::new(),
            terms_hash: ContentHash::from_bytes([0; 32]),
        }
    }

    /// Complete schedule when Gamma published an active program.
    #[must_use]
    pub const fn schedule(&self) -> Option<&MarketMakerRebateSchedule> {
        match self {
            Self::Available { schedule } => Some(schedule),
            Self::NoProgram { .. } | Self::Unavailable { .. } => None,
        }
    }

    /// Whether the source state is sufficient for passive economic decisions.
    #[must_use]
    pub const fn is_decidable(&self) -> bool {
        matches!(self, Self::NoProgram { .. } | Self::Available { .. })
    }

    /// Stable identity of the exact upstream fee/rebate fields.
    #[must_use]
    pub const fn terms_hash(&self) -> ContentHash {
        match self {
            Self::NoProgram { terms_hash } | Self::Unavailable { terms_hash, .. } => *terms_hash,
            Self::Available { schedule } => schedule.terms_hash,
        }
    }
}

/// Decision-time maker-rebate terms carried into the executable order.
///
/// This projection deliberately retains the independent Gamma identities and
/// curve terms needed to account for a later authenticated maker fill. It is
/// never reconstructed from a process-current catalog after the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrozenMakerRebateSchedule {
    pub terms_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub platform_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub exponent: Decimal,
    pub taker_only: bool,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub rebate_rate: Decimal,
}

impl FrozenMakerRebateSchedule {
    /// Validate the frozen terms at the decision boundary.
    pub fn validate_at(self, decision_at: DateTime<Utc>) -> Result<(), &'static str> {
        if self.available_at > decision_at {
            return Err("maker rebate schedule is not point-in-time visible");
        }
        if self.platform_rate < Decimal::ZERO
            || self.platform_rate > Decimal::ONE
            || self.exponent <= Decimal::ZERO
            || self.exponent > Decimal::from(8)
            || self.rebate_rate < Decimal::ZERO
            || self.rebate_rate > Decimal::ONE
        {
            return Err("maker rebate schedule contains an invalid rate or exponent");
        }
        Ok(())
    }
}

impl MarketMakerRebateSchedule {
    /// Validate the source terms independently from catalog observation time.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.platform_rate < Decimal::ZERO
            || self.platform_rate > Decimal::ONE
            || self.exponent <= Decimal::ZERO
            || self.exponent > Decimal::from(8)
            || self.rebate_rate < Decimal::ZERO
            || self.rebate_rate > Decimal::ONE
        {
            return Err("maker rebate schedule contains an invalid rate or exponent");
        }
        Ok(())
    }
}

/// Immediate account cost of one filled venue execution.
///
/// `cash_outlay_usd` is always the principal plus both immediate fee
/// components. A caller projecting a SELL cash flow subtracts the fees from
/// principal; delayed incentives never enter this structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImmediateExecutionCost {
    pub principal_usd: Usd,
    pub venue_fee_usd: Usd,
    pub builder_fee_usd: Usd,
    pub cash_outlay_usd: Usd,
}

impl ImmediateExecutionCost {
    /// Construct a cost only when the additive cash invariant holds exactly.
    pub fn new(
        principal_usd: Usd,
        venue_fee_usd: Usd,
        builder_fee_usd: Usd,
    ) -> Result<Self, &'static str> {
        if principal_usd.is_negative()
            || venue_fee_usd.is_negative()
            || builder_fee_usd.is_negative()
        {
            return Err("immediate execution cost cannot contain negative values");
        }
        Ok(Self {
            principal_usd,
            venue_fee_usd,
            builder_fee_usd,
            cash_outlay_usd: principal_usd + venue_fee_usd + builder_fee_usd,
        })
    }

    /// Total immediate fee charged by the venue and attributed builder.
    #[must_use]
    pub fn total_fee_usd(self) -> Usd {
        self.venue_fee_usd + self.builder_fee_usd
    }
}

/// Eligibility state frozen into a maker-rebate accrual estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateEligibility {
    /// A confirmed maker fill is covered by the visible Gamma schedule.
    EligibleMakerFill,
}

/// Delayed venue incentive estimated from one actual or simulated maker fill.
///
/// This is deliberately not a fee and never changes immediate cash outlay,
/// hard reservation, maximum loss, or spendable balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeferredVenueIncentive {
    pub expected_rebate_usd: Usd,
    /// UTC program day containing the maker fill.
    pub program_date: NaiveDate,
    pub source_terms_hash: ContentHash,
    pub eligibility: MakerRebateEligibility,
}
