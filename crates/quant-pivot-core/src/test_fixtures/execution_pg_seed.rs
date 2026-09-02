//! Pure execution value builders used by core unit tests.

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::market::fee::BuilderFeeAttribution,
    enums::common::{OrderType, Side, TickSize},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, Bps, ContentHash, EntryMakerRebateTerms, MarketId, PreparedFeeSchedule,
        PreparedVenueOrder, Price, ResearchEvaluationTrack, ResearchProfileRef, Shares,
        SourceSliceManifestRef, TokenId, Usd, VenueOrderAmount, builtin_research_profiles,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[must_use]
pub fn fixture_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("research profiles")
        .into_iter()
        .find(|profile| {
            profile.spec.activation_eligibility == ResearchEvaluationTrack::ExecutionCandidate
        })
        .expect("weather profile")
        .profile_ref
}

pub struct PreparedOrderFixture {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub order_type: OrderType,
    pub venue_amount: VenueOrderAmount,
    pub expected_fee: Usd,
    pub expected_filled_shares: Shares,
    pub limit_price: Price,
}

impl PreparedOrderFixture {
    #[must_use]
    pub fn build(self) -> PreparedVenueOrder {
        let now = Utc::now();
        let total_cash_delta = match (self.side, self.venue_amount) {
            (Side::Buy, VenueOrderAmount::PrincipalUsd(principal)) => {
                -(principal.inner() + self.expected_fee.inner())
            }
            (Side::Sell, VenueOrderAmount::Shares(shares)) => {
                shares.inner() * self.limit_price.inner() - self.expected_fee.inner()
            }
            _ => Decimal::ZERO,
        };
        PreparedVenueOrder {
            profile_ref: fixture_profile_ref(),
            market_id: self.market_id,
            token_id: self.token_id,
            tick_size: TickSize::Hundredth,
            minimum_order_size: Shares::new(dec!(1)),
            neg_risk: false,
            side: self.side,
            order_type: self.order_type,
            post_only: false,
            limit_price: self.limit_price,
            expected_worst_fill_price: self.limit_price,
            cash_budget: self
                .venue_amount
                .principal_usd()
                .map(|principal| principal + self.expected_fee),
            venue_amount: self.venue_amount,
            requested_shares: self.expected_filled_shares,
            expected_fee: self.expected_fee,
            total_cash_delta,
            expected_filled_shares: self.expected_filled_shares,
            book_hash: content_hash('b'),
            clob_market_info_payload_hash: content_hash('c'),
            fee_schedule: PreparedFeeSchedule {
                schedule_hash: content_hash('f'),
                effective_at: now,
                available_at: now,
                platform_rate: dec!(0.02),
                exponent: Decimal::ONE,
                taker_only: true,
                builder_maker_fee_bps: Bps::ZERO,
                builder_taker_fee_bps: Bps::ZERO,
                builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            },
            maker_rebate_terms: EntryMakerRebateTerms::AggressiveNotApplicable,
            prepared_at: now,
            valid_until: now + Duration::hours(1),
        }
    }
}

#[must_use]
pub fn content_hash(seed: char) -> ContentHash {
    CanonicalDigest::content_hash_json(&seed).expect("canonical fixture content hash")
}

#[must_use]
pub fn source_slice_ref(seed: char) -> SourceSliceManifestRef {
    SourceSliceManifestRef {
        manifest_uri: ArtifactUri::parse(format!("s3://fixture/source-slices/{seed}.json"))
            .expect("source-slice URI"),
        manifest_hash: content_hash(seed),
    }
}
