//! Pure execution value builders used by core unit tests.

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::market::fee::BuilderFeeAttribution,
    enums::common::{OrderType, Side},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, Bps, ContentHash, PreparedFeeSchedule, PreparedVenueOrder, Price,
        ResearchEvaluationTrack, ResearchProfileRef, Shares, SourceSliceManifestRef, TokenId, Usd,
        VenueOrderAmount, builtin_research_profiles,
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
            profile.spec.activation_eligibility == ResearchEvaluationTrack::SemiAutoCandidate
        })
        .expect("weather profile")
        .profile_ref
}

#[must_use]
pub fn prepared_order(
    side: Side,
    order_type: OrderType,
    venue_amount: VenueOrderAmount,
    expected_fee: Usd,
    expected_filled_shares: Shares,
    worst_price: Price,
) -> PreparedVenueOrder {
    let now = Utc::now();
    let total_cash_delta = match (side, venue_amount) {
        (Side::Buy, VenueOrderAmount::GrossUsd(gross)) => -(gross.inner() + expected_fee.inner()),
        (Side::Sell, VenueOrderAmount::Shares(shares)) => {
            shares.inner() * worst_price.inner() - expected_fee.inner()
        }
        _ => Decimal::ZERO,
    };
    PreparedVenueOrder {
        profile_ref: fixture_profile_ref(),
        token_id: TokenId::new("1001"),
        side,
        order_type,
        post_only: false,
        worst_price,
        cash_budget: venue_amount.gross_usd().map(|gross| gross + expected_fee),
        venue_amount,
        expected_fee,
        total_cash_delta,
        expected_filled_shares,
        book_hash: content_hash('b'),
        clob_market_info_hash: content_hash('c'),
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
        prepared_at: now,
        valid_until: now + Duration::hours(1),
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
