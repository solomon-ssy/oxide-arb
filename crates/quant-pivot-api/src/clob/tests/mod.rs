mod order;
mod support;

use chrono::NaiveDate;
use quant_pivot_models::types::EvmAddress;
use rust_decimal_macros::dec;

use super::{RawMakerRebateReportedAccrual, normalize_maker_rebate_award};

#[test]
fn maker_award_validates_identity() {
    let expected_date = NaiveDate::from_ymd_opt(2026, 8, 15).expect("date");
    let expected_maker = EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("maker address");
    let award = normalize_maker_rebate_award(
        RawMakerRebateReportedAccrual {
            date: expected_date,
            condition_id: format!("0x{}", "2".repeat(64)),
            asset_address: format!("0x{}", "3".repeat(40)),
            maker_address: expected_maker.to_string(),
            rebated_fees_usdc: dec!(0.237519),
        },
        expected_date,
        &expected_maker,
    )
    .expect("valid award");

    assert_eq!(award.program_date, expected_date);
    assert_eq!(award.maker_address, expected_maker);
    assert_eq!(award.amount_usd.inner(), dec!(0.237519));
}

#[test]
fn maker_award_rejects_negative() {
    let expected_date = NaiveDate::from_ymd_opt(2026, 8, 15).expect("date");
    let expected_maker = EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("maker address");
    let error = normalize_maker_rebate_award(
        RawMakerRebateReportedAccrual {
            date: expected_date,
            condition_id: format!("0x{}", "2".repeat(64)),
            asset_address: format!("0x{}", "3".repeat(40)),
            maker_address: expected_maker.to_string(),
            rebated_fees_usdc: dec!(-0.01),
        },
        expected_date,
        &expected_maker,
    )
    .expect_err("negative award must fail closed");

    assert!(error.to_string().contains("negative venue award"));
}
