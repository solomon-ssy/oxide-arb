//! Insta JSON contract snapshots for report payload variants.

use insta::assert_json_snapshot;
use support::report_snapshots::{
    TopNReportSnapshot, empty_report, recommendation_immediate_entry, recommendation_limit_entry,
    recommendation_operator_only, recommendation_partial_exits, revoked_report,
};

mod support;

#[test]
fn non_empty_topn_snapshot() {
    assert_json_snapshot!(
        "non_empty_topn_report_snapshot",
        TopNReportSnapshot::non_empty()
    );
}

#[test]
fn empty_report_snapshot() {
    assert_json_snapshot!(empty_report());
}

#[test]
fn recommendation_limit_entry_snapshot() {
    assert_json_snapshot!(recommendation_limit_entry());
}

#[test]
fn recommendation_immediate_entry_snapshot() {
    assert_json_snapshot!(recommendation_immediate_entry());
}

#[test]
fn recommendation_partial_exits_snapshot() {
    assert_json_snapshot!(recommendation_partial_exits());
}

#[test]
fn recommendation_operator_only_snapshot() {
    assert_json_snapshot!(
        "recommendation_operator_only_snapshot",
        recommendation_operator_only()
    );
}

#[test]
fn revoked_report_snapshot() {
    assert_json_snapshot!(revoked_report());
}
