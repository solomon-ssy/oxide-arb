//! Insta JSON contract snapshots for report payload variants.

use insta::assert_json_snapshot;
use support::report_snapshots::{
    empty_report, non_empty_topn_report, recommendation_immediate_entry,
    recommendation_limit_entry, recommendation_not_auto_eligible, recommendation_partial_exits,
    revoked_report,
};

mod support;

#[test]
fn non_empty_topn_report_snapshot() {
    assert_json_snapshot!(non_empty_topn_report());
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
fn recommendation_not_auto_eligible_snapshot() {
    assert_json_snapshot!(recommendation_not_auto_eligible());
}

#[test]
fn revoked_report_snapshot() {
    assert_json_snapshot!(revoked_report());
}
