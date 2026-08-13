//! Fixtures local to report wire-contract snapshots.

pub mod report_fixtures;
pub mod report_snapshots;

use uuid::Uuid;

#[must_use]
fn seeded_uuid(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}
