//! Link-time schema catalog.

use super::{seed::SeedSpec, table::TableSpec};
use linkme::distributed_slice;
use std::collections::BTreeSet;

#[allow(unsafe_code)]
#[distributed_slice]
pub static TABLE_SPECS: [TableSpec] = [..];

/// All discovered tables sorted by table name for deterministic migrations.
pub fn tables() -> Vec<&'static TableSpec> {
    let mut specs = TABLE_SPECS.iter().collect::<Vec<_>>();
    specs.sort_by_key(|spec| (spec.table_name)());
    assert_unique_table_names(&specs);
    specs
}

/// All discovered seeds sorted by id/version for deterministic execution.
pub fn seeds() -> Vec<SeedSpec> {
    let mut specs = tables()
        .into_iter()
        .flat_map(|table| (table.seed_units)())
        .collect::<Vec<_>>();
    specs.sort_by_key(|spec| (spec.id, spec.version));
    specs
}

fn assert_unique_table_names(specs: &[&TableSpec]) {
    let mut seen = BTreeSet::new();
    for spec in specs {
        let name = (spec.table_name)();
        assert!(
            seen.insert(name.clone()),
            "duplicate table schema registered: {name}"
        );
    }
}
