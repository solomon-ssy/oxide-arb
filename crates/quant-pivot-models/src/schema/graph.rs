//! Dependency graph helpers for deterministic schema ordering.

use std::collections::{BTreeMap, BTreeSet};

use super::{catalog, table::TableSpec};

pub fn create_order() -> Vec<&'static TableSpec> {
    topological_order(false)
}

pub fn drop_order() -> Vec<&'static TableSpec> {
    topological_order(true)
}

fn topological_order(reverse: bool) -> Vec<&'static TableSpec> {
    let specs = catalog::tables();
    let by_name = specs
        .iter()
        .map(|spec| ((spec.table_name)(), *spec))
        .collect::<BTreeMap<_, _>>();

    let mut incoming = by_name
        .keys()
        .map(|name| (name.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = by_name
        .keys()
        .map(|name| (name.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();

    for spec in &specs {
        let table = (spec.table_name)();
        for dep in (spec.dependencies)() {
            let parent = (dep.table_name)();
            assert!(
                by_name.contains_key(&parent),
                "table `{table}` depends on unknown table `{parent}`"
            );
            incoming
                .entry(table.clone())
                .or_default()
                .insert(parent.clone());
            outgoing.entry(parent).or_default().insert(table.clone());
        }
    }

    let mut ready = incoming
        .iter()
        .filter_map(|(name, deps)| deps.is_empty().then_some(name.clone()))
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(specs.len());

    while let Some(name) = ready.pop_first() {
        ordered.push(name.clone());
        let children = outgoing.remove(&name).unwrap_or_default();
        for child in children {
            let deps = incoming
                .get_mut(&child)
                .expect("child table must be present in incoming map");
            deps.remove(&name);
            if deps.is_empty() {
                ready.insert(child);
            }
        }
    }

    assert_eq!(
        ordered.len(),
        specs.len(),
        "cycle detected in table dependency graph"
    );

    if reverse {
        ordered.reverse();
    }

    ordered
        .into_iter()
        .map(|name| by_name[&name])
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::{create_order, drop_order};
    use crate::schema::catalog;

    #[test]
    fn drop_order_is_reverse_create_order() {
        let drop = drop_order()
            .iter()
            .map(|spec| (spec.table_name)())
            .collect::<Vec<_>>();
        assert_eq!(
            drop,
            create_order()
                .iter()
                .map(|spec| (spec.table_name)())
                .rev()
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn phase51_fact_plane_tables_are_cataloged_without_legacy_aliases() {
        let tables = catalog::tables()
            .into_iter()
            .map(|spec| (spec.table_name)())
            .collect::<std::collections::BTreeSet<_>>();

        for required in [
            "balance_snapshot",
            "runtime_config_version",
            "runtime_config_activation",
            "control_factor_materialization_run",
            "control_factor_stage_report",
            "control_factor_value",
            "control_factor_publication",
            "control_factor_publication_factor",
            "control_factor_audit_event",
            "control_factor_shadow_decision",
            "control_factor_training_dataset",
        ] {
            assert!(
                tables.contains(required),
                "Phase 5.1 required table `{required}` must be registered in the schema catalog"
            );
        }

        assert!(
            !tables.contains("runtime_config"),
            "legacy mutable runtime_config table must not return"
        );
        assert!(
            tables
                .iter()
                .all(|table| !table.starts_with("analytics_factor_")),
            "legacy analytics_factor_* aliases are forbidden"
        );
    }
}
