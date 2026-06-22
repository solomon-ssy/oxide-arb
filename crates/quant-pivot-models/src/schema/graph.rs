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
    fn phase1_quant_tables_are_cataloged_without_legacy_tables() {
        let tables = catalog::tables()
            .into_iter()
            .map(|spec| (spec.table_name)())
            .collect::<std::collections::BTreeSet<_>>();

        for required in [
            "runtime_config_version",
            "runtime_config_activation",
            "quant_universe_snapshot",
            "quant_universe_member",
            "quant_feature_vector",
            "quant_factor_definition",
            "quant_factor_value",
            "quant_model_spec",
            "quant_model_version",
            "quant_model_run",
            "quant_portfolio_plan",
            "quant_recommendation_report",
            "quant_recommendation",
            "quant_order_intent",
            "quant_execution_order",
            "quant_recommendation_attribution",
        ] {
            assert!(
                tables.contains(required),
                "Phase 1 required table `{required}` must be registered in the schema catalog"
            );
        }

        for deleted in [
            "trade",
            "position",
            "calibration",
            "calibration_outcome",
            "risk_state",
            "risk_audit_event",
            "risk_fill_applied",
            "report",
            "market_pit_snapshot",
            "control_factor_value",
        ] {
            assert!(
                !tables.contains(deleted),
                "legacy table `{deleted}` must not remain in active schema catalog"
            );
        }
    }
}
