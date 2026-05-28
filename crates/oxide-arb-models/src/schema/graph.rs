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
}
