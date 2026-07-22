//! Shared encoding between `casbin_rule` table rows and Casbin policy lines.
//!
//! Every Casbin write/read path in this crate — the [`Adapter`] and the
//! transactional [`sync`] helpers — funnels through these builders, so the
//! `(ptype, v0..v5)` layout (defined once in
//! [`quant_pivot_models::enums::rbac::casbin`]) has exactly one implementation and
//! cannot drift between writer, reader, and matcher.
//!
//! [`Adapter`]: super::adapter::PgCasbinAdapter
//! [`sync`]: super::sync

use quant_pivot_models::{
    entities::casbin_rule::{ActiveModel, Column, Model},
    enums::rbac::{
        Operation, ResourceType,
        casbin::{OBJECT_TYPE_RESOURCE, PTYPE_GROUPING, PTYPE_POLICY, VALUE_COLUMNS},
    },
    types::UserId,
};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait,
    sea_query::{Condition, OnConflict},
};

/// Project a stored row into `(section, ptype, tokens)`, trimming the trailing
/// empty value columns so the token count matches the policy arity. Returns
/// `None` for a row whose `ptype` is empty (it maps to no Casbin section).
pub fn row_to_policy(row: Model) -> Option<(String, String, Vec<String>)> {
    if row.ptype.is_empty() {
        return None;
    }
    let section = row.ptype.chars().next()?.to_string();
    let mut tokens = vec![row.v0, row.v1, row.v2, row.v3, row.v4, row.v5];
    while tokens.last().is_some_and(String::is_empty) {
        tokens.pop();
    }
    Some((section, row.ptype, tokens))
}

/// Build an active-model row from a raw policy line, padding the unused value
/// columns with the empty string so the `uq_casbin_rule` full-tuple unique
/// index is always satisfied.
pub fn policy_to_row(ptype: &str, rule: &[String]) -> ActiveModel {
    let mut values: [String; VALUE_COLUMNS] = Default::default();
    for (slot, token) in values.iter_mut().zip(rule.iter()) {
        slot.clone_from(token);
    }
    let [v0, v1, v2, v3, v4, v5] = values;
    ActiveModel {
        ptype: Set(ptype.to_owned()),
        v0: Set(v0),
        v1: Set(v1),
        v2: Set(v2),
        v3: Set(v3),
        v4: Set(v4),
        v5: Set(v5),
        ..Default::default()
    }
}

/// Build a `g` grouping row binding a user subject to a role code.
pub fn grouping_row(user_id: &UserId, role_code: &str) -> ActiveModel {
    ActiveModel {
        ptype: Set(PTYPE_GROUPING.to_owned()),
        v0: Set(user_id.to_string()),
        v1: Set(role_code.to_owned()),
        v2: Set(String::new()),
        v3: Set(String::new()),
        v4: Set(String::new()),
        v5: Set(String::new()),
        ..Default::default()
    }
}

/// Build a `p` permission row for a role code.
pub fn policy_row(role_code: &str, resource: ResourceType, operation: Operation) -> ActiveModel {
    ActiveModel {
        ptype: Set(PTYPE_POLICY.to_owned()),
        v0: Set(role_code.to_owned()),
        v1: Set(resource.to_string()),
        v2: Set(operation.to_string()),
        v3: Set(OBJECT_TYPE_RESOURCE.to_owned()),
        v4: Set(String::new()),
        v5: Set(String::new()),
        ..Default::default()
    }
}

/// The value column at the given zero-based policy index, if it exists.
pub const fn value_column(index: usize) -> Option<Column> {
    match index {
        0 => Some(Column::V0),
        1 => Some(Column::V1),
        2 => Some(Column::V2),
        3 => Some(Column::V3),
        4 => Some(Column::V4),
        5 => Some(Column::V5),
        _ => None,
    }
}

/// `ON CONFLICT` target over the full `(ptype, v0..v5)` tuple — the exact
/// de-duplication boundary guaranteed by the `uq_casbin_rule` unique index.
pub fn full_tuple_conflict() -> OnConflict {
    OnConflict::columns([
        Column::Ptype,
        Column::V0,
        Column::V1,
        Column::V2,
        Column::V3,
        Column::V4,
        Column::V5,
    ])
}

/// Build an exact full-tuple match, padding unused value columns exactly as
/// [`policy_to_row`] does. A shorter rule must never match a longer rule that
/// merely shares its prefix.
pub fn exact_match(ptype: &str, rule: &[String]) -> Condition {
    let mut values: [String; VALUE_COLUMNS] = Default::default();
    for (slot, token) in values.iter_mut().zip(rule.iter()) {
        slot.clone_from(token);
    }
    Condition::all()
        .add(Column::Ptype.eq(ptype))
        .add(Column::V0.eq(values[0].as_str()))
        .add(Column::V1.eq(values[1].as_str()))
        .add(Column::V2.eq(values[2].as_str()))
        .add(Column::V3.eq(values[3].as_str()))
        .add(Column::V4.eq(values[4].as_str()))
        .add(Column::V5.eq(values[5].as_str()))
}
