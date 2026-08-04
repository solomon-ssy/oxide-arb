//! Seeds Casbin policies: the `admin → super_admin` grouping (`g`) and the full
//! per-role permission matrix (`p`) for the built-in roles.

use std::{collections::HashSet, future::Future, pin::Pin};

use sea_orm::{ActiveValue::Set, DatabaseTransaction, DbErr, EntityTrait, sea_query::OnConflict};

use crate::{
    entities::casbin_rule::{ActiveModel, Column, Entity},
    enums::rbac::{
        Operation, ResourceType,
        casbin::{OBJECT_TYPE_RESOURCE, PTYPE_GROUPING, PTYPE_POLICY},
    },
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{
            ADMIN_USER_ARTIFACT, ROLE_ADMIN, ROLE_ANALYST, ROLE_EMERGENCY_OPERATOR, ROLE_OPERATOR,
            ROLE_RISK_OWNER, ROLE_SUPER_ADMIN, ROLE_VIEWER, ROLES_ARTIFACT,
        },
    },
    types::UserId,
};

const SEED_ID: &str = "rbac.casbin.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(ADMIN_USER_ARTIFACT),
];
const PRODUCES: &[SeedArtifact] = &[];

pub const CASBIN_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: "casbin_rule",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.casbin.bootstrap.v1.runtime-controls",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Resources granted read access to every non-super-admin built-in role.
const READ_RESOURCES: &[ResourceType] = &[
    ResourceType::System,
    ResourceType::Market,
    ResourceType::QuantReport,
    ResourceType::AccountSnapshot,
    ResourceType::EquitySnapshot,
    ResourceType::OrderIntent,
    ResourceType::ExecutionOrder,
    ResourceType::Position,
    ResourceType::Reconciliation,
    ResourceType::SettlementRedeem,
    ResourceType::FactorDefinition,
    ResourceType::DecisionPolicySnapshot,
    ResourceType::Materialization,
    // Read the backtest / comparison report ledgers (research catalog browse);
    // `Replay:Create` remains a risk-owner-only mutation.
    ResourceType::Replay,
    ResourceType::OperationLog,
];

pub fn builtin_role_policies() -> Vec<(&'static str, Vec<(ResourceType, Operation)>)> {
    vec![
        (ROLE_VIEWER, read_only()),
        (ROLE_ANALYST, analyst_policies()),
        (ROLE_OPERATOR, operator_policies()),
        (ROLE_RISK_OWNER, risk_owner_policies()),
        (ROLE_ADMIN, admin_policies()),
        (ROLE_EMERGENCY_OPERATOR, emergency_operator_policies()),
    ]
}

impl SeedContext {
    fn expected_policy_keys(&self) -> Result<HashSet<[String; 7]>, DbErr> {
        let admin_id = self
            .require::<UserId>(ADMIN_USER_ARTIFACT)
            .map_err(|error| DbErr::Custom(error.to_string()))?;
        let mut keys = HashSet::new();
        keys.insert([
            PTYPE_GROUPING.to_owned(),
            admin_id.to_string(),
            ROLE_SUPER_ADMIN.to_owned(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]);
        for (role_code, permissions) in builtin_role_policies() {
            for (resource, operation) in permissions {
                keys.insert([
                    PTYPE_POLICY.to_owned(),
                    role_code.to_owned(),
                    resource.to_string(),
                    operation.to_string(),
                    OBJECT_TYPE_RESOURCE.to_owned(),
                    String::new(),
                    String::new(),
                ]);
            }
        }
        Ok(keys)
    }
}

async fn hydrate(db: &DatabaseTransaction, ctx: &SeedContext) -> Result<(), DbErr> {
    let expected = (ctx).expected_policy_keys()?;
    let actual = Entity::find()
        .all(db)
        .await?
        .into_iter()
        .map(|row| [row.ptype, row.v0, row.v1, row.v2, row.v3, row.v4, row.v5])
        .collect::<HashSet<_>>();
    let missing = expected.difference(&actual).count();
    if missing != 0 {
        return Err(DbErr::Custom(format!(
            "Casbin bootstrap policy seed is missing {missing} canonical rows"
        )));
    }
    Ok(())
}

fn read_only() -> Vec<(ResourceType, Operation)> {
    READ_RESOURCES
        .iter()
        .map(|resource| (*resource, Operation::Read))
        .collect()
}

fn analyst_policies() -> Vec<(ResourceType, Operation)> {
    let mut policies = read_only();
    // Analysts may trigger ad-hoc report generation, but never revoke.
    policies.push((ResourceType::QuantReport, Operation::Enqueue));
    policies
}

fn operator_policies() -> Vec<(ResourceType, Operation)> {
    let mut policies = read_only();
    policies.extend([
        (ResourceType::System, Operation::Halt),
        (ResourceType::System, Operation::Resume),
        (ResourceType::System, Operation::SwitchMode),
        (ResourceType::System, Operation::Emergency),
        (ResourceType::Market, Operation::Update),
        // Operators run and revoke recommendation reports.
        (ResourceType::QuantReport, Operation::Enqueue),
        (ResourceType::QuantReport, Operation::Revoke),
        (ResourceType::OrderIntent, Operation::Create),
        (ResourceType::OrderIntent, Operation::Approve),
        (ResourceType::OrderIntent, Operation::Reject),
        (ResourceType::OrderIntent, Operation::Cancel),
        (ResourceType::Reconciliation, Operation::Resolve),
    ]);
    policies
}

fn risk_owner_policies() -> Vec<(ResourceType, Operation)> {
    let mut policies = read_only();
    policies.extend([
        (ResourceType::DecisionPolicySnapshot, Operation::Create),
        (ResourceType::DecisionPolicySnapshot, Operation::Approve),
        (ResourceType::DecisionPolicySnapshot, Operation::Activate),
        (ResourceType::DecisionPolicySnapshot, Operation::Rollback),
        // Offline research: train models (Materialization:Create) and run
        // PIT backtests (Replay:Create); read access is granted to all roles.
        (ResourceType::Materialization, Operation::Create),
        (ResourceType::Replay, Operation::Create),
        (ResourceType::Publication, Operation::Publish),
        (ResourceType::Publication, Operation::Authorize),
        (ResourceType::Publication, Operation::Activate),
        (ResourceType::Publication, Operation::Reject),
        (ResourceType::Publication, Operation::Rollback),
        (ResourceType::Publication, Operation::Retire),
        // Risk owners revoke published reports (money-risk authority).
        (ResourceType::QuantReport, Operation::Revoke),
        (ResourceType::OrderIntent, Operation::Reject),
        (ResourceType::OrderIntent, Operation::Cancel),
        (ResourceType::Reconciliation, Operation::Resolve),
    ]);
    policies
}

fn admin_policies() -> Vec<(ResourceType, Operation)> {
    let mut policies = read_only();
    for resource in [ResourceType::User, ResourceType::Role] {
        policies.extend([
            (resource, Operation::Read),
            (resource, Operation::Create),
            (resource, Operation::Update),
            (resource, Operation::Delete),
            (resource, Operation::Assign),
        ]);
    }
    policies.extend([
        (ResourceType::Menu, Operation::Read),
        (ResourceType::Menu, Operation::Create),
        (ResourceType::Menu, Operation::Update),
        (ResourceType::Menu, Operation::Delete),
        (ResourceType::Permission, Operation::Read),
        (ResourceType::System, Operation::Halt),
        (ResourceType::System, Operation::Resume),
        (ResourceType::System, Operation::SwitchMode),
        (ResourceType::System, Operation::Emergency),
        (ResourceType::Reconciliation, Operation::Resolve),
        (ResourceType::SettlementRedeem, Operation::Create),
        (ResourceType::SettlementRedeem, Operation::Approve),
        (ResourceType::SettlementRedeem, Operation::Revoke),
    ]);
    policies
}

fn emergency_operator_policies() -> Vec<(ResourceType, Operation)> {
    let mut policies = read_only();
    policies.extend([
        (ResourceType::System, Operation::Halt),
        (ResourceType::System, Operation::Emergency),
        (ResourceType::Reconciliation, Operation::Resolve),
    ]);
    policies
}

fn policy_row(role_code: &str, resource: ResourceType, operation: Operation) -> ActiveModel {
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

fn grouping_row(subject: &str, role_code: &str) -> ActiveModel {
    ActiveModel {
        ptype: Set(PTYPE_GROUPING.to_owned()),
        v0: Set(subject.to_owned()),
        v1: Set(role_code.to_owned()),
        v2: Set(String::new()),
        v3: Set(String::new()),
        v4: Set(String::new()),
        v5: Set(String::new()),
        ..Default::default()
    }
}

pub async fn load(db: &DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let admin_id = *ctx
        .require::<UserId>(ADMIN_USER_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?;

    let mut models = vec![grouping_row(&admin_id.to_string(), ROLE_SUPER_ADMIN)];
    for (role_code, permissions) in builtin_role_policies() {
        for (resource, operation) in permissions {
            models.push(policy_row(role_code, resource, operation));
        }
    }

    Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([
                Column::Ptype,
                Column::V0,
                Column::V1,
                Column::V2,
                Column::V3,
                Column::V4,
                Column::V5,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec_without_returning(db)
        .await
}

fn load_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn hydrate_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>> {
    Box::pin(hydrate(db, ctx))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{READ_RESOURCES, builtin_role_policies};
    use crate::{
        enums::rbac::{Operation, ResourceType},
        seed::rbac::{ROLE_RISK_OWNER, ROLE_VIEWER},
    };

    #[test]
    fn seeded_policy_permission_catalog() {
        for (role_code, permissions) in builtin_role_policies() {
            for (resource, operation) in permissions {
                assert!(
                    resource.allows(operation),
                    "role `{role_code}` grants {resource:?}:{operation:?} which is not in \
                     RESOURCE_OPERATIONS"
                );
            }
        }
    }

    #[test]
    fn policies_no_duplicates_role() {
        for (role_code, permissions) in builtin_role_policies() {
            let mut seen = HashSet::new();
            for pair in &permissions {
                assert!(
                    seen.insert(*pair),
                    "role `{role_code}` has duplicate policy {pair:?}"
                );
            }
        }
    }

    #[test]
    fn read_resources_are_readable() {
        for resource in READ_RESOURCES {
            assert!(
                resource.allows(Operation::Read),
                "{resource:?} is in READ_RESOURCES but has no Read operation"
            );
        }
    }

    #[test]
    fn factor_permissions_read_only() {
        for (role_code, permissions) in builtin_role_policies() {
            let factor_permissions = permissions
                .into_iter()
                .filter(|(resource, _)| *resource == ResourceType::FactorDefinition)
                .collect::<Vec<_>>();
            assert_eq!(
                factor_permissions,
                [(ResourceType::FactorDefinition, Operation::Read)],
                "role `{role_code}` must not receive factor-catalog mutation authority"
            );
        }
    }

    #[test]
    fn feature_integrity_readable_govern() {
        let policies = builtin_role_policies();
        let viewer = policies
            .iter()
            .find(|(role, _)| *role == ROLE_VIEWER)
            .map(|(_, permissions)| permissions)
            .expect("viewer policies");
        let risk_owner = policies
            .iter()
            .find(|(role, _)| *role == ROLE_RISK_OWNER)
            .map(|(_, permissions)| permissions)
            .expect("risk-owner policies");
        let read = (ResourceType::Materialization, Operation::Read);
        let govern = (ResourceType::Materialization, Operation::Create);

        assert!(viewer.contains(&read));
        assert!(!viewer.contains(&govern));
        assert!(risk_owner.contains(&read));
        assert!(risk_owner.contains(&govern));
    }
}
