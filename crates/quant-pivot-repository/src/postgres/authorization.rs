//! Transaction-scoped authorization for governed repository mutations.

use quant_pivot_error::{rbac::RbacError, storage::StorageError};
use quant_pivot_models::{
    entities::{
        casbin_rule::{Column as CasbinColumn, Entity as CasbinEntity},
        role::{Column as RoleColumn, Entity as RoleEntity},
        user::Entity as UserEntity,
        user_role::Entity as UserRoleEntity,
    },
    enums::rbac::{
        Operation, ResourceType, RoleStatus, UserStatus,
        casbin::{OBJECT_TYPE_RESOURCE, PTYPE_GROUPING, PTYPE_POLICY},
    },
    seed::rbac::ROLE_SUPER_ADMIN,
    types::{RoleCode, UserId},
};
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect};

/// Database-resolved identity frozen by the transaction's shared locks.
pub struct AuthorizedGovernedActor {
    pub user_id: UserId,
    pub username: String,
    pub role: RoleCode,
}

/// Lock and verify the relational + exact Casbin authorization preimage.
///
/// Shared row locks remain compatible with actor foreign-key key-share locks
/// while blocking account, role, membership, and policy mutation until the
/// governed transaction commits.
pub async fn authorize_actor<E>(
    transaction: &DatabaseTransaction,
    user_id: UserId,
    acting_role: &RoleCode,
    resource: ResourceType,
    operation: Operation,
) -> Result<AuthorizedGovernedActor, E>
where
    E: From<RbacError> + From<StorageError>,
{
    let denied = || {
        E::from(RbacError::PermissionDenied {
            actor_user_id: user_id.to_string(),
            acting_role: acting_role.to_string(),
            resource: resource.as_str(),
            operation: operation.as_str(),
        })
    };
    let Some(user) = UserEntity::find_by_id(user_id)
        .lock_shared()
        .one(transaction)
        .await
        .map_err(StorageError::from)
        .map_err(E::from)?
    else {
        return Err(denied());
    };
    if user.status != UserStatus::Active {
        return Err(denied());
    }

    let Some(role) = RoleEntity::find()
        .filter(RoleColumn::Code.eq(acting_role.as_str()))
        .lock_shared()
        .one(transaction)
        .await
        .map_err(StorageError::from)
        .map_err(E::from)?
    else {
        return Err(denied());
    };
    if role.status != RoleStatus::Enabled {
        return Err(denied());
    }

    let membership = UserRoleEntity::find_by_id((user_id, role.id))
        .lock_shared()
        .one(transaction)
        .await
        .map_err(StorageError::from)
        .map_err(E::from)?;
    if membership.is_none() {
        return Err(denied());
    }

    let subject = user_id.to_string();
    let grouping = CasbinEntity::find()
        .filter(CasbinColumn::Ptype.eq(PTYPE_GROUPING))
        .filter(CasbinColumn::V0.eq(&subject))
        .filter(CasbinColumn::V1.eq(acting_role.as_str()))
        .filter(CasbinColumn::V2.eq(""))
        .filter(CasbinColumn::V3.eq(""))
        .filter(CasbinColumn::V4.eq(""))
        .filter(CasbinColumn::V5.eq(""))
        .lock_shared()
        .one(transaction)
        .await
        .map_err(StorageError::from)
        .map_err(E::from)?;
    if grouping.is_none() {
        return Err(denied());
    }

    if acting_role.as_str() != ROLE_SUPER_ADMIN {
        let policy = CasbinEntity::find()
            .filter(CasbinColumn::Ptype.eq(PTYPE_POLICY))
            .filter(CasbinColumn::V0.eq(acting_role.as_str()))
            .filter(CasbinColumn::V1.eq(resource.as_str()))
            .filter(CasbinColumn::V2.eq(operation.as_str()))
            .filter(CasbinColumn::V3.eq(OBJECT_TYPE_RESOURCE))
            .filter(CasbinColumn::V4.eq(""))
            .filter(CasbinColumn::V5.eq(""))
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)
            .map_err(E::from)?;
        if policy.is_none() {
            return Err(denied());
        }
    }

    Ok(AuthorizedGovernedActor {
        user_id: user.id,
        username: user.username,
        role: role.code,
    })
}
