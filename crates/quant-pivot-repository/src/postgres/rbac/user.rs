//! Postgres implementation of [`UserRepository`].

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity::USER};
use quant_pivot_models::{
    domain::{
        api::UserPageQuery,
        pagination::{PageWindow, Paginated},
        rbac::{ChangeUserPassword, NewUser, UserInfo, UserPatch},
    },
    entities::{
        user::{Column, Entity},
        user_role::{Column as UserRoleColumn, Entity as UserRoleEntity},
    },
    enums::rbac::UserStatus,
    types::UserId,
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, TransactionTrait,
    sea_query::{Condition, Expr, extension::postgres::PgExpr},
};

use crate::{
    postgres::{
        error, primitives,
        query::{non_empty, paginate_mapped},
        rbac::casbin::sync,
    },
    traits::rbac::UserRepository,
};

/// User account repository backed by Postgres.
pub struct PgUserRepository {
    db: DatabaseConnection,
}

impl PgUserRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn do_find_by_username(
    db: &impl ConnectionTrait,
    username: &str,
) -> Result<Option<UserInfo>, StorageError> {
    Ok(Entity::find()
        .filter(Column::Username.eq(username))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into))
}

async fn do_find_by_id(db: &impl ConnectionTrait, id: &UserId) -> Result<UserInfo, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or_else(|| error::not_found(USER, id))
}

async fn do_create(db: &impl ConnectionTrait, new: NewUser) -> Result<UserInfo, StorageError> {
    let username = new.username.clone();
    let model = Entity::insert(new.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(|error| error::map_unique(error, USER, &username))?;
    Ok(model.into())
}

async fn do_update(
    db: &impl ConnectionTrait,
    id: &UserId,
    patch: UserPatch,
) -> Result<UserInfo, StorageError> {
    let mut active = patch.into_active_model();
    active.id = Set(id.clone());
    active.updated_at = Set(Utc::now());
    match active.update(db).await {
        Ok(model) => Ok(model.into()),
        Err(DbErr::RecordNotUpdated) => Err(error::not_found(USER, id)),
        Err(error) => Err(StorageError::from(error)),
    }
}

async fn do_change_status(
    db: &impl ConnectionTrait,
    id: &UserId,
    status: UserStatus,
) -> Result<(), StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::Status, primitives::enum_value(&status))
        .filter(Column::Id.eq(id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(error::not_found(USER, id));
    }
    Ok(())
}

async fn do_change_password(
    db: &impl ConnectionTrait,
    id: &UserId,
    change: ChangeUserPassword,
) -> Result<(), StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::PasswordHash, Expr::value(change.password_hash))
        .filter(Column::Id.eq(id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(error::not_found(USER, id));
    }
    Ok(())
}

async fn do_delete(db: &DatabaseConnection, id: &UserId) -> Result<(), StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    UserRoleEntity::delete_many()
        .filter(UserRoleColumn::UserId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    sync::do_revoke_all_roles_for_user(&txn, id).await?;

    let result = Entity::delete_by_id(id.clone())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::not_found(USER, id));
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

fn page_condition(query: &UserPageQuery) -> Condition {
    let mut condition =
        Condition::all().add_option(query.status.map(|status| Column::Status.eq(status)));
    if let Some(keyword) = non_empty(query.keyword.as_deref()) {
        let pattern = format!("%{keyword}%");
        condition = condition.add(
            Condition::any()
                .add(Expr::col(Column::Username).ilike(pattern.clone()))
                .add(Expr::col(Column::Nickname).ilike(pattern)),
        );
    }
    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: UserPageQuery,
) -> Result<Paginated<UserInfo>, StorageError> {
    paginate_mapped(
        Entity::find()
            .filter(page_condition(&query))
            .order_by_desc(Column::CreatedAt),
        db,
        PageWindow::from_query(&query),
        Into::into,
    )
    .await
}

#[async_trait::async_trait]
impl UserRepository for PgUserRepository {
    async fn find_by_username(&self, username: &str) -> Result<Option<UserInfo>, StorageError> {
        do_find_by_username(&self.db, username).await
    }

    async fn find_by_id(&self, id: &UserId) -> Result<UserInfo, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn create(&self, user: NewUser) -> Result<UserInfo, StorageError> {
        do_create(&self.db, user).await
    }

    async fn update(&self, id: &UserId, patch: UserPatch) -> Result<UserInfo, StorageError> {
        do_update(&self.db, id, patch).await
    }

    async fn change_status(&self, id: &UserId, status: UserStatus) -> Result<(), StorageError> {
        do_change_status(&self.db, id, status).await
    }

    async fn change_password(
        &self,
        id: &UserId,
        change: ChangeUserPassword,
    ) -> Result<(), StorageError> {
        do_change_password(&self.db, id, change).await
    }

    async fn delete(&self, id: &UserId) -> Result<(), StorageError> {
        do_delete(&self.db, id).await
    }

    async fn page(&self, query: UserPageQuery) -> Result<Paginated<UserInfo>, StorageError> {
        do_page(&self.db, query).await
    }
}
