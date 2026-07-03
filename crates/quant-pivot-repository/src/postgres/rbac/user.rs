//! Postgres implementation of [`UserRepository`].

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ChangeUserPassword, NewUser, PageWindow, Paginated, UserInfo, UserPageQuery, UserPatch,
    },
    entities::{user, user_role},
    enums::rbac::UserStatus,
    schema::column,
    types::UserId,
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, TransactionTrait,
    sea_query::{Condition, Expr, extension::postgres::PgExpr},
};

use crate::{
    postgres::{
        error,
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
    Ok(user::Entity::find()
        .filter(user::Column::Username.eq(username))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into))
}

async fn do_find_by_id(db: &impl ConnectionTrait, id: &UserId) -> Result<UserInfo, StorageError> {
    user::Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or_else(|| error::not_found(entity::USER, id))
}

async fn do_create(db: &impl ConnectionTrait, new: NewUser) -> Result<UserInfo, StorageError> {
    let username = new.username.clone();
    let model = user::Entity::insert(new.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(|error| error::map_unique(error, entity::USER, &username))?;
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
        Err(sea_orm::DbErr::RecordNotUpdated) => Err(error::not_found(entity::USER, id)),
        Err(error) => Err(StorageError::from(error)),
    }
}

async fn do_change_status(
    db: &impl ConnectionTrait,
    id: &UserId,
    status: UserStatus,
) -> Result<(), StorageError> {
    let result = user::Entity::update_many()
        .col_expr(user::Column::Status, column::pg_enum_value(&status))
        .filter(user::Column::Id.eq(id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(error::not_found(entity::USER, id));
    }
    Ok(())
}

async fn do_change_password(
    db: &impl ConnectionTrait,
    id: &UserId,
    change: ChangeUserPassword,
) -> Result<(), StorageError> {
    let result = user::Entity::update_many()
        .col_expr(
            user::Column::PasswordHash,
            Expr::value(change.password_hash),
        )
        .filter(user::Column::Id.eq(id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(error::not_found(entity::USER, id));
    }
    Ok(())
}

async fn do_delete(db: &DatabaseConnection, id: &UserId) -> Result<(), StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    user_role::Entity::delete_many()
        .filter(user_role::Column::UserId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    sync::do_revoke_all_roles_for_user(&txn, id).await?;

    let result = user::Entity::delete_by_id(id.clone())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::not_found(entity::USER, id));
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

fn page_condition(query: &UserPageQuery) -> Condition {
    let mut condition =
        Condition::all().add_option(query.status.map(|status| user::Column::Status.eq(status)));
    if let Some(keyword) = non_empty(query.keyword.as_deref()) {
        let pattern = format!("%{keyword}%");
        condition = condition.add(
            Condition::any()
                .add(Expr::col(user::Column::Username).ilike(pattern.clone()))
                .add(Expr::col(user::Column::Nickname).ilike(pattern)),
        );
    }
    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: UserPageQuery,
) -> Result<Paginated<UserInfo>, StorageError> {
    paginate_mapped(
        user::Entity::find()
            .filter(page_condition(&query))
            .order_by_desc(user::Column::CreatedAt),
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
