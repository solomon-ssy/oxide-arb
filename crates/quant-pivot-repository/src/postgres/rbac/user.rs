//! Postgres implementation of [`UserRepository`].

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ChangeUserPassword, NewUser, Paginated, UserInfo, UserPageQuery, UserPatch},
    entities::{user, user_role},
    enums::rbac::UserStatus,
    types::UserId,
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Condition, Expr, extension::postgres::PgExpr},
};

use crate::{
    postgres::rbac::{casbin::sync, util},
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
        .ok_or_else(|| util::not_found("user", id))
}

async fn do_create(db: &impl ConnectionTrait, new: NewUser) -> Result<UserInfo, StorageError> {
    let username = new.username.clone();
    let model = user::Entity::insert(new.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(|error| util::map_unique(error, "user", &username))?;
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
        Err(sea_orm::DbErr::RecordNotUpdated) => Err(util::not_found("user", id)),
        Err(error) => Err(StorageError::from(error)),
    }
}

async fn do_change_status(
    db: &impl ConnectionTrait,
    id: &UserId,
    status: UserStatus,
) -> Result<(), StorageError> {
    let result = user::Entity::update_many()
        .col_expr(user::Column::Status, Expr::value(status))
        .filter(user::Column::Id.eq(id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(util::not_found("user", id));
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
        return Err(util::not_found("user", id));
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
        return Err(util::not_found("user", id));
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: UserPageQuery,
) -> Result<Paginated<UserInfo>, StorageError> {
    let window = query.page.normalized();

    let mut condition = Condition::all();
    if let Some(status) = query.status {
        condition = condition.add(user::Column::Status.eq(status));
    }
    if let Some(keyword) = query.keyword.as_deref().filter(|kw| !kw.is_empty()) {
        let pattern = format!("%{keyword}%");
        condition = condition.add(
            Condition::any()
                .add(Expr::col(user::Column::Username).ilike(pattern.clone()))
                .add(Expr::col(user::Column::Nickname).ilike(pattern)),
        );
    }

    let total = user::Entity::find()
        .filter(condition.clone())
        .count(db)
        .await
        .map_err(StorageError::from)?;

    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), total, &window));
    }

    let models = user::Entity::find()
        .filter(condition)
        .order_by_desc(user::Column::CreatedAt)
        .offset(window.offset())
        .limit(window.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = models.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window))
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
