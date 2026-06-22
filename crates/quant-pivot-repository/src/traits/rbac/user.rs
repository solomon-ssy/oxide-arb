//! User account repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ChangeUserPassword, NewUser, Paginated, UserInfo, UserPageQuery, UserPatch},
    enums::rbac::UserStatus,
    types::UserId,
};

/// Persistence operations for user accounts.
///
/// `UserInfo` carries `password_hash` because the login path needs it; the web
/// layer projects it into a credential-free response type.
#[async_trait::async_trait]
pub trait UserRepository: Send + Sync {
    /// Look up a user by their unique username, or `None` if absent.
    async fn find_by_username(&self, username: &str) -> Result<Option<UserInfo>, StorageError>;

    /// Fetch a user by id, erroring with `NotFound` when absent.
    async fn find_by_id(&self, id: &UserId) -> Result<UserInfo, StorageError>;

    /// Insert a new user. A duplicate username surfaces as `Conflict`.
    async fn create(&self, user: NewUser) -> Result<UserInfo, StorageError>;

    /// Apply a partial profile/status update and return the refreshed row.
    async fn update(&self, id: &UserId, patch: UserPatch) -> Result<UserInfo, StorageError>;

    /// Toggle the account status flag.
    async fn change_status(&self, id: &UserId, status: UserStatus) -> Result<(), StorageError>;

    /// Replace the stored credential with a pre-hashed argon2id PHC string.
    async fn change_password(
        &self,
        id: &UserId,
        change: ChangeUserPassword,
    ) -> Result<(), StorageError>;

    /// Delete a user, cascading their `user_role` rows and Casbin `g` grants in
    /// the same transaction.
    async fn delete(&self, id: &UserId) -> Result<(), StorageError>;

    /// Paginated, filtered listing ordered by `created_at desc`.
    async fn page(&self, query: UserPageQuery) -> Result<Paginated<UserInfo>, StorageError>;
}
