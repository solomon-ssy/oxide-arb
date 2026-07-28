//! `PostgreSQL` governed promotion-permit command repository.

use quant_pivot_error::{
    feedback::{FeedbackError, PromotionPermitCommandError},
    rbac::RbacError,
    storage::{StorageError, entity::QUANT_FEEDBACK_PROMOTION_PERMIT},
};
use quant_pivot_models::{
    domain::quant::{
        IssuePromotionPermit, NewPromotionPermit, PromotionPermitActor, PromotionPermitInfo,
        PromotionPermitIssueInput, PromotionPermitRevocation, PromotionPermitRevocationCheck,
        RevokePromotionPermit,
    },
    entities::{
        casbin_rule::{Column as CasbinColumn, Entity as CasbinEntity},
        quant_feedback_promotion_permit::{Column as PermitColumn, Entity as PermitEntity},
        role::{Column as RoleColumn, Entity as RoleEntity},
        user::Entity as UserEntity,
        user_role::Entity as UserRoleEntity,
    },
    enums::rbac::{
        Operation, ResourceType, RoleStatus, UserStatus,
        casbin::{OBJECT_TYPE_RESOURCE, PTYPE_GROUPING, PTYPE_POLICY},
    },
    seed::rbac::ROLE_SUPER_ADMIN,
    types::{PromotionPermitId, RoleCode, UserId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, EntityTrait, ExprTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait, TryInsertResult,
    sea_query::{Expr, OnConflict},
};

use crate::{
    postgres::primitives,
    traits::{
        PromotionPermitIssueOutcome, PromotionPermitRepository, PromotionPermitRevokeOutcome,
    },
};

struct AuthorizedPromotionActor {
    user_id: UserId,
    username: String,
    role: RoleCode,
}

/// `PostgreSQL` owner of atomic permit authorization and mutation.
pub struct PgPromotionPermitRepository {
    db: DatabaseConnection,
}

impl PgPromotionPermitRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn authorize_actor(
        transaction: &DatabaseTransaction,
        actor: &PromotionPermitActor,
        operation: Operation,
    ) -> Result<AuthorizedPromotionActor, PromotionPermitCommandError> {
        let denied = || {
            PromotionPermitCommandError::from(RbacError::PermissionDenied {
                actor_user_id: actor.user_id.to_string(),
                acting_role: actor.acting_role.to_string(),
                resource: ResourceType::Publication.as_str(),
                operation: operation.as_str(),
            })
        };
        let Some(user) = UserEntity::find_by_id(actor.user_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(denied());
        };
        if user.status != UserStatus::Active {
            return Err(denied());
        }

        let Some(role) = RoleEntity::find()
            .filter(RoleColumn::Code.eq(actor.acting_role.as_str()))
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(denied());
        };
        if role.status != RoleStatus::Enabled {
            return Err(denied());
        }

        let membership = UserRoleEntity::find_by_id((actor.user_id, role.id))
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?;
        if membership.is_none() {
            return Err(denied());
        }

        let subject = actor.user_id.to_string();
        let grouping = CasbinEntity::find()
            .filter(CasbinColumn::Ptype.eq(PTYPE_GROUPING))
            .filter(CasbinColumn::V0.eq(&subject))
            .filter(CasbinColumn::V1.eq(actor.acting_role.as_str()))
            .filter(CasbinColumn::V2.eq(""))
            .filter(CasbinColumn::V3.eq(""))
            .filter(CasbinColumn::V4.eq(""))
            .filter(CasbinColumn::V5.eq(""))
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?;
        if grouping.is_none() {
            return Err(denied());
        }

        if actor.acting_role.as_str() != ROLE_SUPER_ADMIN {
            let policy = CasbinEntity::find()
                .filter(CasbinColumn::Ptype.eq(PTYPE_POLICY))
                .filter(CasbinColumn::V0.eq(actor.acting_role.as_str()))
                .filter(CasbinColumn::V1.eq(ResourceType::Publication.as_str()))
                .filter(CasbinColumn::V2.eq(operation.as_str()))
                .filter(CasbinColumn::V3.eq(OBJECT_TYPE_RESOURCE))
                .filter(CasbinColumn::V4.eq(""))
                .filter(CasbinColumn::V5.eq(""))
                .lock_shared()
                .one(transaction)
                .await
                .map_err(StorageError::from)?;
            if policy.is_none() {
                return Err(denied());
            }
        }

        Ok(AuthorizedPromotionActor {
            user_id: user.id,
            username: user.username,
            role: role.code,
        })
    }

    async fn permit_candidates(
        transaction: &DatabaseTransaction,
        expected: &NewPromotionPermit,
    ) -> Result<Vec<PromotionPermitInfo>, PromotionPermitCommandError> {
        PermitEntity::find()
            .filter(
                Condition::any()
                    .add(PermitColumn::PromotionPermitId.eq(expected.promotion_permit_id()))
                    .add(PermitColumn::IdempotencyKey.eq(expected.idempotency_key()))
                    .add(PermitColumn::ScopeHash.eq(expected.scope_hash()))
                    .add(PermitColumn::IssuanceHash.eq(expected.issuance_hash())),
            )
            .order_by_asc(PermitColumn::PromotionPermitId)
            .lock_exclusive()
            .into_partial_model::<PromotionPermitInfo>()
            .all(transaction)
            .await
            .map_err(StorageError::from)
            .map_err(PromotionPermitCommandError::from)
    }

    fn single_candidate(
        mut candidates: Vec<PromotionPermitInfo>,
        expected: &NewPromotionPermit,
    ) -> Result<PromotionPermitInfo, PromotionPermitCommandError> {
        if candidates.len() != 1 {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_PROMOTION_PERMIT,
                Some(expected.promotion_permit_id()),
                format!(
                    "permit natural keys resolved to {} rows; expected exactly one",
                    candidates.len()
                ),
            )
            .into());
        }
        let stored = candidates.pop().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_PROMOTION_PERMIT),
                "single permit candidate disappeared",
            )
        })?;
        if !stored.has_same_issuance(expected)? {
            return Err(FeedbackError::PromotionPermitConflict {
                detail: "permit idempotency or content key was reused with immutable drift"
                    .to_owned(),
            }
            .into());
        }
        Ok(stored)
    }

    fn insert_applied(result: &TryInsertResult<u64>) -> Result<bool, StorageError> {
        match result {
            TryInsertResult::Inserted(1) => Ok(true),
            TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => Ok(false),
            TryInsertResult::Inserted(rows) => Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_PROMOTION_PERMIT),
                format!("single permit insert affected {rows} rows"),
            )),
            TryInsertResult::Empty => Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_PROMOTION_PERMIT),
                "non-empty permit insert produced no statement",
            )),
        }
    }

    async fn lock_permit(
        transaction: &DatabaseTransaction,
        permit_id: PromotionPermitId,
    ) -> Result<PromotionPermitInfo, PromotionPermitCommandError> {
        PermitEntity::find_by_id(permit_id)
            .lock_exclusive()
            .into_partial_model::<PromotionPermitInfo>()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_PROMOTION_PERMIT, permit_id).into()
            })
    }
}

#[async_trait::async_trait]
impl PromotionPermitRepository for PgPromotionPermitRepository {
    async fn load(
        &self,
        permit_id: &PromotionPermitId,
    ) -> Result<PromotionPermitInfo, PromotionPermitCommandError> {
        let permit = PermitEntity::find_by_id(*permit_id)
            .into_partial_model::<PromotionPermitInfo>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEEDBACK_PROMOTION_PERMIT, permit_id))?;
        permit.validate()?;
        Ok(permit)
    }

    async fn issue(
        &self,
        command: IssuePromotionPermit,
    ) -> Result<PromotionPermitIssueOutcome, PromotionPermitCommandError> {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized =
            Self::authorize_actor(&transaction, &command.actor, Operation::Publish).await?;
        let expected = NewPromotionPermit::try_seal(PromotionPermitIssueInput {
            idempotency_key: command.idempotency_key,
            scope: command.scope,
            preflight_hash: command.preflight_hash,
            issued_by_user_id: authorized.user_id,
            issued_by_username: authorized.username,
            issued_by_role: authorized.role,
            issuance_reason: command.reason,
        })?;

        let existing = Self::permit_candidates(&transaction, &expected).await?;
        if !existing.is_empty() {
            let stored = Self::single_candidate(existing, &expected)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(PromotionPermitIssueOutcome::ExactReplay(stored));
        }

        let database_now = primitives::statement_timestamp(&transaction).await?;
        if expected.scope().expires_at() <= database_now {
            return Err(FeedbackError::InvalidPromotionPermit {
                detail: "new permit expiry must be later than the PostgreSQL statement clock"
                    .to_owned(),
            }
            .into());
        }

        let insert = PermitEntity::insert(expected.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert)?;
        let stored = Self::single_candidate(
            Self::permit_candidates(&transaction, &expected).await?,
            &expected,
        )?;
        transaction.commit().await.map_err(StorageError::from)?;
        if inserted {
            Ok(PromotionPermitIssueOutcome::Issued(stored))
        } else {
            Ok(PromotionPermitIssueOutcome::ExactReplay(stored))
        }
    }

    async fn revoke(
        &self,
        command: RevokePromotionPermit,
    ) -> Result<PromotionPermitRevokeOutcome, PromotionPermitCommandError> {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized =
            Self::authorize_actor(&transaction, &command.actor, Operation::Retire).await?;
        let stored = Self::lock_permit(&transaction, command.promotion_permit_id).await?;
        stored.validate()?;

        if let Some(revoked_at) = stored.revoked_at {
            let replay = PromotionPermitRevocation {
                revoked_by_user_id: authorized.user_id,
                revoked_by_username: authorized.username.clone(),
                revoked_by_role: authorized.role.clone(),
                revocation_reason: command.reason.clone(),
                revoked_at,
            };
            if stored.check_revocation(&replay)? == PromotionPermitRevocationCheck::ExactReplay {
                transaction.commit().await.map_err(StorageError::from)?;
                return Ok(PromotionPermitRevokeOutcome::ExactReplay(stored));
            }
        }

        if stored.revision != command.expected_revision {
            return Err(FeedbackError::PromotionPermitConflict {
                detail: format!(
                    "permit revoke revision mismatch: expected {}, got {}",
                    command.expected_revision, stored.revision
                ),
            }
            .into());
        }
        let update = PermitEntity::update_many()
            .col_expr(
                PermitColumn::RevokedByUserId,
                Expr::value(Some(authorized.user_id)),
            )
            .col_expr(
                PermitColumn::RevokedByUsername,
                Expr::value(Some(authorized.username)),
            )
            .col_expr(
                PermitColumn::RevokedByRole,
                Expr::value(Some(authorized.role)),
            )
            .col_expr(
                PermitColumn::RevocationReason,
                Expr::value(Some(command.reason)),
            )
            .col_expr(
                PermitColumn::Revision,
                Expr::col(PermitColumn::Revision).add(1),
            )
            .filter(PermitColumn::PromotionPermitId.eq(command.promotion_permit_id))
            .filter(PermitColumn::Revision.eq(command.expected_revision))
            .filter(PermitColumn::RevokedByUserId.is_null())
            .filter(PermitColumn::RevokedByUsername.is_null())
            .filter(PermitColumn::RevokedByRole.is_null())
            .filter(PermitColumn::RevocationReason.is_null())
            .filter(PermitColumn::RevokedAt.is_null())
            .exec(&transaction)
            .await
            .map_err(StorageError::from)?;
        if update.rows_affected != 1 {
            return Err(FeedbackError::PromotionPermitConflict {
                detail: "permit revoke row-lock/CAS affected no row".to_owned(),
            }
            .into());
        }
        let revoked = Self::lock_permit(&transaction, command.promotion_permit_id).await?;
        revoked.validate()?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(PromotionPermitRevokeOutcome::Revoked(revoked))
    }
}
