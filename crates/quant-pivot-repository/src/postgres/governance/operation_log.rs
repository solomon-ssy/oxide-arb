//! Postgres implementation of [`OperationLogRepository`].
//!
//! The table is append-only at the database level (WORM trigger), so this
//! repository exposes only INSERT and SELECT. No method ever issues an UPDATE
//! or DELETE.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::OperationLogQuery,
        governance::{NewOperationLog, OperationLogInfo},
        pagination::{PageWindow, Paginated},
    },
    entities::operation_log::{Column, Entity},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    TransactionTrait, sea_query::Condition,
};

use crate::{
    postgres::{
        query::{non_empty, paginate_mapped},
        write::insert_many_chunked,
    },
    traits::OperationLogRepository,
};

/// Operation-log repository backed by Postgres.
pub struct PgOperationLogRepository {
    db: DatabaseConnection,
}

impl PgOperationLogRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn page_condition(query: &OperationLogQuery) -> Condition {
        Condition::all()
            .add_option(
                query
                    .actor_user_id
                    .as_ref()
                    .map(|id| Column::ActorUserId.eq(*id)),
            )
            .add_option(query.category.map(|category| Column::Category.eq(category)))
            .add_option(
                query
                    .resource_type
                    .map(|resource| Column::ResourceType.eq(resource)),
            )
            .add_option(
                non_empty(query.resource_id.as_deref())
                    .map(|resource_id| Column::ResourceId.eq(resource_id)),
            )
            .add_option(query.outcome.map(|outcome| Column::Outcome.eq(outcome)))
            .add_option(
                non_empty(query.request_id.as_deref())
                    .map(|request_id| Column::RequestId.eq(request_id)),
            )
            .add_option(
                query
                    .governance_audit_event_id
                    .as_ref()
                    .map(|event_id| Column::GovernanceAuditEventId.eq(*event_id)),
            )
            .add_option(query.from.map(|from| Column::OccurredAt.gte(from)))
            .add_option(query.to.map(|to| Column::OccurredAt.lt(to)))
    }
}

#[async_trait::async_trait]
impl OperationLogRepository for PgOperationLogRepository {
    async fn append(&self, log: NewOperationLog) -> Result<(), StorageError> {
        Entity::insert(log.into_active_model())
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn append_batch(&self, logs: Vec<NewOperationLog>) -> Result<(), StorageError> {
        if logs.is_empty() {
            return Ok(());
        }
        // One transaction so all chunks commit atomically. `insert_many_chunked`
        // handles bind-limit chunking and aligns the nullable `resource_type` enum
        // across mixed rows (see `align_partial_columns`).
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        insert_many_chunked::<Entity, NewOperationLog>(&txn, logs).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn page(
        &self,
        query: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError> {
        paginate_mapped(
            Entity::find()
                .filter(Self::page_condition(&query))
                .order_by_desc(Column::OccurredAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        domain::{api::OperationLogQuery, pagination::PageRequest},
        enums::{
            operation_log::{OperationCategory, OperationOutcome},
            rbac::ResourceType,
        },
        types::UserId,
    };
    use sea_orm::{DbBackend, EntityTrait, QueryFilter, QueryTrait};

    use super::{Entity, PgOperationLogRepository};

    #[test]
    fn page_adds_optional_sql() {
        let query = OperationLogQuery {
            actor_user_id: Some(UserId::from_v7()),
            category: Some(OperationCategory::Auth),
            resource_type: Some(ResourceType::User),
            resource_id: Some("user-1".to_owned()),
            outcome: Some(OperationOutcome::Success),
            request_id: Some(String::new()),
            governance_audit_event_id: None,
            from: None,
            to: None,
            page: PageRequest::default(),
        };

        let sql = Entity::find()
            .filter(PgOperationLogRepository::page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""operation_log"."actor_user_id" ="#));
        assert!(sql.contains(r#""operation_log"."category" ="#));
        assert!(sql.contains(r#""operation_log"."resource_type" ="#));
        assert!(sql.contains(r#""operation_log"."resource_id" ="#));
        assert!(sql.contains(r#""operation_log"."outcome" ="#));
        assert!(!sql.contains(r#""operation_log"."request_id" ="#));
    }

    #[test]
    fn page_empty_matches_rows() {
        let query = OperationLogQuery::default();
        let sql = Entity::find()
            .filter(PgOperationLogRepository::page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();
        assert!(!sql.contains(r#""operation_log"."actor_user_id" ="#));
        assert!(!sql.contains(r#""operation_log"."category" ="#));
    }
}
