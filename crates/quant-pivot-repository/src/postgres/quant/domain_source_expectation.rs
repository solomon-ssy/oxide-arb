//! Postgres-backed expected domain-source binding repository.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity::QUANT_DOMAIN_SOURCE_EXPECTATION};
use quant_pivot_models::{
    domain::data_plane::{
        DomainSourceExpectationInfo, DomainSourceExpectationTransition,
        UpsertDomainSourceExpectation,
    },
    entities::quant_domain_source_expectation::{Column, Entity},
    types::DomainSourceExpectationId,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::{Expr, OnConflict},
};

use crate::{postgres::primitives, traits::DomainSourceExpectationRepository};

pub struct PgDomainSourceExpectationRepository {
    db: DatabaseConnection,
}

impl PgDomainSourceExpectationRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl DomainSourceExpectationRepository for PgDomainSourceExpectationRepository {
    async fn find(
        &self,
        expectation_id: &DomainSourceExpectationId,
    ) -> Result<Option<DomainSourceExpectationInfo>, StorageError> {
        Entity::find_by_id(*expectation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert(
        &self,
        mut expectation: UpsertDomainSourceExpectation,
    ) -> Result<DomainSourceExpectationInfo, StorageError> {
        expectation
            .validate()
            .map_err(|detail| StorageError::InvariantViolation {
                entity: Some(QUANT_DOMAIN_SOURCE_EXPECTATION),
                detail,
            })?;
        expectation.updated_at = Utc::now();
        Entity::insert(expectation.into_active_model())
            .on_conflict(
                OnConflict::columns([Column::SourceId, Column::InstrumentKey])
                    .update_columns([
                        Column::ExpectationId,
                        Column::Family,
                        Column::CapabilityRegistryHash,
                        Column::BindingHash,
                        Column::Required,
                        Column::CredentialRequired,
                        Column::FreshnessSecs,
                        Column::AffectedMarketIds,
                        Column::AffectedProfileIds,
                        Column::Status,
                        Column::StatusReason,
                        Column::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn transition(
        &self,
        transition: DomainSourceExpectationTransition,
    ) -> Result<DomainSourceExpectationInfo, StorageError> {
        transition
            .validate()
            .map_err(|detail| StorageError::InvariantViolation {
                entity: Some(QUANT_DOMAIN_SOURCE_EXPECTATION),
                detail,
            })?;
        let result = Entity::update_many()
            .col_expr(Column::Status, primitives::enum_value(&transition.to))
            .col_expr(Column::StatusReason, Expr::value(transition.reason))
            .col_expr(Column::UpdatedAt, Expr::value(Utc::now()))
            .filter(Column::ExpectationId.eq(transition.expectation_id))
            .filter(Column::Status.eq(transition.from))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            let current = self.find(&transition.expectation_id).await?;
            return Err(match current {
                Some(row) => StorageError::IllegalTransition {
                    entity: QUANT_DOMAIN_SOURCE_EXPECTATION,
                    id: Some(transition.expectation_id.to_string()),
                    from: row.status.to_string(),
                    to: transition.to.to_string(),
                },
                None => StorageError::NotFound {
                    entity: QUANT_DOMAIN_SOURCE_EXPECTATION,
                    id: transition.expectation_id.to_string(),
                },
            });
        }
        self.find(&transition.expectation_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: QUANT_DOMAIN_SOURCE_EXPECTATION,
                id: transition.expectation_id.to_string(),
            })
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceExpectationInfo>, StorageError> {
        Entity::find()
            .order_by_asc(Column::Family)
            .order_by_asc(Column::SourceId)
            .order_by_asc(Column::InstrumentKey)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
