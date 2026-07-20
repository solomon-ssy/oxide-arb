//! Postgres-backed expected domain-source binding repository.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        DomainSourceExpectationInfo, DomainSourceExpectationTransition,
        UpsertDomainSourceExpectation,
    },
    entities::quant_domain_source_expectation,
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
        quant_domain_source_expectation::Entity::find_by_id(expectation_id.clone())
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
                entity: Some(entity::QUANT_DOMAIN_SOURCE_EXPECTATION),
                detail,
            })?;
        expectation.updated_at = Utc::now();
        quant_domain_source_expectation::Entity::insert(expectation.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_domain_source_expectation::Column::SourceId,
                    quant_domain_source_expectation::Column::InstrumentKey,
                ])
                .update_columns([
                    quant_domain_source_expectation::Column::ExpectationId,
                    quant_domain_source_expectation::Column::Family,
                    quant_domain_source_expectation::Column::CapabilityRegistryHash,
                    quant_domain_source_expectation::Column::BindingHash,
                    quant_domain_source_expectation::Column::Required,
                    quant_domain_source_expectation::Column::CredentialRequired,
                    quant_domain_source_expectation::Column::FreshnessSecs,
                    quant_domain_source_expectation::Column::AffectedMarketIds,
                    quant_domain_source_expectation::Column::AffectedProfileIds,
                    quant_domain_source_expectation::Column::Status,
                    quant_domain_source_expectation::Column::StatusReason,
                    quant_domain_source_expectation::Column::UpdatedAt,
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
                entity: Some(entity::QUANT_DOMAIN_SOURCE_EXPECTATION),
                detail,
            })?;
        let result = quant_domain_source_expectation::Entity::update_many()
            .col_expr(
                quant_domain_source_expectation::Column::Status,
                primitives::enum_value(&transition.to),
            )
            .col_expr(
                quant_domain_source_expectation::Column::StatusReason,
                Expr::value(transition.reason),
            )
            .col_expr(
                quant_domain_source_expectation::Column::UpdatedAt,
                Expr::value(Utc::now()),
            )
            .filter(
                quant_domain_source_expectation::Column::ExpectationId
                    .eq(transition.expectation_id.clone()),
            )
            .filter(quant_domain_source_expectation::Column::Status.eq(transition.from))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            let current = self.find(&transition.expectation_id).await?;
            return Err(match current {
                Some(row) => StorageError::IllegalTransition {
                    entity: entity::QUANT_DOMAIN_SOURCE_EXPECTATION,
                    id: Some(transition.expectation_id.to_string()),
                    from: row.status.to_string(),
                    to: transition.to.to_string(),
                },
                None => StorageError::NotFound {
                    entity: entity::QUANT_DOMAIN_SOURCE_EXPECTATION,
                    id: transition.expectation_id.to_string(),
                },
            });
        }
        self.find(&transition.expectation_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: entity::QUANT_DOMAIN_SOURCE_EXPECTATION,
                id: transition.expectation_id.to_string(),
            })
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceExpectationInfo>, StorageError> {
        quant_domain_source_expectation::Entity::find()
            .order_by_asc(quant_domain_source_expectation::Column::Family)
            .order_by_asc(quant_domain_source_expectation::Column::SourceId)
            .order_by_asc(quant_domain_source_expectation::Column::InstrumentKey)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
