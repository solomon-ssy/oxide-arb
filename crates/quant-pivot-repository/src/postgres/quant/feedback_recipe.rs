//! `PostgreSQL` owner of the immutable feedback recipe-template catalog.

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::ports::FeedbackRecipeTemplate,
    entities::quant_feedback_recipe_template::{ActiveModel, Column, Entity},
    enums::{model::ModelFamily, quant::FeedbackRecipeTemplateStatus},
    runtime_config::BuyModelRoute,
    types::{FeedbackRecipeTemplateId, ResearchProfileRef},
};
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, TryInsertResult,
    sea_query::OnConflict,
};

use crate::traits::{FeedbackRecipeTemplateRepository, FeedbackRecipeTemplateWriteOutcome};

const TABLE: &str = "quant_feedback_recipe_template";

pub struct PgFeedbackRecipeTemplateRepository {
    db: DatabaseConnection,
}

impl PgFeedbackRecipeTemplateRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FeedbackRecipeTemplateRepository for PgFeedbackRecipeTemplateRepository {
    async fn insert(
        &self,
        template: FeedbackRecipeTemplate,
    ) -> QuantResult<FeedbackRecipeTemplateWriteOutcome> {
        template.validate()?;
        let revision = i32::try_from(template.revision).map_err(|error| {
            StorageError::invariant_violation(
                Some(TABLE),
                format!("recipe revision exceeds PostgreSQL integer: {error}"),
            )
        })?;
        let active = ActiveModel {
            recipe_template_id: Set(template.recipe_template_id),
            revision: Set(revision),
            research_profile_artifact_id: Set(template.profile_ref.artifact_id()),
            route: Set(template.route),
            model_family: Set(template.model_family),
            model_spec_id: Set(template.training_spec.model_spec_id),
            status: Set(template.status),
            catalog_priority: Set(template.catalog_priority),
            approved_by_user_id: Set(template.approved_by_user_id),
            approved_by_role: Set(template.approved_by_role.clone()),
            approved_at: Set(template.approved_at),
            governance_reason: Set(template.governance_reason.clone()),
            template_hash: Set(template.template_hash),
            template: Set(template.clone()),
            created_at: NotSet,
        };
        let result = Entity::insert(active)
            .on_conflict(
                OnConflict::columns([Column::RecipeTemplateId, Column::Revision])
                    .do_nothing()
                    .to_owned(),
            )
            .try_insert()
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let inserted = match result {
            TryInsertResult::Inserted(1) => true,
            TryInsertResult::Conflicted | TryInsertResult::Inserted(0) => false,
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some(TABLE),
                    format!("single recipe-template insert affected {rows} rows"),
                )
                .into());
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(TABLE),
                    "non-empty recipe-template insert produced no statement",
                )
                .into());
            }
        };
        let stored = Entity::find_by_id((template.recipe_template_id, revision))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    TABLE,
                    format!("{}@{revision}", template.recipe_template_id),
                )
            })?;
        if stored.template != template
            || stored.template_hash != template.template_hash
            || stored.model_spec_id != template.training_spec.model_spec_id
            || stored.status != template.status
            || stored.approved_by_user_id != template.approved_by_user_id
            || stored.approved_by_role != template.approved_by_role
            || stored.approved_at != template.approved_at
            || stored.governance_reason != template.governance_reason
        {
            return Err(StorageError::state_conflict(
                TABLE,
                Some(template.recipe_template_id),
                "recipe-template replay has semantic drift",
            )
            .into());
        }
        Ok(if inserted {
            FeedbackRecipeTemplateWriteOutcome::Inserted
        } else {
            FeedbackRecipeTemplateWriteOutcome::ExactReplay
        })
    }

    async fn list_approved(
        &self,
        profile_ref: &ResearchProfileRef,
        route: BuyModelRoute,
        model_family: ModelFamily,
    ) -> QuantResult<Vec<FeedbackRecipeTemplate>> {
        let rows = Entity::find()
            .filter(Column::ResearchProfileArtifactId.eq(profile_ref.artifact_id()))
            .filter(Column::Route.eq(route))
            .filter(Column::ModelFamily.eq(model_family))
            .filter(Column::Status.is_in([
                FeedbackRecipeTemplateStatus::Approved,
                FeedbackRecipeTemplateStatus::Retired,
            ]))
            .order_by_asc(Column::RecipeTemplateId)
            .order_by_desc(Column::Revision)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut latest = BTreeMap::<FeedbackRecipeTemplateId, FeedbackRecipeTemplate>::new();
        for row in rows {
            row.template.validate()?;
            latest.entry(row.recipe_template_id).or_insert(row.template);
        }
        Ok(latest
            .into_values()
            .filter(|template| {
                template.status == FeedbackRecipeTemplateStatus::Approved
                    && template.profile_ref == *profile_ref
                    && template.route == route
                    && template.model_family == model_family
            })
            .collect())
    }
}
