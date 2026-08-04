use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ports::FeedbackRecipeTemplate, enums::model::ModelFamily,
    runtime_config::BuyModelRoute, types::ResearchProfileRef,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackRecipeTemplateWriteOutcome {
    Inserted,
    ExactReplay,
}

#[async_trait]
pub trait FeedbackRecipeTemplateRepository: Send + Sync {
    async fn insert(
        &self,
        template: FeedbackRecipeTemplate,
    ) -> QuantResult<FeedbackRecipeTemplateWriteOutcome>;

    async fn list_approved(
        &self,
        profile_ref: &ResearchProfileRef,
        route: BuyModelRoute,
        model_family: ModelFamily,
    ) -> QuantResult<Vec<FeedbackRecipeTemplate>>;
}
