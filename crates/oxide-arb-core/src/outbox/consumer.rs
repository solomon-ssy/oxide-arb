use oxide_arb_error::OxideError;
use oxide_arb_models::domain::outbox::OutboxEventInfo;

#[async_trait::async_trait]
pub trait OutboxConsumer: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn consume(&self, event: &OutboxEventInfo) -> Result<(), OxideError>;
}
