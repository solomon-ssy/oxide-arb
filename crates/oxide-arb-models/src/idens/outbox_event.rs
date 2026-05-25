use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum OutboxEvent {
    Table,
    EventId,
    AggregateType,
    AggregateId,
    EventType,
    Payload,
    PublishAttempts,
    PublishedAt,
    LastError,
    CreatedAt,
    DeadLetterReason,
}
