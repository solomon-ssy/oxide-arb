//! Fresh-stack system contract across all three persistence services.

use std::time::Duration;

use quant_pivot_storage::cache::CacheBackend;
use quant_pivot_system_tests::stack::SystemStack;

#[tokio::test]
async fn fresh_stack_schema_ready() {
    let stack = Box::pin(SystemStack::start())
        .await
        .expect("start disposable system-test stack");

    assert!(stack.postgres_schema.migration_count > 0);
    assert!(stack.postgres_schema.required_table_count > 0);
    assert!(stack.clickhouse_schema.current_version > 0);
    assert!(stack.clickhouse_schema.required_object_count > 0);

    stack
        .redis
        .set("infrastructure-boot", b"ready", Duration::from_secs(30))
        .await
        .expect("write Redis system-test readiness marker");
    assert_eq!(
        stack
            .redis
            .get("infrastructure-boot")
            .await
            .expect("read Redis system-test readiness marker"),
        Some(b"ready".to_vec())
    );
}
