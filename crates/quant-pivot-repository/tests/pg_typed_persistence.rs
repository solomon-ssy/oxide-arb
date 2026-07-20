//! Real `PostgreSQL` corruption/decode tests for typed persistence boundaries.

use quant_pivot_models::types::{
    ArtifactVersion, AttestationKeyId, EvmAddress, EvmTransactionHash, OperationDetailDocument,
    PortfolioRiskBudget, ReaderContractVersion, ReportRunId, ReportTriggerKey, ResearchJobParams,
    SchemaContractVersion, TradePolicyCandidateId,
};
use quant_pivot_test_support::pg::setup_pg;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, QueryResult, Statement, TryGetError, TryGetable,
};
use serde::Serialize;
use serde_json::json;

async fn postgres_json(value: impl Serialize, db: &DatabaseConnection) -> QueryResult {
    let encoded = serde_json::to_string(&value).expect("serialize test JSON document");
    db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT $1::jsonb AS document",
        [encoded.into()],
    ))
    .await
    .expect("query PostgreSQL JSONB value")
    .expect("SELECT returns one row")
}

async fn postgres_text(value: &str, db: &DatabaseConnection) -> QueryResult {
    db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT $1::text AS semantic_value",
        [value.to_owned().into()],
    ))
    .await
    .expect("query PostgreSQL text value")
    .expect("SELECT returns one row")
}

fn decode_json<T: TryGetable>(row: &QueryResult) -> Result<T, TryGetError> {
    T::try_get(row, "", "document")
}

fn decode_text<T: TryGetable>(row: &QueryResult) -> Result<T, TryGetError> {
    T::try_get(row, "", "semantic_value")
}

async fn insert_queued_report_run(
    trigger_key: &str,
    db: &DatabaseConnection,
) -> Result<sea_orm::ExecResult, sea_orm::DbErr> {
    let run_id = ReportRunId::from_v7();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO quant_report_run (report_run_id, trigger_kind, trigger_key, request_id, requested_at, status) VALUES ($1, 'ad_hoc', $2, $3, now(), 'queued')",
        [
            run_id.as_uuid().into(),
            trigger_key.to_owned().into(),
            format!("request:{run_id}").into(),
        ],
    ))
    .await
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn typed_jsonb_rejects_postgres_corruption_without_fallback() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();

    let valid = PortfolioRiskBudget::default();
    let row = postgres_json(&valid, db).await;
    assert_eq!(
        decode_json::<PortfolioRiskBudget>(&row).expect("valid decode"),
        valid
    );

    let mut unknown_field = serde_json::to_value(&valid).expect("serialize valid document");
    unknown_field["future_field"] = json!(true);
    let row = postgres_json(unknown_field, db).await;
    assert!(
        decode_json::<PortfolioRiskBudget>(&row).is_err(),
        "project-owned documents must reject unknown fields returned by PostgreSQL"
    );

    let row = postgres_json(json!([]), db).await;
    assert!(
        decode_json::<PortfolioRiskBudget>(&row).is_err(),
        "object documents must reject an array shape"
    );

    let row = postgres_json(json!({ "kind": "future_job", "params": {} }), db).await;
    assert!(
        decode_json::<ResearchJobParams>(&row).is_err(),
        "tagged documents must reject unknown discriminators"
    );

    let row = postgres_json(json!({ "password": "redacted-is-still-forbidden" }), db).await;
    assert!(
        decode_json::<OperationDetailDocument>(&row).is_err(),
        "controlled-open audit documents must revalidate sensitive keys on DB decode"
    );

    let row = postgres_json(json!(["not", "an", "object"]), db).await;
    assert!(
        decode_json::<OperationDetailDocument>(&row).is_err(),
        "controlled-open audit documents must remain object-shaped"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn semantic_text_revalidates_postgres_decode_and_database_checks() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();

    let valid_address = format!("0x{}", "a".repeat(40));
    let valid_tx_hash = format!("0x{}", "b".repeat(64));
    let valid_cases = [
        ("source_slice_reader_v1", "reader_contract"),
        ("source_slice_schema_v1", "schema_contract"),
        ("schedule:daily", "report_trigger"),
        ("candidate-1", "candidate_id"),
        ("artifact-v1", "artifact_version"),
        ("operator-2026-07", "attestation_key"),
    ];
    for (value, label) in valid_cases {
        let row = postgres_text(value, db).await;
        let accepted = match label {
            "reader_contract" => decode_text::<ReaderContractVersion>(&row).is_ok(),
            "schema_contract" => decode_text::<SchemaContractVersion>(&row).is_ok(),
            "report_trigger" => decode_text::<ReportTriggerKey>(&row).is_ok(),
            "candidate_id" => decode_text::<TradePolicyCandidateId>(&row).is_ok(),
            "artifact_version" => decode_text::<ArtifactVersion>(&row).is_ok(),
            "attestation_key" => decode_text::<AttestationKeyId>(&row).is_ok(),
            _ => false,
        };
        assert!(accepted, "valid {label} must decode from PostgreSQL");
    }
    assert!(
        decode_text::<EvmAddress>(&postgres_text(&valid_address, db).await).is_ok(),
        "canonical EVM address must decode"
    );
    assert!(
        decode_text::<EvmTransactionHash>(&postgres_text(&valid_tx_hash, db).await).is_ok(),
        "canonical EVM transaction hash must decode"
    );

    let whitespace = postgres_text("contains whitespace", db).await;
    assert!(decode_text::<ReportTriggerKey>(&whitespace).is_err());
    assert!(decode_text::<ArtifactVersion>(&whitespace).is_err());
    assert!(decode_text::<AttestationKeyId>(&whitespace).is_err());
    let uppercase_address = postgres_text(&format!("0x{}", "A".repeat(40)), db).await;
    assert!(decode_text::<EvmAddress>(&uppercase_address).is_err());
    let short_hash = postgres_text("0xdeadbeef", db).await;
    assert!(decode_text::<EvmTransactionHash>(&short_hash).is_err());

    let maximum_trigger_key = "a".repeat(256);
    assert!(ReportTriggerKey::parse(&maximum_trigger_key).is_ok());
    insert_queued_report_run(&maximum_trigger_key, db)
        .await
        .expect("256-byte report trigger key must satisfy the PostgreSQL constraint");
    assert!(
        insert_queued_report_run(&"a".repeat(257), db)
            .await
            .is_err(),
        "PostgreSQL must reject report trigger keys beyond the Rust boundary"
    );
    assert!(
        insert_queued_report_run("ad_hoc:\\forbidden", db)
            .await
            .is_err(),
        "PostgreSQL must reject forbidden semantic-key characters"
    );

    assert!(
        db.execute_unprepared("UPDATE role SET code = 'Risk Owner' WHERE code = 'admin'")
            .await
            .is_err(),
        "role code constraint must reject non-canonical storage values"
    );
    assert!(
        db.execute_unprepared(
            "INSERT INTO quant_trade_tape_block_cursor (source, contract_address, last_finalized_block, last_log_index, head_lag_blocks, status, created_at, updated_at) VALUES ('on_chain', '0xBAD', 0, 0, 0, 'bootstrap', now(), now())",
        )
        .await
        .is_err(),
        "EVM address constraint must reject non-canonical storage values"
    );
}
