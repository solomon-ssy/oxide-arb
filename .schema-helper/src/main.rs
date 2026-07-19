use quant_pivot_models::security::hash_password;
use quant_pivot_storage::postgres::migration::finalize_schema_deployment;
use sea_orm::{ConnectionTrait, Database};

const MANIFEST_RUNTIME_ROLE: &str = "quant_pivot_schema_manifest_runtime";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PRINT_POLICY_DEFAULT").is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &quant_pivot_models::runtime_config::DecisionPolicySnapshot::default(),
            )?
        );
        return Ok(());
    }
    let url = std::env::var("DATABASE_URL")?;
    let database = Database::connect(url).await?;
    database
        .execute_unprepared(&format!("CREATE ROLE {MANIFEST_RUNTIME_ROLE} NOLOGIN"))
        .await?;
    quant_pivot_migration::apply(&database).await?;
    let password_hash = hash_password("admin")?;
    finalize_schema_deployment(&database, MANIFEST_RUNTIME_ROLE, &password_hash).await?;
    Ok(())
}
