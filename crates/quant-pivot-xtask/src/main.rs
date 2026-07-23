mod account_read_smoke;
mod architecture;
mod performance;
mod public_read_smoke;

use std::{
    env, fs,
    fs::{File, OpenOptions, Permissions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration as StdDuration,
};

use anyhow::{Context, Error, Result, bail};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use quant_pivot_allocator as _;
use quant_pivot_migration::{
    acquire_schema_mutation_lease, apply_under_schema_mutation_lease,
    inspect_preproduction_postgres, plan as plan_postgres_migrations,
    release_schema_mutation_lease, reset_preproduction_postgres,
};
use quant_pivot_models::{
    config::{ClickHouseConfig, DeployConfig, PostgresConfig},
    domain::api::{ConfigApiContractSchema, ResearchModelApiContractSchema},
    hashing::CanonicalDigest,
    runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot},
    security::hash_password,
    types::{ContentHash, PreproductionResetNonce},
};
use quant_pivot_repository::{
    postgres::{
        PgModelRegistryRepository, PgPolicyRepository,
        governance::policy_bootstrap::ensure_default_policy_bundle,
    },
    traits::PolicyRepository,
};
use quant_pivot_storage::{
    cache::{count_preproduction_namespace, unlink_preproduction_namespace},
    clickhouse::{
        ClickHousePool, active_preproduction_query_count, apply_offline_schema_migrations,
        apply_online_schema_migrations, database_object_count, plan_schema,
        render_schema_manifest as render_clickhouse_schema_manifest,
        reset_preproduction_database as reset_clickhouse_preproduction_database, verify_schema,
    },
    postgres::{
        PostgresPool,
        migration::{
            finalize_schema_deployment, generate_disposable_schema_manifest,
            inspect_schema_manifest, render_schema_manifest,
            verify_schema as verify_postgres_schema,
        },
    },
};
use quant_pivot_system_tests::{performance::PerformanceProfile, production_stack};
use rustls::crypto::aws_lc_rs;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use serde::{Deserialize, Serialize};
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;
use zeroize::Zeroizing;

const BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV: &str = "QUANT_PIVOT_BOOTSTRAP__ADMIN_PASSWORD_FILE";
const CLEAN_BOOTSTRAP_CONFIRMATION: &str = "DELETE_ALL_PREPRODUCTION_DATA_AND_REBOOTSTRAP";

#[derive(Parser)]
#[command(name = "quant-pivot-xtask")]
#[command(about = "Task runner for quant-pivot", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Enforce workspace dependency direction and production/test boundaries.
    Architecture {
        #[command(subcommand)]
        command: ArchitectureCommand,
    },
    /// Run the real production binary against disposable infrastructure.
    #[command(name = "production-stack")]
    ProductionStack {
        #[command(subcommand)]
        command: ProductionStackCommand,
    },
    /// Run explicitly classified external read-only smoke checks.
    Smoke {
        #[command(subcommand)]
        command: SmokeCommand,
    },
    /// Run reproducible kernel and production-stack performance gates.
    Performance {
        #[command(subcommand)]
        command: PerformanceCommand,
    },
    /// Plan, apply online-safe migrations, or verify the `ClickHouse` schema.
    #[command(name = "clickhouse-schema")]
    ClickHouseSchema {
        #[command(subcommand)]
        command: ClickHouseSchemaCommand,
    },
    /// Plan/apply/verify immutable migrations or generate their semantic manifest.
    #[command(name = "postgres-schema")]
    PostgresSchema {
        #[command(subcommand)]
        command: PostgresSchemaCommand,
    },
    /// Derive the report hard ceiling from the recent 30-day catalog-visible p99.
    #[command(name = "report-capacity-ceiling")]
    ReportCapacityCeiling {
        #[arg(long)]
        catalog_visible_p99: u64,
    },
    /// Render the canonical boot policy bundle for schema and inventory tooling.
    #[command(name = "render-boot-policy")]
    RenderBootPolicy,
    /// Generate the Rust-owned Config API JSON Schema used for TypeScript codegen.
    #[command(name = "config-api-schema")]
    ConfigApiSchema {
        #[arg(long, default_value = "schema/api/config-v1.schema.json")]
        output: PathBuf,
    },
    /// Generate the Rust-owned research-model API schema used by the SPA.
    #[command(name = "research-model-api-schema")]
    ResearchModelApiSchema {
        #[arg(long, default_value = "schema/api/research-model-v1.schema.json")]
        output: PathBuf,
    },
    /// Plan, apply, or verify the exact preproduction clean-boot reset scope.
    #[command(name = "preproduction-reset")]
    PreproductionReset {
        #[command(subcommand)]
        command: PreproductionResetCommand,
    },
}

#[derive(Subcommand)]
enum ArchitectureCommand {
    /// Validate the current Cargo metadata graph without changing the workspace.
    Check,
}

#[derive(Subcommand)]
enum ProductionStackCommand {
    /// Serve until terminated for browser E2E or local system verification.
    Serve(ProductionStackServeArgs),
    /// Boot, probe, and stop fresh stacks repeatedly.
    Verify(ProductionStackVerifyArgs),
}

#[derive(Subcommand)]
enum SmokeCommand {
    /// Verify configured account identity and venue reads without submitting.
    #[command(name = "account-read")]
    AccountRead(ConfigDirArgs),
    /// Probe public endpoints without credentials or money-moving operations.
    #[command(name = "public-read")]
    PublicRead(PublicReadSmokeArgs),
}

#[derive(Subcommand)]
enum PerformanceCommand {
    /// Build release gates, run kernel repetitions, then run the system profile.
    Run(PerformanceRunArgs),
}

#[derive(Args)]
struct PerformanceRunArgs {
    #[arg(long, value_enum, default_value_t = PerformanceProfile::Full)]
    profile: PerformanceProfile,
    #[arg(long, default_value = "target/performance-evidence")]
    output: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PublicReadSmokeScope {
    All,
    Binance,
    Polymarket,
    Weather,
}

#[derive(Args)]
struct PublicReadSmokeArgs {
    #[arg(long, value_enum, default_value_t = PublicReadSmokeScope::All)]
    scope: PublicReadSmokeScope,
    #[arg(long, default_value_t = 90)]
    stream_timeout_secs: u64,
}

#[derive(Args)]
struct ProductionStackServeArgs {
    #[arg(long, default_value_t = 8088)]
    listen_port: u16,
    /// Seed the smallest published-report/pending-intent/model lineage needed by browser E2E.
    #[arg(long)]
    browser_fixture: bool,
    /// Retain the isolated run directory so CI can archive backend logs after a failure.
    #[arg(long)]
    retain_artifacts: bool,
}

#[derive(Args)]
struct ProductionStackVerifyArgs {
    #[arg(long, default_value_t = 2)]
    runs: u16,
}

#[derive(Subcommand)]
enum ClickHouseSchemaCommand {
    /// Print pending migrations without changing the target database.
    Plan(ConfigDirArgs),
    /// Create the database and apply pending online-safe migrations.
    ApplyOnline(ConfigDirArgs),
    /// Apply pending offline migrations after proving destructive source tables are empty.
    ApplyOffline(ConfigDirArgs),
    /// Verify the migration ledger and runtime schema contract read-only.
    Verify(ConfigDirArgs),
    /// Generate the normalized SHOW CREATE semantic manifest after migrations.
    Manifest(ConfigDirArgs),
}

#[derive(Subcommand)]
enum PostgresSchemaCommand {
    /// Print pending migrations without mutating the target database.
    Plan(ConfigDirArgs),
    /// Apply pending migrations and versioned catalog seeds with deploy credentials.
    Apply(ConfigDirArgs),
    /// Verify checksums, seeds, and the runtime schema contract read-only.
    Verify(ConfigDirArgs),
    /// Generate the normalized `pg_catalog` manifest after applying migrations.
    Manifest(ConfigDirArgs),
    /// Generate both manifests from a clean, owned disposable `PostgreSQL` 16 container.
    ManifestClean,
    /// Regenerate only the immutable compiled migration-artifact manifest.
    MigrationManifest,
}

#[derive(Args)]
struct ConfigDirArgs {
    /// Directory containing quant-pivot.toml.
    #[arg(long, env = "QUANT_PIVOT_CONFIG_DIR", default_value = "config")]
    config_dir: PathBuf,
}

#[derive(Subcommand)]
enum PreproductionResetCommand {
    /// Inspect exact targets and create a short-lived one-time journal.
    Plan(PreproductionResetPlanArgs),
    /// Consume a planned journal and perform the guarded reset.
    Apply(PreproductionResetApplyArgs),
    /// Verify a completed operation by its explicit operation id.
    Verify(PreproductionResetVerifyArgs),
}

#[derive(Args)]
struct PreproductionResetPlanArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(
        long,
        default_value = ".local/preproduction-reset/active-operation.json"
    )]
    journal_file: PathBuf,
}

#[derive(Args)]
struct PreproductionResetApplyArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(
        long,
        default_value = ".local/preproduction-reset/active-operation.json"
    )]
    journal_file: PathBuf,
    #[arg(long)]
    confirm_nonce: PreproductionResetNonce,
    /// Exact destructive scope acknowledgement printed by `plan`.
    #[arg(long)]
    confirm: String,
}

#[derive(Args)]
struct PreproductionResetVerifyArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(
        long,
        default_value = ".local/preproduction-reset/active-operation.json"
    )]
    journal_file: PathBuf,
    #[arg(long)]
    operation_id: Uuid,
}

#[tokio::main]
async fn main() -> ExitCode {
    if aws_lc_rs::default_provider().install_default().is_err() {
        eprintln!("rustls crypto provider already installed");
    }
    match Box::pin(run()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Commands::Architecture { command } => match command {
            ArchitectureCommand::Check => architecture::run(),
        },
        Commands::ProductionStack { command } => match command {
            ProductionStackCommand::Serve(args) => {
                Box::pin(production_stack::serve(
                    args.listen_port,
                    args.browser_fixture,
                    args.retain_artifacts,
                ))
                .await
            }
            ProductionStackCommand::Verify(args) => production_stack::verify(args.runs).await,
        },
        Commands::Smoke { command } => match command {
            SmokeCommand::AccountRead(args) => account_read_smoke::run(&args.config_dir).await,
            SmokeCommand::PublicRead(args) => {
                if args.stream_timeout_secs == 0 {
                    bail!("public-read smoke requires --stream-timeout-secs greater than zero");
                }
                Box::pin(public_read_smoke::run(
                    matches!(
                        args.scope,
                        PublicReadSmokeScope::All | PublicReadSmokeScope::Binance
                    ),
                    matches!(
                        args.scope,
                        PublicReadSmokeScope::All | PublicReadSmokeScope::Polymarket
                    ),
                    matches!(
                        args.scope,
                        PublicReadSmokeScope::All | PublicReadSmokeScope::Weather
                    ),
                    StdDuration::from_secs(args.stream_timeout_secs),
                ))
                .await
            }
        },
        Commands::Performance { command } => match command {
            PerformanceCommand::Run(args) => performance::run(args.profile, &args.output),
        },
        Commands::ClickHouseSchema { command } => clickhouse_schema(command).await,
        Commands::PostgresSchema { command } => postgres_schema(command).await,
        Commands::ReportCapacityCeiling {
            catalog_visible_p99,
        } => {
            println!("{}", report_capacity_ceiling(catalog_visible_p99)?);
            Ok(())
        }
        Commands::RenderBootPolicy => {
            println!(
                "{}",
                serde_json::to_string_pretty(&DecisionPolicySnapshot::default())
                    .context("render boot policy bundle")?
            );
            Ok(())
        }
        Commands::ConfigApiSchema { output } => write_config_api_schema(&output),
        Commands::ResearchModelApiSchema { output } => write_research_model_api_schema(&output),
        Commands::PreproductionReset { command } => preproduction_reset(command).await,
    }
}

fn write_config_api_schema(output: &PathBuf) -> Result<()> {
    let schema = schemars::schema_for!(ConfigApiContractSchema);
    let mut rendered =
        serde_json::to_string_pretty(&schema).context("render Config API contract schema")?;
    rendered.push('\n');
    fs::create_dir_all(
        output
            .parent()
            .context("Config API schema path has no parent")?,
    )
    .with_context(|| format!("create {}", output.display()))?;
    fs::write(output, rendered).with_context(|| format!("write {}", output.display()))?;
    println!("generated {}", output.display());
    Ok(())
}

fn write_research_model_api_schema(output: &PathBuf) -> Result<()> {
    let schema = schemars::schema_for!(ResearchModelApiContractSchema);
    let mut rendered = serde_json::to_string_pretty(&schema)
        .context("render research-model API contract schema")?;
    rendered.push('\n');
    fs::create_dir_all(
        output
            .parent()
            .context("research-model API schema path has no parent")?,
    )
    .with_context(|| format!("create {}", output.display()))?;
    fs::write(output, rendered).with_context(|| format!("write {}", output.display()))?;
    println!("generated {}", output.display());
    Ok(())
}

const RESET_JOURNAL_FORMAT_VERSION: u32 = 2;
const RESET_PLAN_TTL_MINUTES: i64 = 15;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResetTargetFingerprints {
    postgres: ContentHash,
    clickhouse: ContentHash,
    redis: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResetObjectInventory {
    postgres_database_exists: bool,
    postgres_object_count: i64,
    postgres_connection_count: i64,
    clickhouse_object_count: u64,
    clickhouse_active_query_count: u64,
    redis_namespace_key_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResetStage {
    Planned,
    Applying,
    PostgresReset,
    ClickhouseReset,
    RedisCleared,
    SchemasApplied,
    Verified,
    Completed,
    Failed,
}

impl ResetStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Applying => "applying",
            Self::PostgresReset => "postgres_reset",
            Self::ClickhouseReset => "clickhouse_reset",
            Self::RedisCleared => "redis_cleared",
            Self::SchemasApplied => "schemas_applied",
            Self::Verified => "verified",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResetFailure {
    failed_stage: ResetStage,
    failed_at: DateTime<Utc>,
    summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PreproductionResetJournal {
    format_version: u32,
    operation_id: Uuid,
    nonce: PreproductionResetNonce,
    stage: ResetStage,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    targets: ResetTargetFingerprints,
    inventory: ResetObjectInventory,
    failure: Option<ResetFailure>,
    journal_hash: ContentHash,
}

#[derive(Serialize)]
struct ResetJournalDigest<'a> {
    format_version: u32,
    operation_id: Uuid,
    nonce: &'a PreproductionResetNonce,
    stage: ResetStage,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    targets: &'a ResetTargetFingerprints,
    inventory: &'a ResetObjectInventory,
    failure: &'a Option<ResetFailure>,
}

async fn preproduction_reset(command: PreproductionResetCommand) -> Result<()> {
    match command {
        PreproductionResetCommand::Plan(args) => Box::pin(preproduction_reset_plan(args)).await,
        PreproductionResetCommand::Apply(args) => Box::pin(preproduction_reset_apply(args)).await,
        PreproductionResetCommand::Verify(args) => preproduction_reset_verify(args).await,
    }
}

async fn preproduction_reset_plan(args: PreproductionResetPlanArgs) -> Result<()> {
    let deploy = load_reset_deploy(&args.config)?;
    let inventory = reset_inventory(&deploy).await?;
    if inventory.postgres_connection_count != 0 {
        bail!(
            "{} PostgreSQL target connections remain; stop project-owned processes before planning reset",
            inventory.postgres_connection_count
        );
    }
    if inventory.clickhouse_active_query_count != 0 {
        bail!(
            "{} active ClickHouse project queries or server-wide mutations remain; stop their owners before planning reset",
            inventory.clickhouse_active_query_count
        );
    }
    if args.journal_file.exists() {
        let existing = read_reset_journal(&args.journal_file)?;
        if existing.stage == ResetStage::Planned && existing.expires_at > Utc::now() {
            bail!(
                "an unexpired reset operation already exists at {}; use its nonce or wait for expiry",
                args.journal_file.display()
            );
        }
        archive_reset_journal(&args.journal_file, &existing)?;
    }
    let now = Utc::now();
    let mut journal = PreproductionResetJournal {
        format_version: RESET_JOURNAL_FORMAT_VERSION,
        operation_id: Uuid::now_v7(),
        nonce: PreproductionResetNonce::from_v7(),
        stage: ResetStage::Planned,
        created_at: now,
        updated_at: now,
        expires_at: now + Duration::minutes(RESET_PLAN_TTL_MINUTES),
        completed_at: None,
        targets: reset_target_fingerprints(&deploy)?,
        inventory,
        failure: None,
        journal_hash: CanonicalDigest::content_hash_json(&"pending")?,
    };
    journal.journal_hash = reset_journal_hash(&journal)?;
    write_private_json_atomic(&args.journal_file, &journal)?;
    println!("preproduction reset operation planned");
    println!("operation_id={}", journal.operation_id);
    println!("journal_file={}", args.journal_file.display());
    println!("confirmation_nonce={}", journal.nonce);
    println!("required_confirmation={CLEAN_BOOTSTRAP_CONFIRMATION}");
    println!("expires_at={}", journal.expires_at.to_rfc3339());
    println!("postgres_endpoint_fingerprint={}", journal.targets.postgres);
    println!(
        "clickhouse_endpoint_fingerprint={}",
        journal.targets.clickhouse
    );
    println!("redis_endpoint_fingerprint={}", journal.targets.redis);
    println!(
        "postgres_objects={}",
        journal.inventory.postgres_object_count
    );
    println!(
        "clickhouse_objects={}",
        journal.inventory.clickhouse_object_count
    );
    println!(
        "clickhouse_active_queries={}",
        journal.inventory.clickhouse_active_query_count
    );
    println!(
        "redis_qp_namespace_keys={}",
        journal.inventory.redis_namespace_key_count
    );
    Ok(())
}

async fn preproduction_reset_apply(args: PreproductionResetApplyArgs) -> Result<()> {
    let deploy = load_reset_deploy(&args.config)?;
    let mut journal = read_reset_journal(&args.journal_file)?;
    validate_reset_journal_for_apply(&journal, &deploy, &args.confirm_nonce, &args.confirm)?;
    let current_inventory = reset_inventory(&deploy).await?;
    if current_inventory != journal.inventory {
        bail!("reset target inventory changed after planning; create a new reset plan");
    }
    if current_inventory.postgres_connection_count != 0 {
        bail!("PostgreSQL target still has active connections");
    }

    let lease = acquire_schema_mutation_lease(&deploy.db.postgres)
        .await
        .context("acquire reset schema mutation lease")?;
    let locked_inventory = reset_inventory(&deploy).await?;
    if locked_inventory != journal.inventory || locked_inventory.postgres_connection_count != 0 {
        release_schema_mutation_lease(lease)
            .await
            .context("release reset schema mutation lease after stale plan")?;
        bail!("reset target changed while acquiring the schema mutation lease; create a new plan");
    }
    transition_reset_journal(&args.journal_file, &mut journal, ResetStage::Applying)?;

    let mutation_result = tokio::select! {
      result = async {
        reset_preproduction_postgres(&deploy.db.postgres, &lease)
            .await
            .context("recreate exact PostgreSQL quant_pivot database")?;
        transition_reset_journal(
            &args.journal_file,
            &mut journal,
            ResetStage::PostgresReset,
        )?;
        reset_clickhouse_preproduction_database(&deploy.db.clickhouse)
            .await
            .context("recreate exact ClickHouse quant_pivot database")?;
        transition_reset_journal(
            &args.journal_file,
            &mut journal,
            ResetStage::ClickhouseReset,
        )?;
        let deleted = unlink_preproduction_namespace(&deploy.cache.redis)
            .await
            .context("unlink exact Redis qp:* namespace")?;
        transition_reset_journal(
            &args.journal_file,
            &mut journal,
            ResetStage::RedisCleared,
        )?;

        let postgres = PostgresPool::connect_schema(&deploy.db.postgres)
            .await
            .context("connect recreated PostgreSQL database")?;
        apply_under_schema_mutation_lease(postgres.connection(), &lease)
            .await
            .context("apply unique PostgreSQL boot migration")?;
        let bootstrap_admin_password_hash = bootstrap_admin_password_hash()?;
        finalize_schema_deployment(
            postgres.connection(),
            &bootstrap_admin_password_hash,
        )
        .await
        .context("finalize recreated PostgreSQL schema")?;
        let policy_bundle = ensure_postgres_boot_domain_facts(
            postgres.connection(),
            "guarded preproduction clean boot",
        )
        .await?;
        let postgres_status = verify_postgres_schema(postgres.connection())
            .await
            .context("verify recreated PostgreSQL schema")?;
        let clickhouse_status = apply_online_schema_migrations(&deploy.db.clickhouse)
            .await
            .context("apply unique ClickHouse boot migration")?;
        transition_reset_journal(
            &args.journal_file,
            &mut journal,
            ResetStage::SchemasApplied,
        )?;
        let redis_remaining = count_preproduction_namespace(&deploy.cache.redis)
            .await
            .context("verify Redis qp:* namespace")?;
        if redis_remaining != 0 {
            bail!("Redis qp:* namespace is not empty after reset");
        }
        verify_clean_bootstrap_facts(&postgres, &deploy.db.clickhouse).await?;
        transition_reset_journal(
            &args.journal_file,
            &mut journal,
            ResetStage::Verified,
        )?;
        postgres.close().await;
        println!("redis_unlinked={deleted}");
        println!(
            "postgres_boot_version={} postgres_migrations={}",
            postgres_status.current_version, postgres_status.migration_count
        );
        println!(
            "clickhouse_boot_version={} clickhouse_objects={}",
            clickhouse_status.current_version, clickhouse_status.required_object_count
        );
        println!(
            "policy_bundle_generation={} policy_snapshot_id={} policy_snapshot_hash={}",
            policy_bundle.generation,
            policy_bundle.decision_policy_snapshot_id,
            policy_bundle.snapshot_hash
        );
        Ok::<(), anyhow::Error>(())
      } => result,
      () = lease.cancelled() => Err(anyhow::anyhow!(
          "canonical PostgreSQL schema mutation lease was lost during reset"
      )),
    };
    let release_result = release_schema_mutation_lease(lease)
        .await
        .context("release reset schema mutation lease");
    match (mutation_result, release_result) {
        (Err(error), _) | (Ok(()), Err(error)) => {
            mark_reset_journal_failed(&args.journal_file, &mut journal)?;
            Err(error)
        }
        (Ok(()), Ok(())) => {
            transition_reset_journal(&args.journal_file, &mut journal, ResetStage::Completed)?;
            println!("preproduction reset completed and verified");
            println!("operation_id={}", journal.operation_id);
            println!("completed_journal={}", args.journal_file.display());
            Ok(())
        }
    }
}

async fn preproduction_reset_verify(args: PreproductionResetVerifyArgs) -> Result<()> {
    let deploy = load_reset_deploy(&args.config)?;
    let journal = read_reset_journal(&args.journal_file)?;
    validate_completed_reset_journal(&journal, &deploy, args.operation_id)?;
    let lease = acquire_schema_mutation_lease(&deploy.db.postgres)
        .await
        .context("acquire reset verification schema mutation lease")?;
    let verification_result = tokio::select! {
        result = preproduction_reset_verify_under_lease(&deploy) => result,
        () = lease.cancelled() => Err(anyhow::anyhow!(
            "canonical PostgreSQL schema mutation lease was lost during reset verification"
        )),
    };
    let active_result = lease.ensure_active().map_err(Error::from);
    let release_result = release_schema_mutation_lease(lease)
        .await
        .context("release reset verification schema mutation lease");
    match (verification_result, active_result, release_result) {
        (Err(error), _, _) | (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(()), Ok(())) => {
            println!("verified_operation_id={}", journal.operation_id);
            Ok(())
        }
    }
}

async fn preproduction_reset_verify_under_lease(deploy: &DeployConfig) -> Result<()> {
    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .context("connect configured PostgreSQL identity")?;
    let postgres_status = verify_postgres_schema(postgres.connection())
        .await
        .context("verify PostgreSQL boot manifest")?;
    let clickhouse_status = verify_schema(&deploy.db.clickhouse)
        .await
        .context("verify ClickHouse boot manifest")?;
    verify_clean_bootstrap_facts(&postgres, &deploy.db.clickhouse).await?;
    let redis_keys = count_preproduction_namespace(&deploy.cache.redis)
        .await
        .context("count Redis qp:* namespace")?;
    if redis_keys != 0 {
        bail!("Redis qp:* namespace contains {redis_keys} keys");
    }
    let repository = PgPolicyRepository::new(postgres.connection().clone());
    if repository.load_current_bundle().await?.is_none() {
        bail!("PostgreSQL boot schema has no active six-resource policy bundle");
    }
    println!(
        "preproduction reset verified: pg_version={} ch_version={} redis_qp_keys=0",
        postgres_status.current_version, clickhouse_status.current_version
    );
    postgres.close().await;
    Ok(())
}

async fn verify_clean_bootstrap_facts(
    postgres: &PostgresPool,
    clickhouse_config: &ClickHouseConfig,
) -> Result<()> {
    let clickhouse = ClickHousePool::connect(clickhouse_config)
        .await
        .context("connect clean-bootstrap ClickHouse target")?;
    let ledger_rows = clickhouse
        .client()
        .query("SELECT count() FROM quant_book_l2_ledger")
        .fetch_one::<u64>()
        .await
        .context("count clean-bootstrap L2 ledger")?;
    if ledger_rows != 0 {
        bail!("clean bootstrap unexpectedly contains {ledger_rows} L2 ledger rows");
    }

    let evidence_row = postgres
        .connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT (SELECT COUNT(*) FROM quant_source_slice) + \
                    (SELECT COUNT(*) FROM quant_training_dataset) + \
                    (SELECT COUNT(*) FROM quant_backtest_path_set) + \
                    (SELECT COUNT(*) FROM quant_backtest_report) + \
                    (SELECT COUNT(*) FROM quant_feature_parity_run) + \
                    (SELECT COUNT(*) FROM quant_trade_policy_validation) + \
                    (SELECT COUNT(*) FROM quant_research_readiness_evidence) \
                    AS l2_dependent_evidence_count",
        ))
        .await
        .context("count clean-bootstrap L2-dependent evidence")?
        .context("PostgreSQL returned no clean-bootstrap evidence count")?;
    let evidence_rows = evidence_row
        .try_get::<i64>("", "l2_dependent_evidence_count")
        .context("decode clean-bootstrap evidence count")?;
    if evidence_rows != 0 {
        bail!("clean bootstrap unexpectedly contains {evidence_rows} L2-dependent evidence rows");
    }
    Ok(())
}

fn load_reset_deploy(args: &ConfigDirArgs) -> Result<DeployConfig> {
    let config_dir = args
        .config_dir
        .to_str()
        .context("preproduction reset config directory is not valid UTF-8")?;
    let deploy =
        DeployConfig::load_for_migration(config_dir).context("load reset deploy config")?;
    if !deploy.deployment.permits_destructive_reset() {
        bail!("destructive fresh-boot reset is forbidden in the production environment");
    }
    Ok(deploy)
}

async fn reset_inventory(deploy: &DeployConfig) -> Result<ResetObjectInventory> {
    let (
        postgres,
        clickhouse_object_count,
        clickhouse_active_query_count,
        redis_namespace_key_count,
    ) = tokio::try_join!(
        async {
            inspect_preproduction_postgres(&deploy.db.postgres)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            database_object_count(&deploy.db.clickhouse)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            active_preproduction_query_count(&deploy.db.clickhouse)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            count_preproduction_namespace(&deploy.cache.redis)
                .await
                .map_err(anyhow::Error::from)
        },
    )?;
    Ok(ResetObjectInventory {
        postgres_database_exists: postgres.database_exists,
        postgres_object_count: postgres.object_count,
        postgres_connection_count: postgres.connection_count,
        clickhouse_object_count,
        clickhouse_active_query_count,
        redis_namespace_key_count,
    })
}

fn reset_target_fingerprints(deploy: &DeployConfig) -> Result<ResetTargetFingerprints> {
    Ok(ResetTargetFingerprints {
        postgres: CanonicalDigest::content_hash_json(&(
            deploy.db.postgres.host.as_str(),
            deploy.db.postgres.port,
            deploy.db.postgres.database.as_str(),
            deploy.db.postgres.schema.as_str(),
            deploy.db.postgres.user.as_str(),
        ))?,
        clickhouse: CanonicalDigest::content_hash_json(&(
            deploy.db.clickhouse.url.as_str(),
            deploy.db.clickhouse.database.as_str(),
            deploy.db.clickhouse.user.as_str(),
            deploy.db.clickhouse.deployment_id.as_str(),
        ))?,
        redis: CanonicalDigest::content_hash_json(&(
            deploy.cache.redis.host.as_str(),
            deploy.cache.redis.port,
            deploy.cache.redis.database,
            deploy.cache.redis.user.as_str(),
            deploy.cache.redis.key_prefix.as_str(),
        ))?,
    })
}

fn reset_journal_hash(journal: &PreproductionResetJournal) -> Result<ContentHash> {
    CanonicalDigest::content_hash_json(&ResetJournalDigest {
        format_version: journal.format_version,
        operation_id: journal.operation_id,
        nonce: &journal.nonce,
        stage: journal.stage,
        created_at: journal.created_at,
        updated_at: journal.updated_at,
        expires_at: journal.expires_at,
        completed_at: journal.completed_at,
        targets: &journal.targets,
        inventory: &journal.inventory,
        failure: &journal.failure,
    })
    .context("hash preproduction reset journal")
}

fn validate_reset_journal_integrity(
    journal: &PreproductionResetJournal,
    deploy: &DeployConfig,
) -> Result<()> {
    if journal.format_version != RESET_JOURNAL_FORMAT_VERSION
        || journal.targets != reset_target_fingerprints(deploy)?
        || journal.journal_hash != reset_journal_hash(journal)?
    {
        bail!("reset journal is tampered or targets another endpoint");
    }
    Ok(())
}

fn validate_reset_journal_for_apply(
    journal: &PreproductionResetJournal,
    deploy: &DeployConfig,
    confirm_nonce: &PreproductionResetNonce,
    confirmation: &str,
) -> Result<()> {
    validate_reset_journal_integrity(journal, deploy)?;
    if confirmation != CLEAN_BOOTSTRAP_CONFIRMATION {
        bail!(
            "destructive clean bootstrap requires exact --confirm {CLEAN_BOOTSTRAP_CONFIRMATION}"
        );
    }
    if journal.stage != ResetStage::Planned
        || &journal.nonce != confirm_nonce
        || journal.expires_at <= Utc::now()
        || journal.completed_at.is_some()
        || journal.failure.is_some()
    {
        bail!("reset journal is not an unexpired planned operation or nonce mismatched");
    }
    Ok(())
}

fn validate_completed_reset_journal(
    journal: &PreproductionResetJournal,
    deploy: &DeployConfig,
    operation_id: Uuid,
) -> Result<()> {
    validate_reset_journal_integrity(journal, deploy)?;
    if journal.operation_id != operation_id
        || journal.stage != ResetStage::Completed
        || journal.completed_at.is_none()
        || journal.failure.is_some()
    {
        bail!("reset verify requires the exact completed --operation-id");
    }
    Ok(())
}

fn read_reset_journal(path: &Path) -> Result<PreproductionResetJournal> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect reset journal {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("reset journal must be a regular non-symlink file");
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!("reset journal permissions must be 0600");
    }
    let bytes = fs::read(path).with_context(|| format!("read reset journal {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse reset journal {}", path.display()))
}

fn transition_reset_journal(
    path: &Path,
    journal: &mut PreproductionResetJournal,
    stage: ResetStage,
) -> Result<()> {
    journal.stage = stage;
    journal.updated_at = Utc::now();
    if stage == ResetStage::Completed {
        journal.completed_at = Some(journal.updated_at);
    }
    journal.journal_hash = reset_journal_hash(journal)?;
    write_private_json_atomic(path, journal)
}

fn mark_reset_journal_failed(path: &Path, journal: &mut PreproductionResetJournal) -> Result<()> {
    let failed_at = Utc::now();
    let failed_stage = journal.stage;
    journal.stage = ResetStage::Failed;
    journal.updated_at = failed_at;
    journal.failure = Some(ResetFailure {
        failed_stage,
        failed_at,
        summary: "reset operation failed; inspect the command diagnostics before planning a new operation"
            .to_owned(),
    });
    journal.journal_hash = reset_journal_hash(journal)?;
    write_private_json_atomic(path, journal)
}

fn archive_reset_journal(path: &Path, journal: &PreproductionResetJournal) -> Result<PathBuf> {
    let archived = path.with_file_name(format!(
        "operation-{}-{}.json",
        journal.operation_id,
        journal.stage.as_str()
    ));
    if archived.exists() {
        bail!(
            "reset journal archive already exists at {}",
            archived.display()
        );
    }
    fs::rename(path, &archived).with_context(|| {
        format!(
            "archive reset journal {} to {}",
            path.display(),
            archived.display()
        )
    })?;
    sync_parent_directory(path)?;
    Ok(archived)
}

fn write_private_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("reset journal path has no parent")?;
    fs::create_dir_all(parent).context("create reset journal directory")?;
    let parent_metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("inspect reset journal directory {}", parent.display()))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        bail!("reset journal parent must be a regular non-symlink directory");
    }
    fs::set_permissions(parent, Permissions::from_mode(0o700))
        .with_context(|| format!("restrict reset journal directory {}", parent.display()))?;
    if let Ok(metadata) = fs::symlink_metadata(path)
        && (metadata.file_type().is_symlink() || !metadata.is_file())
    {
        bail!("reset journal target must be a regular non-symlink file");
    }

    let mut rendered = serde_json::to_vec_pretty(value).context("render reset journal")?;
    rendered.push(b'\n');
    let temp_path = parent.join(format!(".reset-journal-{}.tmp", Uuid::now_v7()));
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temp_path)
            .with_context(|| format!("create reset journal temp file {}", temp_path.display()))?;
        file.write_all(&rendered)
            .with_context(|| format!("write reset journal temp file {}", temp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("fsync reset journal temp file {}", temp_path.display()))?;
        fs::rename(&temp_path, path).with_context(|| {
            format!(
                "atomically replace reset journal {} with {}",
                path.display(),
                temp_path.display()
            )
        })?;
        sync_parent_directory(path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result?;
    Ok(())
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path.parent().context("reset journal path has no parent")?;
    File::open(parent)
        .with_context(|| format!("open reset journal directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("fsync reset journal directory {}", parent.display()))
}

async fn postgres_schema(command: PostgresSchemaCommand) -> Result<()> {
    let args = match &command {
        PostgresSchemaCommand::Plan(args)
        | PostgresSchemaCommand::Apply(args)
        | PostgresSchemaCommand::Verify(args)
        | PostgresSchemaCommand::Manifest(args) => Some(args),
        PostgresSchemaCommand::ManifestClean | PostgresSchemaCommand::MigrationManifest => None,
    };
    let Some(args) = args else {
        return match command {
            PostgresSchemaCommand::ManifestClean => generate_clean_postgres_manifest().await,
            PostgresSchemaCommand::MigrationManifest => {
                write_postgres_migration_manifest()?;
                Ok(())
            }
            _ => unreachable!("commands carrying config arguments were handled above"),
        };
    };
    let config_dir = args
        .config_dir
        .to_str()
        .context("PostgreSQL schema config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load_for_migration(config_dir).context("load deploy config")?;
    match command {
        PostgresSchemaCommand::Plan(_) => {
            let pool = PostgresPool::connect_existing(&deploy.db.postgres)
                .await
                .context("connect PostgreSQL for migration plan")?;
            let plan = plan_postgres_migrations(pool.connection())
                .await
                .context("plan PostgreSQL schema")?;
            println!("migration_ledger_exists={}", plan.migration_ledger_exists);
            println!("applied_versions={:?}", plan.applied_versions);
            for migration in plan.pending_migrations {
                println!(
                    "pending_migration version={} checksum={}",
                    migration.version, migration.checksum
                );
            }
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Apply(_) => apply_postgres_schema(&deploy).await,
        PostgresSchemaCommand::Verify(_) => {
            let pool = PostgresPool::connect_existing(&deploy.db.postgres)
                .await
                .context("connect configured PostgreSQL identity")?;
            let lease = acquire_schema_mutation_lease(&deploy.db.postgres)
                .await
                .context("acquire PostgreSQL verification schema mutation lease")?;
            let verification_result = tokio::select! {
                result = verify_postgres_schema(pool.connection()) => {
                    result.context("verify PostgreSQL schema")
                }
                () = lease.cancelled() => Err(anyhow::anyhow!(
                    "canonical PostgreSQL schema mutation lease was lost during schema verification"
                )),
            };
            let active_result = lease.ensure_active().map_err(Error::from);
            let release_result = release_schema_mutation_lease(lease)
                .await
                .context("release PostgreSQL verification schema mutation lease");
            let status = match (verification_result, active_result, release_result) {
                (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => {
                    return Err(error);
                }
                (Ok(status), Ok(()), Ok(())) => status,
            };
            println!(
                "PostgreSQL schema verified: version={}, migrations={}, tables={}, indexes={}",
                status.current_version,
                status.migration_count,
                status.required_table_count,
                status.required_index_count
            );
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Manifest(_) => {
            let pool = PostgresPool::connect_existing(&deploy.db.postgres)
                .await
                .context("connect configured PostgreSQL identity")?;
            let manifest = inspect_schema_manifest(pool.connection())
                .await
                .context("inspect PostgreSQL semantic manifest")?;
            let rendered =
                render_schema_manifest(&manifest).context("render PostgreSQL semantic manifest")?;
            let path = workspace_root()?
                .join("schema")
                .join("postgres")
                .join("manifest.json");
            fs::create_dir_all(path.parent().context("manifest path has no parent")?)
                .context("create PostgreSQL manifest directory")?;
            fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
            let migration_path = write_postgres_migration_manifest()?;
            println!("generated {}", path.display());
            println!("generated {}", migration_path.display());
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::ManifestClean | PostgresSchemaCommand::MigrationManifest => Ok(()),
    }
}

async fn apply_postgres_schema(deploy: &DeployConfig) -> Result<()> {
    let bootstrap_admin_password_hash = bootstrap_admin_password_hash()?;
    let pool = PostgresPool::connect_schema(&deploy.db.postgres)
        .await
        .context("connect PostgreSQL schema identity")?;
    let lease = acquire_schema_mutation_lease(&deploy.db.postgres)
        .await
        .context("acquire PostgreSQL deployment schema mutation lease")?;
    let deployment_result = tokio::select! {
        result = async {
            apply_under_schema_mutation_lease(pool.connection(), &lease)
                .await
                .context("apply audited SeaORM PostgreSQL migrations")?;
            let status =
                finalize_schema_deployment(pool.connection(), &bootstrap_admin_password_hash)
                    .await
                    .context("finalize PostgreSQL schema deployment")?;
            let policy_bundle = ensure_postgres_boot_domain_facts(
                pool.connection(),
                "canonical fresh PostgreSQL deployment",
            )
            .await?;
            Ok::<_, Error>((status, policy_bundle))
        } => result,
        () = lease.cancelled() => Err(anyhow::anyhow!(
            "canonical PostgreSQL schema mutation lease was lost during schema deployment"
        )),
    };
    let active_result = lease.ensure_active().map_err(Error::from);
    let release_result = release_schema_mutation_lease(lease)
        .await
        .context("release PostgreSQL deployment schema mutation lease");
    let (status, policy_bundle) = match (deployment_result, active_result, release_result) {
        (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => {
            return Err(error);
        }
        (Ok(deployment), Ok(()), Ok(())) => deployment,
    };
    println!(
        "PostgreSQL schema deployed: version={}, migrations={}, tables={}, indexes={}",
        status.current_version,
        status.migration_count,
        status.required_table_count,
        status.required_index_count
    );
    println!(
        "Policy bundle deployed: generation={}, snapshot_id={}, snapshot_hash={}",
        policy_bundle.generation,
        policy_bundle.decision_policy_snapshot_id,
        policy_bundle.snapshot_hash
    );
    pool.close().await;
    Ok(())
}

async fn ensure_postgres_boot_domain_facts(
    db: &DatabaseConnection,
    reason: &str,
) -> Result<ActivePolicyBundle> {
    PgModelRegistryRepository::new(db.clone())
        .ensure_builtin_research_profiles()
        .await
        .context("seed immutable research profile artifacts")?;
    ensure_default_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        "quant-pivot-xtask",
        reason,
    )
    .await
    .context("seed canonical six-resource policy bundle")
}

async fn generate_clean_postgres_manifest() -> Result<()> {
    const DATABASE: &str = "quant_pivot_manifest_codegen";

    let password = Zeroizing::new(Uuid::now_v7().to_string());
    let container = Postgres::default()
        .with_db_name(DATABASE)
        .with_user("postgres")
        .with_password(password.as_str())
        .with_tag("16")
        .start()
        .await
        .context("start disposable PostgreSQL manifest container")?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .context("resolve disposable PostgreSQL manifest port")?;
    let config = PostgresConfig {
        port,
        user: "postgres".to_owned(),
        password: password.as_str().into(),
        database: DATABASE.to_owned(),
        min_connections: 1,
        max_connections: 2,
        verify_session_params: false,
        application_name: "quant-pivot-manifest-codegen".to_owned(),
        ..PostgresConfig::default()
    };

    let pool = PostgresPool::connect(&config)
        .await
        .context("connect disposable PostgreSQL manifest database")?;
    let bootstrap_password = Zeroizing::new(Uuid::now_v7().to_string());
    let bootstrap_password_hash =
        hash_password(bootstrap_password.as_str()).context("hash disposable bootstrap password")?;
    let lease = acquire_schema_mutation_lease(&config)
        .await
        .context("acquire disposable manifest schema mutation lease")?;
    let manifest_result = tokio::select! {
        result = async {
            apply_under_schema_mutation_lease(pool.connection(), &lease)
                .await
                .context("apply migrations to disposable PostgreSQL manifest database")?;
            generate_disposable_schema_manifest(pool.connection(), &bootstrap_password_hash)
                .await
                .context("generate disposable PostgreSQL semantic manifest")
        } => result,
        () = lease.cancelled() => Err(anyhow::anyhow!(
            "canonical PostgreSQL schema mutation lease was lost during manifest generation"
        )),
    };
    let active_result = lease.ensure_active().map_err(Error::from);
    let release_result = release_schema_mutation_lease(lease)
        .await
        .context("release disposable manifest schema mutation lease");
    let manifest = match (manifest_result, active_result, release_result) {
        (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => {
            return Err(error);
        }
        (Ok(manifest), Ok(()), Ok(())) => manifest,
    };
    let rendered = render_schema_manifest(&manifest).context("render PostgreSQL manifest")?;
    let path = workspace_root()?
        .join("schema")
        .join("postgres")
        .join("manifest.json");
    fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
    let migration_path = write_postgres_migration_manifest()?;
    pool.close().await;
    drop(container);
    println!("generated {} from disposable PostgreSQL 16", path.display());
    println!("generated {}", migration_path.display());
    Ok(())
}

fn write_postgres_migration_manifest() -> Result<PathBuf> {
    let path = workspace_root()?
        .join("schema")
        .join("postgres")
        .join("migrations.json");
    fs::create_dir_all(
        path.parent()
            .context("migration manifest path has no parent")?,
    )
    .context("create PostgreSQL migration manifest directory")?;
    let manifest =
        quant_pivot_migration::render_manifest().context("render PostgreSQL migration manifest")?;
    fs::write(&path, manifest).with_context(|| format!("write {}", path.display()))?;
    println!("generated {}", path.display());
    Ok(path)
}

async fn clickhouse_schema(command: ClickHouseSchemaCommand) -> Result<()> {
    let config_dir = match &command {
        ClickHouseSchemaCommand::Plan(args)
        | ClickHouseSchemaCommand::ApplyOnline(args)
        | ClickHouseSchemaCommand::ApplyOffline(args)
        | ClickHouseSchemaCommand::Verify(args)
        | ClickHouseSchemaCommand::Manifest(args) => &args.config_dir,
    };
    let config_dir = config_dir
        .to_str()
        .context("ClickHouse schema config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load_for_migration(config_dir).context("load deploy config")?;
    let config = &deploy.db.clickhouse;
    let mutates_schema = matches!(
        &command,
        ClickHouseSchemaCommand::ApplyOnline(_) | ClickHouseSchemaCommand::ApplyOffline(_)
    );
    let schema_mutation_lease =
        if mutates_schema || matches!(&command, ClickHouseSchemaCommand::Verify(_)) {
            Some(
                acquire_schema_mutation_lease(&deploy.db.postgres)
                    .await
                    .context("acquire cross-system schema mutation lease")?,
            )
        } else {
            None
        };
    let result = match command {
        ClickHouseSchemaCommand::Plan(_) => {
            let plan = plan_schema(config)
                .await
                .context("plan ClickHouse schema")?;
            println!("database_exists={}", plan.database_exists);
            println!("migration_ledger_exists={}", plan.migration_ledger_exists);
            println!("applied_versions={:?}", plan.applied_versions);
            if plan.pending_migrations.is_empty() {
                println!("pending_migrations=[]");
            } else {
                for migration in plan.pending_migrations {
                    println!(
                        "pending_migration version={} name={} safety={:?} checksum={}",
                        migration.version, migration.name, migration.safety, migration.checksum
                    );
                }
            }
            Ok(())
        }
        ClickHouseSchemaCommand::ApplyOnline(_) => {
            let status_result = if let Some(lease) = schema_mutation_lease.as_ref() {
                tokio::select! {
                result = apply_online_schema_migrations(config) => {
                    result.context("deploy ClickHouse schema")
                }
                () = lease.cancelled() => Err(anyhow::anyhow!(
                    "canonical PostgreSQL schema mutation lease was lost during ClickHouse migration"
                )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "ClickHouse schema mutation lease is absent"
                ))
            };
            status_result.map(|status| {
                println!(
                    "ClickHouse schema deployed: version={}, required_objects={}",
                    status.current_version, status.required_object_count
                );
            })
        }
        ClickHouseSchemaCommand::ApplyOffline(_) => {
            let status_result = if let Some(lease) = schema_mutation_lease.as_ref() {
                tokio::select! {
                result = apply_offline_schema_migrations(config) => {
                    result.context("deploy offline ClickHouse schema")
                }
                () = lease.cancelled() => Err(anyhow::anyhow!(
                    "canonical PostgreSQL schema mutation lease was lost during offline ClickHouse migration"
                )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "ClickHouse schema mutation lease is absent"
                ))
            };
            status_result.map(|status| {
                println!(
                    "ClickHouse offline schema deployed: version={}, required_objects={}",
                    status.current_version, status.required_object_count
                );
            })
        }
        ClickHouseSchemaCommand::Verify(_) => {
            let status_result = if let Some(lease) = schema_mutation_lease.as_ref() {
                tokio::select! {
                    result = verify_schema(config) => result.context("verify ClickHouse schema"),
                    () = lease.cancelled() => Err(anyhow::anyhow!(
                        "canonical PostgreSQL schema mutation lease was lost during ClickHouse verification"
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "ClickHouse verification schema mutation lease is absent"
                ))
            };
            status_result.map(|status| {
                println!(
                    "ClickHouse schema verified: version={}, required_objects={}",
                    status.current_version, status.required_object_count
                );
            })
        }
        ClickHouseSchemaCommand::Manifest(_) => {
            let rendered = render_clickhouse_schema_manifest(config)
                .await
                .context("render ClickHouse semantic schema manifest")?;
            let path = workspace_root()?
                .join("schema")
                .join("clickhouse")
                .join("manifest.json");
            fs::create_dir_all(path.parent().context("manifest path has no parent")?)
                .context("create ClickHouse manifest directory")?;
            fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
            println!("generated {}", path.display());
            Ok(())
        }
    };
    let active_result = schema_mutation_lease
        .as_ref()
        .map_or(Ok(()), |lease| lease.ensure_active().map_err(Error::from));
    let release_result = match schema_mutation_lease {
        Some(lease) => release_schema_mutation_lease(lease)
            .await
            .context("release cross-system schema mutation lease"),
        None => Ok(()),
    };
    match (result, active_result, release_result) {
        (Err(error), _, _) | (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(()), Ok(())) => Ok(()),
    }
}

fn bootstrap_admin_password_hash() -> Result<Zeroizing<String>> {
    let path = env::var_os(BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV)
        .map(PathBuf::from)
        .with_context(|| {
            format!(
                "deployment secret file `{BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV}` is required for PostgreSQL schema apply"
            )
        })?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("inspect bootstrap admin password file {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "bootstrap admin password path {} is not a regular file",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "bootstrap admin password file {} must not be accessible by group or others",
                path.display()
            );
        }
    }

    let password = fs::read_to_string(&path)
        .with_context(|| format!("read bootstrap admin password file {}", path.display()))?;
    let password = Zeroizing::new(password.trim_end_matches(['\r', '\n']).to_owned());
    let character_count = password.chars().count();
    if !(16..=256).contains(&character_count) {
        bail!("bootstrap admin password must contain 16..=256 characters");
    }
    if password.eq_ignore_ascii_case("admin") || password.as_str() == "quant-pivot" {
        bail!("bootstrap admin password is a forbidden template value");
    }
    hash_password(password.as_str())
        .map(Zeroizing::new)
        .context("hash bootstrap admin password with Argon2id")
}

fn report_capacity_ceiling(catalog_visible_p99: u64) -> Result<u64> {
    const MINIMUM: u64 = 100_000;
    const QUANTUM: u64 = 1_000;

    let doubled = catalog_visible_p99
        .checked_mul(2)
        .context("catalog-visible p99 overflow while applying the 2x runway")?;
    let raw = doubled.max(MINIMUM);
    raw.checked_add(QUANTUM - 1)
        .map(|value| value / QUANTUM * QUANTUM)
        .context("catalog-visible report capacity ceiling overflow")
}

fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates| crates.parent())
        .context("quant-pivot-xtask must live under crates/")
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use std::{env, fs, os::unix::fs::PermissionsExt};

    use chrono::{Duration, Utc};
    use quant_pivot_models::{
        config::DeployConfig, hashing::CanonicalDigest, types::PreproductionResetNonce,
    };
    use uuid::Uuid;

    use super::{
        CLEAN_BOOTSTRAP_CONFIRMATION, PreproductionResetJournal, RESET_JOURNAL_FORMAT_VERSION,
        RESET_PLAN_TTL_MINUTES, ResetObjectInventory, ResetStage, archive_reset_journal,
        mark_reset_journal_failed, read_reset_journal, report_capacity_ceiling, reset_journal_hash,
        reset_target_fingerprints, transition_reset_journal, validate_completed_reset_journal,
        validate_reset_journal_for_apply, write_private_json_atomic,
    };

    #[test]
    fn report_capacity_ceiling_enforces_floor_double_runway_and_rounding() {
        assert_eq!(report_capacity_ceiling(1).expect("ceiling"), 100_000);
        assert_eq!(report_capacity_ceiling(50_001).expect("ceiling"), 101_000);
        assert_eq!(report_capacity_ceiling(120_000).expect("ceiling"), 240_000);
    }

    #[test]
    fn reset_journal_is_private_atomic_and_operation_bound() {
        let directory = env::temp_dir().join(format!("quant-pivot-journal-{}", Uuid::now_v7()));
        let path = directory.join("active-operation.json");
        let deploy = DeployConfig::default();
        let mut journal = reset_journal_fixture(&deploy);

        write_private_json_atomic(&path, &journal).expect("write reset journal");
        assert_eq!(
            fs::metadata(&path)
                .expect("reset journal metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&directory)
                .expect("reset journal directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            read_reset_journal(&path).expect("read reset journal"),
            journal
        );
        assert!(
            validate_reset_journal_for_apply(
                &journal,
                &deploy,
                &PreproductionResetNonce::from_v7(),
                CLEAN_BOOTSTRAP_CONFIRMATION,
            )
            .is_err(),
            "nonce drift must fail closed"
        );
        let mut tampered = journal.clone();
        tampered.operation_id = Uuid::now_v7();
        assert!(
            validate_reset_journal_for_apply(
                &tampered,
                &deploy,
                &tampered.nonce,
                CLEAN_BOOTSTRAP_CONFIRMATION,
            )
            .is_err(),
            "immutable journal tampering must fail closed"
        );
        assert!(
            validate_reset_journal_for_apply(&journal, &deploy, &journal.nonce, "DELETE_ONLY_L2",)
                .is_err(),
            "an acknowledgement that understates the full reset scope must fail closed"
        );
        let mut stage_tampered = journal.clone();
        stage_tampered.stage = ResetStage::Completed;
        stage_tampered.completed_at = Some(Utc::now());
        assert!(
            validate_completed_reset_journal(
                &stage_tampered,
                &deploy,
                stage_tampered.operation_id,
            )
            .is_err(),
            "stage and completion timestamp tampering must fail closed"
        );

        transition_reset_journal(&path, &mut journal, ResetStage::Completed)
            .expect("complete reset journal");
        validate_completed_reset_journal(&journal, &deploy, journal.operation_id)
            .expect("verify exact completed operation");
        assert!(
            validate_completed_reset_journal(&journal, &deploy, Uuid::now_v7()).is_err(),
            "a different operation id must fail closed"
        );

        let archived = archive_reset_journal(&path, &journal).expect("archive reset journal");
        assert!(!path.exists());
        assert!(archived.exists());
        fs::remove_file(&archived).expect("remove archived reset journal fixture");
        fs::remove_dir(&directory).expect("remove reset journal fixture directory");
    }

    #[test]
    fn reset_journal_records_interrupted_stage_without_resuming() {
        let directory = env::temp_dir().join(format!("quant-pivot-journal-{}", Uuid::now_v7()));
        let path = directory.join("active-operation.json");
        let deploy = DeployConfig::default();
        let mut journal = reset_journal_fixture(&deploy);
        transition_reset_journal(&path, &mut journal, ResetStage::PostgresReset)
            .expect("record reset stage");
        mark_reset_journal_failed(&path, &mut journal).expect("record reset failure");

        let failure = journal.failure.as_ref().expect("reset failure metadata");
        assert_eq!(journal.stage, ResetStage::Failed);
        assert_eq!(failure.failed_stage, ResetStage::PostgresReset);
        assert!(journal.completed_at.is_none());
        assert!(validate_completed_reset_journal(&journal, &deploy, journal.operation_id).is_err());

        let archived = archive_reset_journal(&path, &journal).expect("archive failed operation");
        assert!(archived.ends_with(format!("operation-{}-failed.json", journal.operation_id)));
        fs::remove_file(&archived).expect("remove failed reset journal fixture");
        fs::remove_dir(&directory).expect("remove reset journal fixture directory");
    }

    fn reset_journal_fixture(deploy: &DeployConfig) -> PreproductionResetJournal {
        let now = Utc::now();
        let mut journal = PreproductionResetJournal {
            format_version: RESET_JOURNAL_FORMAT_VERSION,
            operation_id: Uuid::now_v7(),
            nonce: PreproductionResetNonce::from_v7(),
            stage: ResetStage::Planned,
            created_at: now,
            updated_at: now,
            expires_at: now + Duration::minutes(RESET_PLAN_TTL_MINUTES),
            completed_at: None,
            targets: reset_target_fingerprints(deploy).expect("reset target fingerprints"),
            inventory: ResetObjectInventory {
                postgres_database_exists: true,
                postgres_object_count: 1,
                postgres_connection_count: 0,
                clickhouse_object_count: 1,
                clickhouse_active_query_count: 0,
                redis_namespace_key_count: 0,
            },
            failure: None,
            journal_hash: CanonicalDigest::content_hash_json(&"pending").expect("placeholder hash"),
        };
        journal.journal_hash = reset_journal_hash(&journal).expect("reset journal hash");
        journal
    }
}
