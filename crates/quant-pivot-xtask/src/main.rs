use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use quant_pivot_core::app::phase_11_9_evidence_bootstrap::{
    Phase119EvidenceBootstrapOptions, run_phase_11_9_evidence_bootstrap,
};
use quant_pivot_migration::{
    acquire_lifecycle_lease, apply as apply_postgres_migrations, apply_under_lifecycle_lease,
    inspect_preproduction_postgres, plan as plan_postgres_migrations, release_lifecycle_lease,
    reset_preproduction_postgres,
};
use quant_pivot_models::{
    config::{
        CompiledBuildIdentity, DeployConfig, PostgresConfig,
        secret::{SecretText, SystemdCredentialRef},
    },
    domain::{
        ConfigApiContractSchema, LifecycleSchemaVerificationPort, NewProductionEvidence,
        VerifiedSchemaFingerprints,
    },
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        runtime_config::{PolicyActorKind, ProductionEvidenceKind},
    },
    hashing::CanonicalDigest,
    runtime_config::DecisionPolicySnapshot,
    security::hash_password,
    types::{
        ArtifactUri, ContentHash, EventId, PreproductionResetNonce, ProductionEvidenceId,
        domain_capability::DomainContractFamily,
        domain_classification::DomainMarketClassificationOutcome,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelRegistryRepository, PgPolicyRepository,
        catalog::{event::PgEventRepository, market::PgMarketRepository},
        governance::policy_bootstrap::ensure_default_policy_bundle,
    },
    traits::{PolicyRepository, governance::event::EventRepository, market::MarketRepository},
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, build_artifact_store},
    linkage::{
        catalog_classification::DomainCatalogClassifier,
        weather_daily_temperature::WeatherStationRegistry,
    },
};
use quant_pivot_storage::{
    cache::{count_preproduction_namespace, unlink_preproduction_namespace},
    clickhouse::{
        ClickHousePool, apply_offline_schema_migrations, apply_online_schema_migrations,
        database_object_count, plan_schema,
        render_schema_manifest as render_clickhouse_schema_manifest,
        reset_preproduction_database as reset_clickhouse_preproduction_database, verify_schema,
    },
    evidence::FileProductionEvidenceVerifier,
    postgres::{
        PostgresPool,
        migration::{
            finalize_schema_deployment, generate_disposable_schema_manifest,
            inspect_schema_manifest, render_schema_manifest,
            verify_schema as verify_postgres_schema,
        },
    },
};
use rustls::crypto::aws_lc_rs;
use sea_orm::ConnectionTrait;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
    sync::Arc,
};
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;
use zeroize::Zeroizing;

const BOOTSTRAP_ADMIN_PASSWORD_FILE_ENV: &str = "QUANT_PIVOT_BOOTSTRAP__ADMIN_PASSWORD_FILE";

const DOCKER_SUITES: &[(&str, &str)] = &[
    ("quant-pivot-storage", "migration_pg"),
    ("quant-pivot-repository", "pg_catalog_ledger"),
    ("quant-pivot-repository", "pg_feature_parity"),
    ("quant-pivot-repository", "pg_bootstrap"),
    ("quant-pivot-repository", "pg_account_capital"),
    ("quant-pivot-repository", "pg_attribution"),
    ("quant-pivot-repository", "pg_backtest_path_set"),
    ("quant-pivot-repository", "pg_basis_alert"),
    ("quant-pivot-repository", "pg_calibration_artifact"),
    ("quant-pivot-repository", "pg_entry_condition_evaluation"),
    ("quant-pivot-repository", "pg_equity_snapshot"),
    ("quant-pivot-repository", "pg_factor_revision"),
    ("quant-pivot-repository", "pg_market_selection"),
    ("quant-pivot-repository", "pg_market_linkage"),
    ("quant-pivot-repository", "pg_market_page"),
    ("quant-pivot-repository", "pg_governance"),
    ("quant-pivot-repository", "pg_rbac"),
    ("quant-pivot-repository", "pg_report_scheduler"),
    ("quant-pivot-repository", "pg_training_dataset"),
    ("quant-pivot-repository", "pg_model_registry"),
    ("quant-pivot-repository", "pg_research_job"),
    ("quant-pivot-repository", "pg_research_readiness"),
    ("quant-pivot-repository", "pg_trade_policy_trial"),
    ("quant-pivot-repository", "pg_backtest_report"),
    ("quant-pivot-repository", "pg_comparison_report"),
    ("quant-pivot-repository", "pg_domain_projection"),
    ("quant-pivot-repository", "pg_domain_source_expectation"),
    ("quant-pivot-repository", "pg_execution_submission"),
    ("quant-pivot-repository", "pg_policy_governance"),
    ("quant-pivot-repository", "pg_production_lifecycle"),
    (
        "quant-pivot-repository",
        "pg_weather_daily_temperature_projection",
    ),
    ("quant-pivot-repository", "portfolio_optimizer_meta"),
    ("quant-pivot-repository", "ch_fact_read_pit"),
    ("quant-pivot-storage", "redis_integration"),
    ("quant-pivot-storage", "clickhouse_integration"),
    ("quant-pivot-storage", "cache_tiered_integration"),
    ("quant-pivot-core", "health_checker"),
    ("quant-pivot-core", "market_selection_e2e"),
    ("quant-pivot-core", "bootstrap_registry_e2e"),
    ("quant-pivot-core", "equity_snapshot"),
    ("quant-pivot-core", "factor_plane_e2e"),
    ("quant-pivot-core", "governance_e2e"),
    ("quant-pivot-core", "model_train_backtest_e2e"),
    ("quant-pivot-core", "report_pipeline_e2e"),
    ("quant-pivot-core", "weather_linkage_group_e2e"),
    ("quant-pivot-web", "web"),
];

const NETWORK_SUITES: &[(&str, &str)] = &[
    ("quant-pivot-api", "http_gamma_wiremock"),
    ("quant-pivot-api", "http_data_api_wiremock"),
];

const UI_E2E_SERVER_TEST: &str = "ui_e2e_server_impl::serve_protected_ui_e2e";

#[derive(Parser)]
#[command(name = "quant-pivot-xtask")]
#[command(about = "Task runner for quant-pivot", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
    /// Run the default workspace tests, Docker suites, and network suites.
    #[command(name = "test-full")]
    Full,
    /// Run testcontainers-based integration tests (requires Docker daemon).
    #[command(name = "test-docker")]
    Docker,
    /// Run network-shaped API tests that are ignored by default.
    #[command(name = "test-network")]
    Network,
    /// Derive the report hard ceiling from the recent 30-day catalog-visible p99.
    #[command(name = "report-capacity-ceiling")]
    ReportCapacityCeiling {
        #[arg(long)]
        catalog_visible_p99: u64,
    },
    /// Classify every active Crypto/Weather market and write immutable evidence.
    #[command(name = "phase-11-9-classify-catalog")]
    Phase119ClassifyCatalog(ConfigDirArgs),
    /// Bootstrap durable Crypto/Weather evidence and emit a gap-audited manifest.
    #[command(name = "phase-11-9-evidence-bootstrap")]
    Phase119EvidenceBootstrap(Phase119EvidenceBootstrapArgs),
    /// Render the canonical boot policy bundle for schema and inventory tooling.
    #[command(name = "render-boot-policy")]
    RenderBootPolicy,
    /// Generate the Rust-owned Config API JSON Schema used for TypeScript codegen.
    #[command(name = "config-api-schema")]
    ConfigApiSchema {
        #[arg(long, default_value = "schema/api/config-v1.schema.json")]
        output: PathBuf,
    },
    /// Record content-addressed WORM evidence for the irreversible production seal.
    #[command(name = "production-evidence")]
    ProductionEvidence(ProductionEvidenceArgs),
    /// Plan, apply, or verify the exact preproduction clean-boot reset scope.
    #[command(name = "preproduction-reset")]
    PreproductionReset {
        #[command(subcommand)]
        command: PreproductionResetCommand,
    },
}

#[derive(Subcommand)]
enum PreproductionResetCommand {
    /// Inspect targets and create a short-lived one-time confirmation nonce.
    Plan(PreproductionResetPlanArgs),
    /// Apply the planned reset under the cross-system lifecycle lease.
    Apply(PreproductionResetApplyArgs),
    /// Verify boot manifests and prove the Redis namespace is empty.
    Verify(ConfigDirArgs),
}

#[derive(Args)]
struct PreproductionResetPlanArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(long, default_value = ".local/preproduction-reset/active-plan.json")]
    plan_file: PathBuf,
}

#[derive(Args)]
struct PreproductionResetApplyArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(long, default_value = ".local/preproduction-reset/active-plan.json")]
    plan_file: PathBuf,
    #[arg(long)]
    confirm_nonce: PreproductionResetNonce,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProductionEvidenceKindArg {
    BackupRestore,
    ProtectedConfigEndToEnd,
}

impl From<ProductionEvidenceKindArg> for ProductionEvidenceKind {
    fn from(value: ProductionEvidenceKindArg) -> Self {
        match value {
            ProductionEvidenceKindArg::BackupRestore => Self::BackupRestore,
            ProductionEvidenceKindArg::ProtectedConfigEndToEnd => Self::ProtectedConfigEndToEnd,
        }
    }
}

#[derive(Args)]
struct ProductionEvidenceArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    #[arg(long, value_enum)]
    kind: ProductionEvidenceKindArg,
    /// Completed backup/restore or protected-E2E evidence bundle to content-address.
    #[arg(long)]
    artifact_file: PathBuf,
    /// Immutable evidence store directory shared with the sealed deployment.
    #[arg(long, default_value = ".local/production-evidence")]
    evidence_dir: PathBuf,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    actor_label: String,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Phase119EvidenceCategory {
    Crypto,
    Weather,
}

#[derive(Args)]
struct Phase119EvidenceBootstrapArgs {
    #[command(flatten)]
    config: ConfigDirArgs,
    /// Comma-delimited verticals to ingest and audit.
    #[arg(long, value_delimiter = ',', default_value = "crypto,weather")]
    categories: Vec<Phase119EvidenceCategory>,
    /// Content-addressed evidence manifest directory.
    #[arg(long, default_value = ".local/phase-11-9/manifests")]
    manifest_dir: PathBuf,
    /// Optional frozen ICAO station shard; repeat or pass comma-delimited values.
    #[arg(long = "weather-station", value_delimiter = ',')]
    weather_stations: Vec<String>,
    /// Skip the authoritative Gamma keyset reconciliation for an offline rerun.
    #[arg(long)]
    skip_catalog_sync: bool,
    /// Bounded number of one-day Binance archive recovery passes.
    #[arg(long, default_value_t = 64)]
    max_crypto_cycles: u16,
    /// Maximum wait for all public Crypto live bindings to commit a live cursor.
    #[arg(long, default_value_t = 120)]
    crypto_live_timeout_secs: u64,
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
        Commands::ClickHouseSchema { command } => clickhouse_schema(command).await,
        Commands::PostgresSchema { command } => postgres_schema(command).await,
        Commands::Full => test_full(),
        Commands::Docker => test_docker(),
        Commands::Network => test_network(),
        Commands::ReportCapacityCeiling {
            catalog_visible_p99,
        } => {
            println!("{}", report_capacity_ceiling(catalog_visible_p99)?);
            Ok(())
        }
        Commands::Phase119ClassifyCatalog(args) => classify_phase_11_9_catalog(args).await,
        Commands::Phase119EvidenceBootstrap(args) => {
            Box::pin(phase_11_9_evidence_bootstrap(args)).await
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
        Commands::ProductionEvidence(args) => record_production_evidence(args).await,
        Commands::PreproductionReset { command } => Box::pin(preproduction_reset(command)).await,
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

async fn phase_11_9_evidence_bootstrap(args: Phase119EvidenceBootstrapArgs) -> Result<()> {
    let config_dir = args
        .config
        .config_dir
        .to_str()
        .context("evidence bootstrap config directory is not valid UTF-8")?;
    let deploy = Arc::new(DeployConfig::load(config_dir).context("load deploy config")?);
    let categories = args
        .categories
        .into_iter()
        .map(|category| match category {
            Phase119EvidenceCategory::Crypto => DomainFamily::Crypto,
            Phase119EvidenceCategory::Weather => DomainFamily::Weather,
        })
        .collect::<BTreeSet<_>>();
    let weather_stations = args
        .weather_stations
        .into_iter()
        .map(|station| station.trim().to_ascii_uppercase())
        .filter(|station| !station.is_empty())
        .collect::<BTreeSet<_>>();
    let manifest = Box::pin(run_phase_11_9_evidence_bootstrap(
        deploy,
        Phase119EvidenceBootstrapOptions {
            categories,
            weather_stations,
            sync_catalog: !args.skip_catalog_sync,
            max_crypto_cycles: args.max_crypto_cycles,
            crypto_live_timeout_secs: args.crypto_live_timeout_secs,
        },
    ))
    .await
    .context("run Phase 11.9 evidence bootstrap")?;
    let manifest_hash =
        CanonicalDigest::content_hash_json(&("boot_evidence_manifest_v1", &manifest))
            .context("hash Phase 11.9 evidence manifest")?;
    let bytes = serde_json::to_vec_pretty(&manifest).context("encode evidence manifest")?;
    fs::create_dir_all(&args.manifest_dir).with_context(|| {
        format!(
            "create evidence manifest directory {}",
            args.manifest_dir.display()
        )
    })?;
    let path = args
        .manifest_dir
        .join(format!("{}.json", manifest_hash.hex()));
    if path.exists() {
        let current = fs::read(&path)
            .with_context(|| format!("read existing evidence manifest {}", path.display()))?;
        if current != bytes {
            bail!(
                "content-addressed evidence manifest {} has different bytes",
                path.display()
            );
        }
    } else {
        fs::write(&path, bytes)
            .with_context(|| format!("write evidence manifest {}", path.display()))?;
    }
    println!("manifest_path={}", path.display());
    println!("manifest_hash={manifest_hash}");
    println!("passed={}", manifest.passed());
    println!("catalog_markets={}", manifest.catalog_market_count);
    println!("linkages={}", manifest.linkage_count);
    println!("source_expectations={}", manifest.source_expectation_count);
    println!("source_cursors={}", manifest.source_cursor_count);
    println!(
        "crypto_observation_rows={}",
        manifest.crypto_observation_rows
    );
    println!(
        "weather_observation_rows={}",
        manifest.weather_observation_rows
    );
    println!("weather_forecast_rows={}", manifest.weather_forecast_rows);
    if !manifest.passed() {
        bail!(
            "Phase 11.9 evidence gap audit failed: {}",
            manifest.blockers.join(" | ")
        );
    }
    Ok(())
}

async fn classify_phase_11_9_catalog(args: ConfigDirArgs) -> Result<()> {
    let config_dir = args
        .config_dir
        .to_str()
        .context("catalog classification config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load deploy config")?;
    let pool = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .context("connect PostgreSQL for catalog classification")?;
    let market_repo = PgMarketRepository::new(pool.connection().clone());
    let event_repo = PgEventRepository::new(pool.connection().clone());
    let markets = market_repo
        .find_active()
        .await
        .context("load active market catalog")?;
    let event_ids: Vec<EventId> = markets
        .iter()
        .filter(|market| {
            let categories = market.category_set();
            categories.contains(MarketCategory::Crypto)
                || categories.contains(MarketCategory::Weather)
        })
        .map(|market| market.event_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let events: BTreeMap<_, _> = event_repo
        .find_by_ids(&event_ids)
        .await
        .context("load domain events")?
        .into_iter()
        .map(|event| (event.event_id.clone(), event))
        .collect();
    let stations = WeatherStationRegistry::try_new(deploy.domain_sources.weather_stations.clone())
        .context("build Weather station registry")?;
    let classifier =
        DomainCatalogClassifier::new(stations, &deploy.domain_sources.weather_vertical_bindings)
            .context("build catalog classifier")?;
    let artifact = classifier
        .classify_catalog(&markets, &events)
        .context("classify active Crypto/Weather catalog")?;
    let bytes = serde_json::to_vec_pretty(&artifact).context("encode classification artifact")?;
    let store = build_artifact_store(&deploy.research.artifact_store)
        .context("build classification artifact store")?;
    let key = ArtifactKey::new(
        ArtifactNamespace::CapabilityAudit,
        artifact.artifact_hash.hex(),
        "json",
    )
    .context("build classification artifact key")?;
    let uri = store
        .put(key, &bytes)
        .await
        .context("write classification artifact")?;
    let persisted = store
        .get(&uri)
        .await
        .context("read classification artifact back")?;
    if persisted != bytes {
        bail!("catalog classification artifact read-after-write mismatch");
    }

    let mut family_counts = BTreeMap::new();
    let mut contract_counts = BTreeMap::new();
    let mut outcome_counts = BTreeMap::new();
    for row in &artifact.classifications {
        *family_counts
            .entry(family_label(row.family))
            .or_insert(0_usize) += 1;
        if let Some(contract_family) = row.contract_family {
            *contract_counts
                .entry(contract_family_label(contract_family))
                .or_insert(0_usize) += 1;
        }
        *outcome_counts
            .entry(outcome_label(row.outcome))
            .or_insert(0_usize) += 1;
    }
    println!("artifact_uri={uri}");
    println!("artifact_hash={}", artifact.artifact_hash);
    println!("catalog_hash={}", artifact.catalog_hash);
    println!("markets={}", artifact.classifications.len());
    for (family, count) in family_counts {
        println!("family.{family}={count}");
    }
    for (contract_family, count) in contract_counts {
        println!("contract_family.{contract_family}={count}");
    }
    for (outcome, count) in outcome_counts {
        println!("outcome.{outcome}={count}");
    }
    pool.close().await;
    Ok(())
}

const fn family_label(family: DomainFamily) -> &'static str {
    match family {
        DomainFamily::Crypto => "crypto",
        DomainFamily::Weather => "weather",
    }
}

const fn contract_family_label(family: DomainContractFamily) -> &'static str {
    match family {
        DomainContractFamily::CryptoDirection => "crypto_direction",
        DomainContractFamily::CryptoThreshold => "crypto_threshold",
        DomainContractFamily::CryptoBand => "crypto_band",
        DomainContractFamily::WeatherDailyTemperature => "weather_daily_temperature",
        DomainContractFamily::WeatherPrecipitation => "weather_precipitation",
        DomainContractFamily::WeatherAqi => "weather_aqi",
        DomainContractFamily::WeatherTornado => "weather_tornado",
        DomainContractFamily::WeatherTropicalCyclone => "weather_tropical_cyclone",
        DomainContractFamily::WeatherGlobalTemperature => "weather_global_temperature",
        DomainContractFamily::WeatherSeaIce => "weather_sea_ice",
        DomainContractFamily::WeatherWindExtreme => "weather_wind_extreme",
    }
}

const fn outcome_label(outcome: DomainMarketClassificationOutcome) -> &'static str {
    match outcome {
        DomainMarketClassificationOutcome::Supported => "supported",
        DomainMarketClassificationOutcome::CredentialBlocked { .. } => "credential_blocked",
        DomainMarketClassificationOutcome::InsufficientEvidence { .. } => "insufficient_evidence",
        DomainMarketClassificationOutcome::Excluded { .. } => "excluded",
        DomainMarketClassificationOutcome::UnsupportedTemplate { .. } => "unsupported_template",
    }
}

struct XtaskLiveSchemaVerification<'a> {
    postgres: &'a PostgresPool,
    clickhouse: &'a ClickHousePool,
}

#[async_trait]
impl LifecycleSchemaVerificationPort for XtaskLiveSchemaVerification<'_> {
    async fn verify_live(&self) -> quant_pivot_error::QuantResult<VerifiedSchemaFingerprints> {
        let (postgres, clickhouse) = tokio::try_join!(
            verify_postgres_schema(self.postgres.connection()),
            self.clickhouse.verify_schema(),
        )?;
        Ok(VerifiedSchemaFingerprints {
            postgres_schema_fingerprint: postgres.schema_fingerprint,
            clickhouse_schema_fingerprint: clickhouse.schema_fingerprint,
        })
    }
}

async fn record_production_evidence(args: ProductionEvidenceArgs) -> Result<()> {
    if args.reason.trim().is_empty() || args.actor_label.trim().is_empty() {
        bail!("production evidence requires non-empty --reason and --actor-label");
    }
    let config_dir = args
        .config
        .config_dir
        .to_str()
        .context("production evidence config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load runtime deploy config")?;
    let build_identity =
        CompiledBuildIdentity::compiled().context("load compiled build identity")?;
    if !build_identity.clean {
        bail!("production evidence can only be recorded by a clean compiled Git artifact");
    }

    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .context("connect PostgreSQL runtime identity")?;
    let clickhouse = ClickHousePool::connect(&deploy.db.clickhouse)
        .await
        .context("connect ClickHouse runtime identity")?;
    let schema_verification = XtaskLiveSchemaVerification {
        postgres: &postgres,
        clickhouse: &clickhouse,
    };
    let live_schema = schema_verification
        .verify_live()
        .await
        .context("verify live PG/CH schema")?;
    let repository = PgPolicyRepository::new(postgres.connection().clone());
    let bundle = repository
        .load_current_bundle()
        .await
        .context("load DB-authoritative active policy bundle")?
        .context("cannot record production evidence without an active policy bundle")?;
    let (artifact_uri, evidence_hash) =
        persist_evidence_artifact(&args.artifact_file, &args.evidence_dir)?;
    let recorded = repository
        .record_production_evidence(
            NewProductionEvidence {
                production_evidence_id: ProductionEvidenceId::from_v7(),
                kind: args.kind.into(),
                artifact_uri,
                evidence_hash,
                build_commit: build_identity.build_commit,
                postgres_schema_fingerprint: live_schema.postgres_schema_fingerprint,
                clickhouse_schema_fingerprint: live_schema.clickhouse_schema_fingerprint,
                policy_bundle_generation: bundle.generation,
                decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
                policy_bundle_hash: bundle.snapshot_hash,
                recorded_by_kind: PolicyActorKind::Operator,
                recorded_by_user_id: None,
                recorded_by_label: args.actor_label,
                reason: args.reason,
                observed_at: Utc::now(),
            },
            &schema_verification,
            &FileProductionEvidenceVerifier,
        )
        .await
        .context("record exact production evidence under lifecycle lease")?;
    println!(
        "production evidence recorded: id={} kind={} hash={} artifact={}",
        recorded.production_evidence_id,
        recorded.kind.as_str(),
        recorded.evidence_hash,
        recorded.artifact_uri,
    );
    postgres.close().await;
    Ok(())
}

fn persist_evidence_artifact(
    source: &Path,
    evidence_dir: &Path,
) -> Result<(ArtifactUri, ContentHash)> {
    let source_metadata = fs::symlink_metadata(source)
        .with_context(|| format!("inspect evidence source {}", source.display()))?;
    if !source_metadata.is_file() || source_metadata.file_type().is_symlink() {
        bail!("evidence source must be a regular non-symlink file");
    }
    let source_hash = hash_file(source)?;
    fs::create_dir_all(evidence_dir)
        .with_context(|| format!("create evidence directory {}", evidence_dir.display()))?;
    let destination = evidence_dir.join(format!(
        "{}.evidence",
        source_hash
            .as_str()
            .strip_prefix("blake3:")
            .context("content hash has no blake3 prefix")?
    ));
    if destination.exists() {
        if hash_file(&destination)? != source_hash {
            bail!(
                "existing content-addressed evidence artifact {} has the wrong hash",
                destination.display()
            );
        }
    } else {
        fs::copy(source, &destination).with_context(|| {
            format!(
                "copy evidence {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        if hash_file(&destination)? != source_hash {
            fs::remove_file(&destination)
                .with_context(|| format!("remove torn evidence copy {}", destination.display()))?;
            bail!("evidence source changed while it was being content-addressed");
        }
    }
    fs::set_permissions(&destination, fs::Permissions::from_mode(0o444))
        .with_context(|| format!("seal evidence artifact {} read-only", destination.display()))?;
    let canonical = fs::canonicalize(&destination)
        .with_context(|| format!("canonicalize evidence artifact {}", destination.display()))?;
    let uri = url::Url::from_file_path(&canonical)
        .map_err(|()| anyhow::anyhow!("evidence artifact path cannot be represented as file://"))?;
    let artifact_uri = ArtifactUri::parse(uri.to_string()).context("validate evidence URI")?;
    Ok((artifact_uri, source_hash))
}

fn hash_file(path: &Path) -> Result<ContentHash> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    ContentHash::parse(format!("blake3:{}", hasher.finalize().to_hex()))
        .context("validate computed evidence content hash")
}

const RESET_PLAN_FORMAT_VERSION: u32 = 1;
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
    redis_namespace_key_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PreproductionResetPlan {
    format_version: u32,
    nonce: PreproductionResetNonce,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    targets: ResetTargetFingerprints,
    inventory: ResetObjectInventory,
    plan_hash: ContentHash,
}

#[derive(Serialize)]
struct ResetPlanDigest<'a> {
    format_version: u32,
    nonce: &'a PreproductionResetNonce,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    targets: &'a ResetTargetFingerprints,
    inventory: &'a ResetObjectInventory,
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
    if args.plan_file.exists() {
        let existing = read_reset_plan(&args.plan_file)?;
        if existing.expires_at > Utc::now() {
            bail!(
                "an unexpired reset plan already exists at {}; use its nonce or wait for expiry",
                args.plan_file.display()
            );
        }
        let expired = args
            .plan_file
            .with_file_name(format!("expired-{}.json", existing.nonce));
        fs::rename(&args.plan_file, &expired).with_context(|| {
            format!(
                "archive expired reset plan {} to {}",
                args.plan_file.display(),
                expired.display()
            )
        })?;
    }
    let now = Utc::now();
    let mut plan = PreproductionResetPlan {
        format_version: RESET_PLAN_FORMAT_VERSION,
        nonce: PreproductionResetNonce::from_v7(),
        created_at: now,
        expires_at: now + Duration::minutes(RESET_PLAN_TTL_MINUTES),
        targets: reset_target_fingerprints(&deploy)?,
        inventory,
        plan_hash: CanonicalDigest::content_hash_json(&"pending")?,
    };
    plan.plan_hash = reset_plan_hash(&plan)?;
    write_private_json(&args.plan_file, &plan)?;
    println!("preproduction reset plan created");
    println!("plan_file={}", args.plan_file.display());
    println!("confirmation_nonce={}", plan.nonce);
    println!("expires_at={}", plan.expires_at.to_rfc3339());
    println!("postgres_endpoint_fingerprint={}", plan.targets.postgres);
    println!(
        "clickhouse_endpoint_fingerprint={}",
        plan.targets.clickhouse
    );
    println!("redis_endpoint_fingerprint={}", plan.targets.redis);
    println!("postgres_objects={}", plan.inventory.postgres_object_count);
    println!(
        "clickhouse_objects={}",
        plan.inventory.clickhouse_object_count
    );
    println!(
        "redis_qp_namespace_keys={}",
        plan.inventory.redis_namespace_key_count
    );
    Ok(())
}

async fn preproduction_reset_apply(args: PreproductionResetApplyArgs) -> Result<()> {
    let deploy = load_reset_deploy(&args.config)?;
    let plan = read_reset_plan(&args.plan_file)?;
    validate_reset_plan(&plan, &deploy, &args.confirm_nonce)?;
    let current_inventory = reset_inventory(&deploy).await?;
    if current_inventory != plan.inventory {
        bail!("reset target inventory changed after planning; create a new reset plan");
    }
    if current_inventory.postgres_connection_count != 0 {
        bail!("PostgreSQL target still has active connections");
    }

    let postgres_password = migration_password(
        deploy.db.postgres.migration.password.as_ref(),
        "db.postgres.migration.password",
    )?;
    let mut maintenance_config = deploy.db.postgres.migration_connection(postgres_password);
    "postgres".clone_into(&mut maintenance_config.database);
    let maintenance = PostgresPool::connect_existing(&maintenance_config)
        .await
        .context("connect PostgreSQL maintenance database for reset lease")?;
    let lease = acquire_lifecycle_lease(maintenance.connection())
        .await
        .context("acquire reset lifecycle lease")?;
    let locked_inventory = reset_inventory(&deploy).await?;
    if locked_inventory != plan.inventory || locked_inventory.postgres_connection_count != 0 {
        release_lifecycle_lease(lease)
            .await
            .context("release reset lifecycle lease after stale plan")?;
        bail!("reset target changed while acquiring the lifecycle lease; create a new plan");
    }

    let in_progress_path = args
        .plan_file
        .with_file_name(format!("in-progress-{}.json", plan.nonce));
    fs::rename(&args.plan_file, &in_progress_path).with_context(|| {
        format!(
            "consume one-time reset plan {} as {}",
            args.plan_file.display(),
            in_progress_path.display()
        )
    })?;
    let consumed_plan = read_reset_plan(&in_progress_path)?;
    if consumed_plan != plan {
        release_lifecycle_lease(lease)
            .await
            .context("release reset lifecycle lease after plan mismatch")?;
        bail!("reset plan changed before one-time consumption");
    }

    let mutation_result = async {
        reset_preproduction_postgres(&deploy.db.postgres, postgres_password)
            .await
            .context("recreate exact PostgreSQL quant_pivot database")?;
        let clickhouse_password = migration_password(
            deploy.db.clickhouse.migration.password.as_ref(),
            "db.clickhouse.migration.password",
        )?;
        let clickhouse_config = deploy
            .db
            .clickhouse
            .migration_connection(clickhouse_password);
        reset_clickhouse_preproduction_database(&clickhouse_config)
            .await
            .context("recreate exact ClickHouse quant_pivot database")?;
        let deleted = unlink_preproduction_namespace(&deploy.cache.redis)
            .await
            .context("unlink exact Redis qp:* namespace")?;

        let postgres = PostgresPool::connect_migration(&deploy.db.postgres, postgres_password)
            .await
            .context("connect recreated PostgreSQL database")?;
        apply_under_lifecycle_lease(postgres.connection(), &lease)
            .await
            .context("apply unique PostgreSQL boot migration")?;
        let bootstrap_admin_password_hash = bootstrap_admin_password_hash()?;
        finalize_schema_deployment(
            postgres.connection(),
            &deploy.db.postgres.user,
            &bootstrap_admin_password_hash,
        )
        .await
        .context("finalize recreated PostgreSQL schema")?;
        PgModelRegistryRepository::new(postgres.connection().clone())
            .ensure_builtin_research_profiles()
            .await
            .context("seed immutable research profile artifacts")?;
        let policy_repository = PgPolicyRepository::new(postgres.connection().clone());
        let policy_bundle = ensure_default_policy_bundle(
            &policy_repository,
            "quant-pivot-xtask",
            "guarded preproduction clean boot",
        )
        .await
        .context("seed canonical six-resource policy bundle")?;
        let postgres_status = verify_postgres_schema(postgres.connection())
            .await
            .context("verify recreated PostgreSQL schema")?;
        let clickhouse_status = apply_online_schema_migrations(&clickhouse_config)
            .await
            .context("apply unique ClickHouse boot migration")?;
        let redis_remaining = count_preproduction_namespace(&deploy.cache.redis)
            .await
            .context("verify Redis qp:* namespace")?;
        if redis_remaining != 0 {
            bail!("Redis qp:* namespace is not empty after reset");
        }
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
    }
    .await;
    let release_result = release_lifecycle_lease(lease)
        .await
        .context("release reset lifecycle lease");
    match (mutation_result, release_result) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => {
            let completed_path =
                in_progress_path.with_file_name(format!("completed-{}.json", plan.nonce));
            fs::rename(&in_progress_path, &completed_path).with_context(|| {
                format!(
                    "archive completed reset plan {} to {}",
                    in_progress_path.display(),
                    completed_path.display()
                )
            })?;
            println!("preproduction reset completed and verified");
            println!("completed_plan={}", completed_path.display());
            Ok(())
        }
    }
}

async fn preproduction_reset_verify(args: ConfigDirArgs) -> Result<()> {
    let deploy = load_reset_deploy(&args)?;
    let postgres_password = migration_password(
        deploy.db.postgres.migration.password.as_ref(),
        "db.postgres.migration.password",
    )?;
    let inventory = inspect_preproduction_postgres(&deploy.db.postgres, postgres_password)
        .await
        .context("inspect PostgreSQL reset target")?;
    if inventory.production_baseline_exists {
        bail!("production baseline exists; this environment is not resettable");
    }
    let postgres = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .context("connect PostgreSQL runtime identity")?;
    let postgres_status = verify_postgres_schema(postgres.connection())
        .await
        .context("verify PostgreSQL boot manifest")?;
    let clickhouse_status = verify_schema(&deploy.db.clickhouse)
        .await
        .context("verify ClickHouse boot manifest")?;
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

fn load_reset_deploy(args: &ConfigDirArgs) -> Result<DeployConfig> {
    let config_dir = args
        .config_dir
        .to_str()
        .context("preproduction reset config directory is not valid UTF-8")?;
    DeployConfig::load_for_migration(config_dir).context("load reset deploy config")
}

async fn reset_inventory(deploy: &DeployConfig) -> Result<ResetObjectInventory> {
    let postgres_password = migration_password(
        deploy.db.postgres.migration.password.as_ref(),
        "db.postgres.migration.password",
    )?;
    let clickhouse_password = migration_password(
        deploy.db.clickhouse.migration.password.as_ref(),
        "db.clickhouse.migration.password",
    )?;
    let clickhouse_config = deploy
        .db
        .clickhouse
        .migration_connection(clickhouse_password);
    let (postgres, clickhouse_object_count, redis_namespace_key_count) = tokio::try_join!(
        async {
            inspect_preproduction_postgres(&deploy.db.postgres, postgres_password)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            database_object_count(&clickhouse_config)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            count_preproduction_namespace(&deploy.cache.redis)
                .await
                .map_err(anyhow::Error::from)
        },
    )?;
    if postgres.production_baseline_exists {
        bail!("production baseline exists; preproduction reset is forbidden");
    }
    Ok(ResetObjectInventory {
        postgres_database_exists: postgres.database_exists,
        postgres_object_count: postgres.object_count,
        postgres_connection_count: postgres.connection_count,
        clickhouse_object_count,
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
            deploy.db.postgres.migration.user.as_str(),
        ))?,
        clickhouse: CanonicalDigest::content_hash_json(&(
            deploy.db.clickhouse.url.as_str(),
            deploy.db.clickhouse.database.as_str(),
            deploy.db.clickhouse.user.as_str(),
            deploy.db.clickhouse.migration.user.as_str(),
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

fn reset_plan_hash(plan: &PreproductionResetPlan) -> Result<ContentHash> {
    CanonicalDigest::content_hash_json(&ResetPlanDigest {
        format_version: plan.format_version,
        nonce: &plan.nonce,
        created_at: plan.created_at,
        expires_at: plan.expires_at,
        targets: &plan.targets,
        inventory: &plan.inventory,
    })
    .context("hash preproduction reset plan")
}

fn validate_reset_plan(
    plan: &PreproductionResetPlan,
    deploy: &DeployConfig,
    confirm_nonce: &PreproductionResetNonce,
) -> Result<()> {
    if plan.format_version != RESET_PLAN_FORMAT_VERSION
        || &plan.nonce != confirm_nonce
        || plan.expires_at <= Utc::now()
        || plan.targets != reset_target_fingerprints(deploy)?
        || plan.plan_hash != reset_plan_hash(plan)?
    {
        bail!("reset plan is expired, tampered, targets another endpoint, or nonce mismatched");
    }
    Ok(())
}

fn read_reset_plan(path: &Path) -> Result<PreproductionResetPlan> {
    let bytes = fs::read(path).with_context(|| format!("read reset plan {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse reset plan {}", path.display()))
}

fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::create_dir_all(path.parent().context("reset plan path has no parent")?)
        .context("create reset plan directory")?;
    let mut rendered = serde_json::to_vec_pretty(value).context("render reset plan")?;
    rendered.push(b'\n');
    fs::write(path, rendered).with_context(|| format!("write reset plan {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict reset plan {}", path.display()))?;
    Ok(())
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
            let password = migration_password(
                deploy.db.postgres.migration.password.as_ref(),
                "db.postgres.migration.password",
            )?;
            let config = deploy.db.postgres.migration_connection(password);
            let pool = PostgresPool::connect_existing(&config)
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
        PostgresSchemaCommand::Apply(_) => {
            let password = migration_password(
                deploy.db.postgres.migration.password.as_ref(),
                "db.postgres.migration.password",
            )?;
            let bootstrap_admin_password_hash = bootstrap_admin_password_hash()?;
            let pool = PostgresPool::connect_migration(&deploy.db.postgres, password)
                .await
                .context("connect PostgreSQL migration identity")?;
            apply_postgres_migrations(pool.connection())
                .await
                .context("apply audited SeaORM PostgreSQL migrations")?;
            let status = finalize_schema_deployment(
                pool.connection(),
                &deploy.db.postgres.user,
                &bootstrap_admin_password_hash,
            )
            .await
            .context("finalize PostgreSQL schema deployment")?;
            println!(
                "PostgreSQL schema deployed: version={}, migrations={}, tables={}, indexes={}",
                status.current_version,
                status.migration_count,
                status.required_table_count,
                status.required_index_count
            );
            pool.close().await;
            Ok(())
        }
        PostgresSchemaCommand::Verify(_) => {
            let pool = PostgresPool::connect_existing(&deploy.db.postgres)
                .await
                .context("connect PostgreSQL runtime identity")?;
            let status = verify_postgres_schema(pool.connection())
                .await
                .context("verify PostgreSQL schema")?;
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
                .context("connect PostgreSQL runtime identity")?;
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

async fn generate_clean_postgres_manifest() -> Result<()> {
    const DATABASE: &str = "quant_pivot_manifest_codegen";
    const RUNTIME_ROLE: &str = "quant_pivot_manifest_runtime";

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
        password: SystemdCredentialRef::from_resolved(password.as_str().to_owned()),
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
    pool.connection()
        .execute_unprepared("CREATE ROLE quant_pivot_manifest_runtime NOLOGIN")
        .await
        .context("create disposable PostgreSQL runtime role")?;
    apply_postgres_migrations(pool.connection())
        .await
        .context("apply migrations to disposable PostgreSQL manifest database")?;
    let bootstrap_password = Zeroizing::new(Uuid::now_v7().to_string());
    let bootstrap_password_hash =
        hash_password(bootstrap_password.as_str()).context("hash disposable bootstrap password")?;
    let manifest = generate_disposable_schema_manifest(
        pool.connection(),
        RUNTIME_ROLE,
        &bootstrap_password_hash,
    )
    .await
    .context("generate disposable PostgreSQL semantic manifest")?;
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
    let lifecycle_lease = if matches!(
        &command,
        ClickHouseSchemaCommand::ApplyOnline(_) | ClickHouseSchemaCommand::ApplyOffline(_)
    ) {
        let password = migration_password(
            deploy.db.postgres.migration.password.as_ref(),
            "db.postgres.migration.password",
        )?;
        let postgres = PostgresPool::connect_migration(&deploy.db.postgres, password)
            .await
            .context("connect PostgreSQL for cross-system lifecycle lease")?;
        Some(
            acquire_lifecycle_lease(postgres.connection())
                .await
                .context("acquire cross-system lifecycle lease")?,
        )
    } else {
        None
    };
    let result = match command {
        ClickHouseSchemaCommand::Plan(_) => {
            let password = migration_password(
                config.migration.password.as_ref(),
                "db.clickhouse.migration.password",
            )?;
            let migration_config = config.migration_connection(password);
            let plan = plan_schema(&migration_config)
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
            let password = migration_password(
                config.migration.password.as_ref(),
                "db.clickhouse.migration.password",
            )?;
            let migration_config = config.migration_connection(password);
            let status = apply_online_schema_migrations(&migration_config)
                .await
                .context("deploy ClickHouse schema")?;
            println!(
                "ClickHouse schema deployed: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
        }
        ClickHouseSchemaCommand::ApplyOffline(_) => {
            let password = migration_password(
                config.migration.password.as_ref(),
                "db.clickhouse.migration.password",
            )?;
            let migration_config = config.migration_connection(password);
            let status = apply_offline_schema_migrations(&migration_config)
                .await
                .context("deploy offline ClickHouse schema")?;
            println!(
                "ClickHouse offline schema deployed: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
        }
        ClickHouseSchemaCommand::Verify(_) => {
            let status = verify_schema(config)
                .await
                .context("verify ClickHouse schema")?;
            println!(
                "ClickHouse schema verified: version={}, required_objects={}",
                status.current_version, status.required_object_count
            );
            Ok(())
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
    let release_result = match lifecycle_lease {
        Some(lease) => release_lifecycle_lease(lease)
            .await
            .context("release cross-system lifecycle lease"),
        None => Ok(()),
    };
    match (result, release_result) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn migration_password<'a>(
    password: Option<&'a SystemdCredentialRef>,
    field: &'static str,
) -> Result<&'a SecretText> {
    password
        .map(SystemdCredentialRef::secret)
        .filter(|secret| !secret.is_empty())
        .with_context(|| format!("configuration secret `{field}` is required for this command"))
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

fn ensure_docker_daemon() -> Result<()> {
    let status = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to run `docker info`")?;
    if status.success() {
        Ok(())
    } else {
        bail!("Docker daemon is not running (required for testcontainers)");
    }
}

fn test_full() -> Result<()> {
    test_workspace()?;
    test_docker()?;
    test_network()?;

    eprintln!("All tests passed.");
    Ok(())
}

fn test_workspace() -> Result<()> {
    let root = workspace_root()?;

    eprintln!("== workspace :: cargo test --workspace ==");
    let status = Command::new("cargo")
        .current_dir(&root)
        .args(["test", "--workspace"])
        .status()
        .context("failed to run workspace test suite")?;
    if !status.success() {
        bail!("workspace test suite failed");
    }

    Ok(())
}

fn test_docker() -> Result<()> {
    ensure_docker_daemon()?;
    run_ignored_suites(DOCKER_SUITES, "docker")?;

    eprintln!("All Docker integration tests passed.");
    Ok(())
}

fn test_network() -> Result<()> {
    run_ignored_suites(NETWORK_SUITES, "network")?;

    eprintln!("All network integration tests passed.");
    Ok(())
}

fn run_ignored_suites(suites: &[(&str, &str)], label: &str) -> Result<()> {
    let root = workspace_root()?;

    for (pkg, test) in suites {
        eprintln!("== {pkg} :: {test} ==");
        let mut command = Command::new("cargo");
        command.current_dir(&root).args([
            "test",
            "-p",
            pkg,
            "--test",
            test,
            "--",
            "--ignored",
            "--test-threads=1",
        ]);
        if (*pkg, *test) == ("quant-pivot-web", "web") {
            command.args(["--skip", UI_E2E_SERVER_TEST]);
        }
        let status = command
            .status()
            .with_context(|| format!("failed to run {label} suite {pkg}::{test}"))?;
        if !status.success() {
            bail!("{label} suite failed: {pkg}::{test}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use super::{DOCKER_SUITES, report_capacity_ceiling, workspace_root};

    const DOCKER_IGNORE_MARKER: &str = "#[ignore = \"requires Docker\"]";

    #[test]
    fn report_capacity_ceiling_enforces_floor_double_runway_and_rounding() {
        assert_eq!(report_capacity_ceiling(1).expect("ceiling"), 100_000);
        assert_eq!(report_capacity_ceiling(50_001).expect("ceiling"), 101_000);
        assert_eq!(report_capacity_ceiling(120_000).expect("ceiling"), 240_000);
    }

    #[test]
    fn docker_suite_registry_matches_ignored_integration_targets() {
        let root = workspace_root().expect("workspace root");
        let packages = DOCKER_SUITES
            .iter()
            .map(|(package, _)| *package)
            .collect::<BTreeSet<_>>();
        let mut discovered = BTreeSet::new();

        for package in packages {
            let tests_dir = root.join("crates").join(package).join("tests");
            for entry in fs::read_dir(&tests_dir).expect("integration test directory") {
                let path = entry.expect("integration test entry").path();
                if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                    continue;
                }
                let target = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .expect("UTF-8 integration target");
                let module_dir = tests_dir.join(target);
                if path_contains(&path, DOCKER_IGNORE_MARKER)
                    || path_contains(&module_dir, DOCKER_IGNORE_MARKER)
                {
                    discovered.insert((package.to_owned(), target.to_owned()));
                }
            }
        }

        let registered = DOCKER_SUITES
            .iter()
            .map(|(package, target)| ((*package).to_owned(), (*target).to_owned()))
            .collect::<BTreeSet<_>>();
        assert_eq!(registered, discovered);
    }

    fn path_contains(path: &Path, marker: &str) -> bool {
        if path.is_file() {
            return fs::read_to_string(path).is_ok_and(|contents| contents.contains(marker));
        }
        if !path.is_dir() {
            return false;
        }
        fs::read_dir(path)
            .expect("integration module directory")
            .filter_map(Result::ok)
            .any(|entry| path_contains(&entry.path(), marker))
    }
}
