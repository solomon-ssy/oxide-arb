use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use quant_pivot_core::app::phase_11_9_evidence_bootstrap::{
    Phase119EvidenceBootstrapOptions, run_phase_11_9_evidence_bootstrap,
};
use quant_pivot_migration::{apply as apply_postgres_migrations, plan as plan_postgres_migrations};
use quant_pivot_models::{
    config::{DeployConfig, secret::SecretText},
    domain::{NewRuntimeConfigActivation, NewRuntimeConfigVersion},
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{FeedbackConfig, RuntimeConfig, validate_runtime_config},
    security::hash_password,
    types::{
        EventId, RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
        domain_capability::DomainContractFamily,
        domain_classification::DomainMarketClassificationOutcome,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgRuntimeConfigVersionRepository,
        catalog::{event::PgEventRepository, market::PgMarketRepository},
    },
    traits::{
        RuntimeConfigVersionRepository, governance::event::EventRepository,
        market::MarketRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, build_artifact_store},
    linkage::{
        catalog_classification::DomainCatalogClassifier,
        weather_daily_temperature::WeatherStationRegistry,
    },
};
use quant_pivot_storage::{
    clickhouse::{
        apply_offline_schema_migrations, apply_online_schema_migrations, plan_schema,
        render_schema_manifest as render_clickhouse_schema_manifest, verify_schema,
    },
    postgres::{
        PostgresPool,
        migration::{
            finalize_schema_deployment, inspect_schema_manifest, render_schema_manifest,
            verify_schema as verify_postgres_schema,
        },
    },
};
use rustls::crypto::aws_lc_rs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::PathBuf,
    process::{Command, ExitCode, Stdio},
    sync::Arc,
};
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
    ("quant-pivot-repository", "pg_execution_submission"),
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
    /// One-shot audited activation of an active Runtime v17 document as v18.
    #[command(name = "phase-11-9-activate-runtime-v18")]
    Phase119ActivateRuntimeV18(ConfigDirArgs),
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
        Commands::Phase119ActivateRuntimeV18(args) => activate_phase_11_9_runtime_v18(args).await,
    }
}

async fn activate_phase_11_9_runtime_v18(args: ConfigDirArgs) -> Result<()> {
    const ACTOR: &str = "phase_11_9_runtime_v18_migration";
    const REASON: &str = "one-shot Runtime v17 to v18 schema activation for Phase 11.9";

    let config_dir = args
        .config_dir
        .to_str()
        .context("runtime migration config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load deploy config")?;
    let pool = PostgresPool::connect_existing(&deploy.db.postgres)
        .await
        .context("connect PostgreSQL for Runtime v18 activation")?;
    let repo = PgRuntimeConfigVersionRepository::new(pool.connection().clone());
    let current_activation = repo
        .load_current_activation()
        .await
        .context("load current runtime activation")?
        .context("Runtime v18 activation requires an existing current activation")?;
    let current = repo
        .load_current()
        .await
        .context("load current runtime config")?
        .context("Runtime v18 activation requires an existing current config")?;
    if current.schema_version == SchemaVersion::new(18) {
        RuntimeConfig::from_json(&current.config_json)
            .context("current Runtime v18 document is invalid")?;
        println!("status=already_active");
        println!(
            "runtime_config_version_id={}",
            current.runtime_config_version_id
        );
        println!("config_hash={}", current.config_hash);
        pool.close().await;
        return Ok(());
    }

    let config = migrate_runtime_v17_json(&current.config_json)?;
    let validation = validate_runtime_config(&config);
    if validation.has_errors() {
        bail!("migrated Runtime v18 document failed validation: {validation:?}");
    }
    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json)
        .context("hash migrated Runtime v18 document")?;
    let version = match repo
        .load_by_hash(&config_hash)
        .await
        .context("lookup existing Runtime v18 migration candidate")?
    {
        Some(version) => version,
        None => repo
            .create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash: config_hash.clone(),
                schema_version: config.schema_version,
                config_json,
                source: RuntimeConfigVersionSource::Import,
                created_by: ACTOR.to_owned(),
                reason: REASON.to_owned(),
            })
            .await
            .context("create immutable Runtime v18 migration candidate")?,
    };
    let activation = repo
        .activate_version_if_current(
            Some(&current_activation.runtime_config_activation_id),
            NewRuntimeConfigActivation {
                runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
                runtime_config_version_id: version.runtime_config_version_id.clone(),
                runtime_config_approval_id: None,
                activated_by: ACTOR.to_owned(),
                reason: REASON.to_owned(),
                activation_kind: RuntimeConfigActivationKind::Promote,
                previous_runtime_config_version_id: Some(current.runtime_config_version_id),
                rollback_target_version_id: None,
                audit_event_id: None,
            },
        )
        .await
        .context("CAS-activate Runtime v18 migration candidate")?;
    let active = repo
        .load_current()
        .await
        .context("read Runtime v18 activation back")?
        .context("Runtime v18 activation read-back is absent")?;
    if active.runtime_config_version_id != version.runtime_config_version_id
        || active.config_hash != config_hash
    {
        bail!("Runtime v18 activation read-after-write mismatch");
    }
    RuntimeConfig::from_json(&active.config_json)
        .context("activated Runtime v18 document failed typed read-back")?;
    println!("status=activated");
    println!(
        "runtime_config_activation_id={}",
        activation.runtime_config_activation_id
    );
    println!(
        "runtime_config_version_id={}",
        active.runtime_config_version_id
    );
    println!("config_hash={}", active.config_hash);
    pool.close().await;
    Ok(())
}

fn migrate_runtime_v17_json(config_json: &serde_json::Value) -> Result<RuntimeConfig> {
    let mut migrated = config_json.clone();
    let document = migrated
        .as_object_mut()
        .context("Runtime v17 document must be a JSON object")?;
    let schema_version = document
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .context("Runtime v17 document has no integer schema_version")?;
    if schema_version != 17 {
        bail!("one-shot Runtime migration only accepts schema_version 17, found {schema_version}");
    }
    if document.contains_key("feedback") {
        bail!("Runtime v17 document unexpectedly contains feedback; refusing ambiguous migration");
    }
    document.insert("schema_version".to_owned(), serde_json::json!(18));
    document.insert(
        "feedback".to_owned(),
        serde_json::to_value(FeedbackConfig::default())
            .context("encode Runtime v18 feedback defaults")?,
    );
    RuntimeConfig::from_json(&migrated).context("typed parse of migrated Runtime v18 document")
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
        CanonicalDigest::content_hash_json(&("phase_11_9_evidence_manifest_v4", &manifest))
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

async fn postgres_schema(command: PostgresSchemaCommand) -> Result<()> {
    let args = match &command {
        PostgresSchemaCommand::Plan(args)
        | PostgresSchemaCommand::Apply(args)
        | PostgresSchemaCommand::Verify(args)
        | PostgresSchemaCommand::Manifest(args) => Some(args),
        PostgresSchemaCommand::MigrationManifest => None,
    };
    let Some(args) = args else {
        write_postgres_migration_manifest()?;
        return Ok(());
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
        PostgresSchemaCommand::MigrationManifest => Ok(()),
    }
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
    match command {
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
    }
}

fn migration_password<'a>(
    password: Option<&'a SecretText>,
    field: &'static str,
) -> Result<&'a SecretText> {
    password
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

    use quant_pivot_models::runtime_config::{FeedbackConfig, RuntimeConfig};

    use super::{DOCKER_SUITES, migrate_runtime_v17_json, report_capacity_ceiling, workspace_root};

    const DOCKER_IGNORE_MARKER: &str = "#[ignore = \"requires Docker\"]";

    #[test]
    fn report_capacity_ceiling_enforces_floor_double_runway_and_rounding() {
        assert_eq!(report_capacity_ceiling(1).expect("ceiling"), 100_000);
        assert_eq!(report_capacity_ceiling(50_001).expect("ceiling"), 101_000);
        assert_eq!(report_capacity_ceiling(120_000).expect("ceiling"), 240_000);
    }

    #[test]
    fn runtime_v17_migration_is_exact_and_preserves_existing_policy() {
        let mut legacy = RuntimeConfig::default().to_json();
        let document = legacy.as_object_mut().expect("runtime object");
        document.insert("schema_version".to_owned(), serde_json::json!(17));
        document.remove("feedback");
        document
            .get_mut("selection")
            .and_then(serde_json::Value::as_object_mut)
            .expect("selection")
            .insert("max_spread_bps".to_owned(), serde_json::json!(4321));

        let migrated = migrate_runtime_v17_json(&legacy).expect("v17 to v18 migration");
        assert_eq!(migrated.schema_version.get(), 18);
        assert_eq!(migrated.selection.max_spread_bps, 4321);
        assert_eq!(migrated.feedback, FeedbackConfig::default());
    }

    #[test]
    fn runtime_v17_migration_rejects_every_other_document_shape() {
        let current = RuntimeConfig::default().to_json();
        assert!(migrate_runtime_v17_json(&current).is_err());

        let mut ambiguous = current;
        ambiguous
            .as_object_mut()
            .expect("runtime object")
            .insert("schema_version".to_owned(), serde_json::json!(17));
        assert!(migrate_runtime_v17_json(&ambiguous).is_err());
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
