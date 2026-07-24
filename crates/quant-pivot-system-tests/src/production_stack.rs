//! Real-binary system fixture backed by disposable infrastructure.

use std::{
    env, fs,
    fs::File,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use quant_pivot_models::{
    config::DeployConfig,
    entities::{
        market::Entity as MarketEntity,
        quant_feature_parity_run::{Column, Entity},
        quant_settlement_redeem::Entity as QuantSettlementRedeemEntity,
    },
    enums::{
        market::MarketStatus, quant::FeatureParityRunKind, settlement::SettlementEffectivePolicy,
    },
    types::{ContentHash, MarketId, RecommendationReportId},
};
use quant_pivot_repository::postgres::{
    PgExecutionSubmissionRepository, PgModelRegistryRepository, PgPolicyRepository,
    policy_bootstrap::ensure_default_policy_bundle,
};
use reqwest::Client;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder,
};
use serde::Deserialize;
use tokio::{
    process::{Child, Command as TokioCommand},
    signal::{unix, unix::SignalKind},
    time::Instant,
};
use toml::{Table, Value};
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::{
    stack::SystemStack,
    support::execution_pg_seed::{
        ReportSeedConfig, enable_test_admission, fill_entry_lot, seed_approved_intent,
        seed_pending_intent, seed_report_fixture, seed_report_on_infra, seed_shared_demo_infra,
    },
};

const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const FUNDER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const API_KEY: &str = "00000000-0000-0000-0000-000000000000";
const API_PASSPHRASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const API_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const JWT_SIGNING_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";
const EVIDENCE_SIGNING_KEY: &str =
    "0808080808080808080808080808080808080808080808080808080808080808";
const STARTUP_TIMEOUT: Duration = Duration::from_mins(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

const DISABLED_DOMAIN_SOURCES: &[&str] = &[
    "binance",
    "binance_usdm_futures",
    "polymarket_rtds",
    "chainlink_data_streams",
    "aviation_weather",
    "ghcnh",
    "gefs",
    "hko_open_data",
    "airnow",
    "tornado",
    "nhc",
    "nasa_gistemp",
    "nsidc_sea_ice",
    "nws_observation",
];

#[derive(Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    target_directory: PathBuf,
}

struct Workspace {
    root: PathBuf,
    target_directory: PathBuf,
    binary: PathBuf,
}

pub struct ProductionStack {
    child: Child,
    base_url: String,
    run_dir: PathBuf,
    _upstream: MockServer,
    infrastructure: SystemStack,
}

pub async fn serve(listen_port: u16, browser_fixture: bool, retain_artifacts: bool) -> Result<()> {
    if listen_port == 0 {
        bail!("production-stack serve requires a non-zero --listen-port");
    }
    ensure_port_available(listen_port)?;
    let workspace = build_production_binary()?;
    let mut running = start_at(&workspace, listen_port, browser_fixture).await?;
    println!(
        "production stack ready: base_url={} artifacts={} (terminate to stop)",
        running.base_url,
        running.run_dir.display(),
    );

    tokio::select! {
        signal = termination_signal() => signal?,
        status = running.child.wait() => {
            let status = status.context("wait for production binary")?;
            bail!(
                "production binary exited before the fixture was terminated: {status}; logs={}",
                running.log_path().display(),
            );
        }
    }

    Box::pin(running.stop(!retain_artifacts)).await
}

pub async fn start_production_stack() -> Result<ProductionStack> {
    let workspace = build_production_binary()?;
    let listen_port = reserve_port()?;
    start_at(&workspace, listen_port, false).await
}

pub async fn verify(runs: u16) -> Result<()> {
    if runs == 0 {
        bail!("production-stack verify requires --runs greater than zero");
    }
    let workspace = build_production_binary()?;
    for run_number in 1..=runs {
        let listen_port = reserve_port()?;
        let running = start_at(&workspace, listen_port, false)
            .await
            .with_context(|| format!("start production-stack verification run {run_number}"))?;
        Box::pin(running.stop(true))
            .await
            .with_context(|| format!("stop production-stack verification run {run_number}"))?;
        println!("production-stack verification run {run_number}/{runs} passed");
    }
    Ok(())
}

impl ProductionStack {
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    #[must_use]
    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    fn log_path(&self) -> PathBuf {
        self.run_dir.join("backend.log")
    }

    pub async fn stop(mut self, remove_artifacts: bool) -> Result<()> {
        let shutdown_result: Result<()> = async {
            if self
                .child
                .try_wait()
                .context("inspect production binary")?
                .is_none()
            {
                let process_id = self
                    .child
                    .id()
                    .context("production binary has no process id")?;
                let terminate = Command::new("kill")
                    .args(["-TERM", &process_id.to_string()])
                    .status()
                    .context("send SIGTERM to production binary")?;
                if !terminate.success() {
                    bail!(
                        "could not signal production binary {process_id}; logs={}",
                        self.log_path().display(),
                    );
                }
                if let Ok(status) = tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
                    let status = status.context("wait for graceful production shutdown")?;
                    if !status.success() {
                        bail!(
                            "production binary shutdown failed with {status}; logs={}",
                            self.log_path().display(),
                        );
                    }
                } else {
                    self.child
                        .start_kill()
                        .context("force-stop unresponsive production binary")?;
                    let _ = self.child.wait().await;
                    bail!(
                        "production binary exceeded the {SHUTDOWN_TIMEOUT:?} shutdown budget; logs={}",
                        self.log_path().display(),
                    );
                }
            }
            Ok(())
        }
        .await;
        let artifact_result = if remove_artifacts && shutdown_result.is_ok() {
            fs::remove_dir_all(&self.run_dir)
                .with_context(|| format!("remove successful run {}", self.run_dir.display()))
        } else {
            Ok(())
        };
        let infrastructure_result = Box::pin(self.infrastructure.shutdown())
            .await
            .context("remove disposable production-stack infrastructure");

        shutdown_result?;
        artifact_result?;
        infrastructure_result
    }
}

async fn start_at(
    workspace: &Workspace,
    listen_port: u16,
    browser_fixture: bool,
) -> Result<ProductionStack> {
    let upstream = deterministic_upstream().await;
    let infrastructure = Box::pin(SystemStack::start())
        .await
        .context("start disposable production-stack infrastructure")?;
    PgModelRegistryRepository::new(infrastructure.postgres.connection().clone())
        .ensure_builtin_research_profiles()
        .await
        .context("bootstrap immutable fresh-deployment research profiles")?;
    ensure_default_policy_bundle(
        &PgPolicyRepository::new(infrastructure.postgres.connection().clone()),
        "production-stack-fixture",
        "canonical fresh-boot policy for the real-binary system fixture",
    )
    .await
    .context("bootstrap canonical fresh-boot policy bundle")?;
    let browser_report_id = if browser_fixture {
        Some(
            Box::pin(seed_browser_fixture(infrastructure.postgres.connection()))
                .await
                .context("seed browser production fixture")?,
        )
    } else {
        None
    };
    let run_dir = workspace
        .target_directory
        .join("production-stack")
        .join(Uuid::now_v7().to_string());
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("create production-stack run {}", run_dir.display()))?;

    if let Err(error) = render_config(
        &workspace.root,
        &run_dir,
        listen_port,
        &upstream,
        &infrastructure,
    ) {
        return Err(error.context(format!(
            "render production config; retained artifacts={}",
            run_dir.display()
        )));
    }

    let log_path = run_dir.join("backend.log");
    let stdout = File::create(&log_path)
        .with_context(|| format!("create backend log {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("clone backend log handle {}", log_path.display()))?;
    let mut command = TokioCommand::new(&workspace.binary);
    command
        .arg("--config-dir")
        .arg(&run_dir)
        .current_dir(&workspace.root)
        .env("RUST_LOG", "info,polymarket_client_sdk_v2=error")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn production binary {}", workspace.binary.display()))?;
    let base_url = format!("http://127.0.0.1:{listen_port}");
    if let Err(error) = await_startup(&mut child, &base_url).await {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return Err(error.context(format!(
            "production binary did not become ready; logs={}",
            log_path.display()
        )));
    }
    if let Some(report_id) = browser_report_id.as_ref() {
        // The real research worker consumes the report's mandatory sampled
        // parity job. The deterministic browser profile deliberately has no
        // serving facts, so wait for fail-closed containment before exposing
        // the stable, auditable report/intent state to Playwright.
        await_sampled_parity_containment(infrastructure.postgres.connection(), report_id).await?;
        await_browser_settlement_discovery(infrastructure.postgres.connection()).await?;
    }

    Ok(ProductionStack {
        child,
        base_url,
        run_dir,
        _upstream: upstream,
        infrastructure,
    })
}

async fn seed_browser_fixture(db: &DatabaseConnection) -> Result<RecommendationReportId> {
    let infra = seed_shared_demo_infra(db).await;
    enable_test_admission(db, "browser-e2e-fixture").await;
    let settlement_report = seed_report_on_infra(
        db,
        &infra,
        ReportSeedConfig {
            event_id: "browser-settlement-event".to_owned(),
            market_id: "0xbrowser-settlement-market".to_owned(),
            market_question: "Will the browser settlement fixture resolve?".to_owned(),
            market_slug: "browser-settlement-fixture".to_owned(),
            token_id: "12345".to_owned(),
            trigger_key: format!(
                "scheduled:browser-settlement:{}",
                RecommendationReportId::from_v7()
            ),
        },
    )
    .await;
    let settlement_intent = seed_approved_intent(db, &settlement_report).await;
    fill_entry_lot(
        db,
        &PgExecutionSubmissionRepository::new(db.clone()),
        &settlement_report,
        &settlement_intent,
    )
    .await;
    let market = MarketEntity::find_by_id(MarketId::new(&settlement_report.market))
        .one(db)
        .await
        .context("load browser settlement market")?
        .context("browser settlement market is missing")?;
    let mut active_market = market.into_active_model();
    active_market.status = ActiveValue::Set(MarketStatus::Settled);
    active_market.outcome = ActiveValue::Set(Some("Yes".to_owned()));
    active_market.resolved_at = ActiveValue::Set(Some(Utc::now()));
    active_market.content_hash = ActiveValue::Set(ContentHash::from_bytes([0x96; 32]));
    active_market
        .update(db)
        .await
        .context("mark browser settlement market resolved")?;

    // Publish the parity-containment fixture last so the settlement report
    // cannot supersede its still-pending intent before the real worker
    // atomically revokes it.
    let report = seed_report_fixture(db).await;
    seed_pending_intent(db, &report).await;
    Ok(report.report)
}

async fn await_browser_settlement_discovery(db: &DatabaseConnection) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(redeem) = QuantSettlementRedeemEntity::find()
            .one(db)
            .await
            .context("read browser settlement case")?
        {
            if redeem.effective_policy != SettlementEffectivePolicy::ManualOnly {
                bail!(
                    "browser settlement fixture must remain ManualOnly, got {}",
                    redeem.effective_policy
                );
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("production settlement discovery did not create the browser fixture case");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn await_sampled_parity_containment(
    db: &DatabaseConnection,
    report_id: &RecommendationReportId,
) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        let run = Entity::find()
            .filter(Column::Kind.eq(FeatureParityRunKind::Sampled))
            .filter(Column::ReportId.eq(*report_id))
            .order_by_desc(Column::CreatedAt)
            .one(db)
            .await
            .context("read initial automatic feature-parity run")?;
        if let Some(run) = run
            && run.status.is_terminal()
            && run.containment_completed_at.is_some()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("sampled feature-parity containment did not settle before browser handoff");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn build_production_binary() -> Result<Workspace> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let metadata_output = Command::new(&cargo)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .context("resolve workspace and target directories")?;
    if !metadata_output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&metadata_output.stderr).trim()
        );
    }
    let metadata: CargoMetadata =
        serde_json::from_slice(&metadata_output.stdout).context("decode cargo metadata")?;
    let status = Command::new(cargo)
        .args(["build", "-p", "quant-pivot-bin", "-p", "quant-pivot-xtask"])
        .current_dir(&metadata.workspace_root)
        .status()
        .context("build real quant-pivot production binary with the launcher feature set")?;
    if !status.success() {
        bail!("production binary and launcher build failed with {status}");
    }
    let binary_name = if cfg!(windows) {
        "quant-pivot.exe"
    } else {
        "quant-pivot"
    };
    let binary = metadata.target_directory.join("debug").join(binary_name);
    if !binary.is_file() {
        bail!("built production binary is missing: {}", binary.display());
    }
    Ok(Workspace {
        root: metadata.workspace_root,
        target_directory: metadata.target_directory,
        binary,
    })
}

async fn deterministic_upstream() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/auth/derive-api-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apiKey": API_KEY,
            "passphrase": API_PASSPHRASE,
            "secret": API_SECRET,
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/heartbeats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "heartbeat_id": "00000000-0000-0000-0000-000000000001",
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [],
        })))
        .mount(&server)
        .await;
    server
}

fn render_config(
    workspace_root: &Path,
    run_dir: &Path,
    listen_port: u16,
    upstream: &MockServer,
    stack: &SystemStack,
) -> Result<()> {
    let source_path = workspace_root.join("config/quant-pivot.toml");
    let source = fs::read_to_string(&source_path)
        .with_context(|| format!("read canonical deploy config {}", source_path.display()))?;
    let mut config: Value = toml::from_str(&source)
        .with_context(|| format!("parse canonical deploy config {}", source_path.display()))?;
    configure_upstreams(&mut config, upstream)?;
    configure_test_identity(&mut config, run_dir)?;
    configure_infrastructure(&mut config, stack)?;
    configure_web(&mut config, listen_port)?;

    let rendered = toml::to_string_pretty(&config).context("serialize production-stack config")?;
    let config_path = run_dir.join("quant-pivot.toml");
    fs::write(&config_path, rendered)
        .with_context(|| format!("write production-stack config {}", config_path.display()))?;
    let config_dir = run_dir
        .to_str()
        .context("production-stack config path is not valid UTF-8")?;
    DeployConfig::load(config_dir).context("validate generated production-stack config")?;
    Ok(())
}

fn configure_upstreams(config: &mut Value, upstream: &MockServer) -> Result<()> {
    let upstream_url = upstream.uri();
    set(
        config,
        &["polymarket", "clob_base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["polymarket", "clob_ws_url"],
        "ws://127.0.0.1:1/ws",
    )?;
    set(
        config,
        &["polymarket", "onchain", "rpc_endpoint"],
        Value::Table(Table::from_iter([
            ("source".to_owned(), Value::String("public".to_owned())),
            ("url".to_owned(), Value::String(upstream_url.clone())),
        ])),
    )?;
    set(
        config,
        &["market_data", "gamma", "base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["market_data", "data_api", "base_url"],
        upstream_url,
    )?;
    set(
        config,
        &["market_data", "trade_tape_on_chain", "enabled"],
        false,
    )?;
    for source in DISABLED_DOMAIN_SOURCES {
        set(config, &["domain_sources", source, "enabled"], false)?;
    }
    Ok(())
}

fn configure_test_identity(config: &mut Value, run_dir: &Path) -> Result<()> {
    set(config, &["keys", "private_key"], PRIVATE_KEY)?;
    set(config, &["quant", "account", "funder"], FUNDER)?;
    set(
        config,
        &["research", "artifact_store", "prefix"],
        run_dir.join("artifacts").to_string_lossy().into_owned(),
    )?;
    set(
        config,
        &["research", "evidence_attestation", "signing_key"],
        EVIDENCE_SIGNING_KEY,
    )?;
    Ok(())
}

fn configure_infrastructure(config: &mut Value, stack: &SystemStack) -> Result<()> {
    set(
        config,
        &["db", "postgres", "host"],
        stack.postgres_config.host.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "port"],
        i64::from(stack.postgres_config.port),
    )?;
    set(
        config,
        &["db", "postgres", "user"],
        stack.postgres_config.user.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "password"],
        stack.postgres_config.password.expose_secret(),
    )?;
    set(
        config,
        &["db", "postgres", "database"],
        stack.postgres_config.database.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "schema"],
        stack.postgres_config.schema.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "application_name"],
        "quant-pivot-production-stack",
    )?;
    set(
        config,
        &["db", "clickhouse", "deployment_id"],
        stack.clickhouse_config.deployment_id.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "cluster_id"],
        stack.clickhouse_config.cluster_id.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "url"],
        stack.clickhouse_config.url.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "database"],
        stack.clickhouse_config.database.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "user"],
        stack.clickhouse_config.user.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "password"],
        stack.clickhouse_config.password.expose_secret(),
    )?;
    set(
        config,
        &["cache", "redis", "host"],
        stack.redis_config.host.clone(),
    )?;
    set(
        config,
        &["cache", "redis", "port"],
        i64::from(stack.redis_config.port),
    )?;
    set(
        config,
        &["cache", "redis", "key_prefix"],
        stack.redis_config.key_prefix.clone(),
    )?;
    Ok(())
}

fn configure_web(config: &mut Value, listen_port: u16) -> Result<()> {
    set(config, &["web", "listen_host"], "127.0.0.1")?;
    set(config, &["web", "listen_port"], i64::from(listen_port))?;
    set(config, &["web", "serve_static_ui"], false)?;
    set(
        config,
        &["web", "cors_allowed_origins"],
        Value::Array(vec![Value::String("http://127.0.0.1:6099".to_owned())]),
    )?;
    set(config, &["web", "jwt", "signing_key"], JWT_SIGNING_KEY)?;
    Ok(())
}

fn set<T>(root: &mut Value, path: &[&str], value: T) -> Result<()>
where
    T: Into<Value>,
{
    let (key, parents) = path
        .split_last()
        .context("configuration mutation path cannot be empty")?;
    let mut cursor = root;
    for parent in parents {
        cursor = cursor
            .get_mut(*parent)
            .with_context(|| format!("configuration section is missing: {parent}"))?;
    }
    let table = cursor
        .as_table_mut()
        .with_context(|| format!("configuration parent is not a table: {}", parents.join(".")))?;
    table.insert((*key).to_owned(), value.into());
    Ok(())
}

async fn await_startup(child: &mut Child, base_url: &str) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build startup probe client")?;
    let startup_url = format!("{base_url}/startup");
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().context("inspect production binary")? {
            bail!("production binary exited during startup with {status}");
        }
        if let Ok(response) = client.get(&startup_url).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("startup probe exceeded {STARTUP_TIMEOUT:?}: {startup_url}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("reserve local production-stack port")?;
    let port = listener
        .local_addr()
        .context("read reserved production-stack port")?
        .port();
    drop(listener);
    Ok(port)
}

fn ensure_port_available(port: u16) -> Result<()> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
        .with_context(|| format!("production-stack port {port} is already occupied"))?;
    drop(listener);
    Ok(())
}

async fn termination_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            unix::signal(SignalKind::terminate()).context("install SIGTERM listener")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")?;
    Ok(())
}
