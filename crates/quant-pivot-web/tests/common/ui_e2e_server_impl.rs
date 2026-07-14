//! Dedicated full-stack server for the protected Phase 11.7 Playwright suite.
//!
//! This module is compiled only into the existing web integration-test binary;
//! production route assembly remains unchanged.

use std::sync::Arc;

use actix_web::{App, HttpResponse, HttpServer, middleware::from_fn, web};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    execution::{
        EntryConditionInputSet, ExecutablePriceInput, decide_entry_condition_state,
        evaluate_entry_condition,
    },
    ingest::book_store::BookStore,
    observability::metrics_hub::MetricsHub,
};
use quant_pivot_error::{QuantError, QuantResult, control::ControlError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ApplyEntryConditionEvaluation, BacktestPathSetInfo, BacktestPathSetListQuery,
        BacktestReportInfo, BacktestReportListQuery, BookLevel, EntryConditionArtifactInfo,
        EntryConditionInstanceInfo, FactorCollinearitySource, FactorCollinearityView,
        FactorDefinitionInfo, FactorDefinitionListQuery, JobProgressSink, MarketDataPort,
        ModelComparisonReportInfo, ModelPublishedCatalogQuery, ModelSpecInfo, ModelSpecListQuery,
        ModelTrainingPort, ModelVersionInfo, ModelVersionListQuery, Paginated,
        PublishedModelOptionView, RecommendationInfo, ResearchCatalogPort, TradePolicyArtifactInfo,
        TradePolicyAuditListQuery, TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
        TradePolicyGovernanceAuditInfo, TradePolicyListQuery, TradePolicyPort, TrainModelRequest,
        TrainedModelView, TrainingDatasetInfo, TrainingDatasetListQuery, empty_catalog_page,
    },
    entities::{quant_entry_condition_instance, quant_recommendation},
    enums::quant::{EntryConditionState, TradePolicyGovernanceAction, TradePolicyStatus},
    types::{
        FactorDefinitionId, ModelVersionId, OrderIntentId, Price, RecommendationId,
        RecommendationTradePlan, Shares, TokenId, TradePlanBlocker, TradePolicyArtifactId,
        TradePolicyGovernanceAuditId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEntryConditionRepository, PgExecutionSubmissionRepository, PgModelRegistryRepository,
        PgOrderIntentRepository, PgRecommendationRepository, PgTradePolicyRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        EntryConditionRepository, ExecutionSubmissionRepository, ModelRegistryRepository,
        OrderIntentRepository, RecommendationRepository, TradePolicyRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_test_support::ui_demo_seed::{DemoSeedRecord, seed_ui_demo_pg};
use quant_pivot_web::{middleware, routes};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::harness;

const LISTEN_HOST: &str = "127.0.0.1";
const LISTEN_PORT: u16 = 8088;

struct E2eTradePolicyPort {
    policies: PgTradePolicyRepository,
}

impl E2eTradePolicyPort {
    const fn new(db: DatabaseConnection) -> Self {
        Self {
            policies: PgTradePolicyRepository::new(db),
        }
    }
}

#[async_trait]
impl TradePolicyPort for E2eTradePolicyPort {
    async fn preflight(
        &self,
        _request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        Err(QuantError::NotImplemented(
            "trade-policy fit is outside the UI E2E server".to_owned(),
        ))
    }

    async fn fit(
        &self,
        _request: quant_pivot_models::domain::FitTradePolicyRequest,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        Err(QuantError::NotImplemented(
            "trade-policy fit is outside the UI E2E server".to_owned(),
        ))
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>> {
        self.policies.find(artifact_id).await.map_err(Into::into)
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>> {
        self.policies.page(query).await.map_err(Into::into)
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>> {
        self.policies
            .page_audits(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: Uuid,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let current =
            self.policies
                .find(artifact_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "trade_policy_artifact",
                    id: artifact_id.to_string(),
                })?;
        let action = match target {
            TradePolicyStatus::Validated => TradePolicyGovernanceAction::Validate,
            TradePolicyStatus::Published => TradePolicyGovernanceAction::Publish,
            TradePolicyStatus::Retired => TradePolicyGovernanceAction::Retire,
            TradePolicyStatus::Draft => {
                return Err(StorageError::IllegalTransition {
                    entity: "trade_policy_artifact",
                    id: Some(artifact_id.to_string()),
                    from: current.status.as_str().to_owned(),
                    to: target.as_str().to_owned(),
                }
                .into());
            }
        };
        self.policies
            .transition(
                artifact_id,
                current.status,
                target,
                quant_pivot_models::domain::NewTradePolicyGovernanceAudit {
                    audit_id: TradePolicyGovernanceAuditId::from_v7(),
                    artifact_id: artifact_id.clone(),
                    action,
                    from_status: current.status,
                    to_status: target,
                    content_hash: current.content_hash,
                    actor_id,
                    reason,
                },
            )
            .await
            .map_err(Into::into)
    }
}

struct E2eResearchCatalogPort {
    datasets: PgTrainingDatasetRepository,
    models: PgModelRegistryRepository,
}

struct E2eModelTrainingPort {
    models: PgModelRegistryRepository,
}

impl E2eModelTrainingPort {
    const fn new(db: DatabaseConnection) -> Self {
        Self {
            models: PgModelRegistryRepository::new(db),
        }
    }
}

#[async_trait]
impl ModelTrainingPort for E2eModelTrainingPort {
    async fn train(
        &self,
        _model_version_id: ModelVersionId,
        _request: TrainModelRequest,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
        Err(QuantError::NotImplemented(
            "model training is outside the UI E2E server".to_owned(),
        ))
    }

    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>> {
        self.models
            .find_model_version_by_id(model_version_id)
            .await
            .map_err(Into::into)
    }
}

impl E2eResearchCatalogPort {
    fn new(db: DatabaseConnection) -> Self {
        Self {
            datasets: PgTrainingDatasetRepository::new(db.clone()),
            models: PgModelRegistryRepository::new(db),
        }
    }
}

#[async_trait]
impl ResearchCatalogPort for E2eResearchCatalogPort {
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>> {
        self.datasets.page(query).await.map_err(Into::into)
    }

    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>> {
        self.models.page_versions(query).await.map_err(Into::into)
    }

    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>> {
        self.models.page_specs(query).await.map_err(Into::into)
    }

    async fn list_published_model_options(
        &self,
        _query: ModelPublishedCatalogQuery,
    ) -> QuantResult<Vec<PublishedModelOptionView>> {
        Ok(Vec::new())
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_backtest_path_sets(
        &self,
        query: BacktestPathSetListQuery,
    ) -> QuantResult<Paginated<BacktestPathSetInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_comparison_reports(
        &self,
        query: quant_pivot_models::domain::ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn find_factor(
        &self,
        _factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        Ok(None)
    }

    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: Decimal,
        source: FactorCollinearitySource,
        _neutralize_by_category: bool,
    ) -> QuantResult<FactorCollinearityView> {
        Ok(FactorCollinearityView {
            factors: Vec::new(),
            matrix: Vec::new(),
            violations: Vec::new(),
            threshold,
            observation_count: 0,
            lookback_secs,
            panel_source: source,
        })
    }
}

struct E2eMarketData {
    books: Arc<BookStore>,
}

#[async_trait]
impl MarketDataPort for E2eMarketData {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (
        Option<Arc<quant_pivot_models::domain::BookSnapshot>>,
        Option<Arc<quant_pivot_models::domain::BookSnapshot>>,
    ) {
        (self.books.load(yes_token), self.books.load(no_token))
    }

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> std::collections::HashSet<TokenId> {
        token_ids.iter().cloned().collect()
    }

    fn all_subscribed_tokens(&self) -> std::collections::HashSet<TokenId> {
        std::collections::HashSet::new()
    }

    async fn subscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }

    async fn unsubscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }
}

#[derive(Clone, Serialize)]
struct E2eFixtures {
    fixture_format_version: u32,
    unavailable_recommendation_id: RecommendationId,
    frozen_recommendation_id: RecommendationId,
    pending_intent_id: OrderIntentId,
    waiting_intent_id: OrderIntentId,
    position_id: quant_pivot_models::types::PositionId,
    model_version_id: quant_pivot_models::types::ModelVersionId,
    trade_policy_artifact_id: TradePolicyArtifactId,
}

struct E2eControlState {
    db: DatabaseConnection,
    books: Arc<BookStore>,
    fixtures: E2eFixtures,
}

#[derive(Deserialize)]
struct BookObservationRequest {
    best_ask: Price,
    observed_at: DateTime<Utc>,
    #[serde(default)]
    stale: bool,
}

#[derive(Serialize)]
struct BookObservationResponse {
    entry_condition_state: EntryConditionState,
    confirming_since: Option<DateTime<Utc>>,
    ready_at: Option<DateTime<Utc>>,
}

struct E2eConditionFixture {
    recommendation: RecommendationInfo,
    instance: EntryConditionInstanceInfo,
    artifact: EntryConditionArtifactInfo,
}

async fn get_fixtures(control: web::Data<E2eControlState>) -> web::Json<E2eFixtures> {
    web::Json(control.fixtures.clone())
}

async fn observe_book(
    intent_id: web::Path<OrderIntentId>,
    request: web::Json<BookObservationRequest>,
    control: web::Data<E2eControlState>,
) -> HttpResponse {
    match observe_book_inner(&control, &intent_id, &request).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(error) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": error.to_string(),
        })),
    }
}

async fn observe_book_inner(
    control: &E2eControlState,
    intent_id: &OrderIntentId,
    request: &BookObservationRequest,
) -> QuantResult<BookObservationResponse> {
    let E2eConditionFixture {
        recommendation,
        mut instance,
        artifact,
    } = load_condition_fixture(control, intent_id).await?;
    let timestamp = if request.stale {
        Utc::now() - Duration::seconds(10)
    } else {
        Utc::now()
    };
    apply_book(
        &control.books,
        &recommendation.token_id,
        request.best_ask,
        timestamp,
    )?;
    let snapshot = control.books.load(&recommendation.token_id);
    let best_ask = snapshot
        .as_deref()
        .and_then(quant_pivot_models::domain::BookSnapshot::best_ask)
        .ok_or_else(|| QuantError::config("E2E condition book has no best ask"))?;
    let observed_at = if request.stale {
        request.observed_at - Duration::seconds(10)
    } else {
        request.observed_at
    };
    let evaluation = evaluate_entry_condition(
        &artifact.payload_json,
        &EntryConditionInputSet {
            binding: artifact.payload_json.binding.clone(),
            evaluated_at: request.observed_at,
            prices: vec![ExecutablePriceInput {
                token_id: recommendation.token_id,
                price: best_ask,
                observed_at,
                available_at: observed_at,
                gap_generation: control.books.gap_generation(),
            }],
            factors: Vec::new(),
            crypto: Vec::new(),
            weather: Vec::new(),
        },
    )?;
    let decision = decide_entry_condition_state(
        instance.state,
        instance.confirmation_started_at,
        instance.continuity_hash.as_ref(),
        instance.last_evaluated_at,
        &artifact.payload_json,
        &evaluation,
        request.observed_at,
    );
    let worker_id = Uuid::now_v7();
    instance =
        lease_condition_instance(&control.db, &instance, worker_id, request.observed_at).await?;
    let conditions = PgEntryConditionRepository::new(control.db.clone());
    let updated = conditions
        .apply_evaluation(
            &instance.condition_instance_id,
            worker_id,
            ApplyEntryConditionEvaluation {
                expected_revision: instance.revision,
                expected_lease_epoch: instance.lease_epoch,
                state: decision.state,
                truth: evaluation.truth,
                evaluation_hash: evaluation.evaluation_hash,
                input_fingerprint: evaluation.input_fingerprint,
                continuity_hash: evaluation.continuity_hash,
                confirmation_started_at: decision.confirmation_started_at,
                evaluated_at: request.observed_at,
                next_evaluation_at: Some(request.observed_at + Duration::seconds(1)),
            },
        )
        .await?;
    Ok(BookObservationResponse {
        entry_condition_state: updated.state,
        confirming_since: updated.confirmation_started_at,
        ready_at: (updated.state == EntryConditionState::Qualified).then_some(request.observed_at),
    })
}

async fn load_condition_fixture(
    control: &E2eControlState,
    intent_id: &OrderIntentId,
) -> QuantResult<E2eConditionFixture> {
    let intent = PgOrderIntentRepository::new(control.db.clone())
        .find_by_id(intent_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_order_intent",
            id: intent_id.to_string(),
        })?;
    let recommendation = PgRecommendationRepository::new(control.db.clone())
        .find_by_id(&intent.recommendation_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_recommendation",
            id: intent.recommendation_id.to_string(),
        })?;
    let conditions = PgEntryConditionRepository::new(control.db.clone());
    let instance = conditions
        .find_instance(&intent.condition_instance_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_entry_condition_instance",
            id: intent.condition_instance_id.to_string(),
        })?;
    let artifact_id = instance
        .artifact_id
        .clone()
        .ok_or_else(|| QuantError::config("E2E conditional intent instance has no artifact id"))?;
    let artifact = conditions
        .find_artifact(&artifact_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_entry_condition_artifact",
            id: artifact_id.to_string(),
        })?;
    Ok(E2eConditionFixture {
        recommendation,
        instance,
        artifact,
    })
}

async fn lease_condition_instance(
    db: &DatabaseConnection,
    instance: &EntryConditionInstanceInfo,
    worker_id: Uuid,
    observed_at: DateTime<Utc>,
) -> QuantResult<EntryConditionInstanceInfo> {
    let row =
        quant_entry_condition_instance::Entity::find_by_id(instance.condition_instance_id.clone())
            .one(db)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_entry_condition_instance",
                id: instance.condition_instance_id.to_string(),
            })?;
    let mut active = row.into_active_model();
    active.lease_owner = ActiveValue::Set(Some(worker_id));
    active.lease_expires_at = ActiveValue::Set(Some(observed_at + Duration::seconds(15)));
    Ok(active.update(db).await?.into())
}

fn apply_book(
    books: &BookStore,
    token_id: &TokenId,
    best_ask: Price,
    observed_at: DateTime<Utc>,
) -> QuantResult<()> {
    let bid = Price::new((best_ask.inner() - dec!(0.01)).max(dec!(0.01)));
    let bid_level = BookLevel::from_decimal(bid, Shares::new(dec!(1000)))
        .map_err(|error| QuantError::config(format!("invalid E2E bid level: {error}")))?;
    let ask_level = BookLevel::from_decimal(best_ask, Shares::new(dec!(1000)))
        .map_err(|error| QuantError::config(format!("invalid E2E ask level: {error}")))?;
    let timestamp_ms = u64::try_from(observed_at.timestamp_millis()).unwrap_or(0);
    books.apply_snapshot(
        token_id,
        Arc::<[BookLevel]>::from([bid_level]),
        Arc::<[BookLevel]>::from([ask_level]),
        timestamp_ms,
        None,
    );
    Ok(())
}

fn record<'a>(records: &'a [DemoSeedRecord], slug: &str) -> &'a DemoSeedRecord {
    records
        .iter()
        .find(|record| record.slug == slug)
        .unwrap_or_else(|| panic!("missing UI E2E seed record `{slug}`"))
}

async fn seed_exit_reinference_observation(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
    model_version_id: quant_pivot_models::types::ModelVersionId,
) {
    let now = Utc::now();
    PgExecutionSubmissionRepository::new(db.clone())
        .touch_exit_monitor(
            intent_id,
            now + Duration::seconds(30),
            Some(Price::new(dec!(0.74))),
            Some(now),
            Some(quant_pivot_models::types::ExitReinferenceObservation {
                observed_at: now,
                model_version_id,
                model_artifact_hash: quant_pivot_models::types::ContentHash::parse(concat!(
                    "blake3:",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ))
                .expect("fixture model hash"),
                factor_snapshot_hash: quant_pivot_models::types::ContentHash::parse(concat!(
                    "blake3:",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ))
                .expect("fixture factor snapshot hash"),
                mark: Price::new(dec!(0.69)),
                score: quant_pivot_models::types::Probability::new(dec!(0.67)),
                score_retention: dec!(0.82),
                expected_return_bps: quant_pivot_models::types::Bps::new(dec!(120)),
                execution_eligible: true,
                verdict: quant_pivot_models::types::ExitReinferenceVerdictKind::Holds,
                detail: "test-only governed reinference observation".to_owned(),
                shadow: false,
            }),
        )
        .await
        .expect("seed exit monitor observation");
}

async fn prepare_e2e_fixtures(db: &DatabaseConnection, books: &BookStore) -> E2eFixtures {
    let summary = seed_ui_demo_pg(db, "0x0000000000000000000000000000000000000001").await;
    let unavailable_recommendation_id = summary
        .actionable_recommendation_id
        .clone()
        .expect("UI seed must expose one recommendation without an intent");
    let unavailable =
        quant_recommendation::Entity::find_by_id(unavailable_recommendation_id.clone())
            .one(db)
            .await
            .expect("load unavailable recommendation fixture")
            .expect("unavailable recommendation fixture");
    let mut unavailable = unavailable.into_active_model();
    unavailable.trade_plan = ActiveValue::Set(RecommendationTradePlan::Unavailable {
        blockers: vec![TradePlanBlocker::CohortCoverageInsufficient],
    });
    unavailable
        .update(db)
        .await
        .expect("mark recommendation trade plan unavailable");

    let frozen_record = record(&summary.records, "pending-a");
    let frozen = PgRecommendationRepository::new(db.clone())
        .find_by_id(&frozen_record.recommendation_id)
        .await
        .expect("load frozen recommendation")
        .expect("frozen recommendation");
    let (policy, _, _, _, _) = frozen
        .trade_plan
        .frozen()
        .expect("seeded recommendation must carry a Frozen trade plan");
    let pending_intent_id = frozen_record
        .intent_id
        .clone()
        .expect("pending intent fixture");
    let waiting_record = record(&summary.records, "approved");
    let waiting_intent_id = waiting_record
        .intent_id
        .clone()
        .expect("waiting intent fixture");
    apply_book(books, &frozen.token_id, Price::new(dec!(0.60)), Utc::now())
        .expect("seed frozen recommendation book");

    let position_record = record(&summary.records, "filled-open");
    let position_id = position_record
        .position_id
        .clone()
        .expect("open position fixture");
    let position_intent_id = position_record
        .intent_id
        .clone()
        .expect("open position intent fixture");
    let position_recommendation = PgRecommendationRepository::new(db.clone())
        .find_by_id(&position_record.recommendation_id)
        .await
        .expect("load position recommendation")
        .expect("position recommendation");
    apply_book(
        books,
        &position_recommendation.token_id,
        Price::new(dec!(0.70)),
        Utc::now(),
    )
    .expect("seed open-position book");
    seed_exit_reinference_observation(
        db,
        &position_intent_id,
        position_recommendation
            .evidence_refs
            .model_version_id
            .clone(),
    )
    .await;

    E2eFixtures {
        fixture_format_version: 1,
        unavailable_recommendation_id,
        frozen_recommendation_id: frozen_record.recommendation_id.clone(),
        pending_intent_id,
        waiting_intent_id,
        position_id,
        model_version_id: frozen.evidence_refs.model_version_id,
        trade_policy_artifact_id: policy.artifact_id.clone(),
    }
}

#[actix_web::test]
#[ignore = "long-running Playwright backend; requires Docker"]
async fn serve_protected_ui_e2e() {
    let mut env = harness::TestEnv::start_with_core_report_port().await;
    let books = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let fixtures = prepare_e2e_fixtures(&env.db, &books).await;
    env.state.market_data = Arc::new(E2eMarketData {
        books: Arc::clone(&books),
    });
    env.state.trade_policies = Arc::new(E2eTradePolicyPort::new(env.db.clone()));
    env.state.research_catalog = Arc::new(E2eResearchCatalogPort::new(env.db.clone()));
    env.state.model_training = Arc::new(E2eModelTrainingPort::new(env.db.clone()));

    let control = web::Data::new(E2eControlState {
        db: env.db.clone(),
        books,
        fixtures,
    });
    let state = web::Data::new(env.state.clone());
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .app_data(control.clone())
            .wrap(from_fn(middleware::request_id))
            .wrap(from_fn(middleware::operation_audit))
            .service(
                web::scope("/__test")
                    .route("/fixtures", web::get().to(get_fixtures))
                    .route("/intents/{id}/book", web::post().to(observe_book)),
            )
            .configure(routes::configure)
    })
    .bind((LISTEN_HOST, LISTEN_PORT))
    .expect("bind protected UI E2E server")
    .run();

    eprintln!("protected UI E2E server ready at http://{LISTEN_HOST}:{LISTEN_PORT}");
    server.await.expect("run protected UI E2E server");
}
