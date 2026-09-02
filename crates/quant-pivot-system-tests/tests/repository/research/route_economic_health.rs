//! Route economic-health persistence contracts on real `PostgreSQL`.

use std::{slice, sync::Arc};

use quant_pivot_core::service::route_economic_health::RouteEconomicHealthService;
use quant_pivot_models::{
    domain::pagination::{PageRequest, PageWindow},
    entities::{
        quant_recommendation::Entity as RecommendationEntity,
        quant_report_route_run::Entity as RouteRunEntity,
        quant_route_economic_health::Entity as RouteHealthEntity,
        research_profile_artifact::Entity as ProfileEntity,
    },
    enums::quant::RouteEconomicHealthState,
    hashing::CanonicalDigest,
};
use quant_pivot_repository::{
    postgres::PgRouteEconomicHealthRepository, traits::RouteEconomicHealthRepository,
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::execution_pg_seed::seed_report_fixture,
};
use sea_orm::EntityTrait;

pub async fn insufficient_worm_is_enforced() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let recommendation = RecommendationEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("recommendation read")
        .expect("recommendation exists");
    let route_run = RouteRunEntity::find_by_id(recommendation.report_route_run_id)
        .one(&db)
        .await
        .expect("Route run read")
        .expect("Route run exists");
    let profile_id = route_run
        .research_profile_artifact_id
        .expect("Route profile");
    let profile = ProfileEntity::find_by_id(profile_id.clone())
        .one(&db)
        .await
        .expect("profile read")
        .expect("profile exists");
    let route_identity_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/test-route-economic-health",
        1,
        &(recommendation.route, profile_id.clone()),
    )
    .expect("Route identity hash");
    let repository = Arc::new(PgRouteEconomicHealthRepository::new(db.clone()))
        as Arc<dyn RouteEconomicHealthRepository>;
    let service = RouteEconomicHealthService::new(Arc::clone(&repository));
    let assessed_through = db.statement_time().await;
    let first = service
        .assess(
            &recommendation.route,
            route_identity_hash,
            profile_id.clone(),
            &profile.spec.feedback_policy,
            assessed_through,
        )
        .await
        .expect("insert insufficient Route health");
    let replay = service
        .assess(
            &recommendation.route,
            route_identity_hash,
            profile_id.clone(),
            &profile.spec.feedback_policy,
            assessed_through,
        )
        .await
        .expect("replay insufficient Route health");
    assert_eq!(first, replay);
    assert_eq!(first.state, RouteEconomicHealthState::InsufficientEvidence);
    assert_eq!(first.due_observation_count, 0);
    let latest = repository
        .latest(&route_identity_hash, &profile_id, assessed_through)
        .await
        .expect("latest Route health")
        .expect("latest Route health exists");
    assert_eq!(latest, first);
    let latest_by_route = repository
        .latest_for_route(&recommendation.route, assessed_through)
        .await
        .expect("latest health by Route")
        .expect("Route health exists");
    assert_eq!(latest_by_route, first);
    let page = repository
        .page_for_route(
            &recommendation.route,
            assessed_through,
            PageWindow::harden(PageRequest {
                page: 0,
                size: u64::MAX,
            }),
        )
        .await
        .expect("Route health page");
    assert_eq!(page.total, 1);
    assert_eq!(page.page, 1);
    assert_eq!(page.size, PageRequest::MAX_SIZE);
    assert_eq!(page.items.as_slice(), slice::from_ref(&first));
    assert!(
        RouteHealthEntity::delete_by_id(first.route_economic_health_id)
            .exec(&db)
            .await
            .is_err(),
        "Route economic health must be append-only",
    );
}
