//! Authorization integration tests.

use actix_web::{
    HttpResponse,
    http::{StatusCode, header::AUTHORIZATION},
    test::TestRequest,
};
use serde_json::{Value, json};

use crate::harness::{self, API_VERSION, TestEnv};

fn bearer(token: &str) -> (actix_web::http::header::HeaderName, String) {
    (AUTHORIZATION, format!("Bearer {token}"))
}

/// Log in and return the access token.
async fn login(env: &TestEnv, username: &str, password: &str) -> String {
    let req = TestRequest::post()
        .uri("/api/auth/login")
        .insert_header(API_VERSION)
        .set_json(json!({ "username": username, "password": password }));
    let res = harness::call(&env.state, req).await;
    assert_eq!(
        res.status,
        StatusCode::OK,
        "login for {username} should succeed"
    );
    res.json()["data"]["access_token"]
        .as_str()
        .expect("access_token")
        .to_owned()
}

async fn get(env: &TestEnv, uri: &str, token: &str) -> harness::Resp {
    let req = TestRequest::get()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token));
    harness::call(&env.state, req).await
}

async fn post(env: &TestEnv, uri: &str, token: &str, body: Value) -> harness::Resp {
    let req = TestRequest::post()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token))
        .set_json(body);
    harness::call(&env.state, req).await
}

async fn put(env: &TestEnv, uri: &str, token: &str, body: Value) -> harness::Resp {
    let req = TestRequest::put()
        .uri(uri)
        .insert_header(API_VERSION)
        .insert_header(bearer(token))
        .set_json(body);
    harness::call(&env.state, req).await
}

/// Create a custom role (as admin) and return its id.
async fn create_role(env: &TestEnv, admin: &str, code: &str) -> String {
    let res = post(
        env,
        "/api/roles",
        admin,
        json!({ "code": code, "name": code }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "create role {code}");
    res.json()["data"]["id"]
        .as_str()
        .expect("role id")
        .to_owned()
}

/// Create a user (as admin) and return its id.
async fn create_user(env: &TestEnv, admin: &str, username: &str, password: &str) -> String {
    let res = post(
        env,
        "/api/users",
        admin,
        json!({ "username": username, "password": password, "nickname": username }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "create user {username}");
    res.json()["data"]["id"]
        .as_str()
        .expect("user id")
        .to_owned()
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn super_admin_bypasses_every_rbac_route() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    assert_eq!(get(&env, "/api/users", &admin).await.status, StatusCode::OK);
    assert_eq!(get(&env, "/api/roles", &admin).await.status, StatusCode::OK);
    assert_eq!(get(&env, "/api/menus", &admin).await.status, StatusCode::OK);
    assert_eq!(
        get(&env, "/api/permissions/catalog", &admin).await.status,
        StatusCode::OK
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn resource_op_allows_granted_role_and_denies_excess() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let role_id = create_role(&env, &admin, "reader").await;
    assert_eq!(
        put(
            &env,
            &format!("/api/roles/{role_id}/permissions"),
            &admin,
            json!({ "permissions": [{ "resource": "user", "operation": "read" }] }),
        )
        .await
        .status,
        StatusCode::OK
    );
    let user_id = create_user(&env, &admin, "reader1", "password123").await;
    assert_eq!(
        put(
            &env,
            &format!("/api/users/{user_id}/roles"),
            &admin,
            json!({ "role_ids": [role_id] }),
        )
        .await
        .status,
        StatusCode::OK
    );

    let reader = login(&env, "reader1", "password123").await;
    // Granted: User:Read. (Also proves /api/users GET pattern matches its rule.)
    assert_eq!(
        get(&env, "/api/users", &reader).await.status,
        StatusCode::OK
    );
    // Not granted: User:Create.
    assert_eq!(
        post(
            &env,
            "/api/users",
            &reader,
            json!({ "username": "intruder", "password": "password123", "nickname": "x" }),
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    // Not granted at all: Role:Read.
    assert_eq!(
        get(&env, "/api/roles", &reader).await.status,
        StatusCode::FORBIDDEN
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn permission_grant_takes_effect_after_reload() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let role_id = create_role(&env, &admin, "grower").await;
    put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [{ "resource": "user", "operation": "read" }] }),
    )
    .await;
    let user_id = create_user(&env, &admin, "grower1", "password123").await;
    put(
        &env,
        &format!("/api/users/{user_id}/roles"),
        &admin,
        json!({ "role_ids": [role_id] }),
    )
    .await;

    let user = login(&env, "grower1", "password123").await;
    assert_eq!(
        get(&env, "/api/roles", &user).await.status,
        StatusCode::FORBIDDEN
    );

    // Grant Role:Read; the enforcer reloads inside the handler, so the next
    // request is authorized without re-login.
    put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [
            { "resource": "user", "operation": "read" },
            { "resource": "role", "operation": "read" }
        ] }),
    )
    .await;
    assert_eq!(get(&env, "/api/roles", &user).await.status, StatusCode::OK);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn invalid_permission_pair_is_bad_request() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;
    let role_id = create_role(&env, &admin, "bad_perms").await;

    // `user` does not allow `halt` — rejected as Bad Request (400) before persistence.
    let res = put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [{ "resource": "user", "operation": "halt" }] }),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn disabling_a_role_revokes_access_then_enabling_restores_it() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let role_id = create_role(&env, &admin, "toggle").await;
    put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [{ "resource": "user", "operation": "read" }] }),
    )
    .await;
    let user_id = create_user(&env, &admin, "toggle1", "password123").await;
    put(
        &env,
        &format!("/api/users/{user_id}/roles"),
        &admin,
        json!({ "role_ids": [role_id] }),
    )
    .await;

    let user = login(&env, "toggle1", "password123").await;
    assert_eq!(get(&env, "/api/users", &user).await.status, StatusCode::OK);

    // Disable the role → authority revoked immediately (same token).
    assert_eq!(
        put(
            &env,
            &format!("/api/roles/{role_id}/status"),
            &admin,
            json!({ "status": "disabled" }),
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        get(&env, "/api/users", &user).await.status,
        StatusCode::FORBIDDEN
    );

    // Re-enable → authority restored from the surviving membership.
    assert_eq!(
        put(
            &env,
            &format!("/api/roles/{role_id}/status"),
            &admin,
            json!({ "status": "enabled" }),
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(get(&env, "/api/users", &user).await.status, StatusCode::OK);
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn role_menu_assignment_surfaces_in_accessible_tree() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let menu = post(
        &env,
        "/api/menus",
        &admin,
        json!({ "name": "dash", "kind": "menu", "title": "Dashboard" }),
    )
    .await;
    assert_eq!(menu.status, StatusCode::OK);
    let menu_id = menu.json()["data"]["id"]
        .as_str()
        .expect("menu id")
        .to_owned();

    let role_id = create_role(&env, &admin, "menu_holder").await;
    assert_eq!(
        put(
            &env,
            &format!("/api/roles/{role_id}/menus"),
            &admin,
            json!({ "menu_ids": [menu_id] }),
        )
        .await
        .status,
        StatusCode::OK
    );
    let user_id = create_user(&env, &admin, "menu1", "password123").await;
    put(
        &env,
        &format!("/api/users/{user_id}/roles"),
        &admin,
        json!({ "role_ids": [role_id] }),
    )
    .await;

    let user = login(&env, "menu1", "password123").await;
    let res = get(&env, "/api/menus/accessible", &user).await;
    assert_eq!(res.status, StatusCode::OK);
    let titles: Vec<String> = res.json()["data"]
        .as_array()
        .expect("menu array")
        .iter()
        .filter_map(|node| node["title"].as_str().map(ToOwned::to_owned))
        .collect();
    assert!(
        titles.iter().any(|title| title == "Dashboard"),
        "assigned menu must appear in the accessible tree: {titles:?}"
    );
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn authority_is_keyed_by_stable_id_not_profile() {
    let env = TestEnv::start().await;
    let admin = login(&env, "admin", "admin").await;

    let role_id = create_role(&env, &admin, "stable_reader").await;
    put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [{ "resource": "user", "operation": "read" }] }),
    )
    .await;
    let user_id = create_user(&env, &admin, "stable1", "password123").await;
    put(
        &env,
        &format!("/api/users/{user_id}/roles"),
        &admin,
        json!({ "role_ids": [role_id] }),
    )
    .await;

    let user = login(&env, "stable1", "password123").await;
    assert_eq!(get(&env, "/api/users", &user).await.status, StatusCode::OK);

    // Mutating the profile must not disturb authority — the Casbin subject is
    // the immutable user id, not any profile field. The pre-existing token keeps
    // working without re-login.
    assert_eq!(
        put(
            &env,
            &format!("/api/users/{user_id}"),
            &admin,
            json!({ "nickname": "Renamed Profile" }),
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(get(&env, "/api/users", &user).await.status, StatusCode::OK);
}

async fn fail_closed_probe() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[actix_web::test]
#[ignore = "requires Docker"]
async fn fail_closed_unregistered_protected_route_returns_forbidden() {
    use std::sync::Arc;

    use actix_web::{App, middleware::from_fn, test, web};
    use quant_pivot_web::{
        auth::casbin::PermChecker,
        middleware::{authn, authz},
    };

    let env = TestEnv::start().await;

    // super_admin bypasses an empty PermChecker, so use a non-privileged actor.
    let admin = login(&env, "admin", "admin").await;
    let role_id = create_role(&env, &admin, "probe_reader").await;
    put(
        &env,
        &format!("/api/roles/{role_id}/permissions"),
        &admin,
        json!({ "permissions": [{ "resource": "user", "operation": "read" }] }),
    )
    .await;
    let user_id = create_user(&env, &admin, "probe1", "password123").await;
    put(
        &env,
        &format!("/api/users/{user_id}/roles"),
        &admin,
        json!({ "role_ids": [role_id] }),
    )
    .await;
    let token = login(&env, "probe1", "password123").await;

    let mut state = env.state.clone();
    state.perm_checker = Arc::new(PermChecker::new());

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .wrap(from_fn(authn))
            .service(
                web::scope("")
                    .wrap(from_fn(authz))
                    .route("/api/_fail_closed_probe", web::get().to(fail_closed_probe)),
            ),
    )
    .await;

    let req = TestRequest::get()
        .uri("/api/_fail_closed_probe")
        .insert_header(API_VERSION)
        .insert_header(bearer(&token))
        .to_request();

    let status = match test::try_call_service(&app, req).await {
        Ok(res) => res.status(),
        Err(err) => err.error_response().status(),
    };
    assert_eq!(status, StatusCode::FORBIDDEN);
}
