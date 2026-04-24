mod common;

use common::{config, connect_pool, context, implementation, maybe_start_postgres};
use philharmonic_connector_impl_api::{Implementation, ImplementationError};
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn syntax_error_maps_to_invalid_request() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let impl_ = implementation();
    let err = impl_
        .execute(
            &config(&db.connection_url),
            &json!({"sql": "SELEC 1", "params": []}),
            &context(),
        )
        .await
        .unwrap_err();

    match err {
        ImplementationError::InvalidRequest { detail } => {
            assert!(!detail.is_empty());
        }
        other => panic!("expected InvalidRequest, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn parameter_mismatch_maps_to_invalid_request() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let impl_ = implementation();
    let err = impl_
        .execute(
            &config(&db.connection_url),
            &json!({"sql": "SELECT $1::text", "params": []}),
            &context(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        err,
        ImplementationError::InvalidRequest {
            detail: "parameter count mismatch: expected 1, got 0".to_owned(),
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn constraint_violation_maps_to_upstream_error() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let pool = connect_pool(&db.connection_url).await;
    sqlx::query("CREATE TABLE constrained (id BIGINT PRIMARY KEY, email TEXT UNIQUE NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let impl_ = implementation();
    let cfg = config(&db.connection_url);

    impl_
        .execute(
            &cfg,
            &json!({
                "sql": "INSERT INTO constrained (id, email) VALUES ($1, $2)",
                "params": [1, "alice@example.test"]
            }),
            &context(),
        )
        .await
        .unwrap();

    let err = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "INSERT INTO constrained (id, email) VALUES ($1, $2)",
                "params": [2, "alice@example.test"]
            }),
            &context(),
        )
        .await
        .unwrap_err();

    match err {
        ImplementationError::UpstreamError { status, body } => {
            assert_eq!(status, 500);
            assert!(!body.is_empty());
        }
        other => panic!("expected UpstreamError, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn connection_refused_maps_to_upstream_unreachable() {
    let impl_ = implementation();
    let err = impl_
        .execute(
            &json!({
                "connection_url": "postgres://postgres:postgres@127.0.0.1:1/postgres",
                "max_connections": 2,
                "default_timeout_ms": 300,
                "default_max_rows": 100
            }),
            &json!({"sql": "SELECT 1", "params": []}),
            &context(),
        )
        .await
        .unwrap_err();

    match err {
        ImplementationError::UpstreamUnreachable { detail } => {
            assert!(!detail.is_empty());
        }
        other => panic!("expected UpstreamUnreachable, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn per_request_timeout_maps_to_upstream_timeout() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let impl_ = implementation();
    let err = impl_
        .execute(
            &config(&db.connection_url),
            &json!({
                "sql": "SELECT pg_sleep(2)",
                "params": [],
                "timeout_ms": 50
            }),
            &context(),
        )
        .await
        .unwrap_err();

    assert_eq!(err, ImplementationError::UpstreamTimeout);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn integer_overflow_maps_to_upstream_error() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let impl_ = implementation();
    let err = impl_
        .execute(
            &config(&db.connection_url),
            &json!({
                "sql": "SELECT 9223372036854775808::numeric AS too_big",
                "params": []
            }),
            &context(),
        )
        .await
        .unwrap_err();

    match err {
        ImplementationError::UpstreamError { status, body } => {
            assert_eq!(status, 500);
            assert!(body.contains("integer overflow"));
        }
        other => panic!("expected UpstreamError, got {other:?}"),
    }
}
