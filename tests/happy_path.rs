mod common;

use common::{config, connect_pool, context, implementation, maybe_start_postgres};
use philharmonic_connector_impl_api::Implementation;
use philharmonic_connector_impl_sql_postgres::SqlQueryResponse;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn select_insert_update_delete_and_empty_shape() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let pool = connect_pool(&db.connection_url).await;
    sqlx::query(
        "CREATE TABLE items (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            active BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let impl_ = implementation();
    let cfg = config(&db.connection_url);

    let inserted = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "INSERT INTO items (name, active) VALUES ($1, $2)",
                "params": ["Alice", true]
            }),
            &context(),
        )
        .await
        .unwrap();
    let inserted: SqlQueryResponse = serde_json::from_value(inserted).unwrap();
    assert_eq!(inserted.row_count, 1);
    assert!(inserted.rows.is_empty());
    assert!(inserted.columns.is_empty());
    assert!(!inserted.truncated);

    let selected = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "SELECT id, name, active, created_at FROM items WHERE name = $1",
                "params": ["Alice"]
            }),
            &context(),
        )
        .await
        .unwrap();
    let selected: SqlQueryResponse = serde_json::from_value(selected).unwrap();
    assert_eq!(selected.row_count, 1);
    assert_eq!(selected.rows.len(), 1);
    assert_eq!(
        selected
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name", "active", "created_at"]
    );
    assert!(selected.rows[0]["id"].is_number());
    assert_eq!(selected.rows[0]["name"], json!("Alice"));
    assert_eq!(selected.rows[0]["active"], json!(true));
    assert!(selected.rows[0]["created_at"].is_string());

    let updated = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "UPDATE items SET name = $1 WHERE name = $2",
                "params": ["Alice Updated", "Alice"]
            }),
            &context(),
        )
        .await
        .unwrap();
    let updated: SqlQueryResponse = serde_json::from_value(updated).unwrap();
    assert_eq!(updated.row_count, 1);
    assert!(updated.rows.is_empty());
    assert!(updated.columns.is_empty());

    let empty = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "SELECT id, name FROM items WHERE name = $1",
                "params": ["nope"]
            }),
            &context(),
        )
        .await
        .unwrap();
    let empty: SqlQueryResponse = serde_json::from_value(empty).unwrap();
    assert_eq!(empty.row_count, 0);
    assert!(empty.rows.is_empty());
    assert_eq!(
        empty
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );

    let deleted = impl_
        .execute(
            &cfg,
            &json!({
                "sql": "DELETE FROM items WHERE name = $1",
                "params": ["Alice Updated"]
            }),
            &context(),
        )
        .await
        .unwrap();
    let deleted: SqlQueryResponse = serde_json::from_value(deleted).unwrap();
    assert_eq!(deleted.row_count, 1);
    assert!(deleted.rows.is_empty());
    assert!(deleted.columns.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn truncated_true_when_max_rows_clips() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let pool = connect_pool(&db.connection_url).await;
    sqlx::query("CREATE TABLE clipped (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    for i in 1..=3_i64 {
        sqlx::query("INSERT INTO clipped (id, name) VALUES ($1, $2)")
            .bind(i)
            .bind(format!("n{i}"))
            .execute(&pool)
            .await
            .unwrap();
    }

    let impl_ = implementation();
    let response = impl_
        .execute(
            &config(&db.connection_url),
            &json!({
                "sql": "SELECT id, name FROM clipped ORDER BY id ASC",
                "params": [],
                "max_rows": 2
            }),
            &context(),
        )
        .await
        .unwrap();

    let response: SqlQueryResponse = serde_json::from_value(response).unwrap();
    assert_eq!(response.row_count, 2);
    assert_eq!(response.rows.len(), 2);
    assert!(response.truncated);
    assert_eq!(response.rows[0]["id"], json!(1));
    assert_eq!(response.rows[1]["id"], json!(2));
}
