mod common;

use common::{config, connect_pool, context, implementation, maybe_start_postgres};
use philharmonic_connector_impl_api::Implementation;
use philharmonic_connector_impl_sql_postgres::SqlQueryResponse;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker testcontainer"]
#[serial_test::file_serial(docker)]
async fn sql_type_mapping_matches_wire_shape() {
    let Some(db) = maybe_start_postgres().await else {
        return;
    };

    let pool = connect_pool(&db.connection_url).await;

    sqlx::query(
        "CREATE TABLE type_samples (
            i BIGINT NOT NULL,
            f DOUBLE PRECISION NOT NULL,
            n NUMERIC NOT NULL,
            b BOOLEAN NOT NULL,
            t TEXT NOT NULL,
            bin BYTEA NOT NULL,
            d DATE NOT NULL,
            tm TIME NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            arr_i BIGINT[] NOT NULL,
            arr_t TEXT[] NOT NULL,
            js JSONB NOT NULL,
            nullable TEXT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO type_samples
            (i, f, n, b, t, bin, d, tm, ts, arr_i, arr_t, js, nullable)
         VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(42_i64)
    .bind(3.25_f64)
    .bind("123.4500")
    .bind(true)
    .bind("hello")
    .bind(vec![0x61_u8, 0x62_u8, 0x63_u8])
    .bind(sqlx::types::chrono::NaiveDate::from_ymd_opt(2025, 3, 1).unwrap())
    .bind(sqlx::types::chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap())
    .bind(
        "2025-03-01T12:00:00Z"
            .parse::<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>()
            .unwrap(),
    )
    .bind(vec![1_i64, 2_i64, 3_i64])
    .bind(vec!["x".to_owned(), "y".to_owned()])
    .bind(sqlx::types::Json(json!({"k": "v", "n": 1})))
    .bind(Option::<String>::None)
    .execute(&pool)
    .await
    .unwrap();

    let impl_ = implementation();
    let response = impl_
        .execute(
            &config(&db.connection_url),
            &json!({
                "sql": "SELECT i, f, n, b, t, bin, d, tm, ts, arr_i, arr_t, js, nullable FROM type_samples",
                "params": []
            }),
            &context(),
        )
        .await
        .unwrap();

    let response: SqlQueryResponse = serde_json::from_value(response).unwrap();
    assert_eq!(response.row_count, 1);
    assert_eq!(response.rows.len(), 1);

    let row = &response.rows[0];
    assert_eq!(row["i"], json!(42));
    assert_eq!(row["f"], json!(3.25));
    assert_eq!(row["n"], json!("123.4500"));
    assert_eq!(row["b"], json!(true));
    assert_eq!(row["t"], json!("hello"));
    assert_eq!(row["bin"], json!("YWJj"));
    assert_eq!(row["d"], json!("2025-03-01"));
    assert_eq!(row["tm"], json!("12:34:56"));
    assert_eq!(row["ts"], json!("2025-03-01T12:00:00+00:00"));
    assert_eq!(row["arr_i"], json!([1, 2, 3]));
    assert_eq!(row["arr_t"], json!(["x", "y"]));
    assert_eq!(row["js"], json!({"k": "v", "n": 1}));
    assert_eq!(row["nullable"], json!(null));
}
