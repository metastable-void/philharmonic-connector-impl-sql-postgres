//! SQL execution pipeline for `sql_postgres`.

use crate::config::PreparedConfig;
use crate::error::{Error, Result};
use crate::request::SqlQueryRequest;
use crate::response::{Column, SqlQueryResponse, SqlRow};
use crate::types;
use futures_util::StreamExt;
use philharmonic_connector_impl_api::JsonValue;
use sqlx::query::Query;
use sqlx::{Column as SqlxColumn, Describe, Either, Executor, Postgres, TypeInfo};

/// Execute one `sql_query` call against Postgres.
pub(crate) async fn execute_query(
    prepared: &PreparedConfig,
    request: &SqlQueryRequest,
) -> Result<SqlQueryResponse> {
    let limits =
        request.effective_limits(prepared.default_max_rows, prepared.default_timeout_ms)?;

    let describe = prepared
        .pool
        .describe(&request.sql)
        .await
        .map_err(|e| classify_sqlx_error(e, request.params.len()))?;

    if let Some(expected) = describe_parameter_count(&describe)
        && expected != request.params.len()
    {
        return Err(Error::ParameterMismatch {
            expected,
            actual: request.params.len(),
        });
    }

    if describe.columns().is_empty() {
        execute_command(prepared, request, limits.timeout).await
    } else {
        execute_rows(
            prepared,
            request,
            &describe,
            limits.max_rows,
            limits.timeout,
        )
        .await
    }
}

async fn execute_command(
    prepared: &PreparedConfig,
    request: &SqlQueryRequest,
    timeout: std::time::Duration,
) -> Result<SqlQueryResponse> {
    let query = bind_params(&request.sql, &request.params)?;

    let result = tokio::time::timeout(timeout, async { query.execute(&prepared.pool).await })
        .await
        .map_err(|_| Error::UpstreamTimeout)?
        .map_err(|e| classify_sqlx_error(e, request.params.len()))?;

    Ok(SqlQueryResponse {
        rows: Vec::new(),
        row_count: result.rows_affected(),
        columns: Vec::new(),
        truncated: false,
    })
}

async fn execute_rows(
    prepared: &PreparedConfig,
    request: &SqlQueryRequest,
    describe: &Describe<Postgres>,
    max_rows: usize,
    timeout: std::time::Duration,
) -> Result<SqlQueryResponse> {
    let response_columns = response_columns(describe);
    let limit_plus_one = max_rows.checked_add(1).ok_or_else(|| {
        Error::Internal("max_rows overflow while computing truncation sentinel".to_owned())
    })?;

    let mut rows = tokio::time::timeout(timeout, async {
        let mut out = Vec::<SqlRow>::new();
        let mut stream = bind_params(&request.sql, &request.params)?.fetch(&prepared.pool);

        while let Some(next) = stream.next().await {
            let row = next.map_err(|e| classify_sqlx_error(e, request.params.len()))?;

            let mut mapped = SqlRow::with_capacity(response_columns.len());
            for (index, column) in response_columns.iter().enumerate() {
                let value = types::decode_cell(&row, index, &column.name, &column.sql_type)?;
                mapped.insert(column.name.clone(), value);
            }
            out.push(mapped);

            if out.len() == limit_plus_one {
                break;
            }
        }

        Ok::<Vec<SqlRow>, Error>(out)
    })
    .await
    .map_err(|_| Error::UpstreamTimeout)??;

    let truncated = rows.len() == limit_plus_one;
    if truncated {
        rows.pop();
    }

    Ok(SqlQueryResponse {
        row_count: u64::try_from(rows.len()).map_err(|_| {
            Error::Internal("row_count overflow converting usize to u64".to_owned())
        })?,
        rows,
        columns: response_columns,
        truncated,
    })
}

fn bind_params<'q>(
    sql: &'q str,
    params: &'q [JsonValue],
) -> Result<Query<'q, Postgres, sqlx::postgres::PgArguments>> {
    let mut query = sqlx::query(sql);
    for value in params {
        query = bind_one(query, value)?;
    }
    Ok(query)
}

fn bind_one<'q>(
    query: Query<'q, Postgres, sqlx::postgres::PgArguments>,
    value: &'q JsonValue,
) -> Result<Query<'q, Postgres, sqlx::postgres::PgArguments>> {
    let bound = match value {
        JsonValue::Null => query.bind(Option::<String>::None),
        JsonValue::Bool(v) => query.bind(*v),
        JsonValue::Number(v) => {
            if let Some(i) = v.as_i64() {
                query.bind(i)
            } else if let Some(u) = v.as_u64() {
                let i = i64::try_from(u).map_err(|_| {
                    Error::InvalidRequest(format!("integer parameter out of i64 range: {u}"))
                })?;
                query.bind(i)
            } else if let Some(f) = v.as_f64() {
                query.bind(f)
            } else {
                return Err(Error::InvalidRequest(format!(
                    "unsupported numeric parameter value: {v}"
                )));
            }
        }
        JsonValue::String(v) => query.bind(v.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => query.bind(sqlx::types::Json(value.clone())),
    };

    Ok(bound)
}

fn response_columns(describe: &Describe<Postgres>) -> Vec<Column> {
    describe
        .columns()
        .iter()
        .map(|column| Column {
            name: column.name().to_owned(),
            sql_type: column.type_info().name().to_ascii_lowercase(),
        })
        .collect()
}

fn describe_parameter_count(describe: &Describe<Postgres>) -> Option<usize> {
    describe.parameters().map(|parameters| match parameters {
        Either::Left(types) => types.len(),
        Either::Right(count) => count,
    })
}

fn classify_sqlx_error(err: sqlx::Error, actual_parameter_count: usize) -> Error {
    match err {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(std::borrow::Cow::into_owned);
            let message = db_err.message().to_owned();

            if looks_like_parameter_mismatch(code.as_deref(), &message) {
                let expected = extract_expected_parameter_count(&message).unwrap_or(0);
                return Error::ParameterMismatch {
                    expected,
                    actual: actual_parameter_count,
                };
            }

            if code.as_deref().is_some_and(|value| value.starts_with("42")) {
                return Error::InvalidSql(message);
            }

            let rendered = match code {
                Some(code) => format!("sqlstate={code}: {message}"),
                None => message,
            };
            Error::UpstreamDb(rendered)
        }
        sqlx::Error::Io(io_err) => Error::UpstreamUnreachable(io_err.to_string()),
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Protocol(_) => {
            Error::UpstreamUnreachable(err.to_string())
        }
        sqlx::Error::Tls(tls_err) => Error::UpstreamUnreachable(tls_err.to_string()),
        sqlx::Error::Configuration(cfg_err) => Error::InvalidConfig(cfg_err.to_string()),
        sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_) => Error::Internal(err.to_string()),
        _ => Error::Internal(err.to_string()),
    }
}

fn looks_like_parameter_mismatch(code: Option<&str>, message: &str) -> bool {
    matches!(code, Some("08P01") | Some("42P02"))
        || message.contains("bind message supplies")
        || message.contains("there is no parameter $")
        || message.contains("prepared statement")
            && message.contains("requires")
            && message.contains("parameters")
}

fn extract_expected_parameter_count(message: &str) -> Option<usize> {
    if let Some(after_requires) = message.split("requires ").nth(1) {
        return after_requires
            .split_whitespace()
            .next()
            .and_then(|n| n.parse::<usize>().ok());
    }

    if let Some(after_expects) = message.split("expects ").nth(1) {
        return after_expects
            .split_whitespace()
            .next()
            .and_then(|n| n.parse::<usize>().ok());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameter_mismatch_detection_matches_sqlstate_and_message() {
        assert!(looks_like_parameter_mismatch(Some("08P01"), "irrelevant"));
        assert!(looks_like_parameter_mismatch(
            None,
            "bind message supplies 1 parameters, but prepared statement requires 2"
        ));
        assert!(!looks_like_parameter_mismatch(
            Some("23505"),
            "duplicate key"
        ));
    }

    #[test]
    fn extracts_expected_parameter_count() {
        let value = extract_expected_parameter_count(
            "bind message supplies 1 parameters, but prepared statement requires 2",
        );
        assert_eq!(value, Some(2));
    }
}
