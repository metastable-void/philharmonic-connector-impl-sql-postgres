//! SQL-to-JSON conversion for `sql_postgres`.

use crate::error::{Error, Result};
use base64::Engine;
use philharmonic_connector_impl_api::JsonValue;
use sqlx::{Row, ValueRef};
use sqlx::{postgres::PgRow, types::chrono};

pub(crate) fn decode_cell(
    row: &PgRow,
    index: usize,
    column_name: &str,
    sql_type: &str,
) -> Result<JsonValue> {
    let raw = row
        .try_get_raw(index)
        .map_err(|e| Error::Internal(format!("failed to read column '{column_name}': {e}")))?;
    if raw.is_null() {
        return Ok(JsonValue::Null);
    }

    let normalized = normalize_type_name(sql_type);
    if let Some(array_element) = array_element_type(&normalized) {
        return decode_array_cell(row, index, column_name, array_element, &normalized);
    }

    match normalized.as_str() {
        "int2" => integer_to_json(
            column_name,
            i128::from(try_get::<i16>(row, index, column_name, sql_type)?),
        ),
        "int4" => integer_to_json(
            column_name,
            i128::from(try_get::<i32>(row, index, column_name, sql_type)?),
        ),
        "int8" => integer_to_json(
            column_name,
            i128::from(try_get::<i64>(row, index, column_name, sql_type)?),
        ),
        "oid" => integer_to_json(
            column_name,
            i128::from(try_get::<sqlx::postgres::types::Oid>(row, index, column_name, sql_type)?.0),
        ),

        "float4" => float_to_json(f64::from(try_get::<f32>(
            row,
            index,
            column_name,
            sql_type,
        )?)),
        "float8" => float_to_json(try_get::<f64>(row, index, column_name, sql_type)?),

        "numeric" | "decimal" => numeric_string_to_json(
            column_name,
            try_get::<sqlx::types::BigDecimal>(row, index, column_name, sql_type)?.to_string(),
        ),

        "bool" => Ok(JsonValue::Bool(try_get::<bool>(
            row,
            index,
            column_name,
            sql_type,
        )?)),

        "text" | "varchar" | "bpchar" | "char" | "name" | "citext" => Ok(JsonValue::String(
            try_get::<String>(row, index, column_name, sql_type)?,
        )),

        "uuid" => Ok(JsonValue::String(
            try_get::<sqlx::types::Uuid>(row, index, column_name, sql_type)?.to_string(),
        )),

        "bytea" => Ok(JsonValue::String(bytes_to_base64(&try_get::<Vec<u8>>(
            row,
            index,
            column_name,
            sql_type,
        )?))),

        "date" => Ok(JsonValue::String(
            try_get::<chrono::NaiveDate>(row, index, column_name, sql_type)?.to_string(),
        )),

        "time" => Ok(JsonValue::String(
            try_get::<chrono::NaiveTime>(row, index, column_name, sql_type)?.to_string(),
        )),

        // `timetz` is deliberately NOT a first-class arm in v1.
        // Doc 08 §"SQL-to-JSON type mapping" does not list it and
        // sqlx-postgres 0.8's `PgTimeTz` carrier doesn't implement
        // `Display`, so any serialization we synthesized here would
        // be an invented wire format. It falls through to the
        // catch-all (text coercion) instead; deployments that need
        // a structured `timetz` shape can add a dedicated arm in a
        // minor version bump once the wire format is decided.
        "timestamp" => {
            let value = try_get::<chrono::NaiveDateTime>(row, index, column_name, sql_type)?;
            Ok(JsonValue::String(value.and_utc().to_rfc3339()))
        }

        "timestamptz" => Ok(JsonValue::String(
            try_get::<chrono::DateTime<chrono::Utc>>(row, index, column_name, sql_type)?
                .to_rfc3339(),
        )),

        "json" | "jsonb" => Ok(try_get::<JsonValue>(row, index, column_name, sql_type)?),

        _ => {
            if let Ok(value) = row.try_get::<String, _>(index) {
                return Ok(JsonValue::String(value));
            }
            Err(Error::Internal(format!(
                "unsupported sql type '{normalized}' for column '{column_name}'"
            )))
        }
    }
}

fn decode_array_cell(
    row: &PgRow,
    index: usize,
    column_name: &str,
    base_type: &str,
    sql_type: &str,
) -> Result<JsonValue> {
    match base_type {
        "int2" => int_array_to_json(
            column_name,
            try_get::<Vec<i16>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(i128::from)
                .collect(),
        ),
        "int4" => int_array_to_json(
            column_name,
            try_get::<Vec<i32>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(i128::from)
                .collect(),
        ),
        "int8" => int_array_to_json(
            column_name,
            try_get::<Vec<i64>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(i128::from)
                .collect(),
        ),
        "float4" => float_array_to_json(
            try_get::<Vec<f32>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(f64::from)
                .collect(),
        ),
        "float8" => float_array_to_json(try_get::<Vec<f64>>(row, index, column_name, sql_type)?),
        "bool" => Ok(JsonValue::Array(
            try_get::<Vec<bool>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(JsonValue::Bool)
                .collect(),
        )),
        "text" | "varchar" | "bpchar" | "char" | "name" | "citext" => Ok(JsonValue::Array(
            try_get::<Vec<String>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(JsonValue::String)
                .collect(),
        )),
        "uuid" => Ok(JsonValue::Array(
            try_get::<Vec<sqlx::types::Uuid>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(v.to_string()))
                .collect(),
        )),
        "bytea" => Ok(JsonValue::Array(
            try_get::<Vec<Vec<u8>>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(bytes_to_base64(&v)))
                .collect(),
        )),
        "date" => Ok(JsonValue::Array(
            try_get::<Vec<chrono::NaiveDate>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(v.to_string()))
                .collect(),
        )),
        "time" => Ok(JsonValue::Array(
            try_get::<Vec<chrono::NaiveTime>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(v.to_string()))
                .collect(),
        )),
        "timestamp" => Ok(JsonValue::Array(
            try_get::<Vec<chrono::NaiveDateTime>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(v.and_utc().to_rfc3339()))
                .collect(),
        )),
        "timestamptz" => Ok(JsonValue::Array(
            try_get::<Vec<chrono::DateTime<chrono::Utc>>>(row, index, column_name, sql_type)?
                .into_iter()
                .map(|v| JsonValue::String(v.to_rfc3339()))
                .collect(),
        )),
        "json" | "jsonb" => Ok(JsonValue::Array(
            try_get::<Vec<JsonValue>>(row, index, column_name, sql_type)?
                .into_iter()
                .collect(),
        )),
        "numeric" | "decimal" => {
            let values =
                try_get::<Vec<sqlx::types::BigDecimal>>(row, index, column_name, sql_type)?;
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(numeric_string_to_json(column_name, value.to_string())?);
            }
            Ok(JsonValue::Array(out))
        }
        _ => Err(Error::Internal(format!(
            "unsupported sql array type '{base_type}[]' for column '{column_name}'"
        ))),
    }
}

fn normalize_type_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn array_element_type(normalized: &str) -> Option<&str> {
    if let Some(stripped) = normalized.strip_prefix('_') {
        return Some(stripped);
    }
    normalized.strip_suffix("[]")
}

fn try_get<T>(row: &PgRow, index: usize, column_name: &str, sql_type: &str) -> Result<T>
where
    for<'r> T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(index).map_err(|e| {
        Error::Internal(format!(
            "failed to decode column '{column_name}' as '{sql_type}': {e}"
        ))
    })
}

fn integer_to_json(column_name: &str, value: i128) -> Result<JsonValue> {
    let narrowed = i64::try_from(value).map_err(|_| Error::IntegerOverflow {
        column: column_name.to_owned(),
        value: value.to_string(),
    })?;
    Ok(JsonValue::Number(serde_json::Number::from(narrowed)))
}

fn float_to_json(value: f64) -> Result<JsonValue> {
    let number = serde_json::Number::from_f64(value)
        .ok_or_else(|| Error::Internal("non-finite floating point value".to_owned()))?;
    Ok(JsonValue::Number(number))
}

fn int_array_to_json(column_name: &str, values: Vec<i128>) -> Result<JsonValue> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(integer_to_json(column_name, value)?);
    }
    Ok(JsonValue::Array(out))
}

fn float_array_to_json(values: Vec<f64>) -> Result<JsonValue> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(float_to_json(value)?);
    }
    Ok(JsonValue::Array(out))
}

fn numeric_string_to_json(column_name: &str, value: String) -> Result<JsonValue> {
    maybe_integer_overflow(column_name, &value)?;
    Ok(JsonValue::String(value))
}

fn maybe_integer_overflow(column_name: &str, value: &str) -> Result<()> {
    if !looks_integral(value) {
        return Ok(());
    }

    if value.parse::<i64>().is_ok() {
        return Ok(());
    }

    Err(Error::IntegerOverflow {
        column: column_name.to_owned(),
        value: value.to_owned(),
    })
}

fn looks_integral(value: &str) -> bool {
    let trimmed = value.trim();
    let digits = if let Some(rest) = trimmed.strip_prefix('-') {
        rest
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        rest
    } else {
        trimmed
    };

    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

fn bytes_to_base64(value: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integer_mapping_boundaries() {
        assert_eq!(
            integer_to_json("n", i128::from(i64::MIN)).unwrap(),
            json!(i64::MIN)
        );
        assert_eq!(
            integer_to_json("n", i128::from(i64::MAX)).unwrap(),
            json!(i64::MAX)
        );

        let err = integer_to_json("n", i128::from(i64::MAX) + 1).unwrap_err();
        assert_eq!(
            err,
            Error::IntegerOverflow {
                column: "n".to_owned(),
                value: "9223372036854775808".to_owned(),
            }
        );
    }

    #[test]
    fn floating_mapping_to_json_number() {
        assert_eq!(float_to_json(1.5).unwrap(), json!(1.5));
    }

    #[test]
    fn numeric_mapping_keeps_string_precision() {
        assert_eq!(
            numeric_string_to_json("price", "123.4500".to_owned()).unwrap(),
            json!("123.4500")
        );
    }

    #[test]
    fn bytea_mapping_base64_encodes() {
        assert_eq!(bytes_to_base64(&[0x61, 0x62, 0x63]), "YWJj");
    }

    #[test]
    fn timestamp_string_mapping_format() {
        let value = chrono::NaiveDate::from_ymd_opt(2025, 3, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(value.and_utc().to_rfc3339(), "2025-03-01T12:00:00+00:00");
    }

    #[test]
    fn json_and_null_passthrough_helpers() {
        assert_eq!(JsonValue::Null, json!(null));
        assert_eq!(JsonValue::Bool(true), json!(true));
    }

    #[test]
    fn array_integer_mapping() {
        let mapped = int_array_to_json("id", vec![1, 2, 3]).unwrap();
        assert_eq!(mapped, json!([1, 2, 3]));
    }

    #[test]
    fn array_float_mapping() {
        let mapped = float_array_to_json(vec![1.0, 2.5]).unwrap();
        assert_eq!(mapped, json!([1.0, 2.5]));
    }

    #[test]
    fn integral_numeric_out_of_range_maps_to_overflow() {
        let err = numeric_string_to_json("big", "9223372036854775808".to_owned()).unwrap_err();
        assert_eq!(
            err,
            Error::IntegerOverflow {
                column: "big".to_owned(),
                value: "9223372036854775808".to_owned(),
            }
        );
    }

    #[test]
    fn array_type_detection_handles_postgres_forms() {
        assert_eq!(array_element_type("_int8"), Some("int8"));
        assert_eq!(array_element_type("text[]"), Some("text"));
        assert_eq!(array_element_type("text"), None);
    }
}
