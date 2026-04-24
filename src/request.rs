//! Request model for `sql_postgres`.
//!
//! The wire form is direct JSON with per-call optional overrides that
//! are clamped to config defaults.

use crate::error::{Error, Result};
use philharmonic_connector_impl_api::JsonValue;
use std::time::Duration;

/// One SQL query request payload.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    /// SQL text using PostgreSQL placeholder syntax (`$1`, `$2`, ...).
    pub sql: String,
    /// Positional parameter values.
    #[serde(default)]
    pub params: Vec<JsonValue>,
    /// Optional per-request row cap (clamped to config default).
    #[serde(default)]
    pub max_rows: Option<usize>,
    /// Optional per-request timeout in milliseconds (clamped to config default).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Effective runtime limits after request/config clamping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveLimits {
    pub(crate) max_rows: usize,
    pub(crate) timeout: Duration,
}

impl SqlQueryRequest {
    /// Validates request invariants and computes effective runtime limits.
    pub(crate) fn effective_limits(
        &self,
        default_max_rows: usize,
        default_timeout_ms: u64,
    ) -> Result<EffectiveLimits> {
        if self.sql.trim().is_empty() {
            return Err(Error::InvalidRequest("sql must not be empty".to_owned()));
        }

        let requested_max_rows = self.max_rows.unwrap_or(default_max_rows);
        if requested_max_rows == 0 {
            return Err(Error::InvalidRequest(
                "max_rows must be greater than 0".to_owned(),
            ));
        }
        let max_rows = requested_max_rows.min(default_max_rows);

        let requested_timeout_ms = self.timeout_ms.unwrap_or(default_timeout_ms);
        if requested_timeout_ms == 0 {
            return Err(Error::InvalidRequest(
                "timeout_ms must be greater than 0".to_owned(),
            ));
        }
        let timeout_ms = requested_timeout_ms.min(default_timeout_ms);

        Ok(EffectiveLimits {
            max_rows,
            timeout: Duration::from_millis(timeout_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_request_shape() {
        let value = json!({
            "sql": "SELECT $1::text",
            "params": ["abc"],
            "max_rows": 50,
            "timeout_ms": 2000
        });

        let req = serde_json::from_value::<SqlQueryRequest>(value).unwrap();
        assert_eq!(req.sql, "SELECT $1::text");
        assert_eq!(req.params.len(), 1);
        assert_eq!(req.max_rows, Some(50));
        assert_eq!(req.timeout_ms, Some(2000));
    }

    #[test]
    fn deserialize_rejects_unknown_fields() {
        let value = json!({
            "sql": "SELECT 1",
            "params": [],
            "unknown": true
        });

        let err = serde_json::from_value::<SqlQueryRequest>(value).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn effective_limits_clamp_to_config_caps() {
        let req = SqlQueryRequest {
            sql: "SELECT 1".to_owned(),
            params: Vec::new(),
            max_rows: Some(50_000),
            timeout_ms: Some(60_000),
        };

        let limits = req.effective_limits(10_000, 30_000).unwrap();
        assert_eq!(limits.max_rows, 10_000);
        assert_eq!(limits.timeout, Duration::from_millis(30_000));
    }

    #[test]
    fn effective_limits_reject_zero_overrides() {
        let max_rows_zero = SqlQueryRequest {
            sql: "SELECT 1".to_owned(),
            params: Vec::new(),
            max_rows: Some(0),
            timeout_ms: None,
        };
        let err = max_rows_zero.effective_limits(10, 1000).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidRequest("max_rows must be greater than 0".to_owned())
        );

        let timeout_zero = SqlQueryRequest {
            sql: "SELECT 1".to_owned(),
            params: Vec::new(),
            max_rows: None,
            timeout_ms: Some(0),
        };
        let err = timeout_zero.effective_limits(10, 1000).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidRequest("timeout_ms must be greater than 0".to_owned())
        );
    }
}
