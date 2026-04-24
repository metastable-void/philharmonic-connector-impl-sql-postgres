//! PostgreSQL SQL connector implementation for Philharmonic.
//!
//! `sql_postgres` implements the shared
//! [`philharmonic_connector_impl_api::Implementation`] trait for the
//! `sql_query` connector capability described in
//! `docs/design/08-connector-architecture.md`.
//!
//! The implementation receives decrypted `config` and `request` JSON
//! objects, validates and clamps query limits, executes parameterized
//! SQL through `sqlx` (PostgreSQL + tokio + rustls), and returns the
//! normalized response shape `{rows, row_count, columns, truncated}`.
//!
//! ## Notes
//!
//! - Placeholder syntax is PostgreSQL-native (`$1`, `$2`, ...).
//! - The connector never interpolates SQL strings; all parameters are
//!   bound using `sqlx::query(...).bind(...)`.
//! - Connector-layer retries are intentionally absent for SQL.
//! - Numeric/decimal SQL values are serialized as JSON strings to avoid
//!   floating-point precision loss.
//!
//! ## Example
//!
//! ```ignore
//! use philharmonic_connector_impl_api::{ConnectorCallContext, Implementation, JsonValue};
//! use philharmonic_connector_impl_sql_postgres::SqlPostgres;
//! use serde_json::json;
//!
//! async fn run_one(impl_: &SqlPostgres, ctx: &ConnectorCallContext) -> Result<JsonValue, philharmonic_connector_impl_api::ImplementationError> {
//!     impl_.execute(
//!         &json!({
//!             "connection_url": "postgres://user:pass@db.example:5432/app",
//!             "max_connections": 10,
//!             "default_timeout_ms": 30_000,
//!             "default_max_rows": 10_000
//!         }),
//!         &json!({
//!             "sql": "SELECT id, email FROM users WHERE tenant_id = $1",
//!             "params": ["t_123"],
//!             "max_rows": 500,
//!             "timeout_ms": 5_000
//!         }),
//!         ctx,
//!     ).await
//! }
//! ```

mod config;
mod error;
mod execute;
mod request;
mod response;
mod types;

pub use crate::config::{PreparedConfig, SqlPostgresConfig};
pub use crate::request::SqlQueryRequest;
pub use crate::response::{Column, SqlQueryResponse, SqlRow};
pub use philharmonic_connector_impl_api::{
    ConnectorCallContext, Implementation, ImplementationError, JsonValue, async_trait,
};

const NAME: &str = "sql_postgres";

/// `sql_postgres` connector implementation.
#[derive(Clone, Debug, Default)]
pub struct SqlPostgres;

impl SqlPostgres {
    /// Builds a new `sql_postgres` implementation value.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Implementation for SqlPostgres {
    fn name(&self) -> &str {
        NAME
    }

    async fn execute(
        &self,
        config: &JsonValue,
        request: &JsonValue,
        _ctx: &ConnectorCallContext,
    ) -> Result<JsonValue, ImplementationError> {
        let config: SqlPostgresConfig = serde_json::from_value(config.clone())
            .map_err(|e| error::Error::InvalidConfig(e.to_string()))
            .map_err(ImplementationError::from)?;

        let prepared = config.prepare().map_err(ImplementationError::from)?;

        let request: SqlQueryRequest = serde_json::from_value(request.clone())
            .map_err(|e| error::Error::InvalidRequest(e.to_string()))
            .map_err(ImplementationError::from)?;

        let response = execute::execute_query(&prepared, &request)
            .await
            .map_err(ImplementationError::from)?;

        serde_json::to_value(response)
            .map_err(|e| error::Error::Internal(e.to_string()))
            .map_err(ImplementationError::from)
    }
}
