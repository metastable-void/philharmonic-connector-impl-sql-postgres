//! Configuration model for `sql_postgres`.
//!
//! The config includes connection details and upper bounds used to cap
//! per-request overrides.

use crate::error::{Error, Result};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::str::FromStr;

/// Top-level configuration payload for the `sql_postgres` implementation.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPostgresConfig {
    /// SQLx PostgreSQL connection URL.
    pub connection_url: String,
    /// Maximum number of pooled connections for this config.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Default query timeout in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub default_timeout_ms: u64,
    /// Default maximum rows returned for row-producing queries.
    #[serde(default = "default_max_rows")]
    pub default_max_rows: usize,
}

/// Runtime-ready config with a prepared connection pool.
#[derive(Debug, Clone)]
pub struct PreparedConfig {
    /// Prepared Postgres connection pool.
    pub pool: PgPool,
    /// Default query timeout cap in milliseconds.
    pub default_timeout_ms: u64,
    /// Default row-count cap.
    pub default_max_rows: usize,
}

impl SqlPostgresConfig {
    /// Validates config and prepares a runtime pool.
    pub(crate) fn prepare(&self) -> Result<PreparedConfig> {
        // Accept both `postgres://` and `postgresql://` — sqlx
        // treats them as aliases per the libpq convention.
        // Harmonizes with the sibling `sql_mysql` crate's
        // accept-both-scheme-aliases approach.
        if !(self.connection_url.starts_with("postgres://")
            || self.connection_url.starts_with("postgresql://"))
        {
            return Err(Error::InvalidConfig(
                "connection_url must use postgres:// or postgresql:// scheme".to_owned(),
            ));
        }

        if self.max_connections == 0 {
            return Err(Error::InvalidConfig(
                "max_connections must be greater than 0".to_owned(),
            ));
        }

        if self.default_timeout_ms == 0 {
            return Err(Error::InvalidConfig(
                "default_timeout_ms must be greater than 0".to_owned(),
            ));
        }

        if self.default_max_rows == 0 {
            return Err(Error::InvalidConfig(
                "default_max_rows must be greater than 0".to_owned(),
            ));
        }

        let options = PgConnectOptions::from_str(&self.connection_url)
            .map_err(|e| Error::InvalidConfig(format!("invalid connection_url: {e}")))?;

        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect_lazy_with(options);

        Ok(PreparedConfig {
            pool,
            default_timeout_ms: self.default_timeout_ms,
            default_max_rows: self.default_max_rows,
        })
    }
}

const fn default_max_connections() -> u32 {
    10
}

const fn default_timeout_ms() -> u64 {
    30_000
}

const fn default_max_rows() -> usize {
    10_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_rejects_unknown_fields() {
        let value = json!({
            "connection_url": "postgres://user:pass@localhost:5432/db",
            "max_connections": 5,
            "default_timeout_ms": 5000,
            "default_max_rows": 100,
            "extra": true
        });

        let err = serde_json::from_value::<SqlPostgresConfig>(value).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn deserialize_applies_defaults() {
        let value = json!({
            "connection_url": "postgres://user:pass@localhost:5432/db"
        });

        let cfg = serde_json::from_value::<SqlPostgresConfig>(value).unwrap();
        assert_eq!(cfg.max_connections, 10);
        assert_eq!(cfg.default_timeout_ms, 30_000);
        assert_eq!(cfg.default_max_rows, 10_000);
    }

    // Scheme-accept tests are `#[tokio::test]` because sqlx's
    // `PgPoolOptions::connect_lazy_with(...)` touches
    // `sqlx::rt::spawn`, which requires a live tokio runtime
    // even though no connection is actually opened. Matches the
    // sibling `sql_mysql` crate's test pattern.

    #[tokio::test]
    async fn prepare_accepts_postgres_scheme() {
        let cfg = SqlPostgresConfig {
            connection_url: "postgres://user:pass@localhost:5432/db".to_owned(),
            max_connections: 5,
            default_timeout_ms: 5000,
            default_max_rows: 100,
        };

        let prepared = cfg.prepare();
        assert!(prepared.is_ok());
    }

    #[tokio::test]
    async fn prepare_accepts_postgresql_scheme_alias() {
        let cfg = SqlPostgresConfig {
            connection_url: "postgresql://user:pass@localhost:5432/db".to_owned(),
            max_connections: 5,
            default_timeout_ms: 5000,
            default_max_rows: 100,
        };

        let prepared = cfg.prepare();
        assert!(prepared.is_ok());
    }

    #[test]
    fn prepare_rejects_non_postgres_scheme() {
        let cfg = SqlPostgresConfig {
            connection_url: "mysql://user:pass@localhost:3306/db".to_owned(),
            max_connections: 5,
            default_timeout_ms: 5000,
            default_max_rows: 100,
        };

        let err = cfg.prepare().unwrap_err();
        assert_eq!(
            err,
            Error::InvalidConfig(
                "connection_url must use postgres:// or postgresql:// scheme".to_owned()
            )
        );
    }
}
