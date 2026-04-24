//! Internal error model for `sql_postgres`.
//!
//! This module captures implementation-local error variants and maps
//! them to connector-wide `ImplementationError` values.

use philharmonic_connector_impl_api::ImplementationError;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub(crate) enum Error {
    #[error("{0}")]
    InvalidConfig(String),

    #[error("{0}")]
    InvalidRequest(String),

    #[error("{0}")]
    InvalidSql(String),

    #[error("parameter count mismatch: expected {expected}, got {actual}")]
    ParameterMismatch { expected: usize, actual: usize },

    #[error("{0}")]
    UpstreamDb(String),

    #[error("upstream timeout")]
    UpstreamTimeout,

    #[error("{0}")]
    UpstreamUnreachable(String),

    #[error("integer overflow in column '{column}': {value}")]
    IntegerOverflow { column: String, value: String },

    #[error("{0}")]
    Internal(String),
}

impl From<Error> for ImplementationError {
    fn from(value: Error) -> Self {
        match value {
            Error::InvalidConfig(detail) => ImplementationError::InvalidConfig { detail },
            Error::InvalidRequest(detail) | Error::InvalidSql(detail) => {
                ImplementationError::InvalidRequest { detail }
            }
            Error::ParameterMismatch { expected, actual } => ImplementationError::InvalidRequest {
                detail: format!("parameter count mismatch: expected {expected}, got {actual}"),
            },
            Error::UpstreamDb(detail) => ImplementationError::UpstreamError {
                status: 500,
                body: detail,
            },
            Error::UpstreamTimeout => ImplementationError::UpstreamTimeout,
            Error::UpstreamUnreachable(detail) => {
                ImplementationError::UpstreamUnreachable { detail }
            }
            Error::IntegerOverflow { column, value } => ImplementationError::UpstreamError {
                status: 500,
                body: format!("integer overflow in column '{column}': {value}"),
            },
            Error::Internal(detail) => ImplementationError::Internal { detail },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_internal_variant_maps_to_wire() {
        assert_eq!(
            ImplementationError::from(Error::InvalidConfig("cfg".to_owned())),
            ImplementationError::InvalidConfig {
                detail: "cfg".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::InvalidRequest("req".to_owned())),
            ImplementationError::InvalidRequest {
                detail: "req".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::InvalidSql("syntax".to_owned())),
            ImplementationError::InvalidRequest {
                detail: "syntax".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::ParameterMismatch {
                expected: 2,
                actual: 1
            }),
            ImplementationError::InvalidRequest {
                detail: "parameter count mismatch: expected 2, got 1".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamDb("db failed".to_owned())),
            ImplementationError::UpstreamError {
                status: 500,
                body: "db failed".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamTimeout),
            ImplementationError::UpstreamTimeout
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamUnreachable("no route".to_owned())),
            ImplementationError::UpstreamUnreachable {
                detail: "no route".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::IntegerOverflow {
                column: "count".to_owned(),
                value: "999999999999999999999".to_owned(),
            }),
            ImplementationError::UpstreamError {
                status: 500,
                body: "integer overflow in column 'count': 999999999999999999999".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::Internal("boom".to_owned())),
            ImplementationError::Internal {
                detail: "boom".to_owned(),
            }
        );
    }
}
