# philharmonic-connector-impl-sql-postgres

Part of the Philharmonic workspace: https://github.com/metastable-void/philharmonic-workspace

`philharmonic-connector-impl-sql-postgres` provides the `sql_postgres` connector implementation for the `sql_query` capability. It validates decrypted connector config and request payloads, executes parameterized PostgreSQL SQL via `sqlx` (tokio + rustls), applies timeout and row-cap clamping rules from the connector wire protocol, maps SQL result values into the normalized JSON response shape (`rows`, `row_count`, `columns`, `truncated`), and maps execution failures into typed `ImplementationError` variants used by the connector service framework.

## Contributing

This crate is developed as a submodule of the Philharmonic
workspace. Workspace-wide development conventions — git workflow,
script wrappers, Rust code rules, versioning, terminology — live
in the workspace meta-repo at
[metastable-void/philharmonic-workspace](https://github.com/metastable-void/philharmonic-workspace),
authoritatively in its
[`CONTRIBUTING.md`](https://github.com/metastable-void/philharmonic-workspace/blob/main/CONTRIBUTING.md).

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
