//! Response model for `sql_postgres`.
//!
//! Query rows are represented as JSON objects keyed by column name,
//! while column metadata preserves original query column order.

use philharmonic_connector_impl_api::JsonValue;

/// One response row object (`column_name -> mapped JSON value`).
pub type SqlRow = serde_json::Map<String, JsonValue>;

/// SQL query response payload.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SqlQueryResponse {
    /// Returned rows mapped to JSON values.
    pub rows: Vec<SqlRow>,
    /// Number of rows represented by this response.
    pub row_count: u64,
    /// Ordered column metadata.
    pub columns: Vec<Column>,
    /// Whether `max_rows` truncation clipped an additional row.
    pub truncated: bool,
}

/// Metadata for one response column.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    /// Column name as reported by Postgres.
    pub name: String,
    /// Postgres SQL type name.
    pub sql_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_metadata_order_matches_row_key_insertion() {
        let mut row = SqlRow::new();
        row.insert("id".to_owned(), JsonValue::String("u_1".to_owned()));
        row.insert("name".to_owned(), JsonValue::String("Alice".to_owned()));

        let response = SqlQueryResponse {
            rows: vec![row],
            row_count: 1,
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    sql_type: "text".to_owned(),
                },
                Column {
                    name: "name".to_owned(),
                    sql_type: "text".to_owned(),
                },
            ],
            truncated: false,
        };

        let keys: Vec<String> = response.rows[0].keys().cloned().collect();
        let cols: Vec<String> = response.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(keys, cols);
    }
}
