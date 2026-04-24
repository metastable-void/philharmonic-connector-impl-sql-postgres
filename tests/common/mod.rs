use philharmonic_connector_common::{UnixMillis, Uuid};
use philharmonic_connector_impl_api::{ConnectorCallContext, JsonValue};
use philharmonic_connector_impl_sql_postgres::SqlPostgres;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

pub type PostgresContainer = ContainerAsync<GenericImage>;

pub struct TestDb {
    pub _container: PostgresContainer,
    pub connection_url: String,
}

pub fn context() -> ConnectorCallContext {
    ConnectorCallContext {
        tenant_id: Uuid::nil(),
        instance_id: Uuid::nil(),
        step_seq: 1,
        config_uuid: Uuid::nil(),
        issued_at: UnixMillis(0),
        expires_at: UnixMillis(60_000),
    }
}

pub fn implementation() -> SqlPostgres {
    SqlPostgres::new()
}

pub fn config(connection_url: &str) -> JsonValue {
    serde_json::json!({
        "connection_url": connection_url,
        "max_connections": 8,
        "default_timeout_ms": 30_000,
        "default_max_rows": 10_000
    })
}

pub async fn maybe_start_postgres() -> Option<TestDb> {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await;

    let container = match container {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Docker unavailable for testcontainers, skipping test module: {err}");
            return None;
        }
    };

    let host = match container.get_host().await {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to get testcontainer host, skipping tests: {err}");
            return None;
        }
    };

    let port = match container.get_host_port_ipv4(5432.tcp()).await {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to get postgres mapped port, skipping tests: {err}");
            return None;
        }
    };

    let connection_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    if let Err(err) = wait_for_postgres(&connection_url, Duration::from_secs(20)).await {
        eprintln!("postgres container did not become ready, skipping tests: {err}");
        return None;
    }

    Some(TestDb {
        _container: container,
        connection_url,
    })
}

pub async fn connect_pool(url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
        .expect("test pool should connect")
}

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<(), sqlx::Error> {
    let deadline = Instant::now() + timeout;
    loop {
        match PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect(url)
            .await
        {
            Ok(pool) => {
                pool.close().await;
                return Ok(());
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
