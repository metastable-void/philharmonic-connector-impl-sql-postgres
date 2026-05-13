use dockerlet::{Container, GenericImage, IntoContainerPort, WaitFor};
use philharmonic_connector_common::{UnixMillis, Uuid};
use philharmonic_connector_impl_api::{ConnectorCallContext, JsonValue};
use philharmonic_connector_impl_sql_postgres::SqlPostgres;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

/// Warm shared Postgres container; same pattern as the MySQL
/// consumers (D23 round 01 follow-up, 2026-05-13). Per-test
/// isolation by unique database name.
static SHARED_POSTGRES: OnceCell<Option<SharedPostgres>> = OnceCell::const_new();

struct SharedPostgres {
    _container: Container,
    /// `postgres://postgres:postgres@{host}:{port}` (no path).
    base_url: String,
}

async fn shared_postgres() -> Option<&'static SharedPostgres> {
    let cached = SHARED_POSTGRES
        .get_or_init(|| async {
            let container = match GenericImage::new("postgres", "16-alpine")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_DB", "postgres")
                .with_startup_timeout(Duration::from_secs(180))
                .start()
                .await
            {
                Ok(c) => c,
                Err(err) => {
                    eprintln!("Docker unavailable for dockerlet, skipping test module: {err}");
                    return None;
                }
            };

            let host = match container.get_host().await {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("failed to get dockerlet host, skipping tests: {err}");
                    return None;
                }
            };
            let port = match container.get_host_port_ipv4(5432.tcp()).await {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("failed to get postgres mapped port, skipping tests: {err}");
                    return None;
                }
            };
            let base_url = format!("postgres://postgres:postgres@{host}:{port}");
            let admin_url = format!("{base_url}/postgres");

            // Postgres prints "ready to accept connections" several
            // times during startup (once on the internal socket,
            // again on the TCP socket); wait until a TCP connect
            // genuinely succeeds before handing out a base URL.
            if let Err(err) = wait_for_postgres(&admin_url, Duration::from_secs(30)).await {
                eprintln!("postgres container did not become ready, skipping tests: {err}");
                return None;
            }

            Some(SharedPostgres {
                _container: container,
                base_url,
            })
        })
        .await;
    cached.as_ref()
}

fn unique_db_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("dl_t_{}_{n}", std::process::id())
}

pub struct TestDb {
    db_name: String,
    pub connection_url: String,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let db_name = std::mem::take(&mut self.db_name);
        let base_url = SHARED_POSTGRES
            .get()
            .and_then(|opt| opt.as_ref())
            .map(|s| s.base_url.clone())
            .unwrap_or_default();
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            runtime.block_on(async move {
                let admin_url = format!("{base_url}/postgres");
                if let Ok(pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect(&admin_url)
                    .await
                {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{db_name}\""))
                        .execute(&pool)
                        .await;
                }
            });
        });
    }
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
    let shared = shared_postgres().await?;
    let db_name = unique_db_name();

    let admin_pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!("{}/postgres", shared.base_url))
        .await
    {
        Ok(pool) => pool,
        Err(err) => {
            eprintln!("failed to acquire admin pool, skipping tests: {err}");
            return None;
        }
    };
    if let Err(err) = sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin_pool)
        .await
    {
        eprintln!("failed to create per-test database, skipping tests: {err}");
        return None;
    }
    drop(admin_pool);

    let connection_url = format!("{}/{db_name}", shared.base_url);
    Some(TestDb {
        db_name,
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
