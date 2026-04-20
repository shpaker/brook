//! Сборка и жизненный цикл демона `brookd`.
//!
//! Вынесено из `main.rs`, чтобы интеграционные тесты могли запускать
//! демон напрямую, подсовывая свои `shutdown`-сигналы вместо
//! `SIGTERM`/`SIGINT`.
//!
//! ## Жизненный цикл
//!
//! ```text
//! build_runtime(paths)
//!     ├── .brook.lock (flock)
//!     ├── Settings::load_or_init
//!     ├── SqliteQueueRepository::open + миграции
//!     ├── HttpClientBuilder (один reqwest::Client → inspect + range)
//!     ├── LocalPieceStorageFactory
//!     └── DownloadManager::new + bootstrap()
//!
//! serve(runtime, shutdown_future)
//!     ├── tonic::Server::serve_with_shutdown   ← shutdown_future разблокирует
//!     └── DownloadManager::shutdown(30 s)      ← драйним engines до Paused
//! ```

use std::future::Future;
use std::net::SocketAddr;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{
    Context,
    Result,
    anyhow,
};
use brook_api::{
    BrookService,
    BrookServiceServer,
    trace_interceptor,
};
use brook_core::{
    DownloadManager,
    ManagerConfig,
};
use brook_http::{
    HttpClientBuilder,
    HttpInspectClient,
    RangeFetchClient,
};
use fs4::fs_std::FileExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{
    info,
    warn,
};

use crate::config::{
    DEFAULT_CONFIG_FILENAME,
    DaemonRuntime,
    Settings,
};
use crate::storage::factory::LocalPieceStorageFactory;
use crate::storage::queue::SqliteQueueRepository;

/// Имя lock-файла в CWD (гарантирует single-instance).
pub const LOCK_FILENAME: &str = ".brook.lock";
/// Имя БД очереди в CWD.
pub const DB_FILENAME: &str = "brook.db";
/// Дедлайн graceful-shutdown после приёма сигнала.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Конкретные типы адаптеров, с которыми параметризуется менеджер в
/// проде и интеграционных тестах.
pub type ProdFactory = LocalPieceStorageFactory<HttpInspectClient>;
pub type ProdQueue = SqliteQueueRepository;
pub type ProdFetch = RangeFetchClient;
pub type ProdManager = DownloadManager<ProdFactory, ProdQueue, ProdFetch>;
pub type ProdService = BrookService<ProdFactory, ProdQueue, ProdFetch>;

/// Пути к артефактам демона (БД, lock, конфиг). В проде всё в CWD; в
/// интеграционных тестах — в tempdir.
pub struct Paths {
    pub lock: PathBuf,
    pub db: PathBuf,
    pub config: PathBuf,
}

impl Paths {
    pub fn in_cwd() -> Self {
        Self::in_dir(Path::new("."))
    }

    pub fn in_dir(dir: &Path) -> Self {
        Self {
            lock: dir.join(LOCK_FILENAME),
            db: dir.join(DB_FILENAME),
            config: dir.join(DEFAULT_CONFIG_FILENAME),
        }
    }
}

/// Собранный рантайм демона. Живёт от `build_runtime` до конца `serve`.
pub struct Runtime {
    /// Держим файл на время работы демона — при drop flock снимается.
    lock: std::fs::File,
    #[allow(dead_code)] // пригодится, когда появится live-reload
    pub daemon: DaemonRuntime,
    pub manager: Arc<ProdManager>,
    pub addr: SocketAddr,
    pub svc: ProdService,
    /// Уже связанный listener. Биндим в `build_runtime`, чтобы падать
    /// раньше сервера и чтобы интеграционные тесты могли запросить
    /// ephemeral-порт (через `api.port = 0` в конфиге).
    pub listener: TcpListener,
}

pub async fn build_runtime(paths: &Paths) -> Result<Runtime> {
    // 1. Lock (блокирует второй запуск в том же CWD).
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&paths.lock)
        .with_context(|| format!("open lock {}", paths.lock.display()))?;
    FileExt::try_lock_exclusive(&lock).map_err(|_| {
        anyhow!(
            "another brookd instance is already running ({})",
            paths.lock.display()
        )
    })?;

    // 2. Config.
    let settings = Settings::load_or_init(&paths.config)
        .with_context(|| format!("load config {}", paths.config.display()))?;
    let daemon = DaemonRuntime::from_settings(&settings).context("derive daemon runtime")?;

    // 3. Queue.
    let queue = Arc::new(SqliteQueueRepository::open(&paths.db).context("open queue database")?);

    // 4. HTTP-стек (один reqwest::Client на inspect + range).
    let http_client = HttpClientBuilder::new().build();
    let inspect = Arc::new(HttpInspectClient::new(http_client.clone()));
    let fetch = Arc::new(RangeFetchClient::new(http_client));

    // 5. Factory.
    let factory = Arc::new(LocalPieceStorageFactory::new(
        Arc::clone(&inspect),
        daemon.defaults,
    ));

    // 6. Manager + bootstrap.
    let manager_cfg = ManagerConfig {
        max_concurrent: daemon.max_concurrent,
        ..Default::default()
    };
    let manager = Arc::new(DownloadManager::new(factory, queue, fetch, manager_cfg));
    manager.bootstrap().await.context("manager bootstrap")?;

    let bind_addr = SocketAddr::new(daemon.api_bind, daemon.api_port);
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("bind gRPC listener on {bind_addr}"))?;
    let addr = listener.local_addr().context("local_addr")?;
    let svc = BrookService::new(Arc::clone(&manager));

    Ok(Runtime {
        lock,
        daemon,
        manager,
        addr,
        svc,
        listener,
    })
}

/// Запустить gRPC-сервер и корректно остановить менеджер после сигнала.
///
/// `shutdown` — будущее, которое разрешается, когда внешняя сторона
/// хочет завершить процесс (в `main` — `SIGTERM`/`SIGINT`; в тестах —
/// `oneshot`).
pub async fn serve(runtime: Runtime, shutdown: impl Future<Output = ()> + Send) -> Result<()> {
    let Runtime {
        lock,
        manager,
        addr,
        svc,
        listener,
        ..
    } = runtime;

    let incoming = TcpListenerStream::new(listener);
    let server = tonic::transport::Server::builder()
        .add_service(BrookServiceServer::with_interceptor(svc, trace_interceptor))
        .serve_with_incoming_shutdown(incoming, shutdown);

    info!(%addr, "brookd listening");
    server.await.context("grpc server")?;
    info!("shutdown signal received — draining engines");

    if let Err(e) = manager.shutdown(SHUTDOWN_DEADLINE).await {
        warn!(error = %e, "manager shutdown did not drain within deadline");
    }

    drop(lock); // явно: flock снимается здесь, а не в конце main.
    Ok(())
}

/// Фьюча, которая разрешается по приходу `SIGTERM` или `SIGINT`.
#[cfg(unix)]
pub fn shutdown_signal() -> impl Future<Output = ()> + Send {
    use tokio::signal::unix::{
        SignalKind,
        signal,
    };
    async {
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv()  => info!("received SIGINT"),
        }
    }
}
