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
//!     ├── SharedDb::open + миграции + SqliteFileRepository
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
    ApiSettings,
    BrookService,
    BrookServiceServer,
    trace_interceptor,
};
use brook_core::{
    DownloadManager,
    ManagerConfig,
    TPieceAttemptRepo,
    TWorkerRepo,
};
use brook_http::{
    HttpClientBuilder,
    HttpInspectClient,
    RangeFetchClient,
};
use fs4::fs_std::FileExt;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{
    info,
    warn,
};

use crate::config::{
    DEFAULT_CONFIG_FILENAME,
    DaemonRuntime,
    OnDuplicateUrl,
    OnFileExists,
    Settings,
};
use crate::storage::db::SharedDb;
use crate::storage::factory::LocalPieceStorageFactory;
use crate::storage::files::SqliteFileRepository;
use crate::storage::piece_attempts::SqlitePieceAttemptRepository;
use crate::storage::pieces::SqlitePieceRepository;
use crate::storage::workers::SqliteWorkerRepository;

/// Имя lock-файла в CWD (гарантирует single-instance).
pub const LOCK_FILENAME: &str = ".brook.lock";
/// Имя БД очереди в CWD.
pub const DB_FILENAME: &str = "brook.db";
/// Дедлайн graceful-shutdown после приёма сигнала.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Конкретные типы адаптеров, с которыми параметризуется менеджер в
/// проде и интеграционных тестах.
pub type ProdFactory = LocalPieceStorageFactory<HttpInspectClient>;
pub type ProdQueue = SqliteFileRepository;
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
    pub daemon: DaemonRuntime,
    pub manager: Arc<ProdManager>,
    pub addr: SocketAddr,
    pub svc: ProdService,
    /// Уже связанный listener. Биндим в `build_runtime`, чтобы падать
    /// раньше сервера и чтобы интеграционные тесты могли запросить
    /// ephemeral-порт (через `api.port = 0` в конфиге).
    pub listener: TcpListener,
    /// Ресивер от `Shutdown` RPC. `serve` селектит его с внешним
    /// `shutdown`-фьючей (в проде — `SIGTERM`/`SIGINT`).
    pub rpc_shutdown_rx: broadcast::Receiver<()>,
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

    // 3. Repositories (поверх общего `brook.db`).
    let shared_db = SharedDb::open(&paths.db).context("open brook.db")?;
    let files_repo = Arc::new(SqliteFileRepository::new(shared_db.clone()));
    let pieces_repo = Arc::new(SqlitePieceRepository::new(shared_db.clone()));
    let workers_repo = Arc::new(SqliteWorkerRepository::new(shared_db.clone()));
    let attempts_repo = Arc::new(SqlitePieceAttemptRepository::new(shared_db));
    let queue = Arc::clone(&files_repo);

    // 3a. Startup recovery: любые `running`-воркеры и `running`-attempt'ы,
    // оставшиеся от предыдущего (возможно, упавшего) инстанса, переводим в
    // `paused`. Делаем это под `.brook.lock` — единственный раз за жизнь
    // процесса, до того, как появится шанс породить новый engine.
    workers_repo
        .pause_all_running_globally()
        .await
        .context("workers recovery sweep")?;
    attempts_repo
        .pause_all_running_globally()
        .await
        .context("piece_attempts recovery sweep")?;

    // 4. HTTP-стек (один reqwest::Client на inspect + range).
    let http_client = HttpClientBuilder::new().build();
    let inspect = Arc::new(HttpInspectClient::new(http_client.clone()));
    let fetch = Arc::new(RangeFetchClient::new(http_client));

    // 5. Factory.
    let factory = Arc::new(LocalPieceStorageFactory::new(
        Arc::clone(&inspect),
        daemon.defaults,
        pieces_repo,
        Arc::clone(&files_repo),
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
    let (rpc_shutdown_tx, rpc_shutdown_rx) = broadcast::channel(1);
    let svc = BrookService::new(Arc::clone(&manager), api_settings(&daemon), rpc_shutdown_tx);

    Ok(Runtime {
        lock,
        daemon,
        manager,
        addr,
        svc,
        listener,
        rpc_shutdown_rx,
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
        mut rpc_shutdown_rx,
        ..
    } = runtime;

    // Объединяем внешний shutdown (сигналы в проде) с RPC-триггером.
    // `recv()` на broadcast-ресивере может вернуть Lagged, но у нас
    // единственный sender и канал ёмкостью 1 — любое событие валидно
    // трактуется как «пора гаситься».
    let combined_shutdown = async move {
        tokio::select! {
            _ = shutdown => {}
            _ = rpc_shutdown_rx.recv() => {
                info!("received Shutdown RPC");
            }
        }
    };

    let incoming = TcpListenerStream::new(listener);
    let server = tonic::transport::Server::builder()
        .add_service(BrookServiceServer::with_interceptor(svc, trace_interceptor))
        .serve_with_incoming_shutdown(incoming, combined_shutdown);

    info!(%addr, "brookd listening");
    server.await.context("grpc server")?;
    info!("shutdown signal received — draining engines");

    if let Err(e) = manager.shutdown(SHUTDOWN_DEADLINE).await {
        warn!(error = %e, "manager shutdown did not drain within deadline");
    }

    drop(lock); // явно: flock снимается здесь, а не в конце main.
    Ok(())
}

/// Перевод `DaemonRuntime` в транспортный снимок для `BrookService::GetSettings`.
/// Здесь же — маппинг YAML-enum'ов в proto-эквиваленты.
fn api_settings(rt: &DaemonRuntime) -> ApiSettings {
    use brook_proto::brook::v1 as proto;
    ApiSettings {
        default_dir: rt.default_dir.to_string_lossy().into_owned(),
        default_workers: rt.defaults.workers,
        max_workers: rt.max_workers,
        max_concurrent: rt.max_concurrent as u32,
        piece_target_count: rt.defaults.piece_target_count,
        piece_size_min: rt.defaults.piece_size_min,
        piece_size_max: rt.defaults.piece_size_max,
        on_duplicate_url: match rt.on_duplicate_url {
            OnDuplicateUrl::Ask => proto::OnDuplicateUrlPolicy::Ask,
            OnDuplicateUrl::Skip => proto::OnDuplicateUrlPolicy::Skip,
            OnDuplicateUrl::Add => proto::OnDuplicateUrlPolicy::Add,
        },
        on_file_exists: match rt.on_file_exists {
            OnFileExists::Ask => proto::OnFileExistsPolicy::Ask,
            OnFileExists::Rename => proto::OnFileExistsPolicy::Rename,
            OnFileExists::Overwrite => proto::OnFileExistsPolicy::Overwrite,
        },
    }
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
