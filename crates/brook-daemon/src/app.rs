//! Сборка и жизненный цикл демона (`brook server`).
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
    AuthInterceptor,
    BrookService,
    BrookServiceServer,
};
use brook_core::{
    DownloadManager,
    ManagerConfig,
    TPathPolicy,
    TPieceAttemptRepo,
    TWorkerRepo,
};
use brook_http::{
    HttpClientBuilder,
    HttpInspectClient,
    RangeFetchClient,
};
use brook_runtime::constants::ENDPOINT_FILENAME;
use brook_runtime::{
    AppPaths,
    Endpoint,
};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{
    info,
    warn,
};

use crate::ServerArgs;
use crate::config::{
    DEFAULT_CONFIG_FILENAME,
    DaemonRuntime,
    Settings,
};
use crate::storage::db::SharedDb;
use crate::storage::factory::LocalPieceStorageFactory;
use crate::storage::files::SqliteFileRepository;
use crate::storage::piece_attempts::SqlitePieceAttemptRepository;
use crate::storage::pieces::SqlitePieceRepository;
use crate::storage::sandbox::{
    ClampedPathPolicy,
    OpenPathPolicy,
};
use crate::storage::workers::SqliteWorkerRepository;

/// Имя lock-файла (гарантирует single-instance демона на пользователя).
pub const LOCK_FILENAME: &str = ".brook.lock";
/// Имя БД очереди.
pub const DB_FILENAME: &str = "brook.db";
/// Дедлайн graceful-shutdown после приёма сигнала.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Конкретные типы адаптеров, с которыми параметризуется менеджер в
/// проде и интеграционных тестах.
///
/// Политика путей — `dyn TPathPolicy`: на старте выбираем между
/// `ClampedPathPolicy` (sandbox) и `OpenPathPolicy` (без sandbox), но в
/// `Runtime` хранится единый dyn-вид, чтобы не плодить две параметризации.
pub type ProdFactory = LocalPieceStorageFactory<HttpInspectClient, dyn TPathPolicy>;
pub type ProdQueue = SqliteFileRepository;
pub type ProdFetch = RangeFetchClient;
pub type ProdWorkerRepo = SqliteWorkerRepository;
pub type ProdAttemptRepo = SqlitePieceAttemptRepository;
pub type ProdManager =
    DownloadManager<ProdFactory, ProdQueue, ProdFetch, ProdWorkerRepo, ProdAttemptRepo>;
pub type ProdService =
    BrookService<ProdFactory, ProdQueue, ProdFetch, ProdWorkerRepo, ProdAttemptRepo>;

/// Пути к артефактам демона (БД, lock, конфиг). В проде они резолвятся из
/// [`AppPaths`] по платформенным правилам; в интеграционных тестах —
/// колокатятся в один tempdir через [`Paths::in_dir`].
pub struct Paths {
    pub lock: PathBuf,
    pub db: PathBuf,
    pub config: PathBuf,
    pub endpoint: PathBuf,
}

impl Paths {
    /// Раскладка, как её видит `brook server` в проде: config/data/cache —
    /// три разных каталога (на macOS config и data совпадают).
    pub fn from_app_paths(app: &AppPaths) -> Self {
        Self {
            lock: app.cache_dir.join(LOCK_FILENAME),
            db: app.data_dir.join(DB_FILENAME),
            config: app.config_dir.join(DEFAULT_CONFIG_FILENAME),
            endpoint: app.endpoint(),
        }
    }

    /// Все четыре файла в одном каталоге. Используется интеграционными
    /// тестами (tempdir).
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            lock: dir.join(LOCK_FILENAME),
            db: dir.join(DB_FILENAME),
            config: dir.join(DEFAULT_CONFIG_FILENAME),
            endpoint: dir.join(ENDPOINT_FILENAME),
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
    /// Путь к sidecar-файлу `.brook.endpoint`. Пишем после `bind`,
    /// удаляем в `serve` на выходе.
    pub endpoint_path: PathBuf,
    /// Ожидаемый bearer-пароль. `None` — auth отключена (loopback/dev).
    pub client_pass: Option<Arc<String>>,
}

pub async fn build_runtime(paths: &Paths, args: &ServerArgs) -> Result<Runtime> {
    // 1. Lock (блокирует второй запуск). Каталоги могут отсутствовать при
    // первом старте — создаём лениво перед `open`.
    ensure_parent(&paths.lock)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&paths.lock)
        .with_context(|| format!("open lock {}", paths.lock.display()))?;
    // `File::try_lock` стабилизирован в stdlib (ранее брали через `fs4`).
    // `TryLockError::WouldBlock` означает, что файл уже залочен другим
    // процессом — это и есть «второй инстанс»; остальные I/O-ошибки
    // прокидываем наверх.
    lock.try_lock().map_err(|e| match e {
        std::fs::TryLockError::WouldBlock => anyhow!(
            "another brook server instance is already running ({})",
            paths.lock.display()
        ),
        std::fs::TryLockError::Error(io) => {
            anyhow::Error::new(io).context(format!("lock {}", paths.lock.display()))
        }
    })?;

    // 2. Config.
    ensure_parent(&paths.config)?;
    let settings = Settings::load_or_init(&paths.config)
        .with_context(|| format!("load config {}", paths.config.display()))?;
    let daemon = DaemonRuntime::from_settings(&settings).context("derive daemon runtime")?;

    // 2a. Sandbox policy + non-loopback guards. Делаем до подъёма
    // тяжёлых зависимостей: ошибка конфигурации должна падать как можно
    // раньше. CLI host побеждает YAML — `0.0.0.0` с дефолтным `api.bind:
    // 127.0.0.1` всё равно требует sandbox + пароль, потому что фактический
    // биндинг будет non-loopback.
    let effective_host = args.host.unwrap_or(daemon.api_bind);
    let policy: Arc<dyn TPathPolicy> = match (&args.directory, effective_host.is_loopback()) {
        (Some(dir), _) => {
            // Канонизируем корень один раз тут, на старте. Если директории нет
            // — создаём; типичный сценарий первого запуска.
            if !dir.exists() {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("create sandbox root {}", dir.display()))?;
            }
            Arc::new(
                ClampedPathPolicy::new(dir)
                    .with_context(|| format!("canonicalize sandbox root {}", dir.display()))?,
            )
        }
        (None, true) => Arc::new(OpenPathPolicy::new()),
        (None, false) => {
            return Err(anyhow!(
                "refusing to bind to {effective_host} without --directory \
                 (sandbox is required for non-loopback hosts)"
            ));
        }
    };

    // 2b. Эффективная папка-prefill для TUI. Приоритет: явный
    // `download.default_dir` из YAML → `--directory` → системная папка
    // загрузок пользователя → `$HOME` → `/`.
    let effective_default_dir = resolve_default_dir(daemon.default_dir.as_deref(), &args.directory);

    // 3. Repositories (поверх общего `brook.db`).
    ensure_parent(&paths.db)?;
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
        Arc::clone(&policy),
    ));

    // 6. Manager + bootstrap.
    let manager_cfg = ManagerConfig::default();
    let manager = Arc::new(DownloadManager::with_tracking(
        factory,
        queue,
        fetch,
        Arc::clone(&workers_repo),
        Arc::clone(&attempts_repo),
        manager_cfg,
    ));
    manager.bootstrap().await.context("manager bootstrap")?;

    // CLI host/port побеждают YAML. `0` — легальный ephemeral.
    let host = effective_host;
    let port = args.port.unwrap_or(daemon.api_port);

    // Non-loopback без пароля — боевая ошибка: тривиальный recipe
    // «порт случайно открыт в сеть, ACL рулит, auth'а нет». Лучше
    // отказаться стартовать, чем позволить любому добавлять загрузки.
    let client_pass = args
        .client_pass
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| Arc::new(s.to_owned()));
    if !host.is_loopback() && client_pass.is_none() {
        return Err(anyhow!(
            "refusing to bind to {host} without --client-pass (set BROOK_CLIENT_PASS or pass --client-pass)"
        ));
    }

    let bind_addr = SocketAddr::new(host, port);
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("bind gRPC listener on {bind_addr}"))?;
    let addr = listener.local_addr().context("local_addr")?;

    // Sidecar-файл: TUI на localhost находит нас по нему, включая случай
    // ephemeral-порта (`api.port = 0`). Пишем атомарно (tempfile +
    // rename), чтобы читатель не увидел полу-записанный YAML.
    let endpoint = Endpoint {
        host: addr.ip().to_string(),
        port: addr.port(),
        pid: std::process::id(),
    };
    endpoint
        .write_atomic(&paths.endpoint)
        .with_context(|| format!("write endpoint {}", paths.endpoint.display()))?;

    let (rpc_shutdown_tx, rpc_shutdown_rx) = broadcast::channel(1);
    let svc = BrookService::new(
        Arc::clone(&manager),
        ApiSettings {
            default_dir: effective_default_dir.to_string_lossy().into_owned(),
        },
        policy,
        rpc_shutdown_tx,
    );

    Ok(Runtime {
        lock,
        daemon,
        manager,
        addr,
        svc,
        listener,
        rpc_shutdown_rx,
        endpoint_path: paths.endpoint.clone(),
        client_pass,
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
        endpoint_path,
        client_pass,
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

    let interceptor = AuthInterceptor::new(client_pass);
    let incoming = TcpListenerStream::new(listener);
    let server = tonic::transport::Server::builder()
        .add_service(BrookServiceServer::with_interceptor(svc, interceptor))
        .serve_with_incoming_shutdown(incoming, combined_shutdown);

    info!(%addr, "brook server listening");
    server.await.context("grpc server")?;
    info!("shutdown signal received — draining engines");

    if let Err(e) = manager.shutdown(SHUTDOWN_DEADLINE).await {
        warn!(error = %e, "manager shutdown did not drain within deadline");
    }

    // Удаляем sidecar до снятия flock — иначе свежестартующий TUI может
    // увидеть endpoint-файл и промахнуться в probe (порт уже свободен).
    Endpoint::remove(&endpoint_path);
    drop(lock); // явно: flock снимается здесь, а не в конце main.
    Ok(())
}

/// Создаёт родительский каталог для `p`, если он ещё не существует.
/// На платформо-зависимой раскладке на первом старте `~/Library/…/brook/`
/// может отсутствовать — это не ошибка.
fn ensure_parent(p: &Path) -> Result<()> {
    if let Some(parent) = p.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    Ok(())
}

/// Эффективная папка-prefill для модалки Add в TUI. Резолвится в
/// порядке: явный YAML → корень sandbox → системная папка загрузок →
/// `$HOME` → `/`. Последний фолбэк нужен только теоретически (если у
/// процесса нет ни HOME, ни UserDirs) — для целей prefill это всё равно
/// лучше, чем пустая строка, которую TUI потом покажет как поле.
fn resolve_default_dir(yaml: Option<&Path>, sandbox: &Option<PathBuf>) -> PathBuf {
    if let Some(p) = yaml {
        return p.to_path_buf();
    }
    if let Some(p) = sandbox {
        return p.clone();
    }
    if let Some(user) = directories::UserDirs::new()
        && let Some(p) = user.download_dir()
    {
        return p.to_path_buf();
    }
    if let Some(base) = directories::BaseDirs::new() {
        return base.home_dir().to_path_buf();
    }
    PathBuf::from("/")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_default_dir_wins_over_sandbox() {
        let yaml = PathBuf::from("/yaml/path");
        let sandbox = Some(PathBuf::from("/sandbox"));
        assert_eq!(resolve_default_dir(Some(&yaml), &sandbox), yaml);
    }

    #[test]
    fn sandbox_used_when_yaml_absent() {
        let sandbox = Some(PathBuf::from("/sandbox"));
        assert_eq!(
            resolve_default_dir(None, &sandbox),
            PathBuf::from("/sandbox")
        );
    }

    #[test]
    fn falls_back_to_user_dirs_when_yaml_and_sandbox_absent() {
        // Точное значение зависит от ОС/окружения, но результат не должен
        // быть пустым — это контракт «всегда что-то отдаём в TUI».
        let got = resolve_default_dir(None, &None);
        assert!(!got.as_os_str().is_empty(), "got {got:?}");
    }
}
