//! TUI-клиент `brook` — библиотечная часть.
//!
//! Порядок запуска, который реализует [`run`]:
//!
//! 1. Принимает [`TuiArgs`] от диспетчера в `crates/brook`.
//! 2. Пробует открыть gRPC-канал к `127.0.0.1:<port>` и вызвать
//!    `GetSettings` (лёгкая unary-проба). Если коннект не удался —
//!    пытается поднять соседний `brook server` через `current_exe()` и
//!    ждёт, пока он поднимется (fixed-backoff до ~3s).
//! 3. Поднимает alternate screen + raw mode под RAII-guard'ом
//!    `TerminalGuard` (восстановит экран даже при panic через Drop).
//! 4. Гоняет event-loop из `app::run` до `q`.

use std::io;
use std::path::Path;
use std::process::{
    Command,
    Stdio,
};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{
    Context,
    Result,
    anyhow,
};
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::{
    GetSettingsRequest,
    GetSettingsResponse,
};
use brook_runtime::Endpoint;
use brook_runtime::constants::{
    DEFAULT_PORT,
    ENDPOINT_FILENAME,
};
use clap::Args;
use crossterm::event::{
    DisableBracketedPaste,
    EnableBracketedPaste,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen,
    LeaveAlternateScreen,
    disable_raw_mode,
    enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tonic::Code;
use tonic::transport::Channel;

use crate::connect::{
    AuthedChannel,
    wrap,
};

mod app;
mod command;
mod connect;
mod events;
mod format;
mod model;
mod ui;
mod watch;

/// Аргументы TUI, которые пробрасывает диспетчер `brook`.
#[derive(Debug, Clone, Default, Args)]
pub struct TuiArgs {
    /// Порт демона на localhost (обязательно через `--remote` для remote).
    #[arg(long)]
    pub port: Option<u16>,

    /// Подключиться к удалённому демону по адресу `host:port`. В этом
    /// случае локальный автоспавн не делается.
    #[arg(long)]
    pub remote: Option<String>,

    /// Bearer-пароль клиента. Если задан на CLI, используем сразу; если
    /// `--remote` без `--pass`, спросим интерактивно (`rpassword`). Можно
    /// передать и через env `BROOK_CLIENT_PASS`.
    #[arg(long, env = "BROOK_CLIENT_PASS")]
    pub pass: Option<String>,
}

/// Запустить TUI. Вызывается диспетчером в `crates/brook`.
pub async fn run(args: TuiArgs) -> Result<()> {
    // Пароль: из CLI/env берём как есть; для `--remote` без пароля
    // спрашиваем ещё до первого probe'а — иначе получим
    // `Unauthenticated` на ровном месте.
    let mut pass: Option<Arc<String>> = args.pass.filter(|p| !p.is_empty()).map(Arc::new);
    if args.remote.is_some() && pass.is_none() {
        pass = Some(Arc::new(prompt_password("remote password: ")?));
    }

    // Выбор адреса: `--remote` побеждает всё. Иначе — endpoint-файл или
    // дефолтный loopback-порт.
    let endpoint_url = if let Some(remote) = &args.remote {
        format!("http://{remote}")
    } else {
        resolve_endpoint_url(args.port)
    };

    let spawned_self;
    let (channel, settings) = match probe(&endpoint_url, pass.clone()).await {
        Ok(v) => {
            spawned_self = false;
            v
        }
        Err(ProbeError::Unauthenticated) => {
            // Коннект живой, пароля нет/неверный — спрашиваем. Remote:
            // пересобираем `pass`, повторяем probe. Local: та же логика.
            let p = Arc::new(prompt_password("daemon password: ")?);
            let v = probe(&endpoint_url, Some(p.clone()))
                .await
                .map_err(|e| anyhow!("auth failed: {e}"))?;
            pass = Some(p);
            spawned_self = false;
            v
        }
        Err(ProbeError::Transport(_)) if args.remote.is_some() => {
            return Err(anyhow!("cannot reach remote daemon at {endpoint_url}"));
        }
        Err(ProbeError::Transport(_)) => {
            // Локальный демон не отвечает — поднимаем сами. Sidecar мог
            // остаться от мёртвого — удаляем.
            Endpoint::remove(Path::new(ENDPOINT_FILENAME));
            spawn_daemon().context("spawn brook server")?;
            spawned_self = true;
            wait_for_daemon(pass.clone()).await?
        }
    };
    let _ = pass;

    // Порт для заголовка UI: если коннектились по endpoint-файлу, в
    // `endpoint_url` уже сидит актуальный порт.
    let port = port_from_url(&endpoint_url).unwrap_or(DEFAULT_PORT);
    let mut guard = TerminalGuard::enter().context("enter alternate screen")?;
    // `can_stop_daemon` = мы сами подняли локального демона. Для remote
    // и для случая, когда демон уже крутился — `false`, и пункт
    // «остановить демон» в QuitConfirm скрыт.
    let res = app::run(&mut guard.terminal, channel, settings, port, spawned_self).await;
    drop(guard); // явно — чтобы экран восстановился до печати ошибки.
    res
}

fn prompt_password(prompt: &str) -> Result<String> {
    let p = rpassword::prompt_password(prompt).context("read password from tty")?;
    if p.is_empty() {
        return Err(anyhow!("password is required"));
    }
    Ok(p)
}

/// Построить URL gRPC-эндпоинта. Явный `--port` побеждает всё (это
/// осознанный override пользователя), иначе пробуем прочитать sidecar,
/// иначе — дефолтный loopback-порт.
fn resolve_endpoint_url(explicit_port: Option<u16>) -> String {
    if let Some(port) = explicit_port {
        return format!("http://127.0.0.1:{port}");
    }
    if let Ok(Some(ep)) = Endpoint::read(Path::new(ENDPOINT_FILENAME)) {
        return format!("http://{}:{}", ep.host, ep.port);
    }
    format!("http://127.0.0.1:{DEFAULT_PORT}")
}

fn port_from_url(url: &str) -> Option<u16> {
    url.rsplit(':').next()?.parse().ok()
}

/// Ошибка пробы. Различаем auth-отказ (нужен prompt пароля) и всё
/// остальное (transport, timeout, Internal, ...).
enum ProbeError {
    Unauthenticated,
    Transport(anyhow::Error),
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated => f.write_str("unauthenticated"),
            Self::Transport(e) => write!(f, "{e:#}"),
        }
    }
}

/// Однократная попытка коннекта + `GetSettings` с заданным паролем.
/// Возвращает готовый `AuthedChannel` — этот же интерцептор едет дальше
/// во все клиентские вызовы.
async fn probe(
    endpoint: &str,
    pass: Option<Arc<String>>,
) -> Result<(AuthedChannel, GetSettingsResponse), ProbeError> {
    let channel = Channel::from_shared(endpoint.to_string())
        .map_err(|e| ProbeError::Transport(anyhow!("invalid endpoint: {e}")))?
        .connect()
        .await
        .map_err(|e| ProbeError::Transport(anyhow!("connect to {endpoint}: {e}")))?;
    let authed = wrap(channel, pass);
    let mut client = BrookServiceClient::new(authed.clone());
    match client.get_settings(GetSettingsRequest {}).await {
        Ok(resp) => Ok((authed, resp.into_inner())),
        Err(st) if st.code() == Code::Unauthenticated => Err(ProbeError::Unauthenticated),
        Err(st) => Err(ProbeError::Transport(anyhow!(
            "GetSettings from daemon at {endpoint}: {st}"
        ))),
    }
}

/// Поднять демон через `current_exe() server` как детач-процесс. Стдио
/// глушим — иначе логи демона полезут в alt-screen и испортят UI.
///
/// `--directory` для спавна — `~/Downloads` (sandbox-root по умолчанию).
/// Если пользователь хочет другой корень — он явно гоняет
/// `brook server --directory X` сам.
fn spawn_daemon() -> Result<()> {
    let exe = std::env::current_exe().context("current_exe")?;
    let default_dir = default_sandbox_dir()
        .context("resolve default sandbox dir (pass `brook server --directory X` manually)")?;
    Command::new(&exe)
        .arg("server")
        .arg("--directory")
        .arg(&default_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn {} server", exe.display()))?;
    Ok(())
}

/// Дефолтный корень sandbox для автоспавна. MVP — `~/Downloads`.
/// Пользовательский YAML-override (`download.default_dir`) пока не
/// учитываем: M4 фиксирует минимальный поток, более гибкий резолв
/// приедет позже без breaking-изменений для CLI.
fn default_sandbox_dir() -> Result<std::path::PathBuf> {
    let base = directories::UserDirs::new().context("resolve user dirs")?;
    if let Some(d) = base.download_dir() {
        return Ok(d.to_path_buf());
    }
    // На платформах без XDG-user-dirs (или без `~/Downloads`) — home/Downloads.
    Ok(base.home_dir().join("Downloads"))
}

/// Ждём, пока свежеподнятый демон примет коннект и ответит на
/// `GetSettings`. Дедлайн щедрый: первый запуск включает миграции и
/// bind TCP, на медленных дисках это заметно.
///
/// Порт демона читаем из sidecar-файла на каждой итерации — он
/// появится только после успешного `bind` (в т.ч. при ephemeral
/// `port = 0`).
async fn wait_for_daemon(
    pass: Option<Arc<String>>,
) -> Result<(AuthedChannel, GetSettingsResponse)> {
    const ATTEMPTS: u32 = 30;
    const DELAY: Duration = Duration::from_millis(100);
    let mut last_err: Option<String> = None;
    for _ in 0..ATTEMPTS {
        if let Ok(Some(ep)) = Endpoint::read(Path::new(ENDPOINT_FILENAME)) {
            let url = format!("http://{}:{}", ep.host, ep.port);
            match probe(&url, pass.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) => last_err = Some(e.to_string()),
            }
        }
        tokio::time::sleep(DELAY).await;
    }
    Err(anyhow!(
        "daemon did not come up in time: {}",
        last_err.as_deref().unwrap_or("no endpoint file")
    ))
}

/// RAII-обёртка над ratatui-терминалом: включает raw mode + alternate
/// screen в конструкторе, выключает в `Drop`. Мы _специально_ глотаем
/// ошибки отката — на выходе ничего полезного с ними не сделать, а
/// пропущенный `disable_raw_mode` сломает шелл пользователю.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enable_raw_mode")?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableBracketedPaste)
            .context("enter alternate screen")?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).context("create terminal")?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}
