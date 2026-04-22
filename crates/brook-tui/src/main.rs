//! Точка входа TUI-клиента `brook`.
//!
//! Порядок запуска — жёстко прямолинейный:
//!
//! 1. Парсим `--port` через `clap`.
//! 2. Пробуем открыть gRPC-канал к `127.0.0.1:<port>` и вызвать
//!    `GetSettings` (лёгкая unary-проба). Если коннект не удался —
//!    пытаемся поднять соседний бинарь `brookd` и подождать, пока он
//!    поднимется (fixed-backoff до ~3s). Независимо от того, сам ли TUI
//!    поднял демон, на выходе модалка предлагает три варианта:
//!    остановить демон / оставить работать / отмена.
//! 3. Поднимаем alternate screen + raw mode, защищаясь RAII-guard'ом
//!    `TerminalGuard` (восстановит экран даже при panic через Drop).
//! 4. Гоняем event-loop из `app::run` до `q`.

use std::io;
use std::path::PathBuf;
use std::process::{
    Command,
    Stdio,
};
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
use clap::Parser;
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
use tonic::transport::Channel;

mod app;
mod command;
mod events;
mod format;
mod model;
mod ui;
mod watch;

#[derive(Debug, Parser)]
#[command(name = "brook", version, about = "brook TUI client")]
struct Cli {
    /// Порт демона `brookd` на localhost.
    #[arg(long, default_value_t = 7090)]
    port: u16,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("brook: {e:#}");
            std::process::ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let endpoint = format!("http://127.0.0.1:{}", cli.port);
    let (channel, settings) = match probe(&endpoint).await {
        Ok((ch, s)) => (ch, s),
        Err(_) => {
            // Первый коннект не прошёл — поднимаем демона сами и ждём.
            spawn_daemon().context("spawn brookd")?;
            wait_for_daemon(&endpoint).await?
        }
    };

    let mut guard = TerminalGuard::enter().context("enter alternate screen")?;
    let res = app::run(&mut guard.terminal, channel, settings, cli.port).await;
    drop(guard); // явно — чтобы экран восстановился до печати ошибки.
    res
}

/// Однократная попытка коннекта + `GetSettings`.
async fn probe(endpoint: &str) -> Result<(Channel, GetSettingsResponse)> {
    let channel = Channel::from_shared(endpoint.to_string())
        .context("invalid endpoint")?
        .connect()
        .await
        .with_context(|| format!("connect to brookd at {endpoint}"))?;
    let mut client = BrookServiceClient::new(channel.clone());
    let settings = client
        .get_settings(GetSettingsRequest {})
        .await
        .with_context(|| format!("GetSettings from brookd at {endpoint}"))?
        .into_inner();
    Ok((channel, settings))
}

/// Поднять соседний `brookd` как детач-процесс. Стдио глушим — иначе
/// логи демона полезут в alt-screen и испортят UI.
fn spawn_daemon() -> Result<()> {
    let exe = std::env::current_exe().context("current_exe")?;
    let dir = exe
        .parent()
        .ok_or_else(|| anyhow!("current_exe has no parent"))?;
    let brookd: PathBuf = dir.join("brookd");
    if !brookd.exists() {
        return Err(anyhow!(
            "brookd binary not found next to TUI at {}",
            brookd.display()
        ));
    }
    Command::new(&brookd)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn {}", brookd.display()))?;
    Ok(())
}

/// Ждём, пока свежеподнятый `brookd` примет коннект и ответит на
/// `GetSettings`. Дедлайн щедрый: первый запуск включает миграции и
/// bind TCP, на медленных дисках это заметно.
async fn wait_for_daemon(endpoint: &str) -> Result<(Channel, GetSettingsResponse)> {
    const ATTEMPTS: u32 = 30;
    const DELAY: Duration = Duration::from_millis(100);
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..ATTEMPTS {
        match probe(endpoint).await {
            Ok(v) => return Ok(v),
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(DELAY).await;
    }
    Err(last_err.unwrap_or_else(|| anyhow!("brookd did not come up in time")))
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
