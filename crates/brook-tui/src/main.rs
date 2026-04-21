//! Точка входа TUI-клиента `brook`.
//!
//! Порядок запуска — жёстко прямолинейный:
//!
//! 1. Парсим `--port` через `clap`.
//! 2. Пробуем открыть gRPC-канал к `127.0.0.1:<port>` и вызвать
//!    `GetSettings` (лёгкая unary-проба). При неудаче пишем в stderr и
//!    выходим с кодом 1 — **до** `EnterAlternateScreen`, чтобы терминал
//!    не «моргал» alt-screen'ом.
//! 3. Поднимаем alternate screen + raw mode, защищаясь RAII-guard'ом
//!    `TerminalGuard` (восстановит экран даже при panic через Drop).
//! 4. Гоняем event-loop из `app::run` до `q`.

use std::io;
use std::process::ExitCode;

use anyhow::{
    Context,
    Result,
};
use brook_proto::brook::v1::GetSettingsRequest;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
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
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("brook: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let addr = format!("127.0.0.1:{}", cli.port);
    let endpoint = format!("http://{addr}");
    let channel = Channel::from_shared(endpoint.clone())
        .context("invalid endpoint")?
        .connect()
        .await
        .with_context(|| format!("connect to brookd at {addr}"))?;
    let mut probe = BrookServiceClient::new(channel.clone());
    let settings = probe
        .get_settings(GetSettingsRequest {})
        .await
        .with_context(|| format!("GetSettings from brookd at {addr}"))?
        .into_inner();

    let mut guard = TerminalGuard::enter().context("enter alternate screen")?;
    let res = app::run(&mut guard.terminal, channel, settings, cli.port).await;
    drop(guard); // явно — чтобы экран восстановился до печати ошибки.
    res
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
