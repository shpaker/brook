//! Единый бинарь `brook`: диспетч на подкоманду `server` (демон) или
//! TUI-клиент (по умолчанию).
//!
//! Маршруты:
//!
//! - `brook` / `brook tui [...]` → [`brook_tui::run`].
//! - `brook server [...]` → [`brook_daemon::run`].

use anyhow::Result;
use clap::{
    Parser,
    Subcommand,
};

#[derive(Debug, Parser)]
#[command(
    name = "brook",
    version,
    about = "brook — download manager (TUI + daemon)",
    arg_required_else_help = false
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Флаги TUI при запуске без подкоманды.
    #[command(flatten)]
    tui: brook_tui::TuiArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Запустить демон (gRPC-сервер).
    Server(brook_daemon::ServerArgs),
    /// Запустить TUI-клиент (то же, что вызов без подкоманды).
    Tui(brook_tui::TuiArgs),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("brook: {e:#}");
            std::process::ExitCode::from(1)
        }
    }
}

async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Command::Server(args)) => {
            tracing_subscriber::fmt()
                .with_writer(std::io::stderr)
                .init();
            let session_id = uuid::Uuid::new_v4();
            let _span = tracing::info_span!("brook-server", %session_id).entered();
            brook_daemon::run(args).await
        }
        Some(Command::Tui(args)) => brook_tui::run(args).await,
        None => brook_tui::run(cli.tui).await,
    }
}
