//! Бинарь `brookd`: тонкая обёртка над `brookd::app`.
//!
//! Реальная сборка рантайма и shutdown-логика живут в [`brookd::app`],
//! чтобы интеграционные тесты могли запускать демон напрямую, подсовывая
//! свои `shutdown`-сигналы вместо `SIGTERM`/`SIGINT`.

use brookd::app::{
    Paths,
    build_runtime,
    serve,
    shutdown_signal,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let session_id = uuid::Uuid::new_v4();
    let _span = tracing::info_span!("brookd", %session_id).entered();

    let paths = Paths::in_cwd();
    let runtime = build_runtime(&paths).await?;
    serve(runtime, shutdown_signal()).await?;
    Ok(())
}
