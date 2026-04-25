//! Демон brook-daemon — библиотечная часть.
//!
//! Внутренности, которые пригодятся как унифицированному бинарю `brook`,
//! так и интеграционным тестам. Публичная точка входа — [`run`].

pub mod app;
pub mod config;
pub mod storage;

use std::net::IpAddr;
use std::path::PathBuf;

use clap::Args;

use crate::app::{
    Paths,
    build_runtime,
    serve,
    shutdown_signal,
};

/// Аргументы подкоманды `brook server`.
///
/// `--directory` опциональный: если задан — корень песочницы; если не
/// задан и host = loopback — sandbox отключён (любой абсолютный путь);
/// если не задан и host = non-loopback — демон отказывается стартовать
/// (та же логика, что для `--client-pass`: ловушка «случайно открыл порт
/// в сеть без ограничений»). `--host`/`--port` override'ят YAML.
#[derive(Debug, Clone, Args)]
pub struct ServerArgs {
    /// Корень песочницы. Если задан — все загрузки клэмпятся под него;
    /// если опущен и host = loopback — sandbox отключён. Для non-loopback
    /// биндинга обязателен.
    #[arg(long)]
    pub directory: Option<PathBuf>,

    /// IP-адрес, на котором слушать gRPC. Если не задан — берётся из
    /// `api.bind` в `brook.yaml` (дефолт `127.0.0.1`).
    #[arg(long)]
    pub host: Option<IpAddr>,

    /// TCP-порт gRPC. Если не задан — из `api.port` в YAML (дефолт 7090).
    /// `0` — ephemeral, фактический порт узнается через endpoint-файл.
    #[arg(long)]
    pub port: Option<u16>,

    /// Bearer-пароль для клиентов. Без него на non-loopback демон
    /// откажется стартовать. Можно задать через env `BROOK_CLIENT_PASS`
    /// — пароль не осядет в истории шелла.
    #[arg(long, env = "BROOK_CLIENT_PASS")]
    pub client_pass: Option<String>,
}

/// Запустить демон с платформо-зависимыми путями для `brook.yaml`,
/// `brook.db`, `.brook.lock` и `.brook.endpoint` (см. `brook_runtime::AppPaths`)
/// и песочницей в `args.directory`. Переменная `BROOK_APP_DIR`
/// переопределяет все четыре файла в один каталог.
pub async fn run(args: ServerArgs) -> anyhow::Result<()> {
    let app = brook_runtime::AppPaths::resolve()?;
    let paths = Paths::from_app_paths(&app);
    let runtime = build_runtime(&paths, &args).await?;
    serve(runtime, shutdown_signal()).await?;
    Ok(())
}
