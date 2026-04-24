//! Резолвер платформо-зависимых путей для артефактов демона.
//!
//! Раскладка через [`directories::ProjectDirs`]:
//!
//! | Файл | macOS | Linux (XDG) | Windows |
//! |---|---|---|---|
//! | `brook.yaml` | `~/Library/Application Support/brook/` | `~/.config/brook/` | `%APPDATA%\brook\config\` |
//! | `brook.db`   | `~/Library/Application Support/brook/` | `~/.local/share/brook/` | `%APPDATA%\brook\data\` |
//! | `.brook.lock`, `.brook.endpoint` | `~/Library/Caches/brook/` | `~/.cache/brook/` | `%LOCALAPPDATA%\brook\cache\` |
//!
//! Переменная `BROOK_APP_DIR` (если непуста) полностью перенаправляет
//! все три «каталога» в один корень — удобно для dev-запусков из
//! checkout'а и для изоляции пользовательских инсталляций. Integration-тесты
//! не используют этот резолвер — они жмут `Paths::in_dir(tempdir)` в
//! `brook-daemon` напрямую.

use std::path::PathBuf;

use anyhow::{
    Context,
    Result,
};

use crate::constants::{
    ENDPOINT_FILENAME,
    STARTUP_LOG_FILENAME,
};

/// Имя env-переменной, полностью переопределяющей app-каталоги.
pub const APP_DIR_ENV: &str = "BROOK_APP_DIR";

/// Три каталога, в которых живут рантайм-артефакты демона.
#[derive(Debug, Clone)]
pub struct AppPaths {
    /// Куда писать `brook.yaml`.
    pub config_dir: PathBuf,
    /// Куда писать `brook.db`.
    pub data_dir: PathBuf,
    /// Куда писать `.brook.lock` и `.brook.endpoint`.
    pub cache_dir: PathBuf,
}

impl AppPaths {
    /// Резолвит пути. Если задан и непуст `BROOK_APP_DIR` — все три
    /// каталога сливаются в один; иначе платформо-зависимый сплит через
    /// `ProjectDirs`.
    pub fn resolve() -> Result<Self> {
        if let Some(root) = env_override() {
            return Ok(Self {
                config_dir: root.clone(),
                data_dir: root.clone(),
                cache_dir: root,
            });
        }
        let pd = directories::ProjectDirs::from("", "", "brook")
            .context("cannot resolve user directories for brook")?;
        Ok(Self {
            config_dir: pd.config_dir().to_path_buf(),
            data_dir: pd.data_dir().to_path_buf(),
            cache_dir: pd.cache_dir().to_path_buf(),
        })
    }

    /// Путь к `.brook.endpoint`. Используется и демоном (write), и TUI
    /// (read) — единая точка согласования.
    pub fn endpoint(&self) -> PathBuf {
        self.cache_dir.join(ENDPOINT_FILENAME)
    }

    /// Путь к `.brook.startup.log`. TUI перенаправляет сюда stdio
    /// спавненного `brook server`, чтобы на таймауте wait_for_daemon
    /// показать хвост и объяснить, почему демон не поднялся.
    pub fn startup_log(&self) -> PathBuf {
        self.cache_dir.join(STARTUP_LOG_FILENAME)
    }
}

fn env_override() -> Option<PathBuf> {
    match std::env::var(APP_DIR_ENV) {
        Ok(s) if !s.is_empty() => Some(PathBuf::from(s)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn startup_log_lives_next_to_endpoint_in_cache_dir() {
        let p = AppPaths {
            config_dir: PathBuf::from("/tmp/cfg"),
            data_dir: PathBuf::from("/tmp/data"),
            cache_dir: PathBuf::from("/tmp/cache"),
        };
        assert_eq!(p.endpoint(), PathBuf::from("/tmp/cache/.brook.endpoint"));
        assert_eq!(
            p.startup_log(),
            PathBuf::from("/tmp/cache/.brook.startup.log")
        );
    }
}
