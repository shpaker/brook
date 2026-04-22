//! Конфигурация демона — YAML-файл рядом с `brook.db` и `.brook.lock`.
//!
//! В MVP конфиг живёт в `./brook.yaml` в CWD (той же директории, откуда
//! запущен `brook server`). Это статическая конфигурация процесса: на
//! лету не перечитывается, правки требуют рестарта демона.
//!
//! ## Модель ключей
//!
//! Часть параметров — **global-only** (порт gRPC, лимит параллельности,
//! bind-адрес, логи): на каждую загрузку они одинаковы. Часть —
//! **per-download defaults**: YAML задаёт дефолт, но клиент через
//! `FileSpec` может прислать override (параметры нарезки, целевой
//! каталог). Число воркеров настройкой не управляется — ядро считает его
//! по размеру файла (см. `brook_core::compute_workers`).
//!
//! Разделение видно в типах: [`DaemonRuntime`] содержит global-only,
//! [`DownloadDefaults`] — overridable. Так границу сложнее случайно
//! сломать при рефакторинге.

use std::net::{
    AddrParseError,
    IpAddr,
};
use std::path::{
    Path,
    PathBuf,
};
use std::str::FromStr;
use std::{
    fs,
    io,
};

use serde::{
    Deserialize,
    Serialize,
};
use thiserror::Error;

/// Имя конфиг-файла по умолчанию (относительно CWD).
pub const DEFAULT_CONFIG_FILENAME: &str = "brook.yaml";

/// Шаблон-дефолт, который пишется при первом запуске. Комментарии помогают
/// пользователю быстро сориентироваться, не читая доки.
const DEFAULT_YAML: &str = "\
# Конфигурация brook server. Перезапустите демон после правок.
download:
  # Сколько загрузок одновременно активны; остальные ждут в очереди.
  max_concurrent: 3
  # Дефолтный каталог назначения (перекрывается FileSpec.target_dir).
  default_dir: ~/Downloads
  # Целевое число piece'ов на файл (перекрывается в FileSpec).
  piece_target_count: 128
  # Нижняя граница размера piece'а, MiB (степень двойки).
  piece_size_min_mib: 16
  # Верхняя граница размера piece'а, MiB (степень двойки).
  piece_size_max_mib: 128
  # Политика при добавлении URL, уже стоящего в очереди.
  on_duplicate_url: ask  # ask | skip | add
  # Политики при существующем файле с тем же именем в target_dir нет:
  # демон всегда возвращает AlreadyExists, а клиент (TUI) сам подбирает
  # `<stem> (N).<ext>` и ретраит Add.
api:
  port: 7090
  bind: 127.0.0.1
log:
  dir: ~/Library/Logs/brook
  rotate_count: 10
  rotate_size_mb: 50
";

// ─── Корневой тип ───────────────────────────────────────────────────────

/// Разобранный YAML — ровно как лежит на диске, без какого-либо
/// преобразования единиц измерения.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    pub download: DownloadSection,
    #[serde(default)]
    pub api: ApiSection,
    #[serde(default)]
    pub log: LogSection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DownloadSection {
    #[serde(default = "d_max_concurrent")]
    pub max_concurrent: u32,
    #[serde(default = "d_default_dir")]
    pub default_dir: String,
    #[serde(default = "d_piece_target_count")]
    pub piece_target_count: u32,
    #[serde(default = "d_piece_size_min_mib")]
    pub piece_size_min_mib: u32,
    #[serde(default = "d_piece_size_max_mib")]
    pub piece_size_max_mib: u32,
    #[serde(default)]
    pub on_duplicate_url: OnDuplicateUrl,
}

impl Default for DownloadSection {
    fn default() -> Self {
        Self {
            max_concurrent: d_max_concurrent(),
            default_dir: d_default_dir(),
            piece_target_count: d_piece_target_count(),
            piece_size_min_mib: d_piece_size_min_mib(),
            piece_size_max_mib: d_piece_size_max_mib(),
            on_duplicate_url: OnDuplicateUrl::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApiSection {
    #[serde(default = "d_api_port")]
    pub port: u16,
    #[serde(default = "d_api_bind")]
    pub bind: String,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            port: d_api_port(),
            bind: d_api_bind(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    #[serde(default = "d_log_dir")]
    pub dir: String,
    #[serde(default = "d_log_rotate_count")]
    pub rotate_count: u32,
    #[serde(default = "d_log_rotate_size_mb")]
    pub rotate_size_mb: u32,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            dir: d_log_dir(),
            rotate_count: d_log_rotate_count(),
            rotate_size_mb: d_log_rotate_size_mb(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnDuplicateUrl {
    #[default]
    Ask,
    Skip,
    Add,
}

// ─── Дефолты ────────────────────────────────────────────────────────────

// `serde(default = "...")` требует функций без аргументов, поэтому
// константы живут внутри функций-геттеров, а не `const`.
fn d_max_concurrent() -> u32 {
    3
}
fn d_default_dir() -> String {
    "~/Downloads".into()
}
fn d_piece_target_count() -> u32 {
    128
}
fn d_piece_size_min_mib() -> u32 {
    16
}
fn d_piece_size_max_mib() -> u32 {
    128
}
fn d_api_port() -> u16 {
    7090
}
fn d_api_bind() -> String {
    "127.0.0.1".into()
}
fn d_log_dir() -> String {
    "~/Library/Logs/brook".into()
}
fn d_log_rotate_count() -> u32 {
    10
}
fn d_log_rotate_size_mb() -> u32 {
    50
}

// ─── Ошибки ─────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read { path: PathBuf, source: io::Error },
    #[error("failed to write {path}: {source}")]
    Write { path: PathBuf, source: io::Error },
    #[error("invalid YAML at {path}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    /// Семантическая валидация: поле заполнено, но значение не годится.
    #[error("invalid value for `{key}`: {reason}")]
    Invalid { key: &'static str, reason: String },
    /// Не удалось раскрыть `~` в пути (нет `HOME`).
    #[error("cannot expand `~` in `{key}`: home directory unavailable")]
    NoHomeDir { key: &'static str },
}

// ─── Загрузка / запись ──────────────────────────────────────────────────

impl Settings {
    /// Прочитать YAML и провалидировать. Не раскрывает `~` — это делают
    /// проекции ([`DaemonRuntime::from_settings`]), чтобы сырой `Settings`
    /// оставался транспортом точного содержимого файла.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let parsed: Settings =
            serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        parsed.validate()?;
        Ok(parsed)
    }

    /// Записать YAML-шаблон с комментариями. Ошибка, если файл уже существует.
    pub fn write_default(path: &Path) -> Result<(), ConfigError> {
        fs::write(path, DEFAULT_YAML).map_err(|source| ConfigError::Write {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Загрузить конфиг, при отсутствии файла — сгенерировать дефолт и
    /// перечитать. Удобно дергать из `main` одним вызовом.
    pub fn load_or_init(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            Self::write_default(path)?;
        }
        Self::load(path)
    }

    /// Семантические инварианты, которые не ловятся serde-парсером.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let d = &self.download;
        if d.max_concurrent == 0 {
            return Err(ConfigError::Invalid {
                key: "download.max_concurrent",
                reason: "must be ≥ 1".into(),
            });
        }
        if d.piece_target_count == 0 {
            return Err(ConfigError::Invalid {
                key: "download.piece_target_count",
                reason: "must be ≥ 1".into(),
            });
        }
        if !is_power_of_two_u32(d.piece_size_min_mib) {
            return Err(ConfigError::Invalid {
                key: "download.piece_size_min_mib",
                reason: format!("{} is not a power of two", d.piece_size_min_mib),
            });
        }
        if !is_power_of_two_u32(d.piece_size_max_mib) {
            return Err(ConfigError::Invalid {
                key: "download.piece_size_max_mib",
                reason: format!("{} is not a power of two", d.piece_size_max_mib),
            });
        }
        if d.piece_size_min_mib > d.piece_size_max_mib {
            return Err(ConfigError::Invalid {
                key: "download.piece_size_min_mib",
                reason: format!(
                    "must be ≤ piece_size_max_mib ({} > {})",
                    d.piece_size_min_mib, d.piece_size_max_mib
                ),
            });
        }
        // `api.port = 0` легально: означает «пусть ОС выберет свободный
        // порт». Реальный адрес узнаём после bind'а в `build_runtime`.
        IpAddr::from_str(&self.api.bind).map_err(|e: AddrParseError| ConfigError::Invalid {
            key: "api.bind",
            reason: format!("not an IP address: {e}"),
        })?;
        Ok(())
    }
}

fn is_power_of_two_u32(n: u32) -> bool {
    n != 0 && n.is_power_of_two()
}

// ─── Проекции в рантайм-конфиг ──────────────────────────────────────────

/// Global-only конфигурация — всё, что не переопределяется в `FileSpec`.
#[derive(Debug, Clone)]
pub struct DaemonRuntime {
    pub max_concurrent: usize,
    pub api_bind: IpAddr,
    pub api_port: u16,
    pub default_dir: PathBuf,
    pub on_duplicate_url: OnDuplicateUrl,
    pub log: LogRuntime,
    pub defaults: DownloadDefaults,
}

#[derive(Debug, Clone)]
pub struct LogRuntime {
    pub dir: PathBuf,
    pub rotate_count: u32,
    pub rotate_size_mb: u32,
}

/// Per-download-overridable defaults. Все размеры уже в байтах.
#[derive(Debug, Clone, Copy)]
pub struct DownloadDefaults {
    pub piece_target_count: u32,
    pub piece_size_min: u64,
    pub piece_size_max: u64,
}

impl DaemonRuntime {
    /// Собрать рантайм-вид из `Settings`. Здесь же раскрываем `~` в путях.
    pub fn from_settings(s: &Settings) -> Result<Self, ConfigError> {
        let d = &s.download;
        let api_bind = IpAddr::from_str(&s.api.bind).map_err(|e| ConfigError::Invalid {
            key: "api.bind",
            reason: format!("not an IP address: {e}"),
        })?;
        Ok(Self {
            max_concurrent: d.max_concurrent as usize,
            api_bind,
            api_port: s.api.port,
            default_dir: expand_home(&d.default_dir, "download.default_dir")?,
            on_duplicate_url: d.on_duplicate_url,
            log: LogRuntime {
                dir: expand_home(&s.log.dir, "log.dir")?,
                rotate_count: s.log.rotate_count,
                rotate_size_mb: s.log.rotate_size_mb,
            },
            defaults: DownloadDefaults {
                piece_target_count: d.piece_target_count,
                piece_size_min: (d.piece_size_min_mib as u64) * 1024 * 1024,
                piece_size_max: (d.piece_size_max_mib as u64) * 1024 * 1024,
            },
        })
    }
}

/// Раскрыть `~` / `~/...` в путях конфига. `~abc` (user-specific) не
/// поддерживаем — это сложнее и в MVP не нужно.
fn expand_home(raw: &str, key: &'static str) -> Result<PathBuf, ConfigError> {
    if raw == "~" {
        return home(key);
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        let mut p = home(key)?;
        p.push(rest);
        return Ok(p);
    }
    Ok(PathBuf::from(raw))
}

fn home(key: &'static str) -> Result<PathBuf, ConfigError> {
    directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .ok_or(ConfigError::NoHomeDir { key })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let s = Settings::default();
        s.validate().expect("default settings must validate");
    }

    #[test]
    fn default_yaml_roundtrips_into_default_settings() {
        let parsed: Settings = serde_yaml::from_str(DEFAULT_YAML).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed, Settings::default());
    }

    #[test]
    fn missing_section_uses_defaults() {
        // Пустой YAML (или только комментарии) — все секции берут дефолты.
        let parsed: Settings = serde_yaml::from_str("{}").unwrap();
        assert_eq!(parsed, Settings::default());
    }

    #[test]
    fn partial_section_fills_rest_from_defaults() {
        let parsed: Settings = serde_yaml::from_str("api:\n  port: 9000\n").unwrap();
        assert_eq!(parsed.api.port, 9000);
        assert_eq!(parsed.api.bind, d_api_bind());
        assert_eq!(parsed.download, DownloadSection::default());
    }

    #[test]
    fn unknown_key_is_rejected() {
        // `deny_unknown_fields` ловит опечатки — лучше жёсткая ошибка,
        // чем тихо игнорируемый ключ.
        let err = serde_yaml::from_str::<Settings>("download:\n  nonsense: 1\n").unwrap_err();
        assert!(err.to_string().contains("nonsense"), "{err}");
    }

    #[test]
    fn invalid_enum_value_mentions_key() {
        let err =
            serde_yaml::from_str::<Settings>("download:\n  on_duplicate_url: maybe\n").unwrap_err();
        let s = err.to_string();
        // Сообщение serde_yaml упоминает и ключ, и список валидных вариантов.
        assert!(s.contains("on_duplicate_url"), "{s}");
    }

    #[test]
    fn pow2_violations_are_caught() {
        let mut s = Settings::default();
        s.download.piece_size_min_mib = 10;
        let err = s.validate().unwrap_err();
        assert!(
            matches!(err, ConfigError::Invalid { key, .. } if key == "download.piece_size_min_mib"),
            "{err:?}"
        );
    }

    #[test]
    fn min_greater_than_max_is_caught() {
        let mut s = Settings::default();
        s.download.piece_size_min_mib = 256;
        s.download.piece_size_max_mib = 128;
        let err = s.validate().unwrap_err();
        assert!(
            matches!(err, ConfigError::Invalid { key, .. } if key == "download.piece_size_min_mib")
        );
    }

    #[test]
    fn port_zero_is_allowed_for_ephemeral_bind() {
        let mut s = Settings::default();
        s.api.port = 0;
        s.validate().unwrap();
    }

    #[test]
    fn bad_bind_address_is_caught() {
        let mut s = Settings::default();
        s.api.bind = "not-an-ip".into();
        let err = s.validate().unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { key, .. } if key == "api.bind"));
    }

    #[test]
    fn write_default_then_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("brook.yaml");
        Settings::write_default(&path).unwrap();
        let loaded = Settings::load(&path).unwrap();
        assert_eq!(loaded, Settings::default());
    }

    #[test]
    fn load_or_init_creates_file_on_first_run() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("brook.yaml");
        assert!(!path.exists());
        let s = Settings::load_or_init(&path).unwrap();
        assert!(path.exists());
        assert_eq!(s, Settings::default());

        // Второй вызов не перезаписывает файл.
        let bytes_before = fs::read(&path).unwrap();
        let _ = Settings::load_or_init(&path).unwrap();
        let bytes_after = fs::read(&path).unwrap();
        assert_eq!(bytes_before, bytes_after);
    }

    #[test]
    fn runtime_projection_expands_home_and_converts_units() {
        let s = Settings::default();
        let rt = DaemonRuntime::from_settings(&s).unwrap();
        assert_eq!(rt.max_concurrent, 3);
        assert_eq!(rt.api_port, 7090);
        assert_eq!(rt.api_bind, IpAddr::from_str("127.0.0.1").unwrap());
        assert!(
            !rt.default_dir.to_string_lossy().starts_with('~'),
            "got {:?}",
            rt.default_dir
        );
        assert_eq!(rt.defaults.piece_size_min, 16 * 1024 * 1024);
        assert_eq!(rt.defaults.piece_size_max, 128 * 1024 * 1024);
    }

    #[test]
    fn expand_home_preserves_absolute_path() {
        let p = expand_home("/var/log", "log.dir").unwrap();
        assert_eq!(p, PathBuf::from("/var/log"));
    }
}
