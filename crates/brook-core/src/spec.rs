//! Параметры создаваемой загрузки — что именно качать и куда.
//!
//! `DownloadSpec` — неизменяемое описание задачи, которое приходит от клиента.
//! После регистрации в очереди spec не меняется: все изменения живут в
//! `DownloadState`/`Progress`/`Download`.

use std::path::PathBuf;

/// Пара «имя заголовка — значение».
///
/// Почему `(String, String)`, а не `HashMap`: порядок заголовков важен
/// (некоторые серверы чувствительны), и дубликаты с одним именем допустимы
/// (например, `Cookie`).
pub type HeaderPair = (String, String);

/// Спецификация одной загрузки.
///
/// `PathBuf` (а не `String`) для путей — чтобы работало с платформенными
/// разделителями и с non-UTF-8 путями на macOS/Linux.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadSpec {
    /// Откуда качать (http/https).
    pub url: String,
    /// Директория назначения. Имя файла выбирается из `filename` или
    /// выводится из `Content-Disposition`/URL на этапе `HttpProbe`.
    pub target_dir: PathBuf,
    /// Явно заданное имя файла (None → определить на этапе probe).
    pub filename: Option<String>,
    /// Дополнительные HTTP-заголовки (Authorization, Cookie и т.п.).
    pub headers: Vec<HeaderPair>,
    /// Сколько параллельных воркеров качают piece'ы этой загрузки.
    pub workers: u32,
}

impl DownloadSpec {
    /// Конструктор с минимальным набором: URL + куда класть.
    /// Остальное — дефолты. Удобно в тестах и в TUI, где клиент сам
    /// заполнит только обязательное.
    pub fn new(url: impl Into<String>, target_dir: impl Into<PathBuf>) -> Self {
        // `impl Into<String>` позволяет передавать `&str`, `String`, `Cow<str>`
        // — любой тип, у которого есть `Into<String>`. Для вызывающего это
        // означает «не надо самому писать `.to_string()`».
        Self {
            url: url.into(),
            target_dir: target_dir.into(),
            filename: None,
            headers: Vec::new(),
            workers: default_workers(),
        }
    }
}

/// Дефолт по числу воркеров. Выносим в функцию, чтобы не дублировать в тестах
/// и чтобы место для изменения было одно.
pub const fn default_workers() -> u32 {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_fills_sensible_defaults() {
        let spec = DownloadSpec::new("https://example.com/f", "/tmp");
        assert_eq!(spec.url, "https://example.com/f");
        assert_eq!(spec.target_dir, PathBuf::from("/tmp"));
        assert_eq!(spec.filename, None);
        assert!(spec.headers.is_empty());
        assert_eq!(spec.workers, default_workers());
    }

    #[test]
    fn equality_is_structural() {
        // Две `DownloadSpec` с одинаковыми полями считаются равными.
        let a = DownloadSpec::new("https://a", "/d");
        let b = DownloadSpec::new("https://a", "/d");
        assert_eq!(a, b);
    }
}
