//! Параметры создаваемого файла — что именно качать и куда.
//!
//! `FileSpec` — неизменяемое описание задачи, которое приходит от клиента.
//! После регистрации в очереди spec не меняется: все изменения живут в
//! `File`/`Progress`.

use std::path::PathBuf;

/// Спецификация одного файла.
///
/// `PathBuf` (а не `String`) для путей — чтобы работало с платформенными
/// разделителями и с non-UTF-8 путями на macOS/Linux.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSpec {
    /// Откуда качать (http/https).
    pub url: String,
    /// Директория назначения. Имя файла выбирается из `filename` или
    /// выводится из `Content-Disposition`/URL на этапе `HttpProbe`.
    pub target_dir: PathBuf,
    /// Явно заданное имя файла (None → определить на этапе probe).
    pub filename: Option<String>,
    /// Загружать piece'ы последовательно (слева направо), без VdC-порядка.
    /// `false` (по умолчанию) — стратифицированный рандом + VdC; chunked bar.
    /// `true`  — классический порядок; обычный filled bar.
    pub linear: bool,
}

impl FileSpec {
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
            linear: false,
        }
    }
}

/// Верхний предел числа воркеров на один файл. Фиксированный клэмп
/// снимает необходимость в настройке `max_workers`: 12 подобрано так,
/// чтобы у больших файлов получалось разумное число соединений, и при
/// этом не упираться в типичный HTTP keep-alive лимит одного хоста.
pub const MAX_WORKERS: u32 = 12;

/// Сколько воркеров пустить на файл размером `total_size` байт.
///
/// Формула: `max(1, min(MAX_WORKERS, ceil(log2(ceil(size_mb / 10)))))`,
/// где `size_mb = ceil(total_size / MiB)`. Логарифмический рост даёт
/// плавную кривую: 100 MB → 4 воркера, 1 GB → 7, 10 GB → 10.
pub fn compute_workers(total_size: u64) -> u32 {
    if total_size == 0 {
        return 1;
    }
    const MIB: u64 = 1024 * 1024;
    let size_mb = total_size.div_ceil(MIB);
    let scaled = size_mb.div_ceil(10);
    if scaled <= 1 {
        return 1;
    }
    // ceil(log2(n)) для n > 1: берём следующую степень двойки и её tz.
    let ceil_log2 = scaled
        .checked_next_power_of_two()
        .map(|p| p.trailing_zeros())
        .unwrap_or(64);
    ceil_log2.clamp(1, MAX_WORKERS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_fills_sensible_defaults() {
        let spec = FileSpec::new("https://example.com/f", "/tmp");
        assert_eq!(spec.url, "https://example.com/f");
        assert_eq!(spec.target_dir, PathBuf::from("/tmp"));
        assert_eq!(spec.filename, None);
    }

    #[test]
    fn equality_is_structural() {
        // Две `FileSpec` с одинаковыми полями считаются равными.
        let a = FileSpec::new("https://a", "/d");
        let b = FileSpec::new("https://a", "/d");
        assert_eq!(a, b);
    }

    #[test]
    fn compute_workers_clamps_small_files_to_one() {
        assert_eq!(compute_workers(0), 1);
        assert_eq!(compute_workers(1), 1);
        assert_eq!(compute_workers(5 * 1024 * 1024), 1);
        assert_eq!(compute_workers(10 * 1024 * 1024), 1);
    }

    #[test]
    fn compute_workers_follows_log_curve() {
        // 40 MB → ceil(log2(4)) = 2
        assert_eq!(compute_workers(40 * 1024 * 1024), 2);
        // 100 MB → ceil(log2(10)) = 4
        assert_eq!(compute_workers(100 * 1024 * 1024), 4);
        // 1 GB → ceil(log2(103)) = 7
        assert_eq!(compute_workers(1024 * 1024 * 1024), 7);
        // 10 GB → ceil(log2(1024)) = 10
        assert_eq!(compute_workers(10 * 1024 * 1024 * 1024), 10);
    }

    #[test]
    fn compute_workers_clamps_to_max() {
        // 1 TB — формула ушла бы выше 12, но клэмп держит её.
        assert_eq!(compute_workers(1024 * 1024 * 1024 * 1024), MAX_WORKERS);
        assert_eq!(compute_workers(u64::MAX), MAX_WORKERS);
    }
}
