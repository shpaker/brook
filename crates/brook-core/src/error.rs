//! Ошибки ядра.
//!
//! `thiserror` — это макрос, который сам генерирует `impl std::error::Error`
//! и `impl Display` для enum'а. Без него пришлось бы писать это руками.
//!
//! `#[from]` автоматически делает `impl From<std::io::Error> for Error`,
//! чтобы `?` в коде превращал `io::Error` в `Error` прозрачно.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    /// Источник (сервер) изменился между запросами — ETag/Last-Modified
    /// больше не совпадают. Продолжать загрузку нельзя, нужно начать заново.
    #[error("source mutated")]
    SourceMutated,

    /// Сервер прислал меньше байт, чем обещал в `Content-Length`/`Content-Range`.
    #[error("truncated response")]
    TruncatedResponse,

    #[error("not found")]
    NotFound,

    /// Целевой файл уже существует в `target_dir`. Политика хардкодится
    /// как «ошибка» — клиент (TUI) сам подбирает свободное имя и
    /// ретраит `Add` с явным `filename`.
    #[error("file already exists: {filename}")]
    FileExists { filename: String },

    /// Клиент прислал `target_dir`, который после канонизации лежит вне
    /// разрешённого корня (sandbox). Маппится на `PermissionDenied` на
    /// проводе. `attempted` — то, что увидел сервер (в человекочитаемой
    /// форме, уже лишённое симлинков).
    #[error("path escapes sandbox root: {attempted}")]
    PathEscapesRoot { attempted: String },

    /// Запасной вариант: ошибка, которую ещё не выделили в отдельный вариант.
    /// По мере взросления кода такие места заменяются на типизированные варианты.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Удобный предикат для adaptrов — проверить, что причина падения —
    /// конфликт имени файла. Используется, например, маппером в
    /// `brook-api`, чтобы отдать `tonic::Code::AlreadyExists`.
    pub fn is_file_exists(&self) -> bool {
        matches!(self, Error::FileExists { .. })
    }
}

/// Алиас для `std::result::Result<T, Error>` — чтобы не писать тип ошибки
/// в каждой сигнатуре. В большинстве крейтов такой алиас есть.
pub type Result<T> = std::result::Result<T, Error>;
