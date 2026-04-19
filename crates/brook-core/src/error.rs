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

    /// Запасной вариант: ошибка, которую ещё не выделили в отдельный вариант.
    /// По мере взросления кода такие места заменяются на типизированные варианты.
    #[error("{0}")]
    Other(String),
}

/// Алиас для `std::result::Result<T, Error>` — чтобы не писать тип ошибки
/// в каждой сигнатуре. В большинстве крейтов такой алиас есть.
pub type Result<T> = std::result::Result<T, Error>;
