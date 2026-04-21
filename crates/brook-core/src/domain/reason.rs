//! Причина перехода состояния: код + опциональное сообщение.
//!
//! `ReasonCode` — закрытый набор категорий, совпадающий 1:1 со
//! справочником `reason_codes` в `brook.db` (см.
//! `crates/brookd/src/storage/db.rs`). Natural-key: строковое имя
//! варианта (`network`, `timeout`, ...) — это и PK в БД, и
//! сериализация в логах/API.
//!
//! `FailureReason` — value-объект: код обязателен, текстовое
//! пояснение опционально. Инвариант из [`docs/todo.md`] §21:
//! при переходе в `Failed` reason обязателен; для остальных
//! переходов — опционален (например, `Cancelled` +
//! `CancelledByUser`).
//!
//! Тип называется `FailureReason`, но по смыслу описывает *любой*
//! мотив смены состояния — не только ошибочный. Имя выбрано под
//! основной кейс (failed-transitions) и согласовано с планом;
//! генеричность сохраняется за счёт вариантов вроде `CancelledByUser`.

use std::fmt;
use std::str::FromStr;

use crate::error::{
    Error,
    Result,
};

/// Закрытый набор категорий причин перехода.
///
/// Строковые имена (через [`ReasonCode::as_str`]) совпадают с PK
/// таблицы `reason_codes` в `brook.db` — это единственная точка
/// истины для сериализации. Ошибки ядра мапятся сюда через
/// [`ReasonCode::from_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReasonCode {
    /// Сетевые ошибки: connect/read, TLS, resolver.
    Network,
    /// Таймаут запроса.
    Timeout,
    /// HTTP 4xx (кроме специально обрабатываемых).
    Http4xx,
    /// HTTP 5xx.
    Http5xx,
    /// Источник изменился между запросами (ETag/Last-Modified mismatch).
    SourceMutated,
    /// Нет места на диске.
    DiskFull,
    /// Сервер отдал некорректный ответ (обрезанный, кривой заголовок).
    InvalidResponse,
    /// Пользователь запросил отмену.
    CancelledByUser,
    /// Прочее — запасной вариант, пока нет отдельной категории.
    Unknown,
}

impl ReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::Http4xx => "http_4xx",
            Self::Http5xx => "http_5xx",
            Self::SourceMutated => "source_mutated",
            Self::DiskFull => "disk_full",
            Self::InvalidResponse => "invalid_response",
            Self::CancelledByUser => "cancelled_by_user",
            Self::Unknown => "unknown",
        }
    }

    /// Эвристический маппинг доменной [`Error`] → код.
    ///
    /// Используется на call-site'ах, где `Error` уже есть, и нужно
    /// построить [`FailureReason`] для `update_state(Failed, …)`.
    /// `Error::Other` (свободный текст) отправляется в `Unknown`.
    pub fn from_error(err: &Error) -> Self {
        match err {
            Error::SourceMutated => Self::SourceMutated,
            Error::TruncatedResponse => Self::InvalidResponse,
            // `ENOSPC` — самый устойчивый индикатор отсутствия места.
            // `ErrorKind::StorageFull` в stable пока не стабилизирован,
            // raw_os_error работает на всех unix.
            Error::Io(io) if io.raw_os_error() == Some(28) => Self::DiskFull,
            Error::Io(_) => Self::Network,
            Error::NotFound | Error::FileExists { .. } | Error::Other(_) => Self::Unknown,
        }
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReasonCode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "network" => Self::Network,
            "timeout" => Self::Timeout,
            "http_4xx" => Self::Http4xx,
            "http_5xx" => Self::Http5xx,
            "source_mutated" => Self::SourceMutated,
            "disk_full" => Self::DiskFull,
            "invalid_response" => Self::InvalidResponse,
            "cancelled_by_user" => Self::CancelledByUser,
            "unknown" => Self::Unknown,
            other => return Err(Error::Other(format!("unknown reason code: {other}"))),
        })
    }
}

/// Причина перехода состояния: категория + свободный текст.
///
/// Используется в `TQueueStore::update_state`. При переходе в
/// `Failed` обязательна; для остальных переходов — опциональна.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReason {
    pub code: ReasonCode,
    pub message: Option<String>,
}

impl FailureReason {
    pub fn new(code: ReasonCode) -> Self {
        Self {
            code,
            message: None,
        }
    }

    pub fn with_message(code: ReasonCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
        }
    }

    /// Построить из доменной `Error`: код — через
    /// [`ReasonCode::from_error`], сообщение — `err.to_string()`.
    pub fn from_error(err: &Error) -> Self {
        Self::with_message(ReasonCode::from_error(err), err.to_string())
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(m) => write!(f, "{}: {m}", self.code),
            None => fmt::Display::fmt(&self.code, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_str_roundtrip_covers_all_variants() {
        for c in [
            ReasonCode::Network,
            ReasonCode::Timeout,
            ReasonCode::Http4xx,
            ReasonCode::Http5xx,
            ReasonCode::SourceMutated,
            ReasonCode::DiskFull,
            ReasonCode::InvalidResponse,
            ReasonCode::CancelledByUser,
            ReasonCode::Unknown,
        ] {
            let parsed: ReasonCode = c.as_str().parse().unwrap();
            assert_eq!(c, parsed);
        }
    }

    #[test]
    fn parse_rejects_unknown_code() {
        assert!("bogus".parse::<ReasonCode>().is_err());
    }

    #[test]
    fn from_error_maps_known_variants() {
        assert_eq!(
            ReasonCode::from_error(&Error::SourceMutated),
            ReasonCode::SourceMutated
        );
        assert_eq!(
            ReasonCode::from_error(&Error::TruncatedResponse),
            ReasonCode::InvalidResponse
        );
        assert_eq!(
            ReasonCode::from_error(&Error::Other("x".into())),
            ReasonCode::Unknown
        );
    }

    #[test]
    fn failure_reason_display_includes_message() {
        let r = FailureReason::with_message(ReasonCode::Timeout, "deadline 30s");
        assert_eq!(r.to_string(), "timeout: deadline 30s");
        let r = FailureReason::new(ReasonCode::Network);
        assert_eq!(r.to_string(), "network");
    }
}
