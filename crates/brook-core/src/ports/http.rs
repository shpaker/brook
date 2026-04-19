//! HTTP-порты ядра: абстракции сетевого ввода-вывода.
//!
//! Ядро не знает про `reqwest`, TLS-бэкенд или middleware — оно только
//! описывает, **что** оно ждёт от HTTP-слоя:
//! - [`THttpInspect`] — осмотр URL, сбор метаданных (размер, Range-способность,
//!   guard-заголовки, имя файла) без скачивания.
//! - [`TRangeFetch`] — потоковые запросы за байтами: частичный диапазон
//!   ([`TRangeFetch::fetch_range`]) или полное тело как fallback, когда сервер
//!   не поддерживает Range ([`TRangeFetch::fetch_full`]).
//!
//! ## Соглашения
//!
//! - Реализации обоих трейтов называются с суффиксом `Client`
//!   (`HttpInspectClient`, `RangeFetchClient`) — это правило workspace'а.
//! - Адаптер **обязан** принимать только `http`/`https` URL и возвращать
//!   [`InspectError::InvalidScheme`] / [`RangeError::InvalidScheme`]
//!   **до** любого сетевого обращения.
//! - Типы из `reqwest` (`Client`, `Response`, `RequestBuilder`, `Error`) и
//!   `reqwest-middleware` не должны утекать в публичные сигнатуры; все ошибки
//!   маппятся в доменные enum'ы ниже.
//! - Дроп [`ByteStream`] обязан отменять in-flight HTTP-запрос.
//!
//! Retry здесь **не делается** — оба error-enum'а дают метод `is_transient()`,
//! чтобы будущий `RetryPolicy` на уровне `DownloadEngine` мог классифицировать
//! отказ как временный или постоянный.

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use thiserror::Error;

/// Поток байт одного HTTP-ответа.
///
/// Отдельный type-alias на `Pin<Box<dyn Stream<...> + Send>>` — чтобы
/// ядро не знало про `reqwest::Response::bytes_stream()` и подобные
/// адаптер-специфичные конструкции. Дроп значения обязан отменять
/// in-flight запрос у адаптера.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, RangeError>> + Send>>;

/// Отчёт об осмотре URL: то, что удалось вытащить из заголовков до скачивания.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectReport {
    /// Общий размер файла, если сервер сообщил `Content-Length`.
    pub total_size: Option<u64>,
    /// `Accept-Ranges: bytes` → `true`; иначе `false`.
    pub accepts_ranges: bool,
    /// Значение заголовка `ETag` (в т.ч. weak — начинается с `W/`).
    pub etag: Option<String>,
    /// Значение заголовка `Last-Modified` (RFC 7231 IMF-fixdate как есть).
    pub last_modified: Option<String>,
    /// Имя файла: `Content-Disposition filename*=` → `filename=` → хвост URL.
    pub filename: Option<String>,
}

/// Guard-заголовок для Range-запросов: защита от мутации источника между пиками.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeGuard {
    /// Пошлётся как `If-Match: <etag>`.
    Etag(String),
    /// Пошлётся как `If-Unmodified-Since: <date>`.
    LastModified(String),
}

impl RangeGuard {
    /// Выбрать guard из отчёта: сильный вариант (`ETag`) предпочтительнее
    /// слабого (`Last-Modified`); если ни того, ни другого нет — `None`
    /// (без guard-а Range-запрос всё равно имеет смысл, просто не защищён
    /// от подмены).
    pub fn from_report(report: &InspectReport) -> Option<Self> {
        if let Some(etag) = &report.etag {
            Some(Self::Etag(etag.clone()))
        } else {
            report.last_modified.clone().map(Self::LastModified)
        }
    }
}

/// Ошибка `THttpInspect::inspect`.
#[derive(Debug, Error)]
pub enum InspectError {
    /// Сбой на уровне транспорта: DNS, TCP, TLS, разрыв соединения.
    #[error("network error: {0}")]
    Network(String),
    /// Вышел `connect`- или `read-idle`-таймаут.
    #[error("timeout")]
    Timeout,
    /// URL не `http`/`https`; выставляется до сетевого обращения.
    #[error("invalid URL scheme: {0}")]
    InvalidScheme(String),
    /// Сервер вернул неожиданный статус (и HEAD, и fallback GET провалились).
    #[error("unexpected HTTP status {code}")]
    UnexpectedStatus { code: u16 },
    /// Ответ синтаксически некорректен (например, `Content-Range` не парсится).
    #[error("malformed response: {0}")]
    Malformed(String),
}

impl InspectError {
    /// Можно ли ретраить этот отказ? Потребитель — `RetryPolicy`.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Network(_) | Self::Timeout => true,
            Self::UnexpectedStatus { code } => is_transient_status(*code),
            Self::InvalidScheme(_) | Self::Malformed(_) => false,
        }
    }
}

/// Ошибка `TRangeFetch::fetch_range` / `fetch_full`.
#[derive(Debug, Error)]
pub enum RangeError {
    #[error("network error: {0}")]
    Network(String),
    #[error("timeout")]
    Timeout,
    #[error("invalid URL scheme: {0}")]
    InvalidScheme(String),
    /// Сервер проверил guard (`If-Match` / `If-Unmodified-Since`) и ответил
    /// `412 Precondition Failed` — источник изменился, продолжать нельзя.
    #[error("source mutated since inspect")]
    SourceMutated,
    /// Сервер ответил `200 OK` на Range-запрос → Range не поддерживает.
    /// Engine должен переключиться на `fetch_full` (один воркер, до EOF).
    #[error("server does not support range requests")]
    RangeNotSupported,
    /// Поток завершился до получения запрошенного количества байт.
    #[error("truncated response")]
    TruncatedResponse,
    #[error("unexpected HTTP status {code}")]
    UnexpectedStatus { code: u16 },
}

impl RangeError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Network(_) | Self::Timeout | Self::TruncatedResponse => true,
            Self::UnexpectedStatus { code } => is_transient_status(*code),
            Self::InvalidScheme(_) | Self::SourceMutated | Self::RangeNotSupported => false,
        }
    }
}

/// HTTP-коды, которые имеет смысл ретраить:
/// - `408 Request Timeout`
/// - `429 Too Many Requests`
/// - `5xx` — серверные проблемы, обычно временные.
fn is_transient_status(code: u16) -> bool {
    code == 408 || code == 429 || (500..=599).contains(&code)
}

/// Порт «осмотреть URL» — собрать метаданные до скачивания.
#[async_trait]
pub trait THttpInspect: Send + Sync {
    async fn inspect(&self, url: &str) -> Result<InspectReport, InspectError>;
}

/// Порт «потянуть байты» — частичный диапазон или целиком.
#[async_trait]
pub trait TRangeFetch: Send + Sync {
    /// Запросить диапазон `[offset, offset + len)`.
    ///
    /// Контракт адаптера:
    /// - `206 Partial Content` + валидный `Content-Range` → успех;
    /// - `200 OK` → [`RangeError::RangeNotSupported`] (без чтения тела);
    /// - `412 Precondition Failed` → [`RangeError::SourceMutated`];
    /// - конец потока до `len` байт → финальный `Err(TruncatedResponse)`.
    ///
    /// `guard` — опциональный `If-Match` / `If-Unmodified-Since`.
    async fn fetch_range(
        &self,
        url: &str,
        offset: u64,
        len: u64,
        guard: Option<&RangeGuard>,
    ) -> Result<ByteStream, RangeError>;

    /// Полный `GET` до EOF — fallback, когда сервер не умеет Range.
    async fn fetch_full(&self, url: &str) -> Result<ByteStream, RangeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_guard_prefers_etag_over_last_modified() {
        let report = InspectReport {
            total_size: Some(100),
            accepts_ranges: true,
            etag: Some("\"abc\"".into()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            filename: None,
        };
        assert_eq!(
            RangeGuard::from_report(&report),
            Some(RangeGuard::Etag("\"abc\"".into()))
        );
    }

    #[test]
    fn range_guard_falls_back_to_last_modified() {
        let report = InspectReport {
            total_size: Some(100),
            accepts_ranges: true,
            etag: None,
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            filename: None,
        };
        assert_eq!(
            RangeGuard::from_report(&report),
            Some(RangeGuard::LastModified(
                "Wed, 21 Oct 2015 07:28:00 GMT".into()
            ))
        );
    }

    #[test]
    fn range_guard_returns_none_when_no_headers() {
        let report = InspectReport {
            total_size: None,
            accepts_ranges: false,
            etag: None,
            last_modified: None,
            filename: None,
        };
        assert_eq!(RangeGuard::from_report(&report), None);
    }

    #[test]
    fn inspect_error_transient_classification() {
        assert!(InspectError::Network("x".into()).is_transient());
        assert!(InspectError::Timeout.is_transient());
        assert!(InspectError::UnexpectedStatus { code: 503 }.is_transient());
        assert!(InspectError::UnexpectedStatus { code: 429 }.is_transient());
        assert!(InspectError::UnexpectedStatus { code: 408 }.is_transient());

        assert!(!InspectError::UnexpectedStatus { code: 404 }.is_transient());
        assert!(!InspectError::UnexpectedStatus { code: 400 }.is_transient());
        assert!(!InspectError::InvalidScheme("ftp".into()).is_transient());
        assert!(!InspectError::Malformed("x".into()).is_transient());
    }

    #[test]
    fn range_error_transient_classification() {
        assert!(RangeError::Network("x".into()).is_transient());
        assert!(RangeError::Timeout.is_transient());
        assert!(RangeError::TruncatedResponse.is_transient());
        assert!(RangeError::UnexpectedStatus { code: 502 }.is_transient());

        assert!(!RangeError::SourceMutated.is_transient());
        assert!(!RangeError::RangeNotSupported.is_transient());
        assert!(!RangeError::InvalidScheme("file".into()).is_transient());
        assert!(!RangeError::UnexpectedStatus { code: 404 }.is_transient());
    }
}
