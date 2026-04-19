//! `RangeFetchClient` — реализация [`brook_core::TRangeFetch`].
//!
//! - `fetch_range(offset, len, guard)`: ставит `Range: bytes=OFFSET-END`,
//!   `If-Match`/`If-Unmodified-Since` из [`RangeGuard`]; валидирует `206` +
//!   `Content-Range`; `200` → [`RangeError::RangeNotSupported`]; `412` →
//!   [`RangeError::SourceMutated`]; конец потока до `len` байт → финальный
//!   `Err(TruncatedResponse)`.
//! - `fetch_full()`: обычный `GET` до EOF.
//!
//! Cancellation: дроп `ByteStream` дропает `reqwest::Response` → соединение
//! закрывается.

use async_trait::async_trait;
use brook_core::{
    ByteStream,
    RangeError,
    RangeGuard,
    TRangeFetch,
};
use bytes::Bytes;
use futures_util::{
    StreamExt,
    stream,
};
use reqwest::StatusCode;
use reqwest::header::{
    CONTENT_RANGE,
    IF_MATCH,
    IF_UNMODIFIED_SINCE,
    RANGE,
};
use reqwest_middleware::{
    ClientWithMiddleware,
    RequestBuilder,
};

use crate::url_scheme::validate_http_url;

pub struct RangeFetchClient {
    client: ClientWithMiddleware,
}

impl RangeFetchClient {
    pub fn new(client: ClientWithMiddleware) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TRangeFetch for RangeFetchClient {
    async fn fetch_range(
        &self,
        url: &str,
        offset: u64,
        len: u64,
        guard: Option<&RangeGuard>,
    ) -> Result<ByteStream, RangeError> {
        let parsed = validate_http_url(url).map_err(classify_url_error)?;
        if len == 0 {
            return Ok(empty_stream());
        }
        let end = offset
            .checked_add(len - 1)
            .ok_or_else(|| RangeError::Network("range offset overflow".into()))?;
        let range_header = format!("bytes={offset}-{end}");

        let mut req = self.client.get(parsed).header(RANGE, range_header.clone());
        req = apply_guard(req, guard);

        let resp = req.send().await.map_err(map_mw_error)?;
        let status = resp.status();

        match status {
            StatusCode::PARTIAL_CONTENT => {
                validate_content_range(resp.headers().get(CONTENT_RANGE), offset, end)?;
                Ok(length_checked_stream(resp, len))
            }
            StatusCode::OK => Err(RangeError::RangeNotSupported),
            StatusCode::PRECONDITION_FAILED => Err(RangeError::SourceMutated),
            other => Err(RangeError::UnexpectedStatus {
                code: other.as_u16(),
            }),
        }
    }

    async fn fetch_full(&self, url: &str) -> Result<ByteStream, RangeError> {
        let parsed = validate_http_url(url).map_err(classify_url_error)?;
        let resp = self.client.get(parsed).send().await.map_err(map_mw_error)?;

        let status = resp.status();
        if !status.is_success() {
            return Err(RangeError::UnexpectedStatus {
                code: status.as_u16(),
            });
        }
        Ok(Box::pin(resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| RangeError::Network(e.to_string()))
        })))
    }
}

fn apply_guard(req: RequestBuilder, guard: Option<&RangeGuard>) -> RequestBuilder {
    match guard {
        Some(RangeGuard::Etag(v)) => req.header(IF_MATCH, v.clone()),
        Some(RangeGuard::LastModified(v)) => req.header(IF_UNMODIFIED_SINCE, v.clone()),
        None => req,
    }
}

/// Валидирует, что `Content-Range: bytes X-Y/TOTAL` действительно совпадает
/// с `offset..=end`. Игнорируем TOTAL — он может быть `*`.
fn validate_content_range(
    value: Option<&reqwest::header::HeaderValue>,
    offset: u64,
    end: u64,
) -> Result<(), RangeError> {
    let Some(v) = value else {
        return Err(RangeError::UnexpectedStatus { code: 206 });
    };
    let s = v
        .to_str()
        .map_err(|_| RangeError::UnexpectedStatus { code: 206 })?
        .trim();
    let rest = s
        .strip_prefix("bytes ")
        .ok_or(RangeError::UnexpectedStatus { code: 206 })?;
    let (range_part, _total) = rest
        .split_once('/')
        .ok_or(RangeError::UnexpectedStatus { code: 206 })?;
    let (got_off, got_end) = range_part
        .split_once('-')
        .ok_or(RangeError::UnexpectedStatus { code: 206 })?;
    let got_off: u64 = got_off
        .parse()
        .map_err(|_| RangeError::UnexpectedStatus { code: 206 })?;
    let got_end: u64 = got_end
        .parse()
        .map_err(|_| RangeError::UnexpectedStatus { code: 206 })?;
    if got_off == offset && got_end == end {
        Ok(())
    } else {
        Err(RangeError::UnexpectedStatus { code: 206 })
    }
}

/// Оборачивает тело ответа так, чтобы:
/// - сетевые ошибки маппились в `RangeError::Network`;
/// - если поток закончился до `expected` байт — финальный элемент
///   `Err(TruncatedResponse)`.
fn length_checked_stream(resp: reqwest::Response, expected: u64) -> ByteStream {
    let inner = resp.bytes_stream();
    let state = TrailState {
        remaining: expected,
    };
    Box::pin(stream::unfold(
        (Box::pin(inner), state, false),
        move |(mut inner, mut state, done)| async move {
            if done {
                return None;
            }
            match inner.next().await {
                Some(Ok(bytes)) => {
                    state.remaining = state.remaining.saturating_sub(bytes.len() as u64);
                    Some((Ok::<Bytes, RangeError>(bytes), (inner, state, false)))
                }
                Some(Err(e)) => Some((
                    Err(RangeError::Network(e.to_string())),
                    (inner, state, true),
                )),
                None => {
                    if state.remaining > 0 {
                        Some((Err(RangeError::TruncatedResponse), (inner, state, true)))
                    } else {
                        None
                    }
                }
            }
        },
    ))
}

fn empty_stream() -> ByteStream {
    Box::pin(stream::empty())
}

struct TrailState {
    remaining: u64,
}

fn classify_url_error(err: String) -> RangeError {
    if err.starts_with("invalid URL ") {
        RangeError::Network(err)
    } else {
        RangeError::InvalidScheme(err)
    }
}

fn map_mw_error(err: reqwest_middleware::Error) -> RangeError {
    match err {
        reqwest_middleware::Error::Reqwest(e) => map_reqwest_error(e),
        reqwest_middleware::Error::Middleware(e) => RangeError::Network(e.to_string()),
    }
}

fn map_reqwest_error(err: reqwest::Error) -> RangeError {
    if err.is_timeout() {
        RangeError::Timeout
    } else if let Some(status) = err.status() {
        RangeError::UnexpectedStatus {
            code: status.as_u16(),
        }
    } else {
        RangeError::Network(err.to_string())
    }
}
