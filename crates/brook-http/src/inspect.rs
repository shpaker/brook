//! `HttpInspectClient` — реализация [`brook_core::THttpInspect`].
//!
//! Алгоритм:
//! 1. `HEAD url`. Если `2xx` и есть полезные заголовки — готово.
//! 2. Иначе (`4xx`/`5xx` или сетевой сбой) — fallback `GET url` с заголовком
//!    `Range: bytes=0-0`. Размер берётся из `Content-Range: bytes 0-0/TOTAL`.
//! 3. Имя файла: `Content-Disposition filename*=` (RFC 5987) → `filename=`
//!    → последний сегмент URL-пути.

use std::time::Duration;

use async_trait::async_trait;
use brook_core::{
    InspectError,
    InspectReport,
    THttpInspect,
};
use reqwest::StatusCode;
use reqwest::header::{
    ACCEPT_RANGES,
    CONTENT_DISPOSITION,
    CONTENT_LENGTH,
    CONTENT_RANGE,
    ETAG,
    HeaderMap,
    LAST_MODIFIED,
    RANGE,
};
use reqwest_middleware::ClientWithMiddleware;
use url::Url;

use crate::url_scheme::validate_http_url;

/// Read-idle timeout для `inspect`: метаданные должны прийти быстро.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpInspectClient {
    client: ClientWithMiddleware,
}

impl HttpInspectClient {
    pub fn new(client: ClientWithMiddleware) -> Self {
        Self { client }
    }
}

#[async_trait]
impl THttpInspect for HttpInspectClient {
    async fn inspect(&self, url: &str) -> Result<InspectReport, InspectError> {
        let parsed = validate_http_url(url).map_err(classify_url_error)?;

        // Сначала HEAD.
        let head = self
            .client
            .head(parsed.clone())
            .timeout(READ_TIMEOUT)
            .send()
            .await;

        match head {
            Ok(resp) if resp.status().is_success() => Ok(report_from_head(resp.headers(), &parsed)),
            // HEAD-неудача (4xx/5xx) → fallback на GET-Range.
            Ok(_) | Err(_) => self.inspect_via_range(&parsed).await,
        }
    }
}

impl HttpInspectClient {
    async fn inspect_via_range(&self, url: &Url) -> Result<InspectReport, InspectError> {
        let resp = self
            .client
            .get(url.clone())
            .header(RANGE, "bytes=0-0")
            .timeout(READ_TIMEOUT)
            .send()
            .await
            .map_err(map_mw_error)?;

        let status = resp.status();
        let headers = resp.headers().clone();

        // Успешные варианты: 206 (Range поддерживается) или 200 (нет, но тело
        // есть — размер тогда из Content-Length).
        if status == StatusCode::PARTIAL_CONTENT {
            let total = parse_content_range_total(headers.get(CONTENT_RANGE))
                .map_err(InspectError::Malformed)?;
            Ok(InspectReport {
                total_size: total,
                accepts_ranges: accepts_ranges(headers.get(ACCEPT_RANGES)) || total.is_some(),
                etag: header_str(&headers, ETAG),
                last_modified: header_str(&headers, LAST_MODIFIED),
                filename: pick_filename(headers.get(CONTENT_DISPOSITION), url),
            })
        } else if status.is_success() {
            Ok(report_from_head(&headers, url))
        } else {
            Err(InspectError::UnexpectedStatus {
                code: status.as_u16(),
            })
        }
    }
}

fn report_from_head(headers: &HeaderMap, url: &Url) -> InspectReport {
    InspectReport {
        total_size: parse_content_length(headers.get(CONTENT_LENGTH)),
        accepts_ranges: accepts_ranges(headers.get(ACCEPT_RANGES)),
        etag: header_str(headers, ETAG),
        last_modified: header_str(headers, LAST_MODIFIED),
        filename: pick_filename(headers.get(CONTENT_DISPOSITION), url),
    }
}

fn header_str(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn parse_content_length(value: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    value?.to_str().ok()?.trim().parse::<u64>().ok()
}

fn accepts_ranges(value: Option<&reqwest::header::HeaderValue>) -> bool {
    value
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().eq_ignore_ascii_case("bytes"))
        .unwrap_or(false)
}

/// Парсит `Content-Range: bytes 0-0/TOTAL`, возвращает TOTAL.
/// `*` в TOTAL → `None`.
fn parse_content_range_total(
    value: Option<&reqwest::header::HeaderValue>,
) -> Result<Option<u64>, String> {
    let Some(v) = value else {
        return Ok(None);
    };
    let s = v
        .to_str()
        .map_err(|_| "Content-Range: non-ASCII header value".to_string())?
        .trim();
    // "bytes 0-0/1234" | "bytes 0-0/*"
    let rest = s
        .strip_prefix("bytes ")
        .ok_or_else(|| format!("Content-Range: unexpected format `{s}`"))?;
    let (_range_part, total_part) = rest
        .split_once('/')
        .ok_or_else(|| format!("Content-Range: missing `/` in `{s}`"))?;
    if total_part == "*" {
        Ok(None)
    } else {
        total_part
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("Content-Range: bad total `{total_part}`"))
    }
}

/// Выбор имени файла: `filename*=UTF-8''…` → `filename="…"` → хвост URL.
fn pick_filename(header: Option<&reqwest::header::HeaderValue>, url: &Url) -> Option<String> {
    if let Some(hv) = header
        && let Ok(raw) = hv.to_str()
        && let Some(name) = parse_content_disposition(raw)
    {
        return Some(name);
    }
    url.path_segments()
        .and_then(|mut s| s.next_back())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .and_then(|s| percent_decode(&s))
}

fn parse_content_disposition(raw: &str) -> Option<String> {
    // Сперва ищем filename*= (RFC 5987), он приоритетен.
    for part in raw.split(';').map(str::trim) {
        if let Some(v) = part.strip_prefix("filename*=") {
            return decode_ext_filename(v);
        }
    }
    for part in raw.split(';').map(str::trim) {
        if let Some(v) = part.strip_prefix("filename=") {
            return Some(strip_quotes(v).to_string()).filter(|s| !s.is_empty());
        }
    }
    None
}

/// Декодер RFC 5987 `charset'lang'percent-encoded-value`.
fn decode_ext_filename(raw: &str) -> Option<String> {
    let (charset, rest) = raw.split_once('\'')?;
    let (_lang, value) = rest.split_once('\'')?;
    if !charset.eq_ignore_ascii_case("utf-8") {
        // Для MVP поддерживаем только UTF-8; прочие кодировки — пропуск.
        return None;
    }
    percent_decode(value)
}

fn percent_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16)?;
            let lo = (bytes[i + 2] as char).to_digit(16)?;
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(b);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn strip_quotes(s: &str) -> &str {
    let s = s.trim();
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

fn classify_url_error(err: String) -> InspectError {
    // validate_http_url возвращает либо строку-схему (ftp/file/...), либо
    // "invalid URL ...": первую считаем InvalidScheme, вторую — Malformed.
    if err.starts_with("invalid URL ") {
        InspectError::Malformed(err)
    } else {
        InspectError::InvalidScheme(err)
    }
}

fn map_mw_error(err: reqwest_middleware::Error) -> InspectError {
    match err {
        reqwest_middleware::Error::Reqwest(e) => map_reqwest_error(e),
        reqwest_middleware::Error::Middleware(e) => InspectError::Network(e.to_string()),
    }
}

fn map_reqwest_error(err: reqwest::Error) -> InspectError {
    if err.is_timeout() {
        InspectError::Timeout
    } else if let Some(status) = err.status() {
        InspectError::UnexpectedStatus {
            code: status.as_u16(),
        }
    } else {
        InspectError::Network(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_from_ext_utf8() {
        let hv = reqwest::header::HeaderValue::from_static(
            "attachment; filename*=UTF-8''%D1%84%D0%B0%D0%B9%D0%BB.bin",
        );
        let name = parse_content_disposition(hv.to_str().unwrap()).unwrap();
        assert_eq!(name, "файл.bin");
    }

    #[test]
    fn filename_quoted_fallback() {
        let hv =
            reqwest::header::HeaderValue::from_static("attachment; filename=\"report-2024.pdf\"");
        let name = parse_content_disposition(hv.to_str().unwrap()).unwrap();
        assert_eq!(name, "report-2024.pdf");
    }

    #[test]
    fn filename_prefers_ext_over_plain() {
        let hv = reqwest::header::HeaderValue::from_static(
            "attachment; filename=\"x\"; filename*=UTF-8''y",
        );
        let name = parse_content_disposition(hv.to_str().unwrap()).unwrap();
        assert_eq!(name, "y");
    }

    #[test]
    fn content_range_total_parses() {
        let hv = reqwest::header::HeaderValue::from_static("bytes 0-0/12345");
        let total = parse_content_range_total(Some(&hv)).unwrap();
        assert_eq!(total, Some(12345));
    }

    #[test]
    fn content_range_unknown_total() {
        let hv = reqwest::header::HeaderValue::from_static("bytes 0-0/*");
        let total = parse_content_range_total(Some(&hv)).unwrap();
        assert_eq!(total, None);
    }

    #[test]
    fn url_tail_fallback() {
        let url = Url::parse("https://example.com/a/b/file.tar.gz").unwrap();
        let name = pick_filename(None, &url).unwrap();
        assert_eq!(name, "file.tar.gz");
    }

    #[test]
    fn url_tail_percent_decoded() {
        let url = Url::parse("https://example.com/a/%D1%84%D0%B0%D0%B9%D0%BB.bin").unwrap();
        let name = pick_filename(None, &url).unwrap();
        assert_eq!(name, "файл.bin");
    }
}
