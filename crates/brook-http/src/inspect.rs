//! `HttpInspectClient` — реализация [`brook_core::THttpInspect`].
//!
//! Алгоритм (smart probe): единственный `GET url` с заголовком
//! `Range: bytes=0-0`. Читаем только status + headers и сразу
//! `drop(resp)` — тело ответа не тянем:
//! - `206 Partial Content` → `Content-Range: bytes 0-0/TOTAL` → размер
//!   известен, `accepts_ranges = true`. Вариант `.../*` → `total_size = None`.
//! - `200 OK + Content-Range` → сервер поддерживает ranges, но возвращает
//!   неправильный статус (старый Apache, ряд CDN). `accepts_ranges = true`.
//! - `200 OK` без `Content-Range` → сервер проигнорировал Range; `total_size`
//!   из `Content-Length` (может быть `None` для chunked), `accepts_ranges = false`
//!   независимо от декларированного `Accept-Ranges` (он соврал).
//! - иначе → `InspectError::UnexpectedStatus`.
//!
//! `effective_url` берётся из `response.url()` до drop'а — пригодится
//! воркерам, чтобы бить по разрешённому (после редиректов) URL.

use std::time::Duration;

use async_trait::async_trait;
use brook_core::{
    InspectError,
    InspectReport,
    THttpInspect,
};
use reqwest::StatusCode;
use reqwest::header::{
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

        let resp = self
            .client
            .get(parsed.clone())
            .header(RANGE, "bytes=0-0")
            .timeout(READ_TIMEOUT)
            .send()
            .await
            .map_err(map_mw_error)?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let final_url = resp.url().clone();
        // СРАЗУ закрываем ответ: НЕ зовём .bytes()/.text()/.chunk().
        // Для серверов, ответивших 200 вместо 206, это спасает нас от
        // полного скачивания тела только ради inspect.
        drop(resp);

        let effective_url = if final_url == parsed {
            None
        } else {
            Some(final_url.to_string())
        };

        if status == StatusCode::PARTIAL_CONTENT {
            let total = parse_content_range_total(headers.get(CONTENT_RANGE))
                .map_err(InspectError::Malformed)?;
            Ok(InspectReport {
                total_size: total,
                accepts_ranges: true,
                etag: header_str(&headers, ETAG),
                last_modified: header_str(&headers, LAST_MODIFIED),
                filename: pick_filename(headers.get(CONTENT_DISPOSITION), &parsed),
                effective_url,
            })
        } else if status.is_success() {
            // 200 OK — формально сервер не выполнил Range. Но некоторые серверы
            // (старый Apache, ряд CDN) возвращают 200 вместо 206 и при этом
            // всё равно кладут Content-Range в заголовки — это значит ranges
            // реально работают, просто статус неправильный.
            // Accept-Ranges игнорируем: сервер мог соврать (и вернуть 200
            // с полным телом + Accept-Ranges: bytes одновременно).
            if let Some(cr) = headers.get(CONTENT_RANGE) {
                // 200 + Content-Range: сервер поддерживает ranges (нестандартно,
                // но надёжно). Отличаем «заголовок присутствует» от «заголовка нет»
                // до вызова parse_content_range_total, потому что та возвращает
                // Ok(None) для обоих случаев (absent и «bytes 0-0/*»).
                let total = parse_content_range_total(Some(cr)).map_err(InspectError::Malformed)?;
                Ok(InspectReport {
                    total_size: total,
                    accepts_ranges: true,
                    etag: header_str(&headers, ETAG),
                    last_modified: header_str(&headers, LAST_MODIFIED),
                    filename: pick_filename(headers.get(CONTENT_DISPOSITION), &parsed),
                    effective_url,
                })
            } else {
                // Нет Content-Range → размер из Content-Length, ranges недоступны.
                Ok(InspectReport {
                    total_size: parse_content_length(headers.get(CONTENT_LENGTH)),
                    accepts_ranges: false,
                    etag: header_str(&headers, ETAG),
                    last_modified: header_str(&headers, LAST_MODIFIED),
                    filename: pick_filename(headers.get(CONTENT_DISPOSITION), &parsed),
                    effective_url,
                })
            }
        } else {
            Err(InspectError::UnexpectedStatus {
                code: status.as_u16(),
            })
        }
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

    // ─── Smart probe integration tests via wiremock ────────────────────
    use reqwest::Client;
    use reqwest_middleware::ClientBuilder;
    use wiremock::matchers::{
        method,
        path,
    };
    use wiremock::{
        Mock,
        MockServer,
        ResponseTemplate,
    };

    fn client() -> ClientWithMiddleware {
        ClientBuilder::new(Client::new()).build()
    }

    #[tokio::test]
    async fn probe_206_parses_content_range_total() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/f.bin"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-0/4096")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("ETag", "\"abc\"")
                    .set_body_bytes(b"X".to_vec()),
            )
            .mount(&server)
            .await;

        let ins = HttpInspectClient::new(client());
        let rep = ins
            .inspect(&format!("{}/f.bin", server.uri()))
            .await
            .unwrap();
        assert_eq!(rep.total_size, Some(4096));
        assert!(rep.accepts_ranges);
        assert_eq!(rep.etag.as_deref(), Some("\"abc\""));
    }

    #[tokio::test]
    async fn probe_200_with_content_length_no_range_support() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/f.bin"))
            .respond_with(
                ResponseTemplate::new(200)
                    // wiremock сам выставит Content-Length из body; этого
                    // и хватит для проверки, что мы читаем размер из
                    // Content-Length при 200-ответе.
                    // Сервер анонсирует Ranges, но на Range ответил 200.
                    .insert_header("Accept-Ranges", "bytes")
                    .set_body_bytes(vec![0u8; 123_456]),
            )
            .mount(&server)
            .await;

        let ins = HttpInspectClient::new(client());
        let rep = ins
            .inspect(&format!("{}/f.bin", server.uri()))
            .await
            .unwrap();
        assert_eq!(rep.total_size, Some(123456));
        assert!(
            !rep.accepts_ranges,
            "200 — Range не поддерживается, невзирая на Accept-Ranges"
        );
    }

    #[tokio::test]
    async fn probe_200_with_content_range_treats_as_range_supported() {
        // Некоторые серверы возвращают 200 вместо 206, но всё равно кладут
        // Content-Range — это значит ranges работают (нестандартно).
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/f.bin"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Range", "bytes 0-0/9999")
                    .set_body_bytes(vec![0u8; 1]),
            )
            .mount(&server)
            .await;

        let ins = HttpInspectClient::new(client());
        let rep = ins
            .inspect(&format!("{}/f.bin", server.uri()))
            .await
            .unwrap();
        assert!(
            rep.accepts_ranges,
            "200 + Content-Range → сервер поддерживает ranges"
        );
        assert_eq!(rep.total_size, Some(9999));
    }

    #[tokio::test]
    async fn probe_200_without_content_length_streams_unknown_size() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/s"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 256]))
            .mount(&server)
            .await;

        let ins = HttpInspectClient::new(client());
        let rep = ins.inspect(&format!("{}/s", server.uri())).await.unwrap();
        assert!(!rep.accepts_ranges);
    }

    #[tokio::test]
    async fn probe_4xx_returns_unexpected_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/g"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let ins = HttpInspectClient::new(client());
        let err = ins
            .inspect(&format!("{}/g", server.uri()))
            .await
            .unwrap_err();
        assert!(matches!(err, InspectError::UnexpectedStatus { code: 404 }));
    }
}
