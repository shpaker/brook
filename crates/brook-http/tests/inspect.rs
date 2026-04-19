//! Интеграционные тесты `HttpInspectClient` через `wiremock`.

use brook_core::{
    InspectError,
    THttpInspect,
};
use brook_http::{
    HttpClientBuilder,
    HttpInspectClient,
};
use wiremock::matchers::{
    header,
    method,
    path,
};
use wiremock::{
    Mock,
    MockServer,
    ResponseTemplate,
};

fn client() -> HttpInspectClient {
    HttpInspectClient::new(HttpClientBuilder::new().build())
}

#[tokio::test]
async fn head_ok_parses_all_headers() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/file.bin"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "1234")
                .insert_header("accept-ranges", "bytes")
                .insert_header("etag", "\"abc\"")
                .insert_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                .insert_header("content-disposition", "attachment; filename=\"report.pdf\""),
        )
        .mount(&server)
        .await;

    let url = format!("{}/file.bin", server.uri());
    let report = client().inspect(&url).await.expect("inspect ok");

    assert_eq!(report.total_size, Some(1234));
    assert!(report.accepts_ranges);
    assert_eq!(report.etag.as_deref(), Some("\"abc\""));
    assert_eq!(
        report.last_modified.as_deref(),
        Some("Wed, 21 Oct 2015 07:28:00 GMT")
    );
    assert_eq!(report.filename.as_deref(), Some("report.pdf"));
}

#[tokio::test]
async fn head_fails_falls_back_to_get_range() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/f"))
        .respond_with(ResponseTemplate::new(405))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/f"))
        .and(header("range", "bytes=0-0"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-0/98765")
                .insert_header("accept-ranges", "bytes")
                .set_body_bytes(vec![0u8; 1]),
        )
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let report = client().inspect(&url).await.expect("inspect ok via GET");

    assert_eq!(report.total_size, Some(98765));
    assert!(report.accepts_ranges);
}

#[tokio::test]
async fn no_accept_ranges_header_means_false() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/f"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "42"))
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let report = client().inspect(&url).await.expect("ok");
    assert!(!report.accepts_ranges);
    assert_eq!(report.total_size, Some(42));
}

#[tokio::test]
async fn filename_url_fallback_when_no_header() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/dir/thing.tar.gz"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "1"))
        .mount(&server)
        .await;

    let url = format!("{}/dir/thing.tar.gz", server.uri());
    let report = client().inspect(&url).await.expect("ok");
    assert_eq!(report.filename.as_deref(), Some("thing.tar.gz"));
}

#[tokio::test]
async fn filename_ext_utf8_decoded() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/x"))
        .respond_with(ResponseTemplate::new(200).insert_header(
            "content-disposition",
            "attachment; filename*=UTF-8''%D1%84%D0%B0%D0%B9%D0%BB.bin",
        ))
        .mount(&server)
        .await;

    let url = format!("{}/x", server.uri());
    let report = client().inspect(&url).await.expect("ok");
    assert_eq!(report.filename.as_deref(), Some("файл.bin"));
}

#[tokio::test]
async fn invalid_scheme_fails_without_network() {
    // Отсутствующий listener — если адаптер полезет в сеть, тест зависнет
    // на connect-timeout (что тоже будет fail). Проверка: ошибка приходит
    // **мгновенно**.
    let err = client()
        .inspect("ftp://example.com/file")
        .await
        .expect_err("should fail");
    assert!(
        matches!(err, InspectError::InvalidScheme(ref s) if s == "ftp"),
        "unexpected: {err:?}"
    );
}
