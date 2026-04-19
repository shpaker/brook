//! Интеграционные тесты `RangeFetchClient` через `wiremock`.

use brook_core::{
    RangeError,
    RangeGuard,
    TRangeFetch,
};
use brook_http::{
    HttpClientBuilder,
    RangeFetchClient,
};
use futures_util::StreamExt;
use wiremock::matchers::{
    header,
    header_exists,
    method,
    path,
};
use wiremock::{
    Mock,
    MockServer,
    ResponseTemplate,
};

fn client() -> RangeFetchClient {
    RangeFetchClient::new(HttpClientBuilder::new().build())
}

async fn collect(mut s: brook_core::ByteStream) -> Result<Vec<u8>, RangeError> {
    let mut out = Vec::new();
    while let Some(chunk) = s.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

#[tokio::test]
async fn range_206_ok_delivers_requested_bytes() {
    let server = MockServer::start().await;
    let payload = b"HELLO_WORLD".to_vec();
    Mock::given(method("GET"))
        .and(path("/f"))
        .and(header("range", "bytes=0-10"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-10/11")
                .set_body_bytes(payload.clone()),
        )
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let stream = client()
        .fetch_range(&url, 0, 11, None)
        .await
        .expect("206 ok");
    let body = collect(stream).await.expect("no stream err");
    assert_eq!(body, payload);
}

#[tokio::test]
async fn server_returns_200_on_range_signals_unsupported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/f"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 5]))
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let result = client().fetch_range(&url, 0, 5, None).await;
    let err = match result {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(matches!(err, RangeError::RangeNotSupported), "{err:?}");
}

#[tokio::test]
async fn precondition_failed_maps_to_source_mutated() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/f"))
        .and(header("if-match", "\"abc\""))
        .respond_with(ResponseTemplate::new(412))
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let err = match client()
        .fetch_range(&url, 0, 10, Some(&RangeGuard::Etag("\"abc\"".into())))
        .await
    {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(matches!(err, RangeError::SourceMutated), "{err:?}");
}

#[tokio::test]
async fn truncated_body_yields_final_truncated_error() {
    let server = MockServer::start().await;
    // Просим 10 байт, сервер отдаёт валидный 206 с правильным Content-Range,
    // но тело — 3 байта.
    Mock::given(method("GET"))
        .and(path("/f"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-9/100")
                .set_body_bytes(vec![1, 2, 3]),
        )
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let mut stream = client().fetch_range(&url, 0, 10, None).await.expect("ok");

    let first = stream.next().await.expect("some").expect("bytes chunk");
    assert!(!first.is_empty());

    // Дочитываем до конца — должен прийти `Err(TruncatedResponse)`.
    let mut last_err = None;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(_) => {} // допустим ещё чанк
            Err(e) => {
                last_err = Some(e);
                break;
            }
        }
    }
    assert!(
        matches!(last_err, Some(RangeError::TruncatedResponse)),
        "{last_err:?}"
    );
}

#[tokio::test]
async fn bad_content_range_is_unexpected_status() {
    let server = MockServer::start().await;
    // 206, но Content-Range не совпадает с запрошенным.
    Mock::given(method("GET"))
        .and(path("/f"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 5-14/100")
                .set_body_bytes(vec![0u8; 10]),
        )
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let err = match client().fetch_range(&url, 0, 10, None).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(matches!(err, RangeError::UnexpectedStatus { code: 206 }));
}

#[tokio::test]
async fn fetch_full_streams_body_to_eof() {
    let server = MockServer::start().await;
    let payload = b"full-body".to_vec();
    Mock::given(method("GET"))
        .and(path("/f"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.clone()))
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let stream = client().fetch_full(&url).await.expect("ok");
    let body = collect(stream).await.expect("no err");
    assert_eq!(body, payload);
}

#[tokio::test]
async fn invalid_scheme_fails_without_network() {
    let err = match client().fetch_range("file:///etc/passwd", 0, 1, None).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, RangeError::InvalidScheme(ref s) if s == "file"),
        "{err:?}"
    );
}

#[tokio::test]
async fn last_modified_guard_sends_if_unmodified_since() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/f"))
        .and(header_exists("if-unmodified-since"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-4/5")
                .set_body_bytes(vec![0u8; 5]),
        )
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let stream = client()
        .fetch_range(
            &url,
            0,
            5,
            Some(&RangeGuard::LastModified(
                "Wed, 21 Oct 2015 07:28:00 GMT".into(),
            )),
        )
        .await
        .expect("ok");
    let body = collect(stream).await.expect("ok");
    assert_eq!(body.len(), 5);
}
