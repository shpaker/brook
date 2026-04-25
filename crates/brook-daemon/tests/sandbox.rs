//! Sandbox — интеграционный тест.
//!
//! Поднимаем настоящий демон с `--directory = <tempdir>` и проверяем,
//! что `Add` с `target_dir` вне этой папки возвращает
//! `PermissionDenied`, а внутри — проходит.
//!
//! Для inspect используется `wiremock`, чтобы не ходить в сеть. Сам
//! download может не успеть завершиться за окно теста — нам важна только
//! реакция на `Add`, а не полный lifecycle.

use std::path::Path;
use std::time::Duration;

use brook_daemon::ServerArgs;
use brook_daemon::app::{
    Paths,
    build_runtime,
    serve,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::Code;
use tonic::transport::Channel;
use wiremock::matchers::{
    method,
    path,
};
use wiremock::{
    Mock,
    MockServer,
    ResponseTemplate,
};

fn write_config(dir: &Path, default_dir: &Path) {
    let yaml = format!(
        "download:\n  \
           default_dir: {}\n  \
           piece_target_count: 1\n  \
           piece_size_min_mib: 1\n  \
           piece_size_max_mib: 1\n\
         api:\n  \
           port: 0\n  \
           bind: 127.0.0.1\n\
         log:\n  \
           dir: {}\n  \
           rotate_count: 1\n  \
           rotate_size_mb: 1\n",
        default_dir.display(),
        default_dir.display(),
    );
    std::fs::write(dir.join("brook.yaml"), yaml).unwrap();
}

async fn mock_server() -> MockServer {
    let server = MockServer::start().await;
    // `HttpInspectClient` делает GET с `Range: bytes=0-0`; отвечаем 206
    // с Content-Range, чтобы inspect увидел total_size и accepts_ranges.
    Mock::given(method("GET"))
        .and(path("/f.bin"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-0/1048576")
                .insert_header("content-length", "1")
                .insert_header("etag", "\"sandbox\""),
        )
        .mount(&server)
        .await;
    server
}

async fn connect(addr: std::net::SocketAddr) -> BrookServiceClient<Channel> {
    let uri = format!("http://{addr}");
    for attempt in 0..40 {
        match BrookServiceClient::connect(uri.clone()).await {
            Ok(c) => return c,
            Err(_) if attempt < 39 => tokio::time::sleep(Duration::from_millis(25)).await,
            Err(e) => panic!("connect failed: {e}"),
        }
    }
    unreachable!()
}

#[tokio::test(flavor = "multi_thread")]
async fn add_rejects_target_dir_outside_sandbox() {
    let workdir = TempDir::new().unwrap();
    let sandbox = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    write_config(workdir.path(), sandbox.path());

    let mock = mock_server().await;
    let url = format!("{}/f.bin", mock.uri());
    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: Some(sandbox.path().to_path_buf()),
        host: None,
        port: None,
        client_pass: None,
    };

    let runtime = build_runtime(&paths, &args).await.expect("build_runtime");
    let addr = runtime.addr;
    let (tx, rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        serve(runtime, async move {
            let _ = rx.await;
        })
        .await
    });

    let mut client = connect(addr).await;

    // 1. Абсолютный путь вне sandbox → PermissionDenied.
    let err = client
        .add(proto::AddRequest {
            spec: Some(proto::FileSpec {
                url: url.clone(),
                target_dir: outside.path().to_string_lossy().into(),
                filename: Some("f.bin".into()),
            }),
        })
        .await
        .expect_err("absolute outside must be rejected");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "want PermissionDenied, got {err:?}"
    );

    // 2. Относительный `../escape` → тоже PermissionDenied.
    let escape = sandbox.path().join("sub/../..");
    let err = client
        .add(proto::AddRequest {
            spec: Some(proto::FileSpec {
                url: url.clone(),
                target_dir: escape.to_string_lossy().into(),
                filename: Some("f.bin".into()),
            }),
        })
        .await
        .expect_err("escape via .. must be rejected");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "want PermissionDenied, got {err:?}"
    );

    // 3. Путь внутри sandbox (сама корневая папка) — принимается.
    client
        .add(proto::AddRequest {
            spec: Some(proto::FileSpec {
                url: url.clone(),
                target_dir: sandbox.path().to_string_lossy().into(),
                filename: Some("ok.bin".into()),
            }),
        })
        .await
        .expect("add inside sandbox must succeed");

    drop(client);
    tx.send(()).unwrap();
    serve_task.await.expect("serve join").expect("serve result");
}
