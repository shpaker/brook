//! Auth — интеграционный тест bearer-пароля.
//!
//! Поднимаем демон с `--client-pass`. Проверяем:
//! - unary без токена → `Unauthenticated`;
//! - unary с верным токеном → `Ok`;
//! - streaming без токена → `Unauthenticated` (интерцептор на сервере
//!   закрывает стрим ещё до первого tick'а).

// `tonic::Status` большой (~176 байт), но в тестовом интерцепторе мы
// возвращаем `Result<_, Status>` — менять на `Box<Status>` здесь —
// лишний шум без пользы, тестовый код, не hotpath.
#![allow(clippy::result_large_err)]

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
use brook_runtime::constants::{
    AUTH_HEADER,
    AUTH_SCHEME,
};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{
    Code,
    Request,
    Status,
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

async fn connect(addr: std::net::SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    for attempt in 0..40 {
        match Channel::from_shared(uri.clone()).unwrap().connect().await {
            Ok(c) => return c,
            Err(_) if attempt < 39 => tokio::time::sleep(Duration::from_millis(25)).await,
            Err(e) => panic!("connect failed: {e}"),
        }
    }
    unreachable!()
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_interceptor_enforces_token() {
    let workdir = TempDir::new().unwrap();
    let sandbox = TempDir::new().unwrap();
    write_config(workdir.path(), sandbox.path());

    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: sandbox.path().to_path_buf(),
        host: None,
        port: None,
        client_pass: Some("s3cr3t".into()),
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

    let channel = connect(addr).await;

    // 1. Без токена — Unauthenticated.
    let mut bare = BrookServiceClient::new(channel.clone());
    let err = bare
        .get_settings(proto::GetSettingsRequest {})
        .await
        .expect_err("must fail without token");
    assert_eq!(err.code(), Code::Unauthenticated, "got {err:?}");

    // 2. Со встроенным интерцептором, который подставляет верный токен.
    let header_value: MetadataValue<_> = format!("{AUTH_SCHEME}s3cr3t").parse().unwrap();
    let interceptor = move |mut req: Request<()>| -> Result<Request<()>, Status> {
        req.metadata_mut().insert(AUTH_HEADER, header_value.clone());
        Ok(req)
    };
    let authed = InterceptedService::new(channel.clone(), interceptor);
    let mut client = BrookServiceClient::new(authed);
    client
        .get_settings(proto::GetSettingsRequest {})
        .await
        .expect("must succeed with correct token");

    // 3. Streaming без токена — тоже Unauthenticated. Интерцептор
    //    отрабатывает в момент открытия стрима, до первого tick'а.
    let mut bare = BrookServiceClient::new(channel.clone());
    let err = bare
        .watch_file(proto::WatchFileRequest {})
        .await
        .expect_err("stream must fail without token");
    assert_eq!(err.code(), Code::Unauthenticated);

    tx.send(()).unwrap();
    serve_task.await.expect("serve join").expect("serve result");
}

#[tokio::test(flavor = "multi_thread")]
async fn non_loopback_without_password_refuses_to_start() {
    let workdir = TempDir::new().unwrap();
    let sandbox = TempDir::new().unwrap();
    write_config(workdir.path(), sandbox.path());

    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: sandbox.path().to_path_buf(),
        host: Some(std::net::IpAddr::from([0, 0, 0, 0])),
        port: None,
        client_pass: None,
    };

    // Runtime не Debug, поэтому expect_err через match.
    match build_runtime(&paths, &args).await {
        Err(e) => {
            let msg = format!("{e:#}");
            assert!(msg.contains("refusing to bind"), "unexpected: {msg}");
        }
        Ok(_) => panic!("must refuse non-loopback without password"),
    }
}
