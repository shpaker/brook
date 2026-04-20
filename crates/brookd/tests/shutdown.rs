//! Интеграционный тест §4.4 — graceful shutdown + persist-resume.
//!
//! Поднимает реальный `brookd` (в tempdir, с ephemeral-портом и
//! wiremock-бэкендом), добавляет загрузку, гасит демона через `oneshot` и
//! проверяет, что после рестарта очередь видит тот же `DownloadId` и
//! нормализованное состояние `Queued`.
//!
//! Подменять сигналы на `oneshot` — единственный способ гонять такой тест
//! на CI: `kill(SIGTERM)` из процесса-теста ломает cargo runner.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use brook_core::{
    Download,
    DownloadState,
    TQueueStore,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brookd::app::{
    Paths,
    build_runtime,
    serve,
};
use brookd::storage::queue::SqliteQueueRepository;
use tempfile::TempDir;
use tokio::sync::oneshot;
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

/// Пишем минимальный `brook.yaml` с `api.port = 0` и default_dir в
/// tempdir'е. `on_duplicate_url`/`on_file_exists` остаются дефолтом.
fn write_config(dir: &Path, default_dir: &Path) {
    let yaml = format!(
        "download:\n  \
           default_workers: 1\n  \
           max_workers: 4\n  \
           max_concurrent: 2\n  \
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

/// Настроить wiremock на HEAD/GET с телом указанной длины и поддержкой
/// `Range`. `ETag` стабильный — гарантирует, что resume проходит guard.
async fn mock_server(total: usize) -> MockServer {
    let server = MockServer::start().await;
    let body: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let etag = "\"brookd-test\"";

    // HEAD: headers без тела.
    Mock::given(method("HEAD"))
        .and(path("/f.bin"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("accept-ranges", "bytes")
                .insert_header("content-length", total.to_string())
                .insert_header("etag", etag),
        )
        .mount(&server)
        .await;

    // GET с Range: честно нарезаем по `bytes=start-end`.
    let body_for_get = body.clone();
    Mock::given(method("GET"))
        .and(path("/f.bin"))
        .respond_with(move |req: &wiremock::Request| {
            let range = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("bytes="))
                .and_then(|s| s.split_once('-'))
                .map(|(a, b)| (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap()));
            match range {
                Some((a, b)) => ResponseTemplate::new(206)
                    .insert_header("content-range", format!("bytes {a}-{b}/{total}"))
                    .insert_header("etag", etag)
                    .set_body_bytes(body_for_get[a..=b].to_vec()),
                None => ResponseTemplate::new(200)
                    .insert_header("etag", etag)
                    .set_body_bytes(body_for_get.clone()),
            }
        })
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
async fn shutdown_persists_and_restart_resumes() {
    let workdir = TempDir::new().unwrap();
    let downloads = TempDir::new().unwrap();
    write_config(workdir.path(), downloads.path());

    // 1 MiB (pow2-пirece_size_min=1 MiB), 1 piece. Этого достаточно,
    // чтобы inspect прошёл и engine что-то реально записал.
    let server = mock_server(1024 * 1024).await;
    let url = format!("{}/f.bin", server.uri());
    let paths = Paths::in_dir(workdir.path());

    // ── round 1: старт + Add + shutdown ────────────────────────────────
    let runtime = build_runtime(&paths).await.expect("build_runtime");
    let addr = runtime.addr;
    let (tx, rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        serve(runtime, async move {
            let _ = rx.await;
        })
        .await
    });

    let mut client = connect(addr).await;
    let id = client
        .add(proto::AddRequest {
            spec: Some(proto::DownloadSpec {
                url: url.clone(),
                target_dir: downloads.path().to_string_lossy().into(),
                filename: Some("f.bin".into()),
                workers: 1,
                ..Default::default()
            }),
        })
        .await
        .expect("add")
        .into_inner()
        .id
        .expect("id");

    // Дать engine стартовать. Нам не важно, завершилась ли загрузка —
    // важно, что запись попала в SQLite и shutdown прошёл чисто.
    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(client);
    tx.send(()).unwrap();
    serve_task.await.expect("serve join").expect("serve result");

    // ── после shutdown: очередь читаема, состояние нормализовано ──────
    {
        let queue = Arc::new(SqliteQueueRepository::open(&paths.db).expect("reopen queue"));
        let all: Vec<Download> = queue.load_all().await.expect("load_all");
        assert_eq!(all.len(), 1, "queue must contain one entry");
        assert_eq!(all[0].id.to_string(), id.value);
        // После второго `bootstrap` Running/Retrying нормализуется, но
        // до него — может быть любым. Ключевое: запись пережила рестарт.
        assert!(
            matches!(
                all[0].state,
                DownloadState::Queued
                    | DownloadState::Running
                    | DownloadState::Paused
                    | DownloadState::Retrying
                    | DownloadState::Done
            ),
            "unexpected state: {:?}",
            all[0].state
        );
    }

    // ── round 2: рестарт → bootstrap нормализует в Queued ─────────────
    let runtime2 = build_runtime(&paths).await.expect("restart build_runtime");
    let manager2 = Arc::clone(&runtime2.manager);
    let (tx2, rx2) = oneshot::channel::<()>();
    let serve_task2 = tokio::spawn(async move {
        serve(runtime2, async move {
            let _ = rx2.await;
        })
        .await
    });

    // После bootstrap снимок должен содержать запись в non-terminal или
    // Done (если уже успела дотянуться на первом запуске).
    let mut snap = Vec::new();
    for _ in 0..20 {
        snap = manager2.snapshot();
        if !snap.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(snap.len(), 1, "manager must restore one download");
    assert_eq!(snap[0].id.to_string(), id.value);
    // Нормализация гарантируется: Running/Retrying превращаются в Queued
    // до публикации snapshot'а.
    assert!(
        !matches!(
            snap[0].state,
            DownloadState::Running | DownloadState::Retrying
        ),
        "bootstrap must normalize Running/Retrying, got {:?}",
        snap[0].state
    );

    tx2.send(()).unwrap();
    serve_task2
        .await
        .expect("serve2 join")
        .expect("serve2 result");
}
