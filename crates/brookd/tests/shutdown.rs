//! Интеграционный тест §4.4 — graceful shutdown + persist-resume.
//!
//! Поднимает реальный `brookd` (в tempdir, с ephemeral-портом и
//! wiremock-бэкендом), добавляет загрузку, гасит демона через `oneshot` и
//! проверяет, что после рестарта очередь видит тот же `FileId` и
//! нормализованное состояние `Queued`.
//!
//! Подменять сигналы на `oneshot` — единственный способ гонять такой тест
//! на CI: `kill(SIGTERM)` из процесса-теста ломает cargo runner.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use brook_core::{
    File,
    FileStatus,
    TQueueStore,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brookd::app::{
    Paths,
    build_runtime,
    serve,
};
use brookd::storage::db::SharedDb;
use brookd::storage::files::SqliteFileRepository;
use rusqlite::params;
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
/// tempdir'е. `on_duplicate_url` остаётся дефолтом.
fn write_config(dir: &Path, default_dir: &Path) {
    let yaml = format!(
        "download:\n  \
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
            spec: Some(proto::FileSpec {
                url: url.clone(),
                target_dir: downloads.path().to_string_lossy().into(),
                filename: Some("f.bin".into()),
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
        let db = SharedDb::open(&paths.db).expect("reopen brook.db");
        let queue = Arc::new(SqliteFileRepository::new(db.clone()));
        let all: Vec<File> = queue.load_all().await.expect("load_all");
        assert_eq!(all.len(), 1, "queue must contain one entry");
        assert_eq!(all[0].id.to_string(), id.value);
        // После второго `bootstrap` Running/Retrying нормализуется, но
        // до него — может быть любым. Ключевое: запись пережила рестарт.
        assert!(
            matches!(
                all[0].status,
                FileStatus::Pending
                    | FileStatus::Running
                    | FileStatus::Paused
                    | FileStatus::Retrying
                    | FileStatus::Done
            ),
            "unexpected state: {:?}",
            all[0].status
        );

        // Stage 9 E2E — наблюдаемости после shutdown:
        //   - на диске рядом с таргетом только `.data.brook` или уже
        //     финализированный `f.bin` (если успели докачать до SIGTERM);
        //     sidecar `.index.brook` не должен появиться никогда.
        //   - в `brook.db` есть piece-строки (`pending`/`done`) пока
        //     загрузка не финализирована, и `state_changes` содержит
        //     хотя бы один переход.
        let dl = downloads.path();
        assert!(
            !dl.join("f.bin.index.brook").exists(),
            "sidecar index file must not exist"
        );
        let data_brook = dl.join("f.bin.data.brook");
        let target = dl.join("f.bin");
        assert!(
            data_brook.exists() || target.exists(),
            "expected .data.brook or finalized target next to the download"
        );

        let id_str = all[0].id.to_string();
        let (pieces_total, state_changes): (i64, i64) = db
            .with_conn(move |c| {
                let pieces: i64 = c.query_row(
                    "SELECT COUNT(*) FROM pieces WHERE file_id = ?",
                    params![id_str.clone()],
                    |r| r.get(0),
                )?;
                let changes: i64 = c.query_row(
                    "SELECT COUNT(*) FROM status_changes WHERE file_id = ?",
                    params![id_str],
                    |r| r.get(0),
                )?;
                Ok((pieces, changes))
            })
            .await
            .expect("brook.db readback");
        assert!(state_changes >= 1, "state_changes must record transitions");
        if all[0].status != FileStatus::Done {
            assert!(pieces_total >= 1, "pieces must be populated until finalize");
        }
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
        !matches!(snap[0].status, FileStatus::Running | FileStatus::Retrying),
        "bootstrap must normalize Running/Retrying, got {:?}",
        snap[0].status
    );

    // Дождаться финализации: `.data.brook` исчезает, таргет появляется,
    // piece-строки подчищены. Движок досылает последние куски и делает
    // `rename`. 4 с с запасом для 1 MiB на wiremock'е.
    let mut finalized = false;
    for _ in 0..160 {
        let snap = manager2.snapshot();
        if snap.iter().any(|d| d.status == FileStatus::Done) {
            finalized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(finalized, "download did not reach Done after restart");

    let dl = downloads.path();
    assert!(dl.join("f.bin").exists(), "target file must exist");
    assert!(
        !dl.join("f.bin.data.brook").exists(),
        ".data.brook must be removed after finalize"
    );

    {
        let db = SharedDb::open(&paths.db).expect("reopen brook.db");
        let id_str = id.value.clone();
        let (pieces_total, final_state): (i64, String) = db
            .with_conn(move |c| {
                let pieces: i64 = c.query_row(
                    "SELECT COUNT(*) FROM pieces WHERE file_id = ?",
                    params![id_str.clone()],
                    |r| r.get(0),
                )?;
                let last: String = c.query_row(
                    "SELECT status_id FROM status_changes
                      WHERE file_id = ?
                      ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    params![id_str],
                    |r| r.get(0),
                )?;
                Ok((pieces, last))
            })
            .await
            .expect("final brook.db readback");
        assert_eq!(pieces_total, 0, "pieces must be cleared after finalize");
        assert_eq!(final_state, "done", "last state_changes entry must be done");
    }

    tx2.send(()).unwrap();
    serve_task2
        .await
        .expect("serve2 join")
        .expect("serve2 result");
}
