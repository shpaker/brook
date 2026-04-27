//! Интеграционный тест: дельта-WatchFile стрим (без initial-sync).
//!
//! Стартовое состояние клиент берёт через `GetRecently`/`GetFiles`;
//! `WatchFile` отдаёт только дельты (`Created`/`StatusChanged`/…) с
//! момента подписки. На переполнении broadcast-канала сервер закрывает
//! стрим с `Status::DataLoss`.

mod common;

use std::time::Duration;

use brook_proto::brook::v1 as proto;
use common::HarnessBuilder;
use tokio_stream::StreamExt;

fn spec(url: &str) -> proto::FileSpec {
    proto::FileSpec {
        url: url.into(),
        target_dir: "/tmp".into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn created_events_delivered_for_new_files() {
    let mut h = HarnessBuilder::default()
        .fetch_delay(Duration::from_millis(500))
        .build()
        .await;

    // Подписываемся ДО Add, чтобы поймать `Created`-event'ы.
    let mut stream = h
        .client
        .watch_file(proto::WatchFileRequest {})
        .await
        .unwrap()
        .into_inner();

    let id_a = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/a")),
        })
        .await
        .unwrap()
        .into_inner()
        .file
        .and_then(|f| f.id)
        .unwrap();
    let id_b = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/b")),
        })
        .await
        .unwrap()
        .into_inner()
        .file
        .and_then(|f| f.id)
        .unwrap();

    let mut seen = std::collections::HashSet::new();
    while seen.len() < 2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for Created event")
            .expect("stream ended")
            .expect("event ok");
        if let Some(proto::file_event::Kind::Created(c)) = ev.kind
            && let Some(id) = c.file.as_ref().and_then(|d| d.id.clone())
        {
            seen.insert(id.value);
        }
    }
    assert!(seen.contains(&id_a.value));
    assert!(seen.contains(&id_b.value));
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_forwards_state_changes() {
    let mut h = HarnessBuilder::default()
        .fetch_delay(Duration::from_millis(500))
        .build()
        .await;

    let mut stream = h
        .client
        .watch_file(proto::WatchFileRequest {})
        .await
        .unwrap()
        .into_inner();

    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/w")),
        })
        .await
        .unwrap()
        .into_inner()
        .file
        .and_then(|f| f.id)
        .unwrap();

    h.client
        .pause(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();

    let mut saw_paused = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !saw_paused {
        let ev = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("event ok");
        if let Some(proto::file_event::Kind::StatusChanged(sc)) = ev.kind
            && sc.id.as_ref().map(|i| i.value.clone()) == Some(id.value.clone())
            && sc.status == proto::FileStatus::Paused as i32
        {
            saw_paused = true;
        }
    }
    assert!(saw_paused, "StatusChanged(Paused) not delivered");
}

// Lagged-поведение (broadcast переполнился → сервер шлёт `Status::DataLoss`)
// проверяется юнит-тестом на маппере и ручным сценарием в TUI: гарантировать
// timing-надёжное переполнение broadcast-канала через RPC из теста сложно
// (события lifecycle идут асинхронно через engine fan-in, и ресивер обычно
// успевает читать).
