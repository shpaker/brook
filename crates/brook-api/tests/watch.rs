//! Интеграционный тест: дельта-WatchStatus стрим.
//!
//! Стартовое состояние клиент берёт через `GetRecently`/`GetFiles`;
//! `WatchStatus` отдаёт только статусные переходы с момента подписки.
//! На переполнении broadcast-канала сервер закрывает стрим с
//! `Status::DataLoss`.

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
async fn watch_status_forwards_pause_event() {
    let mut h = HarnessBuilder::default()
        .fetch_delay(Duration::from_millis(500))
        .build()
        .await;

    let mut stream = h
        .client
        .watch_status(proto::WatchStatusRequest {})
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

    // Ожидаем StatusEvent с status=Paused для нашего id. Промежуточные
    // переходы (Pending/Running) тоже могут прилететь — пропускаем.
    let mut saw_paused = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !saw_paused {
        let ev = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("event ok");
        if ev.id.as_ref().map(|i| i.value.clone()) == Some(id.value.clone())
            && ev.status == proto::FileStatus::Paused as i32
        {
            saw_paused = true;
        }
    }
    assert!(saw_paused, "Paused StatusEvent not delivered");
}

// Lagged-поведение (broadcast переполнился → сервер шлёт `Status::DataLoss`)
// проверяется юнит-тестом на маппере и ручным сценарием в TUI: гарантировать
// timing-надёжное переполнение broadcast-канала через RPC из теста сложно
// (события lifecycle идут асинхронно через engine fan-in, и ресивер обычно
// успевает читать).
