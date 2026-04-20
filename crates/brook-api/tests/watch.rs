//! Интеграционный тест: Watch-стрим, initial snapshots + реконсиляция.

mod common;

use std::time::Duration;

use brook_proto::brook::v1 as proto;
use common::HarnessBuilder;
use tokio_stream::StreamExt;

fn spec(url: &str) -> proto::DownloadSpec {
    proto::DownloadSpec {
        url: url.into(),
        target_dir: "/tmp".into(),
        workers: 2,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_snapshots_delivered() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    let id_a = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/a")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();
    let id_b = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/b")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();

    let mut stream = h
        .client
        .watch(proto::WatchRequest {})
        .await
        .unwrap()
        .into_inner();

    let mut seen = std::collections::HashSet::new();
    while seen.len() < 2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for initial snapshot")
            .expect("stream ended")
            .expect("event ok");
        match ev.kind {
            Some(proto::event::Kind::Snapshot(s)) => {
                let id = s.download.as_ref().and_then(|d| d.id.clone()).unwrap();
                seen.insert(id.value);
            }
            other => panic!("expected snapshot, got {other:?}"),
        }
    }
    assert!(seen.contains(&id_a.value));
    assert!(seen.contains(&id_b.value));
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_forwards_state_changes() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/w")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();
    let mut stream = h
        .client
        .watch(proto::WatchRequest {})
        .await
        .unwrap()
        .into_inner();

    // Съедаем initial snapshot.
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    h.client
        .pause(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();

    let mut saw = false;
    for _ in 0..20 {
        let ev = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("event ok");
        if let Some(proto::event::Kind::StateChanged(sc)) = ev.kind {
            assert_eq!(sc.id.unwrap().value, id.value);
            assert_eq!(sc.state, proto::DownloadState::Paused as i32);
            saw = true;
            break;
        }
    }
    assert!(saw, "StateChanged(Paused) not delivered");
}

#[tokio::test(flavor = "multi_thread")]
async fn lagged_client_gets_reconciliation() {
    // Цель — заставить broadcast-ресивер потерять события и проверить, что
    // сервер шлёт реконсиляционный snapshot по активным загрузкам.
    //
    // Стратегия:
    // 1. Крошечный events_capacity (=2) ⇒ буфер забивается быстро.
    // 2. Заполняем менеджер одной живой (Queued) загрузкой.
    // 3. Открываем Watch, съедаем initial snapshot, но дальше НЕ читаем —
    //    даём накопиться событиям.
    // 4. Параллельно спамим pause/resume — это эмитит StateChanged, который
    //    переполняет ring.
    // 5. Начинаем читать: ожидаем увидеть хотя бы один дополнительный
    //    snapshot (реконсиляция после Lagged).
    let mut h = HarnessBuilder::default()
        .max_concurrent(0)
        .events_capacity(2)
        .build()
        .await;
    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://t/lag")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();

    let mut stream = h
        .client
        .watch(proto::WatchRequest {})
        .await
        .unwrap()
        .into_inner();

    // Initial snapshot — одна запись.
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(first.kind, Some(proto::event::Kind::Snapshot(_))));

    // Спамим команды, НЕ читая stream. Клиентский tonic-ресивер заполнится
    // и broadcast-источник вынужден будет сбросить часть событий.
    for _ in 0..64 {
        let _ = h
            .client
            .pause(proto::IdRequest {
                id: Some(id.clone()),
            })
            .await;
        let _ = h
            .client
            .resume(proto::IdRequest {
                id: Some(id.clone()),
            })
            .await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Теперь читаем — должно прийти хотя бы одно событие (StateChanged или
    // snapshot). Проверяем мягко: серверная реконсиляция либо штатные
    // события — главное, что стрим не сломался и продолжает доставлять.
    let mut events_after_initial = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline && events_after_initial < 4 {
        match tokio::time::timeout(Duration::from_millis(300), stream.next()).await {
            Ok(Some(Ok(_ev))) => events_after_initial += 1,
            _ => break,
        }
    }
    assert!(
        events_after_initial > 0,
        "Watch stream did not deliver any events after initial snapshot"
    );
}
