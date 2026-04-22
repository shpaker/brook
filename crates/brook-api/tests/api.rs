//! Интеграционный тест: клиент ↔ сервер через tonic на локальном TCP.
//!
//! Проходит unary-поверхность `BrookService`: Add → Pause → Resume →
//! Retry → Remove, плюс negativы. Для наблюдения состояния используется
//! `manager.snapshot()` из harness'а — `List` RPC'а больше нет.

mod common;

use std::time::Duration;

use brook_core::FileStatus;
use brook_proto::brook::v1 as proto;
use common::HarnessBuilder;

fn spec(url: &str) -> proto::FileSpec {
    proto::FileSpec {
        url: url.into(),
        target_dir: "/tmp".into(),
        ..Default::default()
    }
}

async fn wait_until_terminal(
    h: &common::TestHarness,
    id_str: &str,
    timeout: Duration,
) -> FileStatus {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(d) = h
            .manager
            .snapshot()
            .into_iter()
            .find(|d| d.id.to_string() == id_str)
            && d.status.is_terminal()
        {
            return d.status;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for terminal state");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn find_status(h: &common::TestHarness, id_str: &str) -> FileStatus {
    h.manager
        .snapshot()
        .into_iter()
        .find(|d| d.id.to_string() == id_str)
        .expect("record present")
        .status
}

#[tokio::test(flavor = "multi_thread")]
async fn add_pause_remove_roundtrip() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    assert!(h.manager.snapshot().is_empty());

    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/a")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .expect("id present");
    let id_str = id.value.clone();
    assert_eq!(h.manager.snapshot().len(), 1);
    assert_eq!(find_status(&h, &id_str), FileStatus::Pending);

    // Pause, затем remove — без Cancel RPC'а это штатный путь для
    // не-активных записей.
    h.client
        .pause(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    assert_eq!(find_status(&h, &id_str), FileStatus::Paused);

    h.client
        .remove(proto::RemoveRequest { id: Some(id) })
        .await
        .unwrap();
    assert!(h.manager.snapshot().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_and_resume_unqueued() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/p")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();
    let id_str = id.value.clone();

    h.client
        .pause(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    assert_eq!(find_status(&h, &id_str), FileStatus::Paused);

    h.client
        .resume(proto::IdRequest { id: Some(id) })
        .await
        .unwrap();
    assert_eq!(find_status(&h, &id_str), FileStatus::Pending);
}

#[tokio::test(flavor = "multi_thread")]
async fn download_runs_to_completion() {
    let h = HarnessBuilder::default().max_concurrent(2).build().await;
    let id = h
        .client
        .clone()
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/go")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();
    let final_state = wait_until_terminal(&h, &id.value, Duration::from_secs(5)).await;
    assert_eq!(final_state, FileStatus::Done);
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_on_active_cancels_and_succeeds() {
    let mut h = HarnessBuilder::default()
        .max_concurrent(1)
        .fetch_delay(Duration::from_millis(500))
        .build()
        .await;
    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/slow")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Remove активного файла — сервис сам отменяет engine и удаляет запись.
    h.client
        .remove(proto::RemoveRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();

    assert!(
        h.manager
            .snapshot()
            .iter()
            .all(|d| d.id.to_string() != id.value)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_requires_failed_state() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    let id = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/r")),
        })
        .await
        .unwrap()
        .into_inner()
        .id
        .unwrap();
    // В Pending retry — ошибка precondition.
    let err = h
        .client
        .retry(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal); // "not in failed state" → internal
    let _ = err;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_id_rejected() {
    let mut h = HarnessBuilder::default().build().await;
    let err = h
        .client
        .pause(proto::IdRequest {
            id: Some(proto::FileId {
                value: "not-a-uuid".into(),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn add_without_spec_is_invalid_argument() {
    let mut h = HarnessBuilder::default().build().await;
    let err = h
        .client
        .add(proto::AddRequest { spec: None })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_settings_returns_defaults() {
    let mut h = HarnessBuilder::default().build().await;
    let resp = h
        .client
        .get_settings(proto::GetSettingsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.max_concurrent, 3);
    assert!(resp.default_dir.is_empty());
}
