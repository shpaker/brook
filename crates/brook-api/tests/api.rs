//! Интеграционный тест: клиент ↔ сервер через tonic на локальном TCP.
//!
//! Проходит всю unary-поверхность `BrookService`: Add → List → Pause →
//! Resume → Cancel → Remove, плюс PauseAll/ResumeAll и пару негативных
//! сценариев.

mod common;

use std::time::Duration;

use brook_proto::brook::v1 as proto;
use common::HarnessBuilder;

fn spec(url: &str) -> proto::DownloadSpec {
    proto::DownloadSpec {
        url: url.into(),
        target_dir: "/tmp".into(),
        workers: 2,
        ..Default::default()
    }
}

async fn wait_until_terminal(
    h: &common::TestHarness,
    id: &proto::DownloadId,
    timeout: Duration,
) -> proto::DownloadState {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let list = h
            .client
            .clone()
            .list(proto::ListRequest {})
            .await
            .unwrap()
            .into_inner();
        if let Some(d) = list
            .downloads
            .iter()
            .find(|d| d.id.as_ref().map(|x| x.value == id.value).unwrap_or(false))
        {
            let state = proto::DownloadState::try_from(d.state).unwrap();
            if matches!(
                state,
                proto::DownloadState::Done
                    | proto::DownloadState::Failed
                    | proto::DownloadState::Cancelled
            ) {
                return state;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for terminal state");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn add_list_cancel_remove_roundtrip() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;

    // List на пустом менеджере.
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(list.downloads.is_empty());

    // Add.
    let resp = h
        .client
        .add(proto::AddRequest {
            spec: Some(spec("https://example.com/a")),
        })
        .await
        .unwrap()
        .into_inner();
    let id = resp.id.expect("id present");

    // List возвращает одну запись в Queued (max_concurrent=0 — engine не стартует).
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.downloads.len(), 1);
    assert_eq!(list.downloads[0].state, proto::DownloadState::Queued as i32);

    // Cancel.
    let status = h
        .client
        .cancel(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(status.ok);

    // State перешёл в Cancelled.
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        list.downloads[0].state,
        proto::DownloadState::Cancelled as i32
    );

    // Remove после отмены — ок.
    h.client
        .remove(proto::RemoveRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(list.downloads.is_empty());
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

    h.client
        .pause(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.downloads[0].state, proto::DownloadState::Paused as i32);

    h.client
        .resume(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.downloads[0].state, proto::DownloadState::Queued as i32);
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_all_and_resume_all() {
    let mut h = HarnessBuilder::default().max_concurrent(0).build().await;
    for u in ["a", "b", "c"] {
        h.client
            .add(proto::AddRequest {
                spec: Some(spec(&format!("https://t/{u}"))),
            })
            .await
            .unwrap();
    }

    h.client.pause_all(proto::PauseAllRequest {}).await.unwrap();
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    for d in &list.downloads {
        assert_eq!(d.state, proto::DownloadState::Paused as i32);
    }

    h.client
        .resume_all(proto::ResumeAllRequest {})
        .await
        .unwrap();
    let list = h
        .client
        .list(proto::ListRequest {})
        .await
        .unwrap()
        .into_inner();
    for d in &list.downloads {
        assert_eq!(d.state, proto::DownloadState::Queued as i32);
    }
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
    let final_state = wait_until_terminal(&h, &id, Duration::from_secs(5)).await;
    assert_eq!(final_state, proto::DownloadState::Done);
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_on_active_is_failed_precondition() {
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

    // Ждём, пока engine стартанёт.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let err = h
        .client
        .remove(proto::RemoveRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Теперь cancel + remove проходят чисто.
    h.client
        .cancel(proto::IdRequest {
            id: Some(id.clone()),
        })
        .await
        .unwrap();
    // ждём, пока engine доедет до терминала и снимется.
    let _ = wait_until_terminal(&h, &id, Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_id_rejected() {
    let mut h = HarnessBuilder::default().build().await;
    let err = h
        .client
        .pause(proto::IdRequest {
            id: Some(proto::DownloadId {
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
