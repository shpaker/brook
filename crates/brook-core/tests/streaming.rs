//! Интеграционный тест стриминг-движка (§2).
//!
//! Проверяет happy-path: `fetch_full` → `TStreamStorage::append_chunk` →
//! `finalize`, с `bytes_total = 0` в `ProgressEvent`-тиках (TUI рендерит
//! indeterminate-gauge).

use std::sync::Arc;

use brook_core::testing::{
    MemoryStreamStorage,
    MockRangeFetch,
    sequential_bytes,
};
use brook_core::{
    DownloadEngine,
    EngineConfig,
    EngineSubscriptions,
    FileCommand,
    FileId,
    FileLifecycleEvent,
    FileSpec,
    FileStatus,
    NoopAttemptRepo,
    NoopWorkerRepo,
    ProgressEvent,
    StreamingEngineInputs,
};
use tokio::sync::broadcast;

async fn drain_lifecycle(
    mut rx: broadcast::Receiver<FileLifecycleEvent>,
) -> Vec<FileLifecycleEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.recv().await {
        let terminal = matches!(
            ev.status,
            FileStatus::Done | FileStatus::Failed | FileStatus::Cancelled
        );
        out.push(ev);
        if terminal {
            break;
        }
    }
    out
}

async fn drain_progress(mut rx: broadcast::Receiver<ProgressEvent>) -> Vec<ProgressEvent> {
    let mut out = Vec::new();
    // Небольшое окно — в happy-path мы успеем увидеть хотя бы один тик.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    while let Ok(Ok(ev)) = tokio::time::timeout_at(deadline, rx.recv()).await {
        out.push(ev);
    }
    out
}

#[tokio::test]
async fn streaming_happy_path_completes_and_finalizes() {
    let bytes = sequential_bytes(4096);
    let fetch = Arc::new(MockRangeFetch::always_ok(bytes.clone()));
    let stream = Arc::new(MemoryStreamStorage::new());

    let inputs = StreamingEngineInputs {
        spec: FileSpec::new("https://host/x", "/tmp"),
        effective_url: None,
    };
    let (
        handle,
        EngineSubscriptions {
            lifecycle,
            progress,
        },
    ) = DownloadEngine::spawn_streaming(
        FileId::new(),
        inputs,
        EngineConfig::default(),
        Arc::clone(&stream),
        fetch,
        Arc::new(NoopWorkerRepo),
        Arc::new(NoopAttemptRepo),
    );
    let lifecycle_task = tokio::spawn(drain_lifecycle(lifecycle));
    let progress_task = tokio::spawn(drain_progress(progress));
    let events = lifecycle_task.await.unwrap();
    let progress_events = progress_task.await.unwrap();
    handle.join().await;

    // Байты сложились целиком и хранилище финализовано.
    assert_eq!(stream.assembled_bytes(), bytes);
    assert!(stream.is_finalized());

    // В lifecycle-стриме — Completed.
    let has_completed = events.iter().any(|e| e.status == FileStatus::Done);
    assert!(has_completed, "expected Completed, got {events:?}");
    // В прогресс-тиках — bytes_total = 0 (indeterminate).
    let any_progress_unknown_size = progress_events.iter().any(|e| {
        matches!(
            e,
            ProgressEvent::Tick { progress, .. } if progress.bytes_total == 0
        )
    });
    assert!(
        any_progress_unknown_size,
        "expected at least one ProgressEvent::Tick with bytes_total=0, \
         got {progress_events:?}"
    );
}

#[tokio::test]
async fn streaming_cancel_aborts_storage() {
    let bytes = sequential_bytes(4096);
    let fetch = Arc::new(
        MockRangeFetch::always_ok(bytes).with_delay(std::time::Duration::from_millis(200)),
    );
    let stream = Arc::new(MemoryStreamStorage::new());

    let inputs = StreamingEngineInputs {
        spec: FileSpec::new("https://host/x", "/tmp"),
        effective_url: None,
    };
    let (
        handle,
        EngineSubscriptions {
            lifecycle,
            progress: _,
        },
    ) = DownloadEngine::spawn_streaming(
        FileId::new(),
        inputs,
        EngineConfig::default(),
        Arc::clone(&stream),
        fetch,
        Arc::new(NoopWorkerRepo),
        Arc::new(NoopAttemptRepo),
    );
    // Успеваем послать Cancel до того, как fetch вернёт поток.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    handle.send(FileCommand::Cancel);
    let _ = drain_lifecycle(lifecycle).await;
    handle.join().await;

    assert!(stream.is_aborted(), "cancel must abort the stream storage");
    assert!(!stream.is_finalized());
}
