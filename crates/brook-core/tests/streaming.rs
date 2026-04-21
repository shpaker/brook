//! Интеграционный тест стриминг-движка (§2).
//!
//! Проверяет happy-path: `fetch_full` → `TStreamStorage::append_chunk` →
//! `finalize`, с `bytes_total = 0` в `Progress`-событиях (TUI рендерит
//! indeterminate-gauge).

use std::sync::Arc;

use brook_core::testing::{
    MemoryStreamStorage,
    MockRangeFetch,
    sequential_bytes,
};
use brook_core::{
    DownloadCommand,
    DownloadEngine,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    EngineConfig,
    FileStatus,
    NoopAttemptRepo,
    NoopWorkerRepo,
    StreamingEngineInputs,
};
use tokio::sync::broadcast;

async fn drain(mut rx: broadcast::Receiver<DownloadEvent>) -> Vec<DownloadEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.recv().await {
        let terminal = matches!(
            ev,
            DownloadEvent::Completed { .. }
                | DownloadEvent::Failed { .. }
                | DownloadEvent::StatusChanged {
                    status: FileStatus::Cancelled,
                    ..
                }
        );
        out.push(ev);
        if terminal {
            break;
        }
    }
    out
}

#[tokio::test]
async fn streaming_happy_path_completes_and_finalizes() {
    let bytes = sequential_bytes(4096);
    let fetch = Arc::new(MockRangeFetch::always_ok(bytes.clone()));
    let stream = Arc::new(MemoryStreamStorage::new());

    let inputs = StreamingEngineInputs {
        spec: DownloadSpec::new("https://host/x", "/tmp"),
        effective_url: None,
    };
    let (handle, rx) = DownloadEngine::spawn_streaming(
        DownloadId::new(),
        inputs,
        EngineConfig::default(),
        Arc::clone(&stream),
        fetch,
        Arc::new(NoopWorkerRepo),
        Arc::new(NoopAttemptRepo),
    );
    let events = drain(rx).await;
    handle.join().await;

    // Байты сложились целиком и хранилище финализовано.
    assert_eq!(stream.assembled_bytes(), bytes);
    assert!(stream.is_finalized());

    // В событиях — Completed, а Progress приходит с bytes_total = 0.
    let has_completed = events
        .iter()
        .any(|e| matches!(e, DownloadEvent::Completed { .. }));
    assert!(has_completed, "expected Completed, got {events:?}");
    let any_progress_unknown_size = events.iter().any(|e| {
        matches!(
            e,
            DownloadEvent::Progress { progress, .. } if progress.bytes_total == 0
        )
    });
    assert!(
        any_progress_unknown_size,
        "expected at least one Progress with bytes_total=0 (indeterminate), got {events:?}"
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
        spec: DownloadSpec::new("https://host/x", "/tmp"),
        effective_url: None,
    };
    let (handle, rx) = DownloadEngine::spawn_streaming(
        DownloadId::new(),
        inputs,
        EngineConfig::default(),
        Arc::clone(&stream),
        fetch,
        Arc::new(NoopWorkerRepo),
        Arc::new(NoopAttemptRepo),
    );
    // Успеваем послать Cancel до того, как fetch вернёт поток.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    handle.send(DownloadCommand::Cancel);
    let _ = drain(rx).await;
    handle.join().await;

    assert!(stream.is_aborted(), "cancel must abort the stream storage");
    assert!(!stream.is_finalized());
}
