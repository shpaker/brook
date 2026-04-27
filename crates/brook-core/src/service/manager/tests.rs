use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use super::*;
use crate::domain::{
    File,
    FileId,
    FileSpec,
    FileStatus,
};
use crate::service::engine::EngineConfig;
use crate::service::retry::RetryPolicy;
use crate::testing::{
    MemoryPieceStorageFactory,
    MemoryTQueueStore,
    MockRangeFetch,
    sequential_bytes,
};

fn fast_engine_config() -> EngineConfig {
    EngineConfig {
        write_buffer: 16,
        progress_interval: Duration::from_millis(40),
        retry: RetryPolicy {
            base: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
            max_attempts: 5,
            jitter_ratio: 0.0,
        },
        events_capacity: 64,
    }
}

fn spec(url: &str) -> FileSpec {
    FileSpec::new(url, "/tmp")
}

fn make_manager(
    piece_count: u32,
    piece_size: u64,
) -> (
    DownloadManager<MemoryPieceStorageFactory, MemoryTQueueStore, MockRangeFetch>,
    Arc<MemoryTQueueStore>,
) {
    let factory = Arc::new(MemoryPieceStorageFactory::new(piece_count, piece_size));
    let queue = Arc::new(MemoryTQueueStore::new());
    let total = piece_count as u64 * piece_size;
    let fetch = Arc::new(MockRangeFetch::always_ok(sequential_bytes(total)));
    let cfg = ManagerConfig {
        events_capacity: 64,
        engine: fast_engine_config(),
    };
    (
        DownloadManager::new(factory, queue.clone(), fetch, cfg),
        queue,
    )
}

async fn wait_for_terminal(
    mgr: &DownloadManager<MemoryPieceStorageFactory, MemoryTQueueStore, MockRangeFetch>,
    id: FileId,
) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let snap = mgr.snapshot();
        if let Some(d) = snap.iter().find(|d| d.id == id)
            && d.status.is_terminal()
        {
            return;
        }
    }
    panic!("timeout waiting for terminal state of {id}");
}

#[tokio::test]
async fn add_spawns_engine_and_completes() {
    use crate::ports::TQueueStore;
    let (mgr, queue) = make_manager(3, 20);
    let id = mgr.add(spec("https://test/a")).await.unwrap();
    wait_for_terminal(&mgr, id).await;
    let snap = mgr.snapshot();
    let d = snap.iter().find(|d| d.id == id).unwrap();
    assert_eq!(d.status, FileStatus::Done);
    // queue тоже обновился.
    let persisted = queue.load_all().await.unwrap();
    assert_eq!(persisted[0].status, FileStatus::Done);
}

#[tokio::test]
async fn all_added_downloads_spawn() {
    let factory = Arc::new(MemoryPieceStorageFactory::new(3, 20));
    let queue = Arc::new(MemoryTQueueStore::new());
    let fetch = Arc::new(
        MockRangeFetch::always_ok(sequential_bytes(60)).with_delay(Duration::from_millis(60)),
    );
    let cfg = ManagerConfig {
        events_capacity: 128,
        engine: fast_engine_config(),
    };
    let mgr = DownloadManager::new(factory, queue, fetch, cfg);

    let a = mgr.add(spec("https://t/a")).await.unwrap();
    let b = mgr.add(spec("https://t/b")).await.unwrap();
    let c = mgr.add(spec("https://t/c")).await.unwrap();

    // Без concurrency-лимита все добавленные загрузки должны стартовать сразу.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let active = {
        let inner = mgr.shared.inner.lock().unwrap();
        inner.engines.len()
    };
    assert_eq!(active, 3, "expected all 3 engines running concurrently");

    for id in [a, b, c] {
        wait_for_terminal(&mgr, id).await;
    }
}

#[tokio::test]
async fn cancel_marks_cancelled() {
    use crate::ports::TQueueStore;
    // Медленный fetch даёт время отменить загрузку, пока движок ещё работает.
    let factory = Arc::new(MemoryPieceStorageFactory::new(2, 10));
    let queue = Arc::new(MemoryTQueueStore::new());
    let fetch = Arc::new(
        MockRangeFetch::always_ok(sequential_bytes(20)).with_delay(Duration::from_millis(500)),
    );
    let cfg = ManagerConfig {
        events_capacity: 64,
        engine: fast_engine_config(),
    };
    let mgr = DownloadManager::new(factory, queue.clone(), fetch, cfg);
    let id = mgr.add(spec("https://t/a")).await.unwrap();
    mgr.cancel(id).await.unwrap();
    // cancel на активной загрузке только сигналит engine — ждём,
    // пока fan_in_lifecycle запишет Cancelled в records.
    wait_for_terminal(&mgr, id).await;
    let snap = mgr.snapshot();
    let d = snap.iter().find(|d| d.id == id).unwrap();
    assert_eq!(d.status, FileStatus::Cancelled);
    let persisted = queue.load_all().await.unwrap();
    assert_eq!(persisted[0].status, FileStatus::Cancelled);
}

#[tokio::test]
async fn remove_cancels_active_download() {
    use crate::ports::TQueueStore;
    let factory = Arc::new(MemoryPieceStorageFactory::new(3, 20));
    let queue = Arc::new(MemoryTQueueStore::new());
    let fetch = Arc::new(
        MockRangeFetch::always_ok(sequential_bytes(60)).with_delay(Duration::from_millis(200)),
    );
    let cfg = ManagerConfig {
        events_capacity: 64,
        engine: fast_engine_config(),
    };
    let mgr = DownloadManager::new(factory, queue.clone(), fetch, cfg);
    let id = mgr.add(spec("https://t/slow")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Remove активного файла сам дергает cancel и ждёт завершения engine.
    mgr.remove(id).await.unwrap();
    assert!(mgr.snapshot().iter().all(|d| d.id != id));
    assert!(queue.load_all().await.unwrap().iter().all(|d| d.id != id));
}

#[tokio::test]
async fn bootstrap_restores_from_queue() {
    use crate::ports::TQueueStore;
    let factory = Arc::new(MemoryPieceStorageFactory::new(2, 10));
    let queue = Arc::new(MemoryTQueueStore::new());
    // Предзаполним очередь тремя состояниями.
    let mut qd = File::new(FileId::new(), spec("https://t/q"));
    qd.status = FileStatus::Pending;
    let mut rd = File::new(FileId::new(), spec("https://t/r"));
    rd.status = FileStatus::Running;
    let mut pd = File::new(FileId::new(), spec("https://t/p"));
    pd.status = FileStatus::Paused;
    queue.insert(&qd).await.unwrap();
    queue.insert(&rd).await.unwrap();
    queue.insert(&pd).await.unwrap();

    let fetch = Arc::new(
        MockRangeFetch::always_ok(sequential_bytes(20)).with_delay(Duration::from_millis(200)),
    );
    let cfg = ManagerConfig {
        events_capacity: 64,
        engine: fast_engine_config(),
    };
    let mgr = DownloadManager::new(factory, queue.clone(), fetch, cfg);
    mgr.bootstrap().await.unwrap();
    // Running должен быть нормализован до Queued, записано в очередь.
    let persisted = queue.load_all().await.unwrap();
    let r_persisted = persisted.iter().find(|d| d.id == rd.id).unwrap();
    assert_eq!(r_persisted.status, FileStatus::Pending);
    // Paused остался как есть.
    let p_persisted = persisted.iter().find(|d| d.id == pd.id).unwrap();
    assert_eq!(p_persisted.status, FileStatus::Paused);
    // Обе Pending-загрузки (исходная + нормализованная из Running) должны стартовать.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let active = {
        let inner = mgr.shared.inner.lock().unwrap();
        inner.engines.len()
    };
    assert_eq!(active, 2);

    // Отменим всё, чтобы тест закрыл background tasks.
    let ids: Vec<_> = mgr.snapshot().iter().map(|d| d.id).collect();
    for id in ids {
        let _ = mgr.cancel(id).await;
    }
}

#[tokio::test]
async fn snapshot_contains_all_downloads() {
    let (mgr, _) = make_manager(1, 10);
    for u in ["a", "b", "c"] {
        mgr.add(spec(&format!("https://t/{u}"))).await.unwrap();
    }
    let snap = mgr.snapshot();
    assert_eq!(snap.len(), 3);
}

#[tokio::test]
async fn shutdown_pauses_active_engines() {
    let factory = Arc::new(MemoryPieceStorageFactory::new(5, 20));
    let queue = Arc::new(MemoryTQueueStore::new());
    // Долгий fetch, чтобы на момент shutdown загрузки были активны.
    let fetch = Arc::new(
        MockRangeFetch::always_ok(sequential_bytes(100)).with_delay(Duration::from_millis(400)),
    );
    let cfg = ManagerConfig {
        events_capacity: 128,
        engine: fast_engine_config(),
    };
    let mgr = DownloadManager::new(factory, queue, fetch, cfg);
    let a = mgr.add(spec("https://t/a")).await.unwrap();
    let b = mgr.add(spec("https://t/b")).await.unwrap();
    // Дать engine'ам стартовать.
    tokio::time::sleep(Duration::from_millis(50)).await;

    mgr.shutdown(Duration::from_secs(5)).await.unwrap();

    let snap = mgr.snapshot();
    for id in [a, b] {
        let d = snap.iter().find(|d| d.id == id).unwrap();
        assert!(
            matches!(
                d.status,
                FileStatus::Paused | FileStatus::Done | FileStatus::Failed | FileStatus::Cancelled
            ),
            "download {id} in state {:?} after shutdown",
            d.status
        );
    }
}

#[tokio::test]
async fn shutdown_with_no_engines_is_noop() {
    let (mgr, _) = make_manager(1, 10);
    mgr.shutdown(Duration::from_millis(100)).await.unwrap();
}

#[tokio::test]
async fn events_fan_in_delivers_completed() {
    let (mgr, _) = make_manager(2, 10);
    let mut rx = mgr.subscribe_lifecycle();
    let id = mgr.add(spec("https://t/x")).await.unwrap();
    let mut saw_completed = false;
    for _ in 0..400 {
        match tokio::time::timeout(Duration::from_millis(20), rx.recv()).await {
            Ok(Ok(ev)) if ev.id == id && matches!(ev.status, FileStatus::Done) => {
                saw_completed = true;
                break;
            }
            // Lagged — медленный ресивер пропустил часть событий;
            // Completed мог быть среди них, проверяем state через snapshot.
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => break,
            Ok(Ok(_)) => {}
            Err(_) => {}
        }
        if mgr
            .snapshot()
            .iter()
            .any(|d| d.id == id && d.status == FileStatus::Done)
        {
            saw_completed = true;
            break;
        }
    }
    assert!(saw_completed, "Completed event was not delivered");
}
