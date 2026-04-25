use std::collections::{
    HashMap,
    VecDeque,
};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;
use tokio::sync::broadcast;

use super::*;
use crate::domain::{
    FileId,
    FileLifecycleEvent,
    FileSpec,
    FileStatus,
    ProgressEvent,
};
use crate::ports::{
    ByteStream,
    InspectError,
    InspectReport,
    RangeError,
    RangeGuard,
    THttpInspect,
    TRangeFetch,
};
use crate::service::retry::RetryPolicy;
use crate::testing::MemoryPieceStorage;

/// Простая «загрузка»: равные piece'ы размера `piece_size`, всего `count`
/// штук — итоговый `total_size` = `piece_size * count`.
#[derive(Clone, Copy)]
struct TestPlan {
    piece_size: u64,
    count: u32,
}

impl TestPlan {
    fn total(self) -> u64 {
        self.piece_size * self.count as u64
    }
}

fn bytes_for(total: u64) -> Vec<u8> {
    (0..total).map(|i| (i % 251) as u8).collect()
}

/// Моковый fetch с программируемым «планом» попыток.
/// Для каждого piece_idx хранится очередь исходов.
enum MockOutcome {
    Ok,
    Transient500,
    Truncated(usize), // отдаёт первые N байт и обрывается
}

struct MockFetch {
    full_bytes: Vec<u8>,
    plans: std::sync::Mutex<HashMap<u32, VecDeque<MockOutcome>>>,
    default_ok: bool,
    plan: TestPlan,
    full_plan: std::sync::Mutex<VecDeque<MockOutcome>>,
    calls: AtomicUsize,
    /// Искусственная задержка перед отдачей ответа — даёт тестам время
    /// на отправку Pause/Cancel до завершения загрузки.
    delay: Duration,
}

impl MockFetch {
    fn always_ok(plan: TestPlan) -> Self {
        Self {
            full_bytes: bytes_for(plan.total()),
            plans: Default::default(),
            default_ok: true,
            plan,
            full_plan: Default::default(),
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn set_piece_plan(&self, idx: u32, outcomes: Vec<MockOutcome>) {
        let mut p = self.plans.lock().unwrap();
        p.insert(idx, outcomes.into());
    }
}

#[async_trait]
impl TRangeFetch for MockFetch {
    async fn fetch_range(
        &self,
        _url: &str,
        offset: u64,
        len: u64,
        _guard: Option<&RangeGuard>,
    ) -> Result<ByteStream, RangeError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        assert_eq!(
            offset % self.plan.piece_size,
            0,
            "test plan expects offsets aligned to piece_size"
        );
        let idx = (offset / self.plan.piece_size) as u32;
        let expected = piece_size_at(idx, self.plan.piece_size, self.plan.total());
        assert_eq!(len, expected, "unexpected Range length");

        let outcome = {
            let mut p = self.plans.lock().unwrap();
            match p.get_mut(&idx).and_then(|q| q.pop_front()) {
                Some(o) => o,
                None if self.default_ok => MockOutcome::Ok,
                None => MockOutcome::Transient500,
            }
        };

        match outcome {
            MockOutcome::Ok => {
                let start = offset as usize;
                let end = start + len as usize;
                let bytes = Bytes::copy_from_slice(&self.full_bytes[start..end]);
                let s = stream::iter(vec![Ok(bytes)]);
                Ok(Box::pin(s))
            }
            MockOutcome::Transient500 => Err(RangeError::UnexpectedStatus { code: 503 }),
            MockOutcome::Truncated(n) => {
                let start = offset as usize;
                let slice = &self.full_bytes[start..start + n];
                let bytes = Bytes::copy_from_slice(slice);
                let s = stream::iter(vec![Ok(bytes)]);
                Ok(Box::pin(s))
            }
        }
    }

    async fn fetch_full(&self, _url: &str) -> Result<ByteStream, RangeError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let outcome = {
            let mut p = self.full_plan.lock().unwrap();
            p.pop_front().unwrap_or(MockOutcome::Ok)
        };
        match outcome {
            MockOutcome::Ok => {
                let bytes = Bytes::copy_from_slice(&self.full_bytes);
                // Разобьём на пару чанков, чтобы покрыть путь «несколько chunk'ов».
                let mid = self.full_bytes.len() / 2;
                let s = stream::iter(vec![
                    Ok(Bytes::copy_from_slice(&self.full_bytes[..mid])),
                    Ok(Bytes::copy_from_slice(&self.full_bytes[mid..])),
                ]);
                let _ = bytes;
                Ok(Box::pin(s))
            }
            MockOutcome::Transient500 => Err(RangeError::UnexpectedStatus { code: 503 }),
            MockOutcome::Truncated(n) => {
                let bytes = Bytes::copy_from_slice(&self.full_bytes[..n]);
                let s = stream::iter(vec![Ok(bytes)]);
                Ok(Box::pin(s))
            }
        }
    }
}

/// Простейший inspect не используется движком, но нужен для компиляции
/// совместимости — оставлен как заглушка.
struct _NoInspect;
#[async_trait]
impl THttpInspect for _NoInspect {
    async fn inspect(&self, _url: &str) -> Result<InspectReport, InspectError> {
        Err(InspectError::Network("not used in engine tests".into()))
    }
}

use crate::ports::{
    NoopAttemptRepo,
    NoopWorkerRepo,
};

fn noop_repos() -> (Arc<NoopWorkerRepo>, Arc<NoopAttemptRepo>) {
    (Arc::new(NoopWorkerRepo), Arc::new(NoopAttemptRepo))
}

fn fast_config() -> EngineConfig {
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

fn inputs_range(plan: TestPlan) -> EngineInputs {
    EngineInputs {
        spec: FileSpec {
            url: "https://test/f".into(),
            target_dir: "/tmp".into(),
            filename: Some("f".into()),
        },
        total_size: plan.total(),
        piece_size: plan.piece_size,
        accepts_ranges: true,
        guard: None,
        effective_url: None,
    }
}

async fn collect_events(
    mut rx: broadcast::Receiver<FileLifecycleEvent>,
) -> Vec<FileLifecycleEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.recv().await {
        let terminal = matches!(
            ev,
            FileLifecycleEvent::Completed { .. }
                | FileLifecycleEvent::Failed { .. }
                | FileLifecycleEvent::StatusChanged {
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
async fn happy_path_range_completes() {
    let plan = TestPlan {
        piece_size: 20,
        count: 3,
    };
    let storage = Arc::new(MemoryPieceStorage::new(3, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        fast_config(),
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    let events = collect_events(rx).await;
    handle.join.await.unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, FileLifecycleEvent::Completed { .. }))
    );
    let snap = storage.snapshot();
    assert!(snap.finalized);
    assert_eq!(snap.committed, vec![0, 1, 2]);
}

#[tokio::test]
async fn transient_500_is_retried_and_succeeds() {
    let plan = TestPlan {
        piece_size: 20,
        count: 2,
    };
    let storage = Arc::new(MemoryPieceStorage::new(2, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    fetch.set_piece_plan(
        0,
        vec![MockOutcome::Transient500, MockOutcome::Transient500],
    );
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        fast_config(),
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    let events = collect_events(rx).await;
    handle.join.await.unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, FileLifecycleEvent::Completed { .. }))
    );
    assert!(storage.snapshot().finalized);
}

#[tokio::test]
async fn truncated_piece_retries_until_success() {
    let plan = TestPlan {
        piece_size: 32,
        count: 1,
    };
    let storage = Arc::new(MemoryPieceStorage::new(1, 32));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    // Первая попытка — отдали 10 байт и отключились; вторая — полный piece.
    fetch.set_piece_plan(0, vec![MockOutcome::Truncated(10)]);
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        fast_config(),
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    let _ = collect_events(rx).await;
    handle.join.await.unwrap();
    assert!(storage.snapshot().finalized);
}

#[tokio::test]
async fn pause_then_resume_emits_states_and_completes() {
    let plan = TestPlan {
        piece_size: 20,
        count: 6,
    };
    let storage = Arc::new(MemoryPieceStorage::new(6, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(30)));
    let (
        handle,
        EngineSubscriptions {
            lifecycle: mut rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        fast_config(),
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    // Дождёмся первого StateChanged(Running).
    let _ = rx.recv().await;
    assert!(handle.pause());
    // Небольшая пауза, чтобы supervisor успел обработать команду.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(handle.resume());
    // Дожидаемся Completed.
    let mut saw_paused = false;
    let mut saw_completed = false;
    while let Ok(ev) = rx.recv().await {
        match ev {
            FileLifecycleEvent::StatusChanged {
                status: FileStatus::Paused,
                ..
            } => {
                saw_paused = true;
            }
            FileLifecycleEvent::Completed { .. } => {
                saw_completed = true;
                break;
            }
            _ => {}
        }
    }
    handle.join.await.unwrap();
    assert!(saw_paused, "expected Paused StateChanged event");
    assert!(saw_completed);
}

#[tokio::test]
async fn cancel_aborts_storage() {
    let plan = TestPlan {
        piece_size: 20,
        count: 6,
    };
    let storage = Arc::new(MemoryPieceStorage::new(6, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(30)));
    let (
        handle,
        EngineSubscriptions {
            lifecycle: mut rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        fast_config(),
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    let _ = rx.recv().await;
    assert!(handle.cancel());
    // Dren events.
    while let Ok(ev) = rx.recv().await {
        if matches!(
            ev,
            FileLifecycleEvent::StatusChanged {
                status: FileStatus::Cancelled,
                ..
            }
        ) {
            break;
        }
    }
    handle.join.await.unwrap();
    assert!(storage.snapshot().aborted);
}

#[tokio::test]
async fn progress_events_throttled_to_interval() {
    let plan = TestPlan {
        piece_size: 20,
        count: 8,
    };
    let storage = Arc::new(MemoryPieceStorage::new(8, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    // Ставим большой интервал — чтобы Progress эмитился редко.
    let mut cfg = fast_config();
    cfg.progress_interval = Duration::from_millis(200);
    let start = std::time::Instant::now();
    let (
        handle,
        EngineSubscriptions {
            lifecycle: mut rx,
            progress: mut progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs_range(plan),
        cfg,
        storage.clone(),
        fetch,
        noop_repos().0,
        noop_repos().1,
    );
    let mut progress_count = 0;
    loop {
        tokio::select! {
            p = progress_rx.recv() => {
                if let Ok(ProgressEvent::Tick { .. }) = p {
                    progress_count += 1;
                }
            }
            l = rx.recv() => {
                match l {
                    Ok(FileLifecycleEvent::Completed { .. }) => break,
                    Ok(FileLifecycleEvent::Failed { .. }) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    }
    handle.join.await.unwrap();
    let elapsed = start.elapsed();
    // За elapsed-время при 5 Hz максимум allowed = elapsed/200ms + 2.
    let max_allowed = (elapsed.as_millis() / 200) as usize + 3;
    assert!(
        progress_count <= max_allowed,
        "progress emitted {progress_count} in {elapsed:?}, allowed <= {max_allowed}"
    );
}

#[tokio::test]
async fn no_range_mode_uses_single_worker_and_completes() {
    let plan = TestPlan {
        piece_size: 10,
        count: 4,
    };
    let storage = Arc::new(MemoryPieceStorage::new(4, 10));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    let mut inputs = inputs_range(plan);
    inputs.accepts_ranges = false;
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        FileId::new(),
        inputs,
        fast_config(),
        storage.clone(),
        fetch.clone(),
        noop_repos().0,
        noop_repos().1,
    );
    let events = collect_events(rx).await;
    handle.join.await.unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, FileLifecycleEvent::Completed { .. }))
    );
    // Всего один fetch_full вызов (без ретраев).
    assert_eq!(fetch.calls.load(Ordering::Relaxed), 1);
    assert!(storage.snapshot().finalized);
}

// ─── Worker / attempt tracking tests ────────────────────────────────

use crate::domain::{
    AttemptStatus,
    WorkerStatus,
};
use crate::testing::{
    MemoryAttemptRepo,
    MemoryWorkerRepo,
};

/// Две engine-сессии на один файл. Первая «падает» на полпути
/// (handle-task дропается до того, как воркеры успели добежать
/// до терминала) — её строки остаются `running`. Вторая сессия
/// в `ensure_slots` делает защитный sweep: старые → `paused`,
/// новые → свежие UUID'ы со статусом `running`/`done`.
#[tokio::test]
async fn two_sessions_produce_disjoint_worker_sets() {
    let plan = TestPlan {
        piece_size: 20,
        count: 4,
    };
    let workers = Arc::new(MemoryWorkerRepo::new());
    let attempts = Arc::new(MemoryAttemptRepo::new());
    let file_id = FileId::new();

    // Сессия 1: медленный fetch — снимаем её до финализации
    // (drop handle на половине работы).
    let storage1 = Arc::new(MemoryPieceStorage::new(4, 20));
    let fetch1 = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(200)));
    let (
        handle1,
        EngineSubscriptions {
            lifecycle: _rx1,
            progress: _progress_rx1,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs_range(plan),
        fast_config(),
        storage1,
        fetch1,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    // Имитация «демон упал»: дропаем engine-task, не дожидаясь
    // terminal-outcome.
    handle1.join.abort();
    let _ = handle1.join.await;
    let first_ids: Vec<_> = workers.by_file(file_id).iter().map(|w| w.id).collect();
    // `compute_workers` на малых test-size даёт 1 воркера; важна не
    // мощность, а disjoint-свойство между сессиями.
    let per_session = crate::compute_workers(plan.total()) as usize;
    assert_eq!(first_ids.len(), per_session);
    // Строки всё ещё `running` — engine не успел их перевести.
    for w in workers.by_file(file_id) {
        assert_eq!(w.status, WorkerStatus::Running);
    }

    // Сессия 2: свежий storage и быстрый fetch.
    let storage2 = Arc::new(MemoryPieceStorage::new(4, 20));
    let fetch2 = Arc::new(MockFetch::always_ok(plan));
    let (
        handle2,
        EngineSubscriptions {
            lifecycle: rx2,
            progress: _progress_rx2,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs_range(plan),
        fast_config(),
        storage2,
        fetch2,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = collect_events(rx2).await;
    handle2.join.await.unwrap();

    let all = workers.by_file(file_id);
    assert_eq!(all.len(), per_session * 2);
    let second_ids: Vec<_> = all
        .iter()
        .filter(|w| !first_ids.contains(&w.id))
        .map(|w| w.id)
        .collect();
    assert_eq!(second_ids.len(), per_session);
    for w in all {
        if first_ids.contains(&w.id) {
            assert_eq!(w.status, WorkerStatus::Paused);
        } else {
            assert_eq!(w.status, WorkerStatus::Done);
        }
    }
}

/// Имитация «демон упал»: предыдущие строки остались `running`,
/// глобальный sweep их закрывает, затем `ensure_slots` выдаёт свежие.
#[tokio::test]
async fn crash_recovery_sweep_paves_the_way_for_next_session() {
    use crate::ports::{
        TPieceAttemptRepo,
        TWorkerRepo,
    };

    let workers = Arc::new(MemoryWorkerRepo::new());
    let attempts = Arc::new(MemoryAttemptRepo::new());
    let file_id = FileId::new();

    // Эмулируем 3 «залипших» running-воркера.
    let stale = workers.ensure_slots(file_id, 3).await.unwrap();
    assert!(
        workers
            .by_file(file_id)
            .iter()
            .all(|w| w.status == WorkerStatus::Running)
    );

    // Daemon startup recovery.
    workers.pause_all_running_globally().await.unwrap();
    attempts.pause_all_running_globally().await.unwrap();
    for w in workers.by_file(file_id) {
        assert_eq!(w.status, WorkerStatus::Paused);
    }

    // Реальный старт новой сессии.
    let plan = TestPlan {
        piece_size: 20,
        count: 1,
    };
    let storage = Arc::new(MemoryPieceStorage::new(1, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs_range(plan),
        fast_config(),
        storage,
        fetch,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = collect_events(rx).await;
    handle.join.await.unwrap();

    let all = workers.by_file(file_id);
    let paused: Vec<_> = all
        .iter()
        .filter(|w| stale.iter().any(|s| s.id == w.id))
        .collect();
    assert_eq!(paused.len(), 3);
    for w in &paused {
        assert_eq!(w.status, WorkerStatus::Paused);
    }
    let fresh: Vec<_> = all
        .iter()
        .filter(|w| !stale.iter().any(|s| s.id == w.id))
        .collect();
    assert!(!fresh.is_empty());
    for w in fresh {
        assert_eq!(w.status, WorkerStatus::Done);
    }
}

/// Piece retry: первая попытка 503 → `failed`, вторая → `done`.
/// В журнале остаются обе строки.
#[tokio::test]
async fn piece_retry_appends_new_attempt_row() {
    let plan = TestPlan {
        piece_size: 20,
        count: 1,
    };
    let storage = Arc::new(MemoryPieceStorage::new(1, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    fetch.set_piece_plan(0, vec![MockOutcome::Transient500]);

    let workers = Arc::new(MemoryWorkerRepo::new());
    let attempts = Arc::new(MemoryAttemptRepo::new());
    let file_id = FileId::new();

    let inputs = inputs_range(plan);
    let (
        handle,
        EngineSubscriptions {
            lifecycle: rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs,
        fast_config(),
        storage,
        fetch,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = collect_events(rx).await;
    handle.join.await.unwrap();

    let rows = attempts.by_piece(file_id, 0);
    assert_eq!(
        rows.len(),
        2,
        "expected failed+done attempts, got {:?}",
        rows
    );
    let statuses: Vec<_> = rows.iter().map(|r| r.status).collect();
    assert!(statuses.contains(&AttemptStatus::Failed));
    assert!(statuses.contains(&AttemptStatus::Done));
}

/// Cancel посреди загрузки: активные воркеры → cancelled,
/// running attempt'ы файла → paused (чистим хвост).
#[tokio::test]
async fn cancel_marks_workers_cancelled() {
    let plan = TestPlan {
        piece_size: 20,
        count: 6,
    };
    let storage = Arc::new(MemoryPieceStorage::new(6, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(50)));
    let workers = Arc::new(MemoryWorkerRepo::new());
    let attempts = Arc::new(MemoryAttemptRepo::new());
    let file_id = FileId::new();

    let (
        handle,
        EngineSubscriptions {
            lifecycle: mut rx,
            progress: _progress_rx,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs_range(plan),
        fast_config(),
        storage,
        fetch,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = rx.recv().await;
    // Даём воркерам начать пилить piece'ы.
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(handle.cancel());
    while let Ok(ev) = rx.recv().await {
        if matches!(
            ev,
            FileLifecycleEvent::StatusChanged {
                status: FileStatus::Cancelled,
                ..
            }
        ) {
            break;
        }
    }
    handle.join.await.unwrap();

    for w in workers.by_file(file_id) {
        assert_eq!(
            w.status,
            WorkerStatus::Cancelled,
            "worker {:?} not cancelled",
            w
        );
    }
    // Любая открытая попытка закрыта (paused/failed).
    let still_running = attempts
        .snapshot()
        .into_iter()
        .filter(|r| r.file_id == file_id && r.status == AttemptStatus::Running)
        .count();
    assert_eq!(still_running, 0);
}

/// Две сессии подряд на один файл: первый набор воркеров → paused/done,
/// вторая сессия создаёт свежие worker-строки.
#[tokio::test]
async fn worker_rows_are_fresh_between_sessions() {
    let plan = TestPlan {
        piece_size: 20,
        count: 2,
    };
    let storage1 = Arc::new(MemoryPieceStorage::new(2, 20));
    let fetch = Arc::new(MockFetch::always_ok(plan));
    let workers = Arc::new(MemoryWorkerRepo::new());
    let attempts = Arc::new(MemoryAttemptRepo::new());
    let file_id = FileId::new();

    let inputs1 = inputs_range(plan);
    let (
        h1,
        EngineSubscriptions {
            lifecycle: rx1,
            progress: _progress_rx1,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs1,
        fast_config(),
        storage1,
        fetch.clone(),
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = collect_events(rx1).await;
    h1.join.await.unwrap();
    let first_ids: Vec<_> = workers.by_file(file_id).iter().map(|w| w.id).collect();
    assert!(!first_ids.is_empty());
    let first_count = first_ids.len();

    let storage2 = Arc::new(MemoryPieceStorage::new(2, 20));
    let inputs2 = inputs_range(plan);
    let (
        h2,
        EngineSubscriptions {
            lifecycle: rx2,
            progress: _progress_rx2,
        },
    ) = DownloadEngine::spawn(
        file_id,
        inputs2,
        fast_config(),
        storage2,
        fetch,
        Arc::clone(&workers),
        Arc::clone(&attempts),
    );
    let _ = collect_events(rx2).await;
    h2.join.await.unwrap();

    let all = workers.by_file(file_id);
    assert_eq!(all.len(), first_count * 2, "старые + свежие идентичности");
    let fresh: Vec<_> = all.iter().filter(|w| !first_ids.contains(&w.id)).collect();
    assert_eq!(fresh.len(), first_count);
}
