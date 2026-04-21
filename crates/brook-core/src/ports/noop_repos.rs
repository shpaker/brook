//! No-op реализации [`TWorkerRepo`] и [`TPieceAttemptRepo`].
//!
//! Сигнатуры намеренно в форме `-> impl Future + Send`, чтобы совпасть
//! с контрактом порта (см. `piece_storage.rs` — почему именно так, а не
//! `async fn`). Clippy-правило `manual_async_fn` подавлено для всего
//! модуля.

#![allow(clippy::manual_async_fn)]
//!
//! Используются как значения по умолчанию для generic-параметров
//! `DownloadManager` / `DownloadEngine`: если вызывающему аналитика
//! попыток не нужна (тесты ядра, in-memory harness'ы), он собирает
//! менеджер без персистентных репозиториев — все методы превращаются
//! в `Ok(())`. Production-путь (brookd) подставляет SQLite-репозитории.

use std::future::Future;

use crate::domain::{
    AttemptId,
    DownloadId,
    WorkerId,
};
use crate::error::Result;
use crate::ports::piece_attempt_repo::{
    AttemptRecord,
    TPieceAttemptRepo,
};
use crate::ports::worker_repo::{
    TWorkerRepo,
    WorkerRecord,
};

/// No-op `TWorkerRepo`: `ensure_slots` возвращает пустой вектор,
/// terminal-методы — `Ok(())`.
#[derive(Default, Debug, Clone, Copy)]
pub struct NoopWorkerRepo;

impl TWorkerRepo for NoopWorkerRepo {
    fn ensure_slots(
        &self,
        _file_id: DownloadId,
        _n: usize,
    ) -> impl Future<Output = Result<Vec<WorkerRecord>>> + Send {
        async { Ok(Vec::new()) }
    }

    fn mark_paused(&self, _worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn mark_done(&self, _worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn mark_failed(
        &self,
        _worker_id: WorkerId,
        _error: &str,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn mark_cancelled(&self, _worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn pause_all_running_for_file(
        &self,
        _file_id: DownloadId,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn pause_all_running_globally(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// No-op `TPieceAttemptRepo`: `start` возвращает фиктивную запись
/// с `AttemptId::new()`, остальные методы — `Ok(())`.
#[derive(Default, Debug, Clone, Copy)]
pub struct NoopAttemptRepo;

impl TPieceAttemptRepo for NoopAttemptRepo {
    fn start(
        &self,
        _file_id: DownloadId,
        _piece_number: u32,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<AttemptRecord>> + Send {
        async move {
            Ok(AttemptRecord {
                id: AttemptId::new(),
                piece_id: String::new(),
                worker_id,
                started_at: 0,
                finished_at: None,
                bytes: 0,
            })
        }
    }

    fn finish(
        &self,
        _attempt_id: AttemptId,
        _bytes: u64,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn fail(
        &self,
        _attempt_id: AttemptId,
        _error: &str,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn cancel(&self, _attempt_id: AttemptId) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn pause(&self, _attempt_id: AttemptId) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn pause_all_running_for_file(
        &self,
        _file_id: DownloadId,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn pause_all_running_globally(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
