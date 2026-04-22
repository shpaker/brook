//! In-memory реализации [`TWorkerRepo`] и [`TPieceAttemptRepo`] для
//! юнит-тестов движка.
//!
//! Семантика совпадает с SQLite-адаптерами: `ensure_slots` защитно
//! сбрасывает running-воркеры этого файла в paused, `start` создаёт
//! свежую строку попытки. Всё состояние в `Arc<Mutex<_>>` — в тестах
//! удобнее, чем настоящая БД.

use std::collections::HashMap;
use std::sync::{
    Arc,
    Mutex,
};

use crate::domain::{
    AttemptId,
    AttemptStatus,
    FileId,
    WorkerId,
    WorkerStatus,
};
use crate::error::Result;
use crate::ports::{
    AttemptRecord,
    TPieceAttemptRepo,
    TWorkerRepo,
    WorkerRecord,
};

/// Снимок воркера в памяти.
#[derive(Debug, Clone)]
pub struct MemoryWorkerRow {
    pub id: WorkerId,
    pub file_id: FileId,
    pub slot_index: usize,
    pub status: WorkerStatus,
    pub error: Option<String>,
}

/// Снимок попытки в памяти.
#[derive(Debug, Clone)]
pub struct MemoryAttemptRow {
    pub id: AttemptId,
    pub file_id: FileId,
    pub piece_number: u32,
    pub worker_id: WorkerId,
    pub status: AttemptStatus,
    pub bytes: u64,
    pub error: Option<String>,
}

#[derive(Default)]
struct WorkerState {
    rows: HashMap<WorkerId, MemoryWorkerRow>,
}

#[derive(Default)]
struct AttemptState {
    rows: HashMap<AttemptId, MemoryAttemptRow>,
}

/// In-memory `TWorkerRepo`.
#[derive(Default, Clone)]
pub struct MemoryWorkerRepo {
    inner: Arc<Mutex<WorkerState>>,
}

impl MemoryWorkerRepo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<MemoryWorkerRow> {
        let g = self.inner.lock().expect("poisoned");
        let mut v: Vec<_> = g.rows.values().cloned().collect();
        v.sort_by_key(|r| (r.file_id.to_string(), r.slot_index));
        v
    }

    pub fn by_file(&self, file_id: FileId) -> Vec<MemoryWorkerRow> {
        self.snapshot()
            .into_iter()
            .filter(|r| r.file_id == file_id)
            .collect()
    }
}

impl TWorkerRepo for MemoryWorkerRepo {
    fn ensure_slots(
        &self,
        file_id: FileId,
        n: usize,
    ) -> impl std::future::Future<Output = Result<Vec<WorkerRecord>>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            // defensive sweep: running-воркеры этого файла → paused.
            for row in g.rows.values_mut() {
                if row.file_id == file_id && row.status == WorkerStatus::Running {
                    row.status = WorkerStatus::Paused;
                }
            }
            let mut out = Vec::with_capacity(n);
            for slot in 0..n {
                let id = WorkerId::new();
                g.rows.insert(
                    id,
                    MemoryWorkerRow {
                        id,
                        file_id,
                        slot_index: slot,
                        status: WorkerStatus::Running,
                        error: None,
                    },
                );
                out.push(WorkerRecord {
                    id,
                    file_id,
                    slot_index: slot,
                    started_at: 0,
                    finished_at: None,
                });
            }
            Ok(out)
        }
    }

    fn mark_paused(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        set_status(
            Arc::clone(&self.inner),
            worker_id,
            WorkerStatus::Paused,
            None,
        )
    }

    fn mark_done(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        set_status(Arc::clone(&self.inner), worker_id, WorkerStatus::Done, None)
    }

    fn mark_failed(
        &self,
        worker_id: WorkerId,
        error: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let err = error.to_owned();
        set_status(
            Arc::clone(&self.inner),
            worker_id,
            WorkerStatus::Failed,
            Some(err),
        )
    }

    fn mark_cancelled(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        set_status(
            Arc::clone(&self.inner),
            worker_id,
            WorkerStatus::Cancelled,
            None,
        )
    }

    fn pause_all_running_for_file(
        &self,
        file_id: FileId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            for row in g.rows.values_mut() {
                if row.file_id == file_id && row.status == WorkerStatus::Running {
                    row.status = WorkerStatus::Paused;
                }
            }
            Ok(())
        }
    }

    fn pause_all_running_globally(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            for row in g.rows.values_mut() {
                if row.status == WorkerStatus::Running {
                    row.status = WorkerStatus::Paused;
                }
            }
            Ok(())
        }
    }
}

async fn set_status(
    inner: Arc<Mutex<WorkerState>>,
    id: WorkerId,
    status: WorkerStatus,
    error: Option<String>,
) -> Result<()> {
    let mut g = inner.lock().expect("poisoned");
    if let Some(row) = g.rows.get_mut(&id) {
        row.status = status;
        if let Some(e) = error {
            row.error = Some(e);
        }
    }
    Ok(())
}

/// In-memory `TPieceAttemptRepo`.
#[derive(Default, Clone)]
pub struct MemoryAttemptRepo {
    inner: Arc<Mutex<AttemptState>>,
}

impl MemoryAttemptRepo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<MemoryAttemptRow> {
        let g = self.inner.lock().expect("poisoned");
        g.rows.values().cloned().collect()
    }

    pub fn by_piece(&self, file_id: FileId, piece: u32) -> Vec<MemoryAttemptRow> {
        self.snapshot()
            .into_iter()
            .filter(|r| r.file_id == file_id && r.piece_number == piece)
            .collect()
    }
}

impl TPieceAttemptRepo for MemoryAttemptRepo {
    fn start(
        &self,
        file_id: FileId,
        piece_number: u32,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = Result<AttemptRecord>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let id = AttemptId::new();
            let mut g = inner.lock().expect("poisoned");
            g.rows.insert(
                id,
                MemoryAttemptRow {
                    id,
                    file_id,
                    piece_number,
                    worker_id,
                    status: AttemptStatus::Running,
                    bytes: 0,
                    error: None,
                },
            );
            Ok(AttemptRecord {
                id,
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
        attempt_id: AttemptId,
        bytes: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            if let Some(r) = g.rows.get_mut(&attempt_id) {
                r.status = AttemptStatus::Done;
                r.bytes = bytes;
            }
            Ok(())
        }
    }

    fn fail(
        &self,
        attempt_id: AttemptId,
        error: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        let err = error.to_owned();
        async move {
            let mut g = inner.lock().expect("poisoned");
            if let Some(r) = g.rows.get_mut(&attempt_id) {
                r.status = AttemptStatus::Failed;
                r.error = Some(err);
            }
            Ok(())
        }
    }

    fn cancel(
        &self,
        attempt_id: AttemptId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            if let Some(r) = g.rows.get_mut(&attempt_id) {
                r.status = AttemptStatus::Cancelled;
            }
            Ok(())
        }
    }

    fn pause(&self, attempt_id: AttemptId) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            if let Some(r) = g.rows.get_mut(&attempt_id) {
                r.status = AttemptStatus::Paused;
            }
            Ok(())
        }
    }

    fn pause_all_running_for_file(
        &self,
        file_id: FileId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            for r in g.rows.values_mut() {
                if r.file_id == file_id && r.status == AttemptStatus::Running {
                    r.status = AttemptStatus::Paused;
                }
            }
            Ok(())
        }
    }

    fn pause_all_running_globally(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut g = inner.lock().expect("poisoned");
            for r in g.rows.values_mut() {
                if r.status == AttemptStatus::Running {
                    r.status = AttemptStatus::Paused;
                }
            }
            Ok(())
        }
    }
}
