//! Порт репозитория воркеров.
//!
//! Воркер — это единица параллелизма одной engine-сессии: фиксированное
//! число воркеров на файл, у каждого свой `slot_index` (0..N-1). Identity
//! воркера живёт ровно одну сессию: после паузы/рестарта старые записи
//! переводятся в `paused`, новая сессия создаёт свежий набор UUID'ов.
//!
//! Это даёт стабильную основу для per-worker аналитики: историю попыток
//! можно GROUP BY worker_id, и пересечения идентификаторов между
//! сессиями исключены.

use std::future::Future;

use crate::domain::{
    FileId,
    WorkerId,
};
use crate::error::Result;

/// Одна строка из таблицы `workers`, возвращаемая репозиторием.
///
/// `started_at` / `finished_at` — unix seconds, как и во всей БД.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRecord {
    pub id: WorkerId,
    pub file_id: FileId,
    pub slot_index: usize,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}

/// Репозиторий таблицы `workers`.
pub trait TWorkerRepo: Send + Sync {
    /// Подготовить набор из `n` слотов для файла.
    ///
    /// Сначала любые `running`-воркеры этого файла переводятся в `paused`
    /// с `finished_at = now` (защитный sweep — покрывает кейс, когда
    /// предыдущая сессия умерла, не успев пометить свои строки). Затем
    /// создаются `n` свежих строк со статусом `running`, `slot_index`
    /// `0..n-1`, и возвращаются вызывающему.
    fn ensure_slots(
        &self,
        file_id: FileId,
        n: usize,
    ) -> impl Future<Output = Result<Vec<WorkerRecord>>> + Send;

    /// Пометить воркер как `paused` (установив `finished_at = now`).
    fn mark_paused(&self, worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send;

    /// Пометить воркер как `done` (`finished_at = now`).
    fn mark_done(&self, worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send;

    /// Пометить воркер как `failed` с текстом ошибки.
    fn mark_failed(
        &self,
        worker_id: WorkerId,
        error: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Пометить воркер как `cancelled`.
    fn mark_cancelled(&self, worker_id: WorkerId) -> impl Future<Output = Result<()>> + Send;

    /// Перевести всех `running`-воркеров файла в `paused` (`finished_at = now`).
    /// Используется, когда engine ставит файл на паузу.
    fn pause_all_running_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Перевести всех `running`-воркеров любого файла в `paused`.
    /// Вызывается ровно один раз при старте демона (recovery) под
    /// `.brook.lock`.
    fn pause_all_running_globally(&self) -> impl Future<Output = Result<()>> + Send;
}
