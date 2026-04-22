//! Порт репозитория попыток скачивания piece'а.
//!
//! Один attempt описывает: «воркер `W` попытался скачать piece `P`;
//! стартовал в `started_at`; в итоге `done | failed | cancelled | paused`».
//! Несколько attempt'ов на один piece — нормальный кейс: ретраи после
//! транзиентных ошибок создают новую строку, а не перезаписывают старую.
//!
//! Таблица — source of truth для per-worker / per-piece аналитики: скорость,
//! bytes, число ретраев — всё это выводится оконными/GROUP BY запросами,
//! никаких суммирующих полей в `workers` не держим.

use std::future::Future;

use crate::domain::{
    AttemptId,
    FileId,
    WorkerId,
};
use crate::error::Result;

/// Одна строка из `piece_attempts`, возвращаемая репозиторием.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub id: AttemptId,
    pub piece_id: String,
    pub worker_id: WorkerId,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub bytes: u64,
}

/// Репозиторий таблицы `piece_attempts`.
pub trait TPieceAttemptRepo: Send + Sync {
    /// Стартовать новую попытку: строка со статусом `running`,
    /// `started_at = now`.
    ///
    /// Адаптер сам резолвит `piece_id` по паре `(file_id, piece_number)` —
    /// engine-слою ядра неоткуда знать DB-UUID piece'а, да и незачем.
    fn start(
        &self,
        file_id: FileId,
        piece_number: u32,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<AttemptRecord>> + Send;

    /// Завершить попытку успешно: статус `done`, `finished_at = now`,
    /// `bytes = <скачано>`.
    fn finish(&self, attempt_id: AttemptId, bytes: u64) -> impl Future<Output = Result<()>> + Send;

    /// Провалить попытку: статус `failed`, текст ошибки.
    fn fail(&self, attempt_id: AttemptId, error: &str) -> impl Future<Output = Result<()>> + Send;

    /// Отменить попытку (файл cancelled): статус `cancelled`.
    fn cancel(&self, attempt_id: AttemptId) -> impl Future<Output = Result<()>> + Send;

    /// Поставить попытку на паузу: статус `paused`.
    fn pause(&self, attempt_id: AttemptId) -> impl Future<Output = Result<()>> + Send;

    /// Перевести все `running`-попытки файла в `paused`.
    fn pause_all_running_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Перевести все `running`-попытки любого файла в `paused`.
    /// Парный к [`TWorkerRepo::pause_all_running_globally`].
    ///
    /// [`TWorkerRepo::pause_all_running_globally`]: super::worker_repo::TWorkerRepo::pause_all_running_globally
    fn pause_all_running_globally(&self) -> impl Future<Output = Result<()>> + Send;
}
