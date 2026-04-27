//! Абстракция над глобальной очередью загрузок.
//!
//! Конкретная реализация живёт в SQLite `brook.db` в CWD демона.
//! `TQueueStore` выдаёт доменные методы — вызывающий не видит `Connection`,
//! SQL или схему таблицы. Это соблюдает правило CLAUDE.md:
//! «SQL и `rusqlite::Connection` никогда не должны утекать за пределы
//! repository-границы».
//!
//! Про форму сигнатур (`-> impl Future + Send`) — см. пояснение в
//! [`super::piece_storage`].

use std::future::Future;
use std::time::SystemTime;

use crate::domain::{
    FailureReason,
    File,
    FileId,
    FileStatus,
};
use crate::error::Result;

pub trait TQueueStore: Send + Sync {
    /// Все файлы из хранилища (при старте демона).
    fn load_all(&self) -> impl Future<Output = Result<Vec<File>>> + Send;

    /// Файлы с активностью >= `since` — последний по времени timestamp
    /// любой модификации в самой записи или в её кусках/попытках. Сортировка
    /// `last_activity_at DESC`. Используется для главного экрана TUI
    /// (`recently`).
    fn list_recently(&self, since: SystemTime) -> impl Future<Output = Result<Vec<File>>> + Send;

    /// Пагинированный список всех файлов, ORDER BY `created_at DESC`.
    /// `limit = 0` → реализация сама решит дефолт. Используется экраном
    /// «История».
    fn list_paginated(
        &self,
        offset: u32,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<File>>> + Send;

    /// Вставить новую запись. Ошибка, если `id` уже существует.
    fn insert(&self, file: &File) -> impl Future<Output = Result<()>> + Send;

    /// Обновить статус существующего файла. Ошибка, если записи нет.
    ///
    /// `reason` обязателен при переходе в [`FileStatus::Failed`]
    /// (инвариант схемы). Для остальных переходов опционален: например,
    /// `Cancelled` + [`ReasonCode::CancelledByUser`] полезно писать в
    /// историю, а для обычных `Running`/`Paused`/`Done` reason, как
    /// правило, `None`.
    ///
    /// [`ReasonCode::CancelledByUser`]: crate::domain::ReasonCode::CancelledByUser
    fn update_status(
        &self,
        id: FileId,
        status: FileStatus,
        reason: Option<FailureReason>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Удалить запись. Ошибка, если записи нет.
    fn remove(&self, id: FileId) -> impl Future<Output = Result<()>> + Send;
}
