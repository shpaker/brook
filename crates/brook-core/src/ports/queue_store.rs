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

use crate::domain::{
    Download,
    DownloadId,
    FailureReason,
    FileStatus,
};
use crate::error::Result;

pub trait TQueueStore: Send + Sync {
    /// Все загрузки из хранилища (при старте демона).
    fn load_all(&self) -> impl Future<Output = Result<Vec<Download>>> + Send;

    /// Вставить новую запись. Ошибка, если `id` уже существует.
    fn insert(&self, download: &Download) -> impl Future<Output = Result<()>> + Send;

    /// Обновить статус существующей загрузки. Ошибка, если записи нет.
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
        id: DownloadId,
        status: FileStatus,
        reason: Option<FailureReason>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Удалить запись. Ошибка, если записи нет.
    fn remove(&self, id: DownloadId) -> impl Future<Output = Result<()>> + Send;
}
