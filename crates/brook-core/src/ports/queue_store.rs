//! Абстракция над глобальной очередью загрузок.
//!
//! Конкретная реализация (1.8) будет в SQLite `brook.db` в CWD демона.
//! `TQueueStore` выдаёт доменные методы — вызывающий не видит `Connection`,
//! SQL или схему таблицы. Это соблюдает правило CLAUDE.md:
//! «SQL и `rusqlite::Connection` никогда не должны утекать за пределы
//! repository-границы».
//!
//! Про форму сигнатур (`-> impl Future + Send`) — см. пояснение в
//! [`super::piece_storage`].

use std::future::Future;

use crate::domain::{Download, DownloadId, DownloadState};
use crate::error::Result;

pub trait TQueueStore: Send + Sync {
    /// Все загрузки из хранилища (при старте демона).
    fn load_all(&self) -> impl Future<Output = Result<Vec<Download>>> + Send;

    /// Вставить новую запись. Ошибка, если `id` уже существует.
    fn insert(&self, download: &Download) -> impl Future<Output = Result<()>> + Send;

    /// Обновить состояние существующей загрузки. Ошибка, если записи нет.
    ///
    /// Зачем отдельный метод, а не «upsert всей `Download`»: типичный
    /// путь — это частый переход состояний + редкое обновление остальных
    /// полей. Узкая сигнатура читается и работает дешевле.
    fn update_state(
        &self,
        id: DownloadId,
        state: DownloadState,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Удалить запись. Ошибка, если записи нет.
    fn remove(&self, id: DownloadId) -> impl Future<Output = Result<()>> + Send;
}
