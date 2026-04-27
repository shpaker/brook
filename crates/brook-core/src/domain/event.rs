//! События от движка к подписчикам (gRPC-стримы `WatchStatus`/`WatchProgress`, TUI).
//!
//! События логически делятся на два потока:
//! - **lifecycle** (`FileLifecycleEvent`) — статусные переходы. Едут по
//!   стриму `WatchStatus`. Initial-sync не делается — стартовое состояние
//!   клиент получает через RPC `GetRecently`/`GetFiles`. Создание новых
//!   записей и физическое удаление через стрим **не** уведомляются:
//!   создающая сторона видит результат через ответ `Add`, удалившая —
//!   через `Remove`. Призраки у наблюдателей лечит ghost-режим TUI.
//! - **progress** (`ProgressEvent`) — высокочастотные тики прогресса.
//!   Едут по стриму `WatchProgress`.

use super::id::FileId;
use super::progress::Progress;
use super::status::FileStatus;

/// Статусный переход одного файла. Плоская структура: `id` нужного
/// файла, новый `status`, и опциональный текст-причина.
///
/// `description` заполняется только при `status == Failed` (несёт
/// сообщение об ошибке). На остальных переходах — `None`.
#[derive(Debug, Clone)]
pub struct FileLifecycleEvent {
    pub id: FileId,
    pub status: FileStatus,
    pub description: Option<String>,
}

impl FileLifecycleEvent {
    /// Стандартный переход без description.
    pub fn status(id: FileId, status: FileStatus) -> Self {
        Self {
            id,
            status,
            description: None,
        }
    }

    /// Переход в `Failed` с текстом ошибки.
    pub fn failed(id: FileId, error: impl Into<String>) -> Self {
        Self {
            id,
            status: FileStatus::Failed,
            description: Some(error.into()),
        }
    }
}

/// Тик прогресса. Отдельный enum (а не вариант `FileLifecycleEvent`) —
/// чтобы lifecycle-стрим не смешивался с высокочастотным прогрессом.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Tick { id: FileId, progress: Progress },
}
