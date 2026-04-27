//! События от движка к подписчикам (gRPC-стримы `WatchFile`/`WatchProgress`, TUI).
//!
//! События логически делятся на два потока:
//! - **lifecycle** (`FileLifecycleEvent`) — дельта-события: создание,
//!   смена статусов, завершение, падения, удаление. Едут по стриму
//!   `WatchFile`. Initial-sync убран — стартовое состояние клиент
//!   получает через RPC `GetRecently`/`GetFiles`.
//! - **progress** (`ProgressEvent`) — высокочастотные тики прогресса.
//!   Едут по стриму `WatchProgress`.

use super::file::File;
use super::id::FileId;
use super::progress::Progress;
use super::status::FileStatus;

/// Дельта-события жизненного цикла файла. Вызывающий делает `match` и
/// компилятор гарантирует, что ни один вариант не забыт.
#[derive(Debug, Clone)]
pub enum FileLifecycleEvent {
    /// Запись добавлена. Шлётся менеджером после persist'а.
    ///
    /// `Box<File>` (а не просто `File`): `File` — самая крупная по размеру
    /// структура из всех вариантов enum'а. Без `Box` enum бы «раздулся» до
    /// её размера на стеке даже для мелких вариантов вроде `Completed`.
    /// Индирекция через `Box` держит enum компактным.
    Created { file: Box<File> },

    /// Смена статуса (`Pending → Running`, `Running → Paused`, …).
    StatusChanged { id: FileId, status: FileStatus },

    /// Файл успешно скачан.
    Completed { id: FileId },

    /// Файл упал окончательно.
    Failed { id: FileId, error: String },

    /// Запись удалена.
    Removed { id: FileId },
}

/// Тик прогресса. Отдельный enum (а не вариант `FileLifecycleEvent`) —
/// чтобы lifecycle-стрим не смешивался с высокочастотным прогрессом.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Tick { id: FileId, progress: Progress },
}

#[cfg(test)]
mod tests {
    use super::super::spec::FileSpec;
    use super::*;

    #[test]
    fn match_exhaustive_compiles() {
        let id = FileId::new();
        let ev = FileLifecycleEvent::Completed { id };
        let _text: &'static str = match ev {
            FileLifecycleEvent::Created { .. } => "created",
            FileLifecycleEvent::StatusChanged { .. } => "status",
            FileLifecycleEvent::Completed { .. } => "done",
            FileLifecycleEvent::Failed { .. } => "failed",
            FileLifecycleEvent::Removed { .. } => "removed",
        };
    }

    #[test]
    fn created_carries_full_file() {
        let d = File::new(
            FileId::new(),
            FileSpec::new("https://example.com/f", "/tmp"),
        );
        let ev = FileLifecycleEvent::Created {
            file: Box::new(d.clone()),
        };
        if let FileLifecycleEvent::Created { file } = ev {
            assert_eq!(file.id, d.id);
        } else {
            panic!("expected Created");
        }
    }
}
