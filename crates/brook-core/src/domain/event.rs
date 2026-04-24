//! События от движка к подписчикам (gRPC-стримы `WatchFile`/`WatchProgress`, TUI).
//!
//! События логически делятся на два потока:
//! - **lifecycle** (`FileLifecycleEvent`) — смена статусов, завершение,
//!   падения, полные снапшоты. Это то, что рендерит стрим `WatchFile`.
//! - **progress** (`ProgressEvent`) — высокочастотные тики прогресса.
//!   Едут по стриму `WatchProgress`.

use super::file::File;
use super::id::FileId;
use super::progress::{
    BarState,
    Progress,
};
use super::status::FileStatus;

/// События жизненного цикла файла. Вызывающий делает `match` и компилятор
/// гарантирует, что ни один вариант не забыт.
#[derive(Debug, Clone)]
pub enum FileLifecycleEvent {
    /// Смена статуса (`Pending → Running`, `Running → Paused`, …).
    StatusChanged { id: FileId, status: FileStatus },

    /// Файл успешно скачан.
    Completed { id: FileId },

    /// Файл упал окончательно.
    Failed { id: FileId, error: String },

    /// Полный снимок — например, при подключении нового клиента к `WatchFile`.
    ///
    /// `Box<File>` (а не просто `File`): `File` — самая крупная по размеру
    /// структура из всех вариантов enum'а. Без `Box` enum бы «раздулся» до
    /// её размера на стеке даже для мелких вариантов вроде `Completed`.
    /// Индирекция через `Box` держит enum компактным.
    Snapshot { file: Box<File> },
}

/// Тик прогресса. Отдельный enum (а не вариант `FileLifecycleEvent`) —
/// чтобы lifecycle-стрим не смешивался с высокочастотным прогрессом.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Tick {
        id: FileId,
        progress: Progress,
        /// Чанкованное состояние прогрессбара.
        /// `None` при `linear=true` или неизвестном размере файла.
        bar: Option<BarState>,
    },
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
            FileLifecycleEvent::StatusChanged { .. } => "status",
            FileLifecycleEvent::Completed { .. } => "done",
            FileLifecycleEvent::Failed { .. } => "failed",
            FileLifecycleEvent::Snapshot { .. } => "snapshot",
        };
    }

    #[test]
    fn snapshot_carries_full_file() {
        let d = File::new(
            FileId::new(),
            FileSpec::new("https://example.com/f", "/tmp"),
        );
        let ev = FileLifecycleEvent::Snapshot {
            file: Box::new(d.clone()),
        };
        if let FileLifecycleEvent::Snapshot { file } = ev {
            assert_eq!(file.id, d.id);
        } else {
            panic!("expected Snapshot");
        }
    }
}
