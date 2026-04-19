//! События от движка к подписчикам (gRPC `Watch` стрим, TUI).
//!
//! Паттерн: **sum type с полезной нагрузкой у каждого варианта**.
//! Вызывающий делает `match` и компилятор гарантирует, что ни один
//! вариант не забыт.

use crate::download::Download;
use crate::id::DownloadId;
use crate::progress::Progress;
use crate::state::DownloadState;

#[derive(Debug, Clone)]
pub enum DownloadEvent {
    /// Тик прогресса (троттлится со стороны движка).
    Progress { id: DownloadId, progress: Progress },

    /// Смена состояния (`Queued → Running`, `Running → Paused`, …).
    StateChanged {
        id: DownloadId,
        state: DownloadState,
    },

    /// Частичное обновление по одному piece'у — для чанкового прогрессбара.
    WorkerUpdate {
        id: DownloadId,
        piece_index: u32,
        /// Доля скачанного в этом piece'е, `0.0..=1.0`.
        fraction: f32,
    },

    /// Загрузка успешно завершена.
    Completed { id: DownloadId },

    /// Загрузка упала окончательно.
    Failed { id: DownloadId, error: String },

    /// Полный снимок — например, при подключении нового клиента к `Watch`.
    ///
    /// `Box<Download>` (а не просто `Download`): `Download` — самая крупная
    /// по размеру структура из всех вариантов enum'а. Без `Box` enum бы
    /// «раздулся» до её размера на стеке даже для мелких вариантов вроде
    /// `Completed`. Индирекция через `Box` держит enum компактным.
    Snapshot { download: Box<Download> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::DownloadSpec;

    #[test]
    fn match_exhaustive_compiles() {
        // Этот тест ничего не проверяет во время выполнения — он «компиляционный».
        // Если в enum добавят новый вариант, `match` здесь перестанет компилироваться,
        // и это хорошо: мы заставим автора нового варианта решить, что с ним делать.
        let id = DownloadId::new();
        let ev = DownloadEvent::Completed { id };
        let _text: &'static str = match ev {
            DownloadEvent::Progress { .. } => "progress",
            DownloadEvent::StateChanged { .. } => "state",
            DownloadEvent::WorkerUpdate { .. } => "worker",
            DownloadEvent::Completed { .. } => "done",
            DownloadEvent::Failed { .. } => "failed",
            DownloadEvent::Snapshot { .. } => "snapshot",
        };
    }

    #[test]
    fn snapshot_carries_full_download() {
        let d = Download::new(
            DownloadId::new(),
            DownloadSpec::new("https://example.com/f", "/tmp"),
        );
        let ev = DownloadEvent::Snapshot {
            download: Box::new(d.clone()),
        };
        if let DownloadEvent::Snapshot { download } = ev {
            assert_eq!(download.id, d.id);
        } else {
            panic!("expected Snapshot");
        }
    }
}
