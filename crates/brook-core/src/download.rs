//! Полная запись о загрузке: spec + runtime-состояние.
//!
//! Это то, что лежит в очереди (`brook.db`) и что возвращается клиенту
//! в ответах `List`/`Watch`.

use std::time::SystemTime;

use crate::id::DownloadId;
use crate::progress::Progress;
use crate::spec::DownloadSpec;
use crate::state::DownloadState;

#[derive(Debug, Clone)]
pub struct Download {
    pub id: DownloadId,
    pub spec: DownloadSpec,
    pub state: DownloadState,
    pub progress: Progress,

    /// Номер текущей попытки (с 1). Увеличивается на каждом retry.
    pub attempt: u32,
    /// Текст последней ошибки — заполняется для `Failed`/`Retrying`.
    /// `Option<String>`, чтобы явно различать «ошибки не было» и «пустая строка».
    pub error: Option<String>,

    /// Момент создания записи.
    pub created_at: SystemTime,
    /// Момент последнего изменения состояния или прогресса.
    pub updated_at: SystemTime,
}

impl Download {
    /// Свежая загрузка: `Queued`, нулевой прогресс, обе метки времени — сейчас.
    pub fn new(id: DownloadId, spec: DownloadSpec) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            spec,
            state: DownloadState::Queued,
            progress: Progress::default(),
            attempt: 0,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_in_queued() {
        let d = Download::new(
            DownloadId::new(),
            DownloadSpec::new("https://example.com/f", "/tmp"),
        );
        assert_eq!(d.state, DownloadState::Queued);
        assert_eq!(d.attempt, 0);
        assert!(d.error.is_none());
        assert_eq!(d.progress, Progress::default());
        // Время создания и изменения должны совпадать у свежей записи.
        assert_eq!(d.created_at, d.updated_at);
    }
}
