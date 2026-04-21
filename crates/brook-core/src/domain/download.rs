//! Полная запись о загрузке: spec + runtime-статус.
//!
//! Это то, что лежит в очереди (`brook.db`) и что возвращается клиенту
//! в ответах `List`/`Watch`.

use std::time::SystemTime;

use super::id::DownloadId;
use super::progress::Progress;
use super::spec::DownloadSpec;
use super::status::FileStatus;

#[derive(Debug, Clone)]
pub struct Download {
    pub id: DownloadId,
    pub spec: DownloadSpec,
    pub status: FileStatus,
    pub progress: Progress,

    /// Номер текущей попытки (с 1). Увеличивается на каждом retry.
    pub attempt: u32,
    /// Текст последней ошибки — заполняется для `Failed`/`Retrying`.
    /// `Option<String>`, чтобы явно различать «ошибки не было» и «пустая строка».
    pub error: Option<String>,

    /// Момент создания записи.
    pub created_at: SystemTime,
    /// Момент последнего изменения статуса или прогресса.
    pub updated_at: SystemTime,
}

impl Download {
    /// Свежая загрузка: `Pending`, нулевой прогресс, обе метки времени — сейчас.
    pub fn new(id: DownloadId, spec: DownloadSpec) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            spec,
            status: FileStatus::Pending,
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
    fn new_starts_in_pending() {
        let d = Download::new(
            DownloadId::new(),
            DownloadSpec::new("https://example.com/f", "/tmp"),
        );
        assert_eq!(d.status, FileStatus::Pending);
        assert_eq!(d.attempt, 0);
        assert!(d.error.is_none());
        assert_eq!(d.progress, Progress::default());
        assert_eq!(d.created_at, d.updated_at);
    }
}
