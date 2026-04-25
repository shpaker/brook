//! Полная запись о файле: spec + runtime-статус.
//!
//! Это то, что лежит в очереди (`brook.db`) и что возвращается клиенту
//! в ответах `List` и стрима `WatchFile`. Прогресс сюда не входит — он
//! едет отдельным стримом `WatchProgress`.

use std::time::SystemTime;

use super::id::FileId;
use super::spec::FileSpec;
use super::status::FileStatus;

#[derive(Debug, Clone)]
pub struct File {
    pub id: FileId,
    pub spec: FileSpec,
    pub status: FileStatus,

    /// Номер текущей попытки (с 1). Увеличивается на каждом retry.
    pub attempt: u32,
    /// Текст последней ошибки — заполняется для `Failed`/`Retrying`.
    /// `Option<String>`, чтобы явно различать «ошибки не было» и «пустая строка».
    pub error: Option<String>,

    /// Момент создания записи.
    pub created_at: SystemTime,
    /// Момент последнего изменения статуса.
    pub updated_at: SystemTime,

    /// Итоговая средняя скорость загрузки (байт/сек). Заполняется
    /// репозиторием on-the-fly только для `Done` (weighted average по
    /// байтам из `piece_attempts`); для остальных статусов — `None`.
    pub avg_speed_bps: Option<f64>,
    /// Кол-во разных воркеров, реально качавших хотя бы один piece этого
    /// файла. Заполняется тем же способом и тоже только для `Done`.
    pub workers_count: Option<u32>,
}

impl File {
    /// Свежий файл: `Pending`, обе метки времени — сейчас.
    pub fn new(id: FileId, spec: FileSpec) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            spec,
            status: FileStatus::Pending,
            attempt: 0,
            error: None,
            created_at: now,
            updated_at: now,
            avg_speed_bps: None,
            workers_count: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_in_pending() {
        let d = File::new(
            FileId::new(),
            FileSpec::new("https://example.com/f", "/tmp"),
        );
        assert_eq!(d.status, FileStatus::Pending);
        assert_eq!(d.attempt, 0);
        assert!(d.error.is_none());
        assert_eq!(d.created_at, d.updated_at);
    }
}
