//! Состояние загрузки — конечный автомат.
//!
//! В Rust enum — это не «набор констант», как в C, а sum-type:
//! значение может быть ровно одним из вариантов. Компилятор заставит
//! `match` покрыть все варианты — один из главных источников надёжности.

use std::fmt;
use std::str::FromStr;

use crate::error::{
    Error,
    Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownloadState {
    /// В очереди, ждёт слота воркера.
    Queued,
    /// Активно качается.
    Running,
    /// Поставлена на паузу пользователем.
    Paused,
    /// Упала, ожидает следующий retry (экспо-бэкофф).
    Retrying,
    /// Успешно завершена.
    Done,
    /// Упала окончательно (исчерпаны retry или невосстановимая ошибка).
    Failed,
    /// Отменена пользователем, файлы удалены.
    Cancelled,
}

impl DownloadState {
    /// Терминальные состояния — из них переходов нет.
    pub fn is_terminal(self) -> bool {
        // `matches!` — макрос-сахар над `match ... => true, _ => false`.
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }

    /// «Живёт ли» загрузка прямо сейчас (держит сокеты/воркеры).
    pub fn is_active(self) -> bool {
        matches!(self, Self::Running | Self::Retrying)
    }

    /// Стабильное строковое имя (для логов и БД).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Retrying => "retrying",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for DownloadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DownloadState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "paused" => Self::Paused,
            "retrying" => Self::Retrying,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(Error::Other(format!("unknown state: {other}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_vs_active() {
        assert!(DownloadState::Done.is_terminal());
        assert!(DownloadState::Failed.is_terminal());
        assert!(DownloadState::Cancelled.is_terminal());
        assert!(!DownloadState::Queued.is_terminal());

        assert!(DownloadState::Running.is_active());
        assert!(DownloadState::Retrying.is_active());
        assert!(!DownloadState::Paused.is_active());
    }

    #[test]
    fn str_roundtrip_covers_all_variants() {
        // Каждый вариант должен сериализоваться и парситься обратно.
        // Если добавили новый — компилятор заставит обработать его в `as_str`
        // и в `from_str`, а этот тест страхует симметричность.
        for s in [
            DownloadState::Queued,
            DownloadState::Running,
            DownloadState::Paused,
            DownloadState::Retrying,
            DownloadState::Done,
            DownloadState::Failed,
            DownloadState::Cancelled,
        ] {
            let parsed: DownloadState = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn parse_rejects_unknown() {
        assert!("bogus".parse::<DownloadState>().is_err());
    }
}
