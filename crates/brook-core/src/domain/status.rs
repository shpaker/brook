//! Единый словарь статусов — для файлов, воркеров, piece'ов и attempt'ов.
//!
//! Чтобы не путать «статус файла» со «статусом воркера», каждая сущность
//! получает свой отдельный enum: подмножество единого вокабуляра
//! (`pending | running | paused | retrying | done | failed | cancelled`).
//! Таблица допустимых значений по сущностям — в `docs/schema.dbml`.
//!
//! Каждый enum сериализуется в то же lowercase-имя, что лежит в БД:
//! `as_str()`/`from_str()` — единый контракт между Rust-кодом и SQLite
//! `CHECK (status_id IN (...))`.

use std::fmt;
use std::str::FromStr;

use crate::error::{
    Error,
    Result,
};

// ─── FileStatus ──────────────────────────────────────────────────────────

/// Статус файла-загрузки. Полный набор.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileStatus {
    Pending,
    Running,
    Paused,
    Retrying,
    Done,
    Failed,
    Cancelled,
}

impl FileStatus {
    /// Терминальные состояния — из них переходов нет.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }

    /// «Живёт ли» загрузка прямо сейчас (держит сокеты/воркеры).
    pub fn is_active(self) -> bool {
        matches!(self, Self::Running | Self::Retrying)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Retrying => "retrying",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FileStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "paused" => Self::Paused,
            "retrying" => Self::Retrying,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(Error::Other(format!("unknown file status: {other}"))),
        })
    }
}

// ─── WorkerStatus ────────────────────────────────────────────────────────

/// Статус одного воркера в рамках одной engine-сессии.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    Running,
    Paused,
    Done,
    Failed,
    Cancelled,
}

impl WorkerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WorkerStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "running" => Self::Running,
            "paused" => Self::Paused,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(Error::Other(format!("unknown worker status: {other}"))),
        })
    }
}

// ─── PieceStatus ─────────────────────────────────────────────────────────

/// Статус piece'а. В БД персистится только `pending` и `done`;
/// `running` — runtime-состояние engine, на диск не пишется
/// (см. policy в `pieces` repo).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PieceStatus {
    Pending,
    Running,
    Done,
}

impl PieceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
        }
    }
}

impl fmt::Display for PieceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PieceStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "done" => Self::Done,
            other => return Err(Error::Other(format!("unknown piece status: {other}"))),
        })
    }
}

// ─── AttemptStatus ───────────────────────────────────────────────────────

/// Статус одной попытки скачать конкретный piece конкретным воркером.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttemptStatus {
    Running,
    Paused,
    Done,
    Failed,
    Cancelled,
}

impl AttemptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for AttemptStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AttemptStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "running" => Self::Running,
            "paused" => Self::Paused,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(Error::Other(format!("unknown attempt status: {other}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_terminal_vs_active() {
        assert!(FileStatus::Done.is_terminal());
        assert!(FileStatus::Failed.is_terminal());
        assert!(FileStatus::Cancelled.is_terminal());
        assert!(!FileStatus::Pending.is_terminal());

        assert!(FileStatus::Running.is_active());
        assert!(FileStatus::Retrying.is_active());
        assert!(!FileStatus::Paused.is_active());
    }

    #[test]
    fn file_status_roundtrip() {
        for s in [
            FileStatus::Pending,
            FileStatus::Running,
            FileStatus::Paused,
            FileStatus::Retrying,
            FileStatus::Done,
            FileStatus::Failed,
            FileStatus::Cancelled,
        ] {
            let parsed: FileStatus = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn worker_status_roundtrip() {
        for s in [
            WorkerStatus::Running,
            WorkerStatus::Paused,
            WorkerStatus::Done,
            WorkerStatus::Failed,
            WorkerStatus::Cancelled,
        ] {
            let parsed: WorkerStatus = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn piece_status_roundtrip() {
        for s in [
            PieceStatus::Pending,
            PieceStatus::Running,
            PieceStatus::Done,
        ] {
            let parsed: PieceStatus = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn attempt_status_roundtrip() {
        for s in [
            AttemptStatus::Running,
            AttemptStatus::Paused,
            AttemptStatus::Done,
            AttemptStatus::Failed,
            AttemptStatus::Cancelled,
        ] {
            let parsed: AttemptStatus = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn parse_rejects_unknown() {
        assert!("bogus".parse::<FileStatus>().is_err());
        assert!("pending".parse::<WorkerStatus>().is_err());
        assert!("retrying".parse::<PieceStatus>().is_err());
        assert!("pending".parse::<AttemptStatus>().is_err());
    }
}
