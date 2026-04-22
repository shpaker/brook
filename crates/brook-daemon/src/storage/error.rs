//! Единый тип ошибок storage-слоя `brook-daemon`.
//!
//! До этого каждый модуль (`pieces`, `files`, `db`, `paths`) имел свою
//! thiserror-enum, а `local.rs` бросал `brook_core::Error::Other(format!(...))`
//! и голые `io::Error`. [`StorageError`] оборачивает всё через `#[from]`,
//! добавляет доменные варианты для файловой части (piece out of range,
//! write past end, after finalize/abort, not enough space), и конвертируется
//! в [`brook_core::Error`] на границе портов `TPieceStorage` / `TStreamStorage`.
//!
//! Внутренний код storage использует `StorageError` и оператор `?`;
//! в `impl TPieceStorage for LocalPieceStorage` (и аналоге для stream)
//! ошибка автоматически конвертируется в `brook_core::Error` через тот же
//! `?`, потому что `From<StorageError> for brook_core::Error` определён
//! в этом же файле.
//!
//! Публичная форма ошибки для ядра (`brook_core::Error::Io` /
//! `brook_core::Error::Other`) сохраняется — никаких изменений в API.

use brook_core::Error as CoreError;
use thiserror::Error;

use super::db::DbError;
use super::files::FilesError;
use super::paths::PathError;
use super::pieces::PiecesError;

/// Ошибки storage-слоя `brook-daemon`. Внутренний тип: наружу не экспортируется,
/// на границе портов конвертируется в [`brook_core::Error`].
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Pieces(#[from] PiecesError),

    #[error(transparent)]
    Files(#[from] FilesError),

    #[error(transparent)]
    Db(#[from] DbError),

    #[error(transparent)]
    Path(#[from] PathError),

    #[error("background task join: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// `statvfs` показал меньше свободного места, чем нужно под преаллокацию.
    #[error("not enough free space: have {have}, need {need}")]
    NotEnoughSpace { have: u64, need: u64 },

    /// Запрошенный `piece_index` выходит за пределы нарезки файла.
    #[error("piece {index} out of range (count {count})")]
    PieceIndexOutOfRange { index: u32, count: u32 },

    /// `offset_in_piece + bytes.len()` превысило `size_for(piece_index)`.
    #[error("write past piece end: piece {index}, end {end} vs size {size}")]
    WritePastPieceEnd { index: u32, end: u64, size: u64 },

    /// Операция вызвана после `finalize()` (файл уже переименован).
    #[error("{op} after finalize")]
    AfterFinalize { op: &'static str },

    /// Операция вызвана после `abort()` (файл уже удалён).
    #[error("{op} after abort")]
    AfterAbort { op: &'static str },

    /// Конструктор получил `piece_size == 0`.
    #[error("piece_size must be > 0")]
    InvalidPieceSize,

    /// `offset_in_piece + bytes.len()` переполнило `u64`.
    #[error("offset overflow")]
    OffsetOverflow,
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

impl From<StorageError> for CoreError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Io(io) => CoreError::Io(io),
            // Сохраняем поведение старого кода: все недомашние варианты
            // схлопываются в `Error::Other`. Ядро всё равно не различает
            // их по типу — differentiate только `is_file_exists()`, а этот
            // сигнал приходит из `FilesError::Duplicate` через другой путь.
            other => CoreError::Other(other.to_string()),
        }
    }
}
