//! Арифметика piece'ов + мелкий путь-хелпер.
//!
//! После перехода на общую `brook.db` геометрия piece'ов не хранится — она
//! восстанавливается из пары `(total_size, piece_size)`, поэтому живёт
//! здесь в виде чистых функций.

use std::ffi::OsString;
use std::path::{
    Path,
    PathBuf,
};

/// Сколько piece'ов получится при заданных `total_size` и `piece_size`.
/// `total_size == 0` → 0 piece'ов; последний piece может быть короче.
pub(super) fn piece_count(total_size: u64, piece_size: u64) -> u32 {
    if total_size == 0 {
        return 0;
    }
    total_size.div_ceil(piece_size) as u32
}

/// Абсолютный offset piece'а `n` в `.data.brook`.
pub(super) fn offset_for(n: u32, piece_size: u64) -> u64 {
    n as u64 * piece_size
}

/// Размер piece'а `n` в байтах. Последний piece может быть короче.
pub(super) fn size_for(n: u32, total_size: u64, piece_size: u64) -> u64 {
    let off = offset_for(n, piece_size);
    // Гарантировано off < total_size по проверкам в write_piece_bytes;
    // saturating на всякий случай — лучше нулевой piece, чем underflow.
    total_size.saturating_sub(off).min(piece_size)
}

/// Добавить суффикс к пути без потери расширения (`foo.iso` → `foo.iso.data.brook`).
pub(super) fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s: OsString = p.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}
