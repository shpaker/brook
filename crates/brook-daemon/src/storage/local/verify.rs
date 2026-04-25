//! Детектор «файл `.data.brook` пропал из-под нас».
//!
//! Хэндл на data-файл живёт в [`super::piece::LocalPieceStorage`] и
//! [`super::stream::LocalStreamStorage`] всю загрузку. На Unix
//! `unlink(2)` открытого файла не закрывает fd: inode остаётся жив,
//! `pwrite` продолжает писать в осиротевший inode — без ошибок, без
//! прогресса наружу. Чтобы поймать удаление пользователем (Finder,
//! `rm`, перенос на другой раздел и т.п.), при `open` мы запоминаем
//! `(dev, ino)` файла одним `fstat(2)`, а в горячем пути сравниваем
//! их со `stat(2)` пути.
//!
//! Любое расхождение или ENOENT трактуем одинаково
//! ([`StorageError::DataFileMissing`]): для engine это permanent-ошибка,
//! загрузка уходит в `Failed`. Различать «удалили» и «заменили» наружу
//! смысла нет — действие пользователя одно и то же.

use std::path::Path;

use crate::storage::error::{
    StorageError,
    StorageResult,
};

/// Проверить, что `data_path` всё ещё указывает на тот же inode,
/// который был открыт при создании storage. Один `stat(2)` syscall.
#[inline]
pub(super) fn verify_data_file_present(
    data_path: &Path,
    expected_dev: u64,
    expected_ino: u64,
) -> StorageResult<()> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(data_path) {
        Ok(m) if m.dev() == expected_dev && m.ino() == expected_ino => Ok(()),
        _ => Err(StorageError::DataFileMissing {
            path: data_path.display().to_string(),
        }),
    }
}

/// Снять `(dev, ino)` с уже открытого `File` — вызывается один раз
/// сразу после `open`. Inode у живого fd не меняется, поэтому кэшируем.
#[inline]
pub(super) fn fd_dev_ino(file: &std::fs::File) -> StorageResult<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let m = file.metadata()?;
    Ok((m.dev(), m.ino()))
}
