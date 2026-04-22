//! Подготовка `.data.brook`: free-space check + open + truncate + preallocate.
//!
//! Всё содержимое оборачивается в `spawn_blocking`: `statvfs`, `File::open`,
//! `preallocate` — синхронные системные вызовы, которые нельзя держать на
//! reactor'ном потоке.

use std::fs::{
    File,
    OpenOptions,
};
use std::path::Path;

use crate::storage::error::{
    StorageError,
    StorageResult,
};
use crate::storage::fs::{
    available_space,
    preallocate,
};

/// Открыть или создать `.data.brook` под заданное `total_size`.
///
/// - `use_resume = true` — открываем уже существующий файл без truncate;
///   поле `total_size` всё равно участвует в free-space-проверке, чтобы
///   устранить случай «диск забили после того как мы начали, но до
///   рестарта».
/// - `use_resume = false` — удаляем возможный хвост, создаём заново,
///   преаллоцируем до `total_size`.
///
/// Выполняется на blocking-пуле; возвращает `File`, открытый на чтение
/// и запись.
pub(super) async fn open_or_preallocate(
    target_dir: &Path,
    data_path: &Path,
    total_size: u64,
    use_resume: bool,
) -> StorageResult<File> {
    let target_dir = target_dir.to_path_buf();
    let data_path = data_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> StorageResult<File> {
        let free = available_space(&target_dir)?;
        if free < total_size {
            return Err(StorageError::NotEnoughSpace {
                have: free,
                need: total_size,
            });
        }
        if use_resume {
            Ok(OpenOptions::new().read(true).write(true).open(&data_path)?)
        } else {
            if data_path.exists() {
                std::fs::remove_file(&data_path)?;
            }
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&data_path)?;
            preallocate(&f, total_size)?;
            Ok(f)
        }
    })
    .await?
}
