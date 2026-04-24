//! Файловые примитивы над `std::fs::File` + `fs4`.
//!
//! Сюда собраны операции, которые нужны `LocalPieceStorage`:
//! - позиционная запись/чтение байтов (`pwrite`/`pread`-стиль);
//! - пре-аллокация файла на заданный размер;
//! - получение свободного места на ФС.
//!
//! **Почему не свои syscalls**: `std` уже обрабатывает `EINTR` внутри
//! `write_all_at`/`read_exact_at`, а пре-аллокация и `statvfs` закрыты
//! крейтом `fs4` — он сам разруливает платформы (macOS `F_PREALLOCATE`,
//! Linux `fallocate`, Windows `SetFileInformationByHandle`).

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use fs4::FileExt as Fs4FileExt;

/// Записать все байты `buf` в файл начиная со смещения `offset`.
///
/// В отличие от `File::write_all`, эта функция **не двигает file position** —
/// это ключевое свойство для `pwrite`-style записи, когда несколько воркеров
/// пишут в один и тот же файл по разным offset'ам без взаимных блокировок.
///
/// `EINTR` и частичные записи (`write` вернул меньше, чем просили) обработаны
/// внутри `write_all_at` — наружу идут только реальные ошибки (`ENOSPC`, `EIO`
/// и т.п.).
pub fn pwrite_full(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    file.write_all_at(buf, offset)
}

/// Прочитать ровно `buf.len()` байт начиная со смещения `offset`.
///
/// Симметричен `pwrite_full`: не трогает file position, обрабатывает EINTR
/// и частичные чтения. EOF до заполнения буфера → `ErrorKind::UnexpectedEof`.
pub fn read_full(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

/// Пре-аллоцировать файл `file` на ровно `size` байт.
///
/// На macOS это `fcntl(F_PREALLOCATE)` + `ftruncate` — блоки резервируются
/// на ФС заранее, чтобы последующие `pwrite` по случайным offset'ам не
/// фрагментировали файл и не приводили к `ENOSPC` посреди загрузки.
///
/// `fs4::allocate` гарантирует: после успешного возврата файл имеет
/// логический размер `size` и под него выделены физические блоки.
pub fn preallocate(file: &File, size: u64) -> io::Result<()> {
    file.allocate(size)
}

/// Свободное место (в байтах) на ФС, которой принадлежит `path`.
///
/// `path` должен существовать — `fs4` вызывает `statvfs` по нему. Обычно
/// передают целевую директорию загрузки.
pub fn available_space(path: &Path) -> io::Result<u64> {
    fs4::available_space(path)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn pwrite_then_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        preallocate(&file, 1024).unwrap();

        let payload = b"hello-brook";
        pwrite_full(&file, payload, 512).unwrap();

        let mut buf = vec![0u8; payload.len()];
        read_full(&file, &mut buf, 512).unwrap();
        assert_eq!(buf, payload);

        // Байты вне записанного диапазона — нули (пре-аллокация).
        let mut zero = vec![0xFFu8; 16];
        read_full(&file, &mut zero, 0).unwrap();
        assert!(zero.iter().all(|&b| b == 0));
    }

    #[test]
    fn read_past_eof_is_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        preallocate(&file, 8).unwrap();

        let mut buf = [0u8; 16];
        let err = read_full(&file, &mut buf, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn preallocate_sets_exact_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        preallocate(&file, 4096).unwrap();
        let len = file.metadata().unwrap().len();
        assert_eq!(len, 4096);
    }

    #[test]
    fn available_space_reports_non_zero_on_tmp() {
        let dir = tempdir().unwrap();
        let free = available_space(dir.path()).unwrap();
        assert!(free > 0, "tmp fs should have free space");
    }
}
