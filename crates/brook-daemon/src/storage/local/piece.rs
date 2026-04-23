//! [`LocalPieceStorage`] — реализация [`TPieceStorage`] поверх файловой
//! системы и общего [`SharedDb`].
//!
//! Вся persistent-геометрия piece'ов лежит в таблице `pieces`
//! (shared `brook.db`), scoping — по `file_id`. Рядом с таргетом адаптер
//! держит только `<name>.data.brook` (после `finalize` → `<name>`).
//!
//! ## Файлы на диске
//!
//! Для загрузки с `filename = "foo.iso"` в `target_dir = "/dl"`:
//! - `"/dl/foo.iso.data.brook"` — преаллоцированный на `total_size`
//!   контейнер, куда воркеры пишут байты по offset'ам (`pwrite`).
//! - `"/dl/foo.iso"` — целевой файл, появляется только после `finalize`
//!   (атомарный `rename` из `.data.brook`).
//!
//! ## Почему `spawn_blocking` для file-операций
//!
//! `File::write_all_at` и `preallocate` — **блокирующие** API. Трейт
//! [`TPieceStorage`] обещает `-> impl Future + Send`, поэтому каждая
//! операция уезжает в `tokio::task::spawn_blocking`, чтобы не «сожрать»
//! reactor'ный поток многомегабайтной записью или fsync'ом. SQLite-операции
//! идут через [`SqlitePieceRepository`] / [`SqliteFileRepository`] —
//! у них уже есть свой `spawn_blocking` внутри [`SharedDb::with_conn`].
//!
//! [`SharedDb`]: crate::storage::db::SharedDb
//! [`SharedDb::with_conn`]: crate::storage::db::SharedDb::with_conn

use std::fs::File;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::atomic::{
    AtomicBool,
    Ordering,
};
use std::sync::{
    Arc,
    RwLock,
};

use brook_core::{
    FileId,
    Result as CoreResult,
    TPieceStorage,
};
use bytes::Bytes;

use super::layout::{
    offset_for,
    piece_count,
    size_for,
    with_suffix,
};
use super::preallocate::open_or_preallocate;
use crate::storage::error::{
    StorageError,
    StorageResult,
};
use crate::storage::files::SqliteFileRepository;
use crate::storage::fs::pwrite_full;
use crate::storage::paths::resolve_target;
use crate::storage::pieces::SqlitePieceRepository;

/// Локальное хранилище piece'ов одной загрузки.
///
/// Создаётся через [`LocalPieceStorage::open`]. Конструктор делает либо
/// «свежий» init (`delete_all` + преаллокация + `init`), либо resume
/// (открытие существующего `.data.brook` без truncate). Решение
/// принимается по парности «inspect-поля в `file_settings` совпадают
/// с переданными» + «`pieces` для `file_id` уже инициализированы»
/// + «`.data.brook` существует».
pub struct LocalPieceStorage {
    inner: Arc<Inner>,
    data_path: PathBuf,
    target_path: PathBuf,
    file_id: FileId,
    pieces_repo: Arc<SqlitePieceRepository>,
    /// Пока не используется (engine персистит state-переходы сам), но
    /// нужен фабрике в той же раскладке: держим про запас, чтобы не
    /// менять сигнатуру `open` под каждый будущий хук.
    #[allow(dead_code)]
    files_repo: Arc<SqliteFileRepository>,
    total_size: u64,
    piece_size: u64,
}

/// Разделяемое состояние хранилища.
///
/// Горячий путь (`write_piece_bytes` / `commit_done`) берёт **read-lock**
/// на `file` — это позволяет N воркерам одновременно вызывать
/// `pwrite_full`/`sync_data` на одном fd (оба — thread-safe syscall'ы,
/// `&File` достаточно). `finalize`/`abort` берут write-lock и
/// дожидаются, пока все in-flight writer'ы отпустят read-lock, после
/// чего забирают `File` из `Option` (`take`) для переименования или
/// удаления. Флаги `finalized`/`aborted` держим отдельными
/// `AtomicBool`: их проверка не требует lock'а и дешевле Mutex'а.
struct Inner {
    /// Handle на `.data.brook`. `None` после `finalize`/`abort`.
    file: RwLock<Option<File>>,
    finalized: AtomicBool,
    aborted: AtomicBool,
}

impl LocalPieceStorage {
    /// Открыть хранилище для `id` (`filename` в `target_dir`) с известными
    /// `total_size` и `piece_size`. Inspect-поля должна была положить
    /// в `file_settings` фабрика **до** этого вызова (stage 6).
    ///
    /// Поведение по состоянию рядом лежащих файлов и БД:
    /// - inspect-поля совпадают, `pieces` инициализированы и `.data.brook`
    ///   существует → **resume**: открываем файл без truncate, pending'и
    ///   читаем из БД.
    /// - иначе → **fresh**: чистим хвосты (`.data.brook`, `pieces` для `id`),
    ///   преаллоцируем и заполняем `pieces` пустой раскладкой.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        target_dir: &Path,
        filename: &str,
        id: FileId,
        total_size: u64,
        piece_size: u64,
        pieces_repo: Arc<SqlitePieceRepository>,
        files_repo: Arc<SqliteFileRepository>,
    ) -> CoreResult<Self> {
        Ok(Self::open_inner(
            target_dir,
            filename,
            id,
            total_size,
            piece_size,
            pieces_repo,
            files_repo,
        )
        .await?)
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_inner(
        target_dir: &Path,
        filename: &str,
        id: FileId,
        total_size: u64,
        piece_size: u64,
        pieces_repo: Arc<SqlitePieceRepository>,
        files_repo: Arc<SqliteFileRepository>,
    ) -> StorageResult<Self> {
        if piece_size == 0 {
            return Err(StorageError::InvalidPieceSize);
        }
        let target_path = resolve_target(target_dir, filename)?;
        let data_path = with_suffix(&target_path, ".data.brook");

        // Решение «resume vs fresh» нуждается в двух async-проверках
        // через репозитории и одной sync-проверке (.data.brook на диске).
        let inspect = files_repo.get_inspect_fields(id).await.map_err(io_to_err)?;
        let stored_count = pieces_repo.count(id).await?;
        let expected_count = piece_count(total_size, piece_size);
        let inspect_ok = matches!(
            &inspect,
            Some(f) if f.total_size == Some(total_size) && f.piece_size == Some(piece_size)
        );
        let data_exists = data_path.exists();
        // `count == expected` — основной сторож нарезки: даже если
        // inspect-поля успели обновиться под новую геометрию, piece-строки
        // всё ещё могут быть старой раскладки (фабрика пишет inspect ДО
        // того, как LocalPieceStorage сам решит resume/fresh). Если числа
        // не совпадают — fresh, сколько бы ни было совпадений по остальным
        // признакам.
        let use_resume =
            inspect_ok && stored_count > 0 && stored_count == expected_count && data_exists;

        if !use_resume {
            // Fresh-ветка: убираем возможные хвосты прошлых попыток.
            // delete_all безопасен и при первом запуске (DELETE по
            // несуществующим строкам — no-op).
            pieces_repo.delete_all(id).await?;
        }

        let data = open_or_preallocate(target_dir, &data_path, total_size, use_resume).await?;

        if !use_resume {
            let count = piece_count(total_size, piece_size);
            pieces_repo.init(id, count).await?;
        }

        Ok(Self {
            inner: Arc::new(Inner {
                file: RwLock::new(Some(data)),
                finalized: AtomicBool::new(false),
                aborted: AtomicBool::new(false),
            }),
            data_path,
            target_path,
            file_id: id,
            pieces_repo,
            files_repo,
            total_size,
            piece_size,
        })
    }
}

/// `files_repo.get_inspect_fields` единственный возвращает `CoreResult`
/// (он делит тип с публичным API фабрики). Оборачиваем обратно в
/// `StorageError::Io` / `StorageError`, не теряя текст.
fn io_to_err(e: brook_core::Error) -> StorageError {
    match e {
        brook_core::Error::Io(io) => StorageError::Io(io),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    }
}

impl TPieceStorage for LocalPieceStorage {
    async fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: Bytes,
    ) -> CoreResult<()> {
        Ok(self
            .write_piece_bytes_inner(piece_index, offset_in_piece, bytes)
            .await?)
    }

    async fn commit_done(&self, piece_index: u32) -> CoreResult<()> {
        Ok(self.commit_done_inner(piece_index).await?)
    }

    async fn pending_pieces(&self) -> CoreResult<Vec<u32>> {
        self.pieces_repo
            .pending_numbers(self.file_id)
            .await
            .map_err(|e| StorageError::from(e).into())
    }

    async fn finalize(&self) -> CoreResult<()> {
        Ok(self.finalize_inner().await?)
    }

    async fn abort(&self) -> CoreResult<()> {
        Ok(self.abort_inner().await?)
    }
}

impl LocalPieceStorage {
    async fn write_piece_bytes_inner(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let count = piece_count(self.total_size, self.piece_size);
        if piece_index >= count {
            return Err(StorageError::PieceIndexOutOfRange {
                index: piece_index,
                count,
            });
        }
        let piece_len = size_for(piece_index, self.total_size, self.piece_size);
        let end = offset_in_piece
            .checked_add(bytes.len() as u64)
            .ok_or(StorageError::OffsetOverflow)?;
        if end > piece_len {
            return Err(StorageError::WritePastPieceEnd {
                index: piece_index,
                end,
                size: piece_len,
            });
        }
        let abs = offset_for(piece_index, self.piece_size) + offset_in_piece;
        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            // Флаги читаем атомиками — без блокировки. Между проверкой
            // и самим pwrite состояние не изменится: finalize/abort
            // берут write-lock на `file`, а мы держим read-lock ниже,
            // так что они дождутся нашего выхода.
            if inner.finalized.load(Ordering::Acquire) {
                return Err(StorageError::AfterFinalize { op: "write" });
            }
            if inner.aborted.load(Ordering::Acquire) {
                return Err(StorageError::AfterAbort { op: "write" });
            }
            let guard = inner.file.read().expect("rwlock poisoned");
            let file = guard
                .as_ref()
                .expect("data handle present while !finalized && !aborted");
            // `pwrite(2)` поточно-безопасен по контракту POSIX: можно
            // звать из N потоков одновременно на одном fd — именно
            // ради этого мы сняли старый Mutex с горячего пути.
            pwrite_full(file, &bytes, abs)?;
            Ok(())
        })
        .await?
    }

    async fn commit_done_inner(&self, piece_index: u32) -> StorageResult<()> {
        // Сначала fsync самих байт — иначе после краша БД будет
        // врать, что piece готов, а данных на диске нет. Инвариант
        // «commit ⇒ persisted» держится именно этим порядком:
        // sync_data() здесь, затем UPDATE в pieces.
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            if inner.finalized.load(Ordering::Acquire) {
                return Err(StorageError::AfterFinalize { op: "commit" });
            }
            if inner.aborted.load(Ordering::Acquire) {
                return Err(StorageError::AfterAbort { op: "commit" });
            }
            let guard = inner.file.read().expect("rwlock poisoned");
            // sync_data на одном fd из нескольких потоков
            // поточно-безопасен (fsync/fdatasync — thread-safe).
            guard.as_ref().expect("data handle present").sync_data()?;
            Ok(())
        })
        .await??;

        self.pieces_repo
            .commit_done(self.file_id, piece_index)
            .await?;
        Ok(())
    }

    async fn finalize_inner(&self) -> StorageResult<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let target_path = self.target_path.clone();

        let renamed = tokio::task::spawn_blocking(move || -> StorageResult<bool> {
            if inner.aborted.load(Ordering::Acquire) {
                return Err(StorageError::AfterAbort { op: "finalize" });
            }
            if inner.finalized.load(Ordering::Acquire) {
                return Ok(false);
            }
            // Write-lock: дожидаемся, пока все in-flight write/commit
            // отпустят read-lock. После этого fd больше никто не держит.
            let mut guard = inner.file.write().expect("rwlock poisoned");
            let file = guard.take().expect("data handle present while !finalized");
            file.sync_all()?;
            drop(file);
            std::fs::rename(&data_path, &target_path)?;
            // Release-запись: write-lock уже гарантирует happens-before
            // относительно будущих read-lock'ов, но флаг — это быстрый
            // путь проверки без lock'а, так что помечаем явно.
            inner.finalized.store(true, Ordering::Release);
            Ok(true)
        })
        .await??;

        if renamed {
            // Piece-строки больше не нужны: файл готов и переименован.
            // Состоянием в `files` управляет engine через
            // `TQueueStore::update_state` — здесь мы трогаем только
            // piece-таблицу.
            self.pieces_repo.delete_all(self.file_id).await?;
        }
        Ok(())
    }

    async fn abort_inner(&self) -> StorageResult<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();

        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            // Тот же приём, что и в finalize: write-lock дожидается
            // in-flight writer'ов, после чего fd уходит в Drop.
            let mut guard = inner.file.write().expect("rwlock poisoned");
            *guard = None;
            let _ = std::fs::remove_file(&data_path);
            inner.aborted.store(true, Ordering::Release);
            Ok(())
        })
        .await??;

        self.pieces_repo.delete_all(self.file_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::PathBuf;

    use brook_core::{
        File,
        FileSpec,
        TQueueStore,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::storage::db::SharedDb;

    /// Тестовая раскладка: 20 байт, piece_size = 8 → 3 piece'а (8/8/4).
    const TOTAL: u64 = 20;
    const PIECE: u64 = 8;

    /// Сахар для тестов: `b"..."` → `Bytes`. `from_static` не копирует.
    fn bs(s: &'static [u8]) -> Bytes {
        Bytes::from_static(s)
    }

    fn read_file(path: &Path) -> Vec<u8> {
        let mut f = std::fs::File::open(path).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        out
    }

    fn sample_spec(target_dir: &Path, filename: &str) -> FileSpec {
        FileSpec {
            url: "https://example.com/x".into(),
            target_dir: target_dir.to_path_buf(),
            filename: Some(filename.into()),
        }
    }

    async fn fixture(
        target_dir: &Path,
        filename: &str,
    ) -> (
        SharedDb,
        Arc<SqliteFileRepository>,
        Arc<SqlitePieceRepository>,
        FileId,
    ) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = Arc::new(SqliteFileRepository::new(db.clone()));
        let pieces = Arc::new(SqlitePieceRepository::new(db.clone()));
        let d = File::new(FileId::new(), sample_spec(target_dir, filename));
        let id = d.id;
        files.insert(&d).await.unwrap();
        files
            .set_inspect_fields(
                id,
                Some(TOTAL),
                Some(PIECE),
                None,
                None,
                None,
                true,
                filename.into(),
            )
            .await
            .unwrap();
        (db, files, pieces, id)
    }

    async fn open_fresh(
        target_dir: &Path,
        filename: &str,
    ) -> (
        SharedDb,
        Arc<SqliteFileRepository>,
        Arc<SqlitePieceRepository>,
        FileId,
        LocalPieceStorage,
    ) {
        let (db, files, pieces, id) = fixture(target_dir, filename).await;
        let storage = LocalPieceStorage::open(
            target_dir,
            filename,
            id,
            TOTAL,
            PIECE,
            Arc::clone(&pieces),
            Arc::clone(&files),
        )
        .await
        .unwrap();
        (db, files, pieces, id, storage)
    }

    #[tokio::test]
    async fn round_trip_write_commit_finalize() {
        let dir = tempdir().unwrap();
        let (_db, _files, pieces, id, s) = open_fresh(dir.path(), "out.bin").await;

        assert_eq!(s.pending_pieces().await.unwrap(), vec![0, 1, 2]);

        s.write_piece_bytes(0, 0, bs(b"AAAA")).await.unwrap();
        s.write_piece_bytes(0, 4, bs(b"BBBB")).await.unwrap();
        s.write_piece_bytes(1, 0, bs(b"CCCCDDDD")).await.unwrap();
        s.write_piece_bytes(2, 0, bs(b"EEEE")).await.unwrap();

        s.commit_done(0).await.unwrap();
        s.commit_done(1).await.unwrap();
        s.commit_done(2).await.unwrap();
        assert!(s.pending_pieces().await.unwrap().is_empty());

        s.finalize().await.unwrap();

        let target = dir.path().join("out.bin");
        assert!(target.exists());
        assert!(!dir.path().join("out.bin.data.brook").exists());
        assert!(!pieces.is_initialized(id).await.unwrap());

        assert_eq!(read_file(&target), b"AAAABBBBCCCCDDDDEEEE".to_vec());
    }

    #[tokio::test]
    async fn resume_after_crash_continues_where_left() {
        let dir = tempdir().unwrap();

        let (db, files, pieces, id) = fixture(dir.path(), "r.bin").await;
        {
            let s = LocalPieceStorage::open(
                dir.path(),
                "r.bin",
                id,
                TOTAL,
                PIECE,
                Arc::clone(&pieces),
                Arc::clone(&files),
            )
            .await
            .unwrap();
            s.write_piece_bytes(0, 0, bs(b"AAAABBBB")).await.unwrap();
            s.write_piece_bytes(1, 0, bs(b"CCCCDDDD")).await.unwrap();
            s.commit_done(0).await.unwrap();
            s.commit_done(1).await.unwrap();
        }
        let _ = db;
        let s = LocalPieceStorage::open(
            dir.path(),
            "r.bin",
            id,
            TOTAL,
            PIECE,
            Arc::clone(&pieces),
            Arc::clone(&files),
        )
        .await
        .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![2]);

        s.write_piece_bytes(2, 0, bs(b"EEEE")).await.unwrap();
        s.commit_done(2).await.unwrap();
        s.finalize().await.unwrap();

        assert_eq!(
            read_file(&dir.path().join("r.bin")),
            b"AAAABBBBCCCCDDDDEEEE".to_vec()
        );
    }

    #[tokio::test]
    async fn abort_removes_data_file_and_blocks_operations() {
        let dir = tempdir().unwrap();
        let (_db, _files, pieces, id, s) = open_fresh(dir.path(), "a.bin").await;

        s.write_piece_bytes(0, 0, bs(b"AAAABBBB")).await.unwrap();
        s.commit_done(0).await.unwrap();
        s.abort().await.unwrap();

        assert!(!dir.path().join("a.bin.data.brook").exists());
        assert!(!dir.path().join("a.bin").exists());
        assert!(!pieces.is_initialized(id).await.unwrap());

        assert!(s.write_piece_bytes(1, 0, bs(b"CC")).await.is_err());
        assert!(s.commit_done(1).await.is_err());
    }

    #[tokio::test]
    async fn mismatched_inspect_restarts_from_scratch() {
        let dir = tempdir().unwrap();
        let (_db, files, pieces, id) = fixture(dir.path(), "m.bin").await;

        {
            let s = LocalPieceStorage::open(
                dir.path(),
                "m.bin",
                id,
                TOTAL,
                PIECE,
                Arc::clone(&pieces),
                Arc::clone(&files),
            )
            .await
            .unwrap();
            s.write_piece_bytes(0, 0, bs(b"XXXXYYYY")).await.unwrap();
            s.commit_done(0).await.unwrap();
        }

        files
            .set_inspect_fields(
                id,
                Some(32),
                Some(PIECE),
                None,
                None,
                None,
                true,
                "m.bin".into(),
            )
            .await
            .unwrap();
        let s = LocalPieceStorage::open(
            dir.path(),
            "m.bin",
            id,
            32,
            PIECE,
            Arc::clone(&pieces),
            Arc::clone(&files),
        )
        .await
        .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn write_past_piece_end_is_error() {
        let dir = tempdir().unwrap();
        let (_db, _files, _pieces, _id, s) = open_fresh(dir.path(), "b.bin").await;

        assert!(s.write_piece_bytes(2, 0, bs(b"EEEEE")).await.is_err());
        assert!(s.write_piece_bytes(99, 0, bs(b"Z")).await.is_err());
    }

    #[tokio::test]
    async fn commit_persists_across_reopen_without_finalize() {
        let dir = tempdir().unwrap();
        let (_db, files, pieces, id) = fixture(dir.path(), "p.bin").await;
        {
            let s = LocalPieceStorage::open(
                dir.path(),
                "p.bin",
                id,
                TOTAL,
                PIECE,
                Arc::clone(&pieces),
                Arc::clone(&files),
            )
            .await
            .unwrap();
            s.write_piece_bytes(0, 0, bs(b"AAAABBBB")).await.unwrap();
            s.commit_done(0).await.unwrap();
        }
        let s = LocalPieceStorage::open(
            dir.path(),
            "p.bin",
            id,
            TOTAL,
            PIECE,
            Arc::clone(&pieces),
            Arc::clone(&files),
        )
        .await
        .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn write_after_finalize_is_rejected() {
        let dir = tempdir().unwrap();
        let (_db, _files, _pieces, _id, s) = open_fresh(dir.path(), "f.bin").await;

        s.write_piece_bytes(0, 0, bs(b"AAAABBBB")).await.unwrap();
        s.write_piece_bytes(1, 0, bs(b"CCCCDDDD")).await.unwrap();
        s.write_piece_bytes(2, 0, bs(b"EEEE")).await.unwrap();
        s.commit_done(0).await.unwrap();
        s.commit_done(1).await.unwrap();
        s.commit_done(2).await.unwrap();
        s.finalize().await.unwrap();

        assert!(s.write_piece_bytes(0, 0, bs(b"ZZZZ")).await.is_err());
        assert!(s.commit_done(0).await.is_err());
    }

    #[allow(dead_code)]
    fn _unused(_: PathBuf) {}
}
