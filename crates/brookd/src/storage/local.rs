//! [`LocalPieceStorage`] — реальная реализация [`TPieceStorage`] поверх
//! файловой системы и общего [`SharedDb`].
//!
//! После миграции на единую `brook.db` (см. [docs/todo.md] §5) sidecar
//! `<name>.index.brook` исчез: всё persisted-состояние piece'ов лежит
//! в общей таблице `pieces`, scoping — по `file_id`. Рядом с таргетом
//! остаётся только `<name>.data.brook` (после `finalize` → `<name>`).
//!
//! ## Файлы на диске
//!
//! Для загрузки с `filename = "foo.iso"` в `target_dir = "/dl"` адаптер
//! держит ровно два пути:
//! - `"/dl/foo.iso.data.brook"` — преаллоцированный на `total_size`
//!   контейнер, куда воркеры пишут байты по offset'ам (`pwrite`).
//! - `"/dl/foo.iso"` — целевой файл, появляется только после `finalize`
//!   (атомарный `rename` из `.data.brook`).
//!
//! ## Геометрия piece'ов — арифметикой, без `PieceLayout`
//!
//! `offset_for(n) = n * piece_size`, `size_for(n) = min(piece_size,
//! total_size - offset_for(n))`. Карта в БД больше не нужна — её
//! роль играют два числа (`total_size`, `piece_size`) из
//! `file_settings`.
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
//! [docs/todo.md]: ../../../../docs/todo.md
//! [`SharedDb`]: super::db::SharedDb
//! [`SharedDb::with_conn`]: super::db::SharedDb::with_conn

use std::ffi::OsString;
use std::fs::{
    File,
    OpenOptions,
};
use std::io;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    Arc,
    Mutex,
};

use brook_core::{
    DownloadId,
    Error,
    Result,
    TPieceStorage,
    TStreamStorage,
};

use super::files::SqliteFileRepository;
use super::fs::{
    available_space,
    preallocate,
    pwrite_full,
};
use super::paths::resolve_target;
use super::pieces::{
    PiecesError,
    SqlitePieceRepository,
};

/// Локальное хранилище piece'ов одной загрузки.
///
/// Создаётся через [`LocalPieceStorage::open`]. Конструктор делает либо
/// «свежий» init (`delete_all` + преаллокация + `init`), либо resume
/// (открытие существующего `.data.brook` без truncate). Решение
/// принимается по парности «inspect-поля в `file_settings` совпадают
/// с переданными» + «`pieces` для `file_id` уже инициализированы»
/// + «`.data.brook` существует».
pub struct LocalPieceStorage {
    inner: Arc<Mutex<Inner>>,
    data_path: PathBuf,
    target_path: PathBuf,
    file_id: DownloadId,
    pieces_repo: Arc<SqlitePieceRepository>,
    /// Пока не используется (engine персистит state-переходы сам), но
    /// нужен фабрике в той же раскладке: держим про запас, чтобы не
    /// менять сигнатуру `open` под каждый будущий хук.
    #[allow(dead_code)]
    files_repo: Arc<SqliteFileRepository>,
    total_size: u64,
    piece_size: u64,
}

struct Inner {
    /// Handle на `.data.brook`. `None` после `finalize`/`abort`.
    data: Option<File>,
    finalized: bool,
    aborted: bool,
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
        id: DownloadId,
        total_size: u64,
        piece_size: u64,
        pieces_repo: Arc<SqlitePieceRepository>,
        files_repo: Arc<SqliteFileRepository>,
    ) -> Result<Self> {
        if piece_size == 0 {
            return Err(Error::Other("piece_size must be > 0".into()));
        }
        let target_path = resolve_target(target_dir, filename)
            .map_err(|e| Error::Other(format!("invalid filename: {e}")))?;
        let data_path = with_suffix(&target_path, ".data.brook");
        let target_dir_owned = target_dir.to_path_buf();

        // Решение «resume vs fresh» нуждается в двух async-проверках
        // через репозитории и одной sync-проверке (.data.brook на диске).
        // sync-проверку делаем здесь же, под join'ом spawn_blocking
        // ничего долгоиграющего нет — просто `Path::exists`.
        let inspect = files_repo
            .get_inspect_fields(id)
            .await
            .map_err(|e| Error::Other(format!("inspect fields: {e}")))?;
        let stored_count = pieces_repo.count(id).await.map_err(piece_err)?;
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
            // Fresh-ветка: убираем возможные хвосты прошлых попыток,
            // чтобы init и preallocate стартовали с пустого места.
            // delete_all безопасен и при первом запуске (DELETE по
            // несуществующим строкам — no-op).
            pieces_repo.delete_all(id).await.map_err(piece_err)?;
        }

        // Файловая часть — целиком в spawn_blocking: statvfs, open,
        // preallocate, fsync — все блокирующие.
        let data_path_for_blocking = data_path.clone();
        let data = tokio::task::spawn_blocking(move || -> Result<File> {
            let free = available_space(&target_dir_owned)?;
            if free < total_size {
                return Err(Error::Io(io::Error::other(format!(
                    "not enough free space: have {free}, need {total_size}"
                ))));
            }
            if use_resume {
                Ok(OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&data_path_for_blocking)?)
            } else {
                if data_path_for_blocking.exists() {
                    std::fs::remove_file(&data_path_for_blocking)?;
                }
                let f = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(&data_path_for_blocking)?;
                preallocate(&f, total_size)?;
                Ok(f)
            }
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))??;

        if !use_resume {
            let count = piece_count(total_size, piece_size);
            pieces_repo.init(id, count).await.map_err(piece_err)?;
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                data: Some(data),
                finalized: false,
                aborted: false,
            })),
            data_path,
            target_path,
            file_id: id,
            pieces_repo,
            files_repo,
            total_size,
            piece_size,
        })
    }

    /// Абсолютный offset piece'а `n` в `.data.brook`.
    fn offset_for(&self, n: u32) -> u64 {
        n as u64 * self.piece_size
    }

    /// Размер piece'а `n` в байтах. Последний piece может быть короче.
    fn size_for(&self, n: u32) -> u64 {
        let off = self.offset_for(n);
        // Гарантировано off < total_size по проверкам в write_piece_bytes;
        // saturating на всякий случай — лучше нулевой piece, чем underflow.
        self.total_size.saturating_sub(off).min(self.piece_size)
    }
}

/// Стриминговое хранилище: один append-файл, без преаллокации, без
/// piece-строк. Используется, когда сервер не сообщил `Content-Length`
/// (§2 плана: streaming / unknown-size). Resume для такого хранилища
/// не поддерживается — при повторном открытии `.data.brook` truncate'ится.
pub struct LocalStreamStorage {
    inner: Arc<Mutex<StreamInner>>,
    data_path: PathBuf,
    target_path: PathBuf,
}

struct StreamInner {
    data: Option<File>,
    finalized: bool,
    aborted: bool,
}

impl LocalStreamStorage {
    /// Открыть append-хранилище. Любые старые байты в `.data.brook`
    /// отбрасываются (streaming не умеет resume — при перезапуске
    /// начинаем с нуля; это не regression, обычный HTTP-поток без Range
    /// всё равно не переиспользуем).
    pub async fn open_streaming(target_dir: &Path, filename: &str) -> Result<Self> {
        let target_path = resolve_target(target_dir, filename)
            .map_err(|e| Error::Other(format!("invalid filename: {e}")))?;
        let data_path = with_suffix(&target_path, ".data.brook");
        let data_path_for_blocking = data_path.clone();
        let data = tokio::task::spawn_blocking(move || -> Result<File> {
            // truncate=true — streaming-mode не делает resume.
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&data_path_for_blocking)?;
            Ok(f)
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))??;
        Ok(Self {
            inner: Arc::new(Mutex::new(StreamInner {
                data: Some(data),
                finalized: false,
                aborted: false,
            })),
            data_path,
            target_path,
        })
    }
}

impl TStreamStorage for LocalStreamStorage {
    async fn append_chunk(&self, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        let bytes = bytes.to_vec();
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.finalized {
                return Err(Error::Other("append after finalize".into()));
            }
            if guard.aborted {
                return Err(Error::Other("append after abort".into()));
            }
            let file = guard
                .data
                .as_mut()
                .expect("data handle present while !finalized && !aborted");
            file.write_all(&bytes)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn finalize(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let target_path = self.target_path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.aborted {
                return Err(Error::Other("finalize after abort".into()));
            }
            if guard.finalized {
                return Ok(());
            }
            let file = guard
                .data
                .take()
                .expect("data handle present while !finalized");
            file.sync_all()?;
            drop(file);
            std::fs::rename(&data_path, &target_path)?;
            guard.finalized = true;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn abort(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.data = None;
            let _ = std::fs::remove_file(&data_path);
            guard.aborted = true;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }
}

fn piece_count(total_size: u64, piece_size: u64) -> u32 {
    if total_size == 0 {
        return 0;
    }
    total_size.div_ceil(piece_size) as u32
}

fn piece_err(e: PiecesError) -> Error {
    Error::Other(format!("pieces: {e}"))
}

/// Добавить суффикс к пути без потери расширения.
fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s: OsString = p.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

impl TPieceStorage for LocalPieceStorage {
    async fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let count = piece_count(self.total_size, self.piece_size);
        if piece_index >= count {
            return Err(Error::Other(format!(
                "piece_index {piece_index} out of range (count {count})"
            )));
        }
        let piece_size = self.size_for(piece_index);
        let end = offset_in_piece
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::Other("offset overflow".into()))?;
        if end > piece_size {
            return Err(Error::Other(format!(
                "write past piece end: piece {piece_index}, end {end} vs size {piece_size}"
            )));
        }
        let abs = self.offset_for(piece_index) + offset_in_piece;
        let bytes = bytes.to_vec();
        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let guard = inner.lock().expect("mutex poisoned");
            if guard.finalized {
                return Err(Error::Other("write after finalize".into()));
            }
            if guard.aborted {
                return Err(Error::Other("write after abort".into()));
            }
            let file = guard
                .data
                .as_ref()
                .expect("data handle present while !finalized && !aborted");
            pwrite_full(file, &bytes, abs)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn commit_done(&self, piece_index: u32) -> Result<()> {
        // Сначала fsync самих байт — иначе после краша БД будет
        // врать, что piece готов, а данных на диске нет. Инвариант
        // «commit ⇒ persisted» держится именно этим порядком:
        // sync_data() здесь, затем UPDATE в pieces.
        let inner = Arc::clone(&self.inner);
        let must_check = tokio::task::spawn_blocking(move || -> Result<()> {
            let guard = inner.lock().expect("mutex poisoned");
            if guard.finalized || guard.aborted {
                return Err(Error::Other("commit after finalize/abort".into()));
            }
            guard
                .data
                .as_ref()
                .expect("data handle present")
                .sync_data()?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?;
        must_check?;

        self.pieces_repo
            .commit_done(self.file_id, piece_index)
            .await
            .map_err(piece_err)
    }

    async fn pending_pieces(&self) -> Result<Vec<u32>> {
        self.pieces_repo
            .pending_numbers(self.file_id)
            .await
            .map_err(piece_err)
    }

    async fn finalize(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let target_path = self.target_path.clone();

        let renamed = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.aborted {
                return Err(Error::Other("finalize after abort".into()));
            }
            if guard.finalized {
                return Ok(false);
            }
            let file = guard
                .data
                .take()
                .expect("data handle present while !finalized");
            file.sync_all()?;
            drop(file);
            std::fs::rename(&data_path, &target_path)?;
            guard.finalized = true;
            Ok(true)
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))??;

        if renamed {
            // Piece-строки больше не нужны: файл готов и переименован.
            // Состоянием в `files` управляет engine через
            // `TQueueStore::update_state` — здесь мы трогаем только
            // piece-таблицу.
            self.pieces_repo
                .delete_all(self.file_id)
                .await
                .map_err(piece_err)?;
        }
        Ok(())
    }

    async fn abort(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.data = None;
            let _ = std::fs::remove_file(&data_path);
            guard.aborted = true;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))??;

        self.pieces_repo
            .delete_all(self.file_id)
            .await
            .map_err(piece_err)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::PathBuf;

    use brook_core::{
        Download,
        DownloadSpec,
        TQueueStore,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::storage::db::SharedDb;

    /// Тестовая раскладка: 20 байт, piece_size = 8 → 3 piece'а (8/8/4).
    const TOTAL: u64 = 20;
    const PIECE: u64 = 8;

    fn read_file(path: &Path) -> Vec<u8> {
        let mut f = File::open(path).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        out
    }

    fn sample_spec(target_dir: &Path, filename: &str) -> DownloadSpec {
        DownloadSpec {
            url: "https://example.com/x".into(),
            target_dir: target_dir.to_path_buf(),
            filename: Some(filename.into()),
            workers: 1,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        }
    }

    /// Минимальная сборка для теста: SharedDb + регистрируем `Download`,
    /// раскладываем inspect-поля (это сделала бы фабрика в stage 6).
    async fn fixture(
        target_dir: &Path,
        filename: &str,
    ) -> (
        SharedDb,
        Arc<SqliteFileRepository>,
        Arc<SqlitePieceRepository>,
        DownloadId,
    ) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = Arc::new(SqliteFileRepository::new(db.clone()));
        let pieces = Arc::new(SqlitePieceRepository::new(db.clone()));
        let d = Download::new(DownloadId::new(), sample_spec(target_dir, filename));
        let id = d.id;
        files.insert(&d).await.unwrap();
        files
            .set_inspect_fields(id, Some(TOTAL), Some(PIECE), None, None, None)
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
        DownloadId,
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

        s.write_piece_bytes(0, 0, b"AAAA").await.unwrap();
        s.write_piece_bytes(0, 4, b"BBBB").await.unwrap();
        s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();

        s.commit_done(0).await.unwrap();
        s.commit_done(1).await.unwrap();
        s.commit_done(2).await.unwrap();
        assert!(s.pending_pieces().await.unwrap().is_empty());

        s.finalize().await.unwrap();

        let target = dir.path().join("out.bin");
        assert!(target.exists());
        assert!(!dir.path().join("out.bin.data.brook").exists());
        // Piece-строки удалены после finalize.
        assert!(!pieces.is_initialized(id).await.unwrap());

        assert_eq!(read_file(&target), b"AAAABBBBCCCCDDDDEEEE".to_vec());
    }

    #[tokio::test]
    async fn resume_after_crash_continues_where_left() {
        let dir = tempdir().unwrap();

        // Стейдж 1: пишем 2 piece'а, коммитим, дропаем storage без finalize.
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
            s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
            s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
            s.commit_done(0).await.unwrap();
            s.commit_done(1).await.unwrap();
        }
        // Имитируем рестарт демона: сборка repo заново, но БД та же
        // и `.data.brook` остался.
        let _ = db; // удерживаем shared_db живым — это и есть «тот же файл»
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

        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();
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

        s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
        s.commit_done(0).await.unwrap();
        s.abort().await.unwrap();

        assert!(!dir.path().join("a.bin.data.brook").exists());
        assert!(!dir.path().join("a.bin").exists());
        assert!(!pieces.is_initialized(id).await.unwrap());

        assert!(s.write_piece_bytes(1, 0, b"CC").await.is_err());
        assert!(s.commit_done(1).await.is_err());
    }

    #[tokio::test]
    async fn mismatched_inspect_restarts_from_scratch() {
        let dir = tempdir().unwrap();
        let (_db, files, pieces, id) = fixture(dir.path(), "m.bin").await;

        // Стейдж 1: пишем piece, коммитим.
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
            s.write_piece_bytes(0, 0, b"XXXXYYYY").await.unwrap();
            s.commit_done(0).await.unwrap();
        }

        // Меняем inspect-поля (как будто фабрика перезаписала после
        // нового inspect: другой total_size). Открытие должно увидеть
        // несоответствие и пересобрать всё.
        files
            .set_inspect_fields(id, Some(32), Some(PIECE), None, None, None)
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
        // 32 / 8 = 4 piece'а, все pending.
        assert_eq!(s.pending_pieces().await.unwrap(), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn write_past_piece_end_is_error() {
        let dir = tempdir().unwrap();
        let (_db, _files, _pieces, _id, s) = open_fresh(dir.path(), "b.bin").await;

        // Piece 2 размером 4 байта — запись 5 байт вылезает.
        assert!(s.write_piece_bytes(2, 0, b"EEEEE").await.is_err());
        // Несуществующий piece (count = 3).
        assert!(s.write_piece_bytes(99, 0, b"Z").await.is_err());
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
            s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
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

        s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
        s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();
        s.commit_done(0).await.unwrap();
        s.commit_done(1).await.unwrap();
        s.commit_done(2).await.unwrap();
        s.finalize().await.unwrap();

        assert!(s.write_piece_bytes(0, 0, b"ZZZZ").await.is_err());
        assert!(s.commit_done(0).await.is_err());
    }

    // Use PathBuf to silence unused-import lints across platforms.
    #[allow(dead_code)]
    fn _unused(_: PathBuf) {}
}
