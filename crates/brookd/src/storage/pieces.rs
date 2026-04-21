//! Репозиторий piece'ов всех загрузок в `./brook.db` поверх общего
//! [`SharedDb`].
//!
//! После миграции на единую БД (см. [docs/todo.md] §4) sidecar
//! `<name>.index.brook` исчезает: все piece-строки лежат в одной таблице
//! `pieces`, scoping — по `file_id` (UUID загрузки).
//!
//! ## Раскладка пропала из БД
//!
//! Раньше [`super::index::PieceIndexRepository`] хранил `offset`/`size`
//! каждого piece'а — это была единственная карта геометрии. Теперь
//! геометрия восстанавливается арифметикой из `total_size`/`piece_size`
//! в `file_settings` (stage 5: хелперы `offset_for`/`size_for` в
//! `LocalPieceStorage`), а в БД остаётся только то, что нельзя посчитать —
//! `status` и `finished_at`.
//!
//! ## Статусы
//!
//! В БД живут только `pending` и `done`. Runtime-состояние `in_progress`
//! engine'а на диск не персистится: после рестарта любой piece не в `done`
//! трактуется как `pending` (см. [docs/todo.md] §21).
//!
//! ## Конкурентность
//!
//! Все публичные async-методы уходят в [`SharedDb::with_conn`] →
//! `spawn_blocking`: `rusqlite` синхронный, держать его на async-потоке
//! нельзя.
//!
//! [docs/todo.md]: ../../../../docs/todo.md
//! [`SharedDb`]: super::db::SharedDb
//! [`SharedDb::with_conn`]: super::db::SharedDb::with_conn

use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use brook_core::DownloadId;
use rusqlite::{
    Connection,
    params,
};
use thiserror::Error;
use uuid::Uuid;

use super::db::{
    DbError,
    SharedDb,
};

#[derive(Debug, Error)]
pub enum PiecesError {
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// `init` вызван на уже инициализированном file_id.
    #[error("pieces already initialized for file")]
    AlreadyInitialized,
}

pub type PiecesResult<T> = std::result::Result<T, PiecesError>;

/// SQLite-репозиторий piece'ов, общий на все загрузки.
///
/// Клонирование дешёвое: всё состояние под `Arc` внутри [`SharedDb`].
#[derive(Clone)]
pub struct SqlitePieceRepository {
    db: SharedDb,
}

impl SqlitePieceRepository {
    pub fn new(db: SharedDb) -> Self {
        Self { db }
    }

    /// Заполнить таблицу для свежей загрузки: `count` строк со статусом
    /// `pending` (`number = 0..count`) одной транзакцией.
    ///
    /// Если для `file_id` уже есть piece-строки — возвращает
    /// `AlreadyInitialized` (перезапись считаем ошибкой программы;
    /// штатный «начать заново» — `delete_all` + `init`).
    pub async fn init(&self, file_id: DownloadId, count: u32) -> PiecesResult<()> {
        run(&self.db, move |c| init_impl(c, file_id, count)).await
    }

    /// Уже ли инициализирован файл (есть хоть одна piece-строка).
    pub async fn is_initialized(&self, file_id: DownloadId) -> PiecesResult<bool> {
        run(&self.db, move |c| is_initialized_impl(c, file_id)).await
    }

    /// Общее количество piece-строк этого файла (pending + done).
    ///
    /// Нужно `LocalPieceStorage::open`, чтобы проверить: совпадает ли
    /// нарезка в БД с геометрией, которую передала фабрика
    /// (`total_size`/`piece_size`). Если нет — решаем «fresh» и
    /// пересобираем таблицу, даже когда inspect-поля формально
    /// совпадают (их мог переписать новый inspect-отчёт).
    pub async fn count(&self, file_id: DownloadId) -> PiecesResult<u32> {
        run(&self.db, move |c| count_impl(c, file_id)).await
    }

    /// Номера piece'ов в статусе `pending`, упорядочены по возрастанию.
    pub async fn pending_numbers(&self, file_id: DownloadId) -> PiecesResult<Vec<u32>> {
        run(&self.db, move |c| pending_numbers_impl(c, file_id)).await
    }

    /// Пометить набор piece'ов как `done` одной транзакцией.
    /// Неизвестные `number` молча игнорируются (rowid'ы из старого
    /// батча отменённого воркера — нормальный кейс).
    pub async fn commit_done_batch(
        &self,
        file_id: DownloadId,
        numbers: Vec<u32>,
    ) -> PiecesResult<()> {
        run(&self.db, move |c| {
            commit_done_batch_impl(c, file_id, &numbers)
        })
        .await
    }

    /// Удалить все piece-строки этого файла. Используется при `abort`,
    /// `finalize` (после ренейма таргета — строки больше не нужны) и при
    /// fresh-restart (`open` обнаружил несовместимость inspect-полей).
    pub async fn delete_all(&self, file_id: DownloadId) -> PiecesResult<()> {
        run(&self.db, move |c| delete_all_impl(c, file_id)).await
    }
}

async fn run<F, T>(db: &SharedDb, f: F) -> PiecesResult<T>
where
    F: FnOnce(&mut Connection) -> PiecesResult<T> + Send + 'static,
    T: Send + 'static,
{
    let inner: PiecesResult<T> = db.with_conn(|c| Ok(f(c))).await.map_err(|e| match e {
        DbError::Db(e) => PiecesError::Db(e),
        DbError::Join(e) => PiecesError::Join(e),
    })?;
    inner
}

// ─── Sync implementations ──────────────────────────────────────────────

fn init_impl(conn: &mut Connection, file_id: DownloadId, count: u32) -> PiecesResult<()> {
    if is_initialized_impl(conn, file_id)? {
        return Err(PiecesError::AlreadyInitialized);
    }
    let now = unix_secs(SystemTime::now());
    let file_id_str = file_id.to_string();
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO pieces (id, file_id, number, status, created_at)
             VALUES (?, ?, ?, 'pending', ?)",
        )?;
        for n in 0..count {
            stmt.execute(params![
                Uuid::new_v4().to_string(),
                file_id_str,
                n as i64,
                now,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn is_initialized_impl(conn: &mut Connection, file_id: DownloadId) -> PiecesResult<bool> {
    Ok(count_impl(conn, file_id)? > 0)
}

fn count_impl(conn: &mut Connection, file_id: DownloadId) -> PiecesResult<u32> {
    let cnt: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pieces WHERE file_id = ?",
        params![file_id.to_string()],
        |r| r.get(0),
    )?;
    Ok(cnt as u32)
}

fn pending_numbers_impl(conn: &mut Connection, file_id: DownloadId) -> PiecesResult<Vec<u32>> {
    let mut stmt = conn.prepare(
        "SELECT number FROM pieces
         WHERE file_id = ? AND status = 'pending'
         ORDER BY number",
    )?;
    let rows = stmt.query_map(params![file_id.to_string()], |row| {
        row.get::<_, i64>(0).map(|v| v as u32)
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

fn commit_done_batch_impl(
    conn: &mut Connection,
    file_id: DownloadId,
    numbers: &[u32],
) -> PiecesResult<()> {
    if numbers.is_empty() {
        return Ok(());
    }
    let now = unix_secs(SystemTime::now());
    let file_id_str = file_id.to_string();
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "UPDATE pieces SET status = 'done', finished_at = ?
             WHERE file_id = ? AND number = ?",
        )?;
        for &n in numbers {
            stmt.execute(params![now, file_id_str, n as i64])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn delete_all_impl(conn: &mut Connection, file_id: DownloadId) -> PiecesResult<()> {
    conn.execute(
        "DELETE FROM pieces WHERE file_id = ?",
        params![file_id.to_string()],
    )?;
    Ok(())
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use brook_core::{
        Download,
        DownloadId,
        DownloadSpec,
    };

    use super::*;
    use crate::storage::files::SqliteFileRepository;

    fn sample_download() -> Download {
        let spec = DownloadSpec {
            url: "https://example.com/file.bin".into(),
            target_dir: PathBuf::from("/tmp/brook"),
            filename: Some("file.bin".into()),
            workers: 4,
            piece_target_count: Some(256),
            piece_size_min: Some(8 * 1024 * 1024),
            piece_size_max: Some(64 * 1024 * 1024),
            on_file_exists_override: Default::default(),
        };
        Download::new(DownloadId::new(), spec)
    }

    /// Создаёт `SharedDb`, репозиторий piece'ов и регистрирует одну
    /// «фейковую» загрузку — нужно, потому что `pieces.file_id` имеет
    /// FK на `files.id`.
    async fn fresh() -> (SharedDb, SqlitePieceRepository, DownloadId) {
        use brook_core::TQueueStore;
        let db = SharedDb::open_in_memory().unwrap();
        let files = SqliteFileRepository::new(db.clone());
        let d = sample_download();
        let id = d.id;
        files.insert(&d).await.unwrap();
        (db.clone(), SqlitePieceRepository::new(db), id)
    }

    #[tokio::test]
    async fn init_then_pending_returns_all_numbers() {
        let (_db, repo, id) = fresh().await;
        assert!(!repo.is_initialized(id).await.unwrap());
        repo.init(id, 5).await.unwrap();
        assert!(repo.is_initialized(id).await.unwrap());
        assert_eq!(repo.pending_numbers(id).await.unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn init_zero_count_is_noop_but_not_initialized() {
        let (_db, repo, id) = fresh().await;
        repo.init(id, 0).await.unwrap();
        assert!(!repo.is_initialized(id).await.unwrap());
        assert!(repo.pending_numbers(id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn init_twice_errors() {
        let (_db, repo, id) = fresh().await;
        repo.init(id, 3).await.unwrap();
        let err = repo.init(id, 3).await.unwrap_err();
        assert!(matches!(err, PiecesError::AlreadyInitialized));
    }

    #[tokio::test]
    async fn commit_marks_done_and_drops_from_pending() {
        let (db, repo, id) = fresh().await;
        repo.init(id, 4).await.unwrap();
        repo.commit_done_batch(id, vec![0, 2]).await.unwrap();
        assert_eq!(repo.pending_numbers(id).await.unwrap(), vec![1, 3]);

        // finished_at заполнился у закоммиченных, у pending — NULL.
        let id_str = id.to_string();
        let (done_with_ts, pending_with_ts): (i64, i64) = db
            .with_conn(move |c| {
                let done_with_ts: i64 = c.query_row(
                    "SELECT COUNT(*) FROM pieces
                     WHERE file_id = ? AND status = 'done' AND finished_at IS NOT NULL",
                    params![id_str.clone()],
                    |r| r.get(0),
                )?;
                let pending_with_ts: i64 = c.query_row(
                    "SELECT COUNT(*) FROM pieces
                     WHERE file_id = ? AND status = 'pending' AND finished_at IS NOT NULL",
                    params![id_str],
                    |r| r.get(0),
                )?;
                Ok((done_with_ts, pending_with_ts))
            })
            .await
            .unwrap();
        assert_eq!(done_with_ts, 2);
        assert_eq!(pending_with_ts, 0);
    }

    #[tokio::test]
    async fn commit_unknown_number_is_noop() {
        let (_db, repo, id) = fresh().await;
        repo.init(id, 3).await.unwrap();
        repo.commit_done_batch(id, vec![42]).await.unwrap();
        assert_eq!(repo.pending_numbers(id).await.unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn commit_empty_batch_is_noop() {
        let (_db, repo, id) = fresh().await;
        repo.init(id, 3).await.unwrap();
        repo.commit_done_batch(id, vec![]).await.unwrap();
        assert_eq!(repo.pending_numbers(id).await.unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn delete_all_clears_only_this_file() {
        // Регистрируем второй файл, чтобы убедиться: delete_all не задевает
        // соседей.
        use brook_core::TQueueStore;
        let (db, repo, a) = fresh().await;
        let files = SqliteFileRepository::new(db.clone());
        let d = sample_download();
        let b = d.id;
        files.insert(&d).await.unwrap();

        repo.init(a, 3).await.unwrap();
        repo.init(b, 2).await.unwrap();

        repo.delete_all(a).await.unwrap();
        assert!(!repo.is_initialized(a).await.unwrap());
        assert_eq!(repo.pending_numbers(b).await.unwrap(), vec![0, 1]);
    }

    #[tokio::test]
    async fn resume_after_reopen_skips_done_pieces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("brook.db");
        let d = sample_download();
        let id = d.id;
        {
            use brook_core::TQueueStore;
            let db = SharedDb::open(&path).unwrap();
            let files = SqliteFileRepository::new(db.clone());
            files.insert(&d).await.unwrap();
            let pieces = SqlitePieceRepository::new(db);
            pieces.init(id, 4).await.unwrap();
            pieces.commit_done_batch(id, vec![0, 2]).await.unwrap();
        }
        let db = SharedDb::open(&path).unwrap();
        let pieces = SqlitePieceRepository::new(db);
        assert!(pieces.is_initialized(id).await.unwrap());
        assert_eq!(pieces.pending_numbers(id).await.unwrap(), vec![1, 3]);
    }

    #[tokio::test]
    async fn two_downloads_do_not_interfere() {
        // Два файла с теми же piece-номерами — pending одного не должен
        // видеть piece'ы другого.
        use brook_core::TQueueStore;
        let (db, repo, a) = fresh().await;
        let files = SqliteFileRepository::new(db.clone());
        let d = sample_download();
        let b = d.id;
        files.insert(&d).await.unwrap();

        repo.init(a, 3).await.unwrap();
        repo.init(b, 3).await.unwrap();
        repo.commit_done_batch(a, vec![0, 1, 2]).await.unwrap();

        assert!(repo.pending_numbers(a).await.unwrap().is_empty());
        assert_eq!(repo.pending_numbers(b).await.unwrap(), vec![0, 1, 2]);
    }
}
