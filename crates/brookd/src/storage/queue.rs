//! Глобальная очередь загрузок на SQLite (`./brook.db`).
//!
//! Реализация [`TQueueStore`]: отдаёт доменные методы, SQL и
//! `rusqlite::Connection` не покидают модуль (см. CLAUDE.md — репозиторная
//! граница).
//!
//! ## Что хранится
//!
//! Только **персистентная** часть `Download`: `spec` + `state` + attempt +
//! error + таймстемпы. Прогресс (`bytes_done`, `pieces_done`, скорость,
//! ETA) НЕ хранится — engine восстанавливает его из `.index.brook` при
//! старте. Дублировать его в двух местах — верный способ получить
//! рассинхронизацию.
//!
//! ## Схема
//!
//! Единственная миграция — `PRAGMA user_version = 1` на пустой БД.
//! Все override-поля из §3.2 (`piece_target_count`, `piece_size_min`,
//! `piece_size_max`) персистятся, иначе после рестарта они потерялись бы,
//! и engine качал бы не то.
//!
//! ## Конкурентность
//!
//! `Arc<Mutex<Connection>>` — единственное соединение защищено обычным
//! `std::sync::Mutex`. Каждый публичный метод оборачивает критсекцию в
//! `tokio::task::spawn_blocking` (контракт `TQueueStore` требует
//! `Future + Send`; блокирующий `rusqlite` нельзя держать на async-потоке).

use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use brook_core::{
    Download,
    DownloadId,
    DownloadSpec,
    DownloadState,
    Error,
    Progress,
    Result as CoreResult,
    TQueueStore,
};
use rusqlite::{
    Connection,
    params,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("download not found")]
    NotFound,
    #[error("download already exists")]
    Duplicate,
    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("corrupt row: {0}")]
    Corrupt(String),
}

impl From<QueueError> for Error {
    fn from(e: QueueError) -> Self {
        match e {
            QueueError::NotFound => Error::NotFound,
            other => Error::Other(other.to_string()),
        }
    }
}

pub type QueueResult<T> = std::result::Result<T, QueueError>;

/// SQLite-репозиторий очереди загрузок.
///
/// Клонирование дешёвое: всё состояние под `Arc<Mutex<_>>`.
#[derive(Clone)]
pub struct SqliteQueueRepository {
    inner: Arc<Mutex<Connection>>,
}

impl SqliteQueueRepository {
    /// Открыть (создав при необходимости) БД. Прогоняет миграцию.
    pub fn open(path: &Path) -> QueueResult<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&conn)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Выполнить замыкание под локом на отдельной blocking-таске.
    ///
    /// Все публичные async-методы сводятся к этому: берём Arc, уходим в
    /// `spawn_blocking`, там лочим Mutex и работаем с соединением. На
    /// async-потоке `rusqlite` держать нельзя — он синхронный.
    async fn with_conn<F, T>(&self, f: F) -> QueueResult<T>
    where
        F: FnOnce(&mut Connection) -> QueueResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let arc = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = arc.lock().expect("queue mutex poisoned");
            f(&mut guard)
        })
        .await?
    }
}

// ─── Trait impl ─────────────────────────────────────────────────────────

impl TQueueStore for SqliteQueueRepository {
    fn load_all(&self) -> impl std::future::Future<Output = CoreResult<Vec<Download>>> + Send {
        let this = self.clone();
        async move {
            this.with_conn(|c| load_all_impl(c))
                .await
                .map_err(Into::into)
        }
    }

    fn insert(
        &self,
        download: &Download,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let this = self.clone();
        let d = download.clone();
        async move {
            this.with_conn(move |c| insert_impl(c, &d))
                .await
                .map_err(Into::into)
        }
    }

    fn update_state(
        &self,
        id: DownloadId,
        state: DownloadState,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let this = self.clone();
        async move {
            this.with_conn(move |c| update_state_impl(c, id, state))
                .await
                .map_err(Into::into)
        }
    }

    fn remove(&self, id: DownloadId) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let this = self.clone();
        async move {
            this.with_conn(move |c| remove_impl(c, id))
                .await
                .map_err(Into::into)
        }
    }
}

// ─── Sync implementations (run под spawn_blocking) ──────────────────────

fn migrate(conn: &Connection) -> QueueResult<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if current < 1 {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                id                 TEXT    PRIMARY KEY,
                url                TEXT    NOT NULL,
                target_dir         TEXT    NOT NULL,
                filename           TEXT,
                workers            INTEGER NOT NULL,
                piece_target_count INTEGER,
                piece_size_min     INTEGER,
                piece_size_max     INTEGER,
                state              TEXT    NOT NULL CHECK (state IN
                    ('queued','running','paused','retrying','done','failed','cancelled')),
                attempt            INTEGER NOT NULL DEFAULT 0,
                error              TEXT,
                created_at         INTEGER NOT NULL,
                updated_at         INTEGER NOT NULL
            );",
        )?;
        conn.pragma_update(None, "user_version", 1i64)?;
    }
    Ok(())
}

fn load_all_impl(conn: &Connection) -> QueueResult<Vec<Download>> {
    let mut stmt = conn.prepare(
        "SELECT id, url, target_dir, filename, workers,
                piece_target_count, piece_size_min, piece_size_max,
                state, attempt, error, created_at, updated_at
         FROM downloads
         ORDER BY created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(RawRow {
            id: row.get(0)?,
            url: row.get(1)?,
            target_dir: row.get(2)?,
            filename: row.get(3)?,
            workers: row.get::<_, i64>(4)? as u32,
            piece_target_count: row.get::<_, Option<i64>>(5)?.map(|v| v as u32),
            piece_size_min: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
            piece_size_max: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
            state: row.get(8)?,
            attempt: row.get::<_, i64>(9)? as u32,
            error: row.get(10)?,
            created_at: row.get::<_, i64>(11)?,
            updated_at: row.get::<_, i64>(12)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(row_to_download(r?)?);
    }
    Ok(out)
}

fn insert_impl(conn: &Connection, d: &Download) -> QueueResult<()> {
    let target_dir = path_to_str(&d.spec.target_dir)?;
    let res = conn.execute(
        "INSERT INTO downloads
           (id, url, target_dir, filename, workers,
            piece_target_count, piece_size_min, piece_size_max,
            state, attempt, error, created_at, updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            d.id.to_string(),
            d.spec.url,
            target_dir,
            d.spec.filename,
            d.spec.workers as i64,
            d.spec.piece_target_count.map(|v| v as i64),
            d.spec.piece_size_min.map(|v| v as i64),
            d.spec.piece_size_max.map(|v| v as i64),
            d.state.as_str(),
            d.attempt as i64,
            d.error,
            unix_secs(d.created_at),
            unix_secs(d.updated_at),
        ],
    );
    match res {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(e, _))
            if e.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Err(QueueError::Duplicate)
        }
        Err(e) => Err(e.into()),
    }
}

fn update_state_impl(conn: &Connection, id: DownloadId, state: DownloadState) -> QueueResult<()> {
    let now = unix_secs(SystemTime::now());
    let n = conn.execute(
        "UPDATE downloads SET state = ?, updated_at = ? WHERE id = ?",
        params![state.as_str(), now, id.to_string()],
    )?;
    if n == 0 {
        return Err(QueueError::NotFound);
    }
    Ok(())
}

fn remove_impl(conn: &Connection, id: DownloadId) -> QueueResult<()> {
    let n = conn.execute(
        "DELETE FROM downloads WHERE id = ?",
        params![id.to_string()],
    )?;
    if n == 0 {
        return Err(QueueError::NotFound);
    }
    Ok(())
}

// ─── Row ↔ Domain ───────────────────────────────────────────────────────

struct RawRow {
    id: String,
    url: String,
    target_dir: String,
    filename: Option<String>,
    workers: u32,
    piece_target_count: Option<u32>,
    piece_size_min: Option<u64>,
    piece_size_max: Option<u64>,
    state: String,
    attempt: u32,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn row_to_download(r: RawRow) -> QueueResult<Download> {
    let id: DownloadId =
        r.id.parse()
            .map_err(|e| QueueError::Corrupt(format!("id: {e}")))?;
    let state: DownloadState = r
        .state
        .parse()
        .map_err(|e| QueueError::Corrupt(format!("state: {e}")))?;
    let spec = DownloadSpec {
        url: r.url,
        target_dir: PathBuf::from(r.target_dir),
        filename: r.filename,
        workers: r.workers,
        piece_target_count: r.piece_target_count,
        piece_size_min: r.piece_size_min,
        piece_size_max: r.piece_size_max,
    };
    Ok(Download {
        id,
        spec,
        state,
        progress: Progress::default(),
        attempt: r.attempt,
        error: r.error,
        created_at: from_unix_secs(r.created_at),
        updated_at: from_unix_secs(r.updated_at),
    })
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn from_unix_secs(s: i64) -> SystemTime {
    if s >= 0 {
        UNIX_EPOCH + std::time::Duration::from_secs(s as u64)
    } else {
        UNIX_EPOCH
    }
}

fn path_to_str(p: &Path) -> QueueResult<String> {
    p.to_str()
        .map(str::to_owned)
        .ok_or_else(|| QueueError::Corrupt(format!("non-utf8 path: {}", p.display())))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use brook_core::{
        Download,
        DownloadId,
        DownloadSpec,
        DownloadState,
        TQueueStore,
    };
    use tempfile::tempdir;

    use super::*;

    fn sample_download() -> Download {
        let spec = DownloadSpec {
            url: "https://example.com/file.bin".into(),
            target_dir: PathBuf::from("/tmp/brook"),
            filename: Some("file.bin".into()),
            workers: 4,
            piece_target_count: Some(256),
            piece_size_min: Some(8 * 1024 * 1024),
            piece_size_max: Some(64 * 1024 * 1024),
        };
        Download::new(DownloadId::new(), spec)
    }

    fn open_fresh() -> (tempfile::TempDir, SqliteQueueRepository) {
        let dir = tempdir().unwrap();
        let repo = SqliteQueueRepository::open(&dir.path().join("brook.db")).unwrap();
        (dir, repo)
    }

    #[tokio::test]
    async fn roundtrip_insert_load_update_remove() {
        let (_d, repo) = open_fresh();
        let download = sample_download();
        let id = download.id;

        repo.insert(&download).await.unwrap();
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        let got = &loaded[0];
        assert_eq!(got.id, id);
        assert_eq!(got.spec, download.spec);
        assert_eq!(got.state, DownloadState::Queued);
        assert_eq!(got.attempt, 0);
        assert!(got.error.is_none());

        repo.update_state(id, DownloadState::Running).await.unwrap();
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded[0].state, DownloadState::Running);
        assert!(loaded[0].updated_at >= loaded[0].created_at);

        repo.remove(id).await.unwrap();
        assert!(repo.load_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn insert_duplicate_is_error() {
        let (_d, repo) = open_fresh();
        let download = sample_download();
        repo.insert(&download).await.unwrap();
        let err = repo.insert(&download).await.unwrap_err();
        // Domain Error::Other — текст содержит "already exists".
        assert!(err.to_string().contains("already exists"), "{err}");
    }

    #[tokio::test]
    async fn update_state_missing_is_not_found() {
        let (_d, repo) = open_fresh();
        let err = repo
            .update_state(DownloadId::new(), DownloadState::Paused)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn remove_missing_is_not_found() {
        let (_d, repo) = open_fresh();
        let err = repo.remove(DownloadId::new()).await.unwrap_err();
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn reopen_preserves_override_fields() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("brook.db");
        let download = sample_download();
        let id = download.id;
        {
            let repo = SqliteQueueRepository::open(&db).unwrap();
            repo.insert(&download).await.unwrap();
        }
        let repo2 = SqliteQueueRepository::open(&db).unwrap();
        let loaded = repo2.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        let got = &loaded[0];
        assert_eq!(got.id, id);
        assert_eq!(got.spec.piece_target_count, Some(256));
        assert_eq!(got.spec.piece_size_min, Some(8 * 1024 * 1024));
        assert_eq!(got.spec.piece_size_max, Some(64 * 1024 * 1024));
        assert_eq!(got.spec.workers, 4);
        assert_eq!(got.spec.filename.as_deref(), Some("file.bin"));
    }

    #[tokio::test]
    async fn load_all_orders_by_created_at() {
        let (_d, repo) = open_fresh();
        let mut a = sample_download();
        let mut b = sample_download();
        a.created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000);
        b.created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_000);
        repo.insert(&b).await.unwrap();
        repo.insert(&a).await.unwrap();
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded[0].id, a.id);
        assert_eq!(loaded[1].id, b.id);
    }
}
