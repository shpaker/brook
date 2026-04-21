//! SQLite-репозиторий попыток скачивания piece'а поверх общего
//! [`SharedDb`]. Реализует [`TPieceAttemptRepo`].
//!
//! ## Конкурентность
//!
//! Все публичные async-методы уходят в [`SharedDb::with_conn`] →
//! `spawn_blocking`: `rusqlite` синхронный, держать его на async-потоке
//! нельзя.
//!
//! [`SharedDb`]: super::db::SharedDb
//! [`SharedDb::with_conn`]: super::db::SharedDb::with_conn

use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use brook_core::{
    AttemptId,
    AttemptRecord,
    DownloadId,
    Error,
    Result as CoreResult,
    TPieceAttemptRepo,
    WorkerId,
};
use rusqlite::{
    Connection,
    params,
};

use super::db::SharedDb;

#[derive(Clone)]
pub struct SqlitePieceAttemptRepository {
    db: SharedDb,
}

impl SqlitePieceAttemptRepository {
    pub fn new(db: SharedDb) -> Self {
        Self { db }
    }
}

impl TPieceAttemptRepo for SqlitePieceAttemptRepository {
    fn start(
        &self,
        file_id: DownloadId,
        piece_number: u32,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = CoreResult<AttemptRecord>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                start_impl(c, file_id, piece_number, worker_id)
            })
            .await
        }
    }

    fn finish(
        &self,
        attempt_id: AttemptId,
        bytes: u64,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_impl(c, attempt_id, "done", Some(bytes), None)
            })
            .await
        }
    }

    fn fail(
        &self,
        attempt_id: AttemptId,
        error: &str,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        let err = error.to_owned();
        async move {
            run(&db, move |c| {
                update_impl(c, attempt_id, "failed", None, Some(err))
            })
            .await
        }
    }

    fn cancel(
        &self,
        attempt_id: AttemptId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_impl(c, attempt_id, "cancelled", None, None)
            })
            .await
        }
    }

    fn pause(
        &self,
        attempt_id: AttemptId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_impl(c, attempt_id, "paused", None, None)
            })
            .await
        }
    }

    fn pause_all_running_for_file(
        &self,
        file_id: DownloadId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move { run(&db, move |c| pause_running_impl(c, Some(file_id))).await }
    }

    fn pause_all_running_globally(
        &self,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move { run(&db, move |c| pause_running_impl(c, None)).await }
    }
}

async fn run<F, T>(db: &SharedDb, f: F) -> CoreResult<T>
where
    F: FnOnce(&mut Connection) -> CoreResult<T> + Send + 'static,
    T: Send + 'static,
{
    db.with_conn(|c| Ok(f(c)))
        .await
        .map_err(|e| Error::Other(format!("db: {e}")))?
}

// ─── Sync implementations ──────────────────────────────────────────────

fn start_impl(
    conn: &mut Connection,
    file_id: DownloadId,
    piece_number: u32,
    worker_id: WorkerId,
) -> CoreResult<AttemptRecord> {
    let now = unix_secs(SystemTime::now());
    let id = AttemptId::new();
    // Резолвим DB-uuid piece'а по натуральному ключу (file_id, number).
    let piece_id: String = conn
        .query_row(
            "SELECT id FROM pieces WHERE file_id = ? AND number = ?",
            params![file_id.to_string(), piece_number as i64],
            |r| r.get::<_, String>(0),
        )
        .map_err(|e| Error::Other(format!("piece_attempts: lookup piece: {e}")))?;
    conn.execute(
        "INSERT INTO piece_attempts
           (id, piece_id, worker_id, status_id, started_at, bytes)
         VALUES (?, ?, ?, 'running', ?, 0)",
        params![id.to_string(), piece_id, worker_id.to_string(), now],
    )
    .map_err(|e| Error::Other(format!("piece_attempts: {e}")))?;
    Ok(AttemptRecord {
        id,
        piece_id,
        worker_id,
        started_at: now,
        finished_at: None,
        bytes: 0,
    })
}

fn update_impl(
    conn: &mut Connection,
    id: AttemptId,
    status: &str,
    bytes: Option<u64>,
    error: Option<String>,
) -> CoreResult<()> {
    let now = unix_secs(SystemTime::now());
    conn.execute(
        "UPDATE piece_attempts
           SET status_id = ?,
               finished_at = ?,
               bytes = COALESCE(?, bytes),
               error = COALESCE(?, error)
         WHERE id = ?",
        params![status, now, bytes.map(|b| b as i64), error, id.to_string()],
    )
    .map_err(|e| Error::Other(format!("piece_attempts: {e}")))?;
    Ok(())
}

fn pause_running_impl(conn: &mut Connection, file_id: Option<DownloadId>) -> CoreResult<()> {
    let now = unix_secs(SystemTime::now());
    match file_id {
        Some(fid) => {
            // Attempt → piece → file.
            conn.execute(
                "UPDATE piece_attempts
                   SET status_id = 'paused', finished_at = ?
                 WHERE status_id = 'running'
                   AND piece_id IN (SELECT id FROM pieces WHERE file_id = ?)",
                params![now, fid.to_string()],
            )
            .map_err(|e| Error::Other(format!("piece_attempts: {e}")))?;
        }
        None => {
            conn.execute(
                "UPDATE piece_attempts
                   SET status_id = 'paused', finished_at = ?
                 WHERE status_id = 'running'",
                params![now],
            )
            .map_err(|e| Error::Other(format!("piece_attempts: {e}")))?;
        }
    }
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
        TQueueStore,
        TWorkerRepo,
    };

    use super::*;
    use crate::storage::files::SqliteFileRepository;
    use crate::storage::pieces::SqlitePieceRepository;
    use crate::storage::workers::SqliteWorkerRepository;

    async fn fresh() -> (
        SharedDb,
        SqlitePieceAttemptRepository,
        SqliteWorkerRepository,
        DownloadId,
    ) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = SqliteFileRepository::new(db.clone());
        let pieces = SqlitePieceRepository::new(db.clone());
        let d = Download::new(
            DownloadId::new(),
            DownloadSpec {
                url: "https://x/f".into(),
                target_dir: PathBuf::from("/tmp"),
                filename: Some("f".into()),
                workers: 2,
                piece_target_count: None,
                piece_size_min: None,
                piece_size_max: None,
                on_file_exists_override: Default::default(),
            },
        );
        let file_id = d.id;
        files.insert(&d).await.unwrap();
        pieces.init(file_id, 3).await.unwrap();
        (
            db.clone(),
            SqlitePieceAttemptRepository::new(db.clone()),
            SqliteWorkerRepository::new(db),
            file_id,
        )
    }

    #[tokio::test]
    async fn start_then_finish_updates_status_and_bytes() {
        let (db, attempts, workers, file_id) = fresh().await;
        let slot = &workers.ensure_slots(file_id, 1).await.unwrap()[0];
        let a = attempts.start(file_id, 0, slot.id).await.unwrap();
        attempts.finish(a.id, 1234).await.unwrap();

        let (status, bytes): (String, i64) = db
            .with_conn(move |c| {
                c.query_row(
                    "SELECT status_id, bytes FROM piece_attempts WHERE id = ?",
                    params![a.id.to_string()],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(status, "done");
        assert_eq!(bytes, 1234);
    }

    #[tokio::test]
    async fn pause_all_running_globally_sweeps() {
        let (db, attempts, workers, file_id) = fresh().await;
        let slot = &workers.ensure_slots(file_id, 1).await.unwrap()[0];
        let a = attempts.start(file_id, 0, slot.id).await.unwrap();
        attempts.pause_all_running_globally().await.unwrap();
        let status: String = db
            .with_conn(move |c| {
                c.query_row(
                    "SELECT status_id FROM piece_attempts WHERE id = ?",
                    params![a.id.to_string()],
                    |r| r.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(status, "paused");
    }
}
