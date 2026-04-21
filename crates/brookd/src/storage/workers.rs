//! SQLite-репозиторий воркеров (`workers`) поверх общего [`SharedDb`].
//!
//! Реализует [`TWorkerRepo`]: SQL и `rusqlite::Connection` не покидают
//! модуль — вызывающий получает доменные методы.
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
    DownloadId,
    Error,
    Result as CoreResult,
    TWorkerRepo,
    WorkerId,
    WorkerRecord,
};
use rusqlite::{
    Connection,
    params,
};

use super::db::SharedDb;

/// SQLite-репозиторий воркеров.
///
/// Клонирование дешёвое: всё состояние под `Arc` внутри [`SharedDb`].
#[derive(Clone)]
pub struct SqliteWorkerRepository {
    db: SharedDb,
}

impl SqliteWorkerRepository {
    pub fn new(db: SharedDb) -> Self {
        Self { db }
    }
}

impl TWorkerRepo for SqliteWorkerRepository {
    fn ensure_slots(
        &self,
        file_id: DownloadId,
        n: usize,
    ) -> impl std::future::Future<Output = CoreResult<Vec<WorkerRecord>>> + Send {
        let db = self.db.clone();
        async move { run(&db, move |c| ensure_slots_impl(c, file_id, n)).await }
    }

    fn mark_paused(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_status_impl(c, worker_id, "paused", None)
            })
            .await
        }
    }

    fn mark_done(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move { run(&db, move |c| update_status_impl(c, worker_id, "done", None)).await }
    }

    fn mark_failed(
        &self,
        worker_id: WorkerId,
        error: &str,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let err = error.to_owned();
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_status_impl(c, worker_id, "failed", Some(err))
            })
            .await
        }
    }

    fn mark_cancelled(
        &self,
        worker_id: WorkerId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                update_status_impl(c, worker_id, "cancelled", None)
            })
            .await
        }
    }

    fn pause_all_running_for_file(
        &self,
        file_id: DownloadId,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                pause_running_impl(c, Some(file_id))?;
                Ok(())
            })
            .await
        }
    }

    fn pause_all_running_globally(
        &self,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            run(&db, move |c| {
                pause_running_impl(c, None)?;
                Ok(())
            })
            .await
        }
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

fn ensure_slots_impl(
    conn: &mut Connection,
    file_id: DownloadId,
    n: usize,
) -> CoreResult<Vec<WorkerRecord>> {
    let now = unix_secs(SystemTime::now());
    let file_id_str = file_id.to_string();
    let tx = conn
        .transaction()
        .map_err(|e| Error::Other(format!("workers: {e}")))?;

    // Защитный sweep: любой running-воркер этого файла → paused.
    tx.execute(
        "UPDATE workers
           SET status_id = 'paused', finished_at = ?
         WHERE file_id = ? AND status_id = 'running'",
        params![now, file_id_str],
    )
    .map_err(|e| Error::Other(format!("workers: {e}")))?;

    // Свежий набор.
    let mut out = Vec::with_capacity(n);
    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO workers
                   (id, file_id, slot_index, status_id, started_at)
                 VALUES (?, ?, ?, 'running', ?)",
            )
            .map_err(|e| Error::Other(format!("workers: {e}")))?;
        for slot in 0..n {
            let id = WorkerId::new();
            stmt.execute(params![id.to_string(), file_id_str, slot as i64, now,])
                .map_err(|e| Error::Other(format!("workers: {e}")))?;
            out.push(WorkerRecord {
                id,
                file_id,
                slot_index: slot,
                started_at: now,
                finished_at: None,
            });
        }
    }

    tx.commit()
        .map_err(|e| Error::Other(format!("workers: {e}")))?;
    Ok(out)
}

fn update_status_impl(
    conn: &mut Connection,
    worker_id: WorkerId,
    status: &str,
    error: Option<String>,
) -> CoreResult<()> {
    let now = unix_secs(SystemTime::now());
    conn.execute(
        "UPDATE workers
           SET status_id = ?, finished_at = ?, error = COALESCE(?, error)
         WHERE id = ?",
        params![status, now, error, worker_id.to_string()],
    )
    .map_err(|e| Error::Other(format!("workers: {e}")))?;
    Ok(())
}

fn pause_running_impl(conn: &mut Connection, file_id: Option<DownloadId>) -> CoreResult<()> {
    let now = unix_secs(SystemTime::now());
    match file_id {
        Some(id) => {
            conn.execute(
                "UPDATE workers
                   SET status_id = 'paused', finished_at = ?
                 WHERE file_id = ? AND status_id = 'running'",
                params![now, id.to_string()],
            )
            .map_err(|e| Error::Other(format!("workers: {e}")))?;
        }
        None => {
            conn.execute(
                "UPDATE workers
                   SET status_id = 'paused', finished_at = ?
                 WHERE status_id = 'running'",
                params![now],
            )
            .map_err(|e| Error::Other(format!("workers: {e}")))?;
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

    fn sample_download() -> Download {
        let spec = DownloadSpec {
            url: "https://example.com/file.bin".into(),
            target_dir: PathBuf::from("/tmp/brook"),
            filename: Some("file.bin".into()),
            workers: 2,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        };
        Download::new(DownloadId::new(), spec)
    }

    async fn fresh() -> (SharedDb, SqliteWorkerRepository, DownloadId) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = SqliteFileRepository::new(db.clone());
        let d = sample_download();
        let id = d.id;
        files.insert(&d).await.unwrap();
        (db.clone(), SqliteWorkerRepository::new(db), id)
    }

    async fn worker_status(db: &SharedDb, id: WorkerId) -> String {
        let id_str = id.to_string();
        db.with_conn(move |c| {
            c.query_row(
                "SELECT status_id FROM workers WHERE id = ?",
                params![id_str],
                |r| r.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn ensure_slots_creates_fresh_set() {
        let (_db, repo, file_id) = fresh().await;
        let slots = repo.ensure_slots(file_id, 3).await.unwrap();
        assert_eq!(slots.len(), 3);
        let ixs: Vec<usize> = slots.iter().map(|w| w.slot_index).collect();
        assert_eq!(ixs, vec![0, 1, 2]);
        assert!(slots.iter().all(|w| w.finished_at.is_none()));
    }

    #[tokio::test]
    async fn ensure_slots_twice_pauses_previous_and_returns_fresh() {
        let (db, repo, file_id) = fresh().await;
        let first = repo.ensure_slots(file_id, 4).await.unwrap();
        let second = repo.ensure_slots(file_id, 2).await.unwrap();

        assert_eq!(second.len(), 2);
        // UUID'ы разные.
        for f in &first {
            for s in &second {
                assert_ne!(f.id, s.id);
            }
        }
        // Первый набор — paused.
        for f in &first {
            assert_eq!(worker_status(&db, f.id).await, "paused");
        }
        // Новый набор — running.
        for s in &second {
            assert_eq!(worker_status(&db, s.id).await, "running");
        }
    }

    #[tokio::test]
    async fn mark_transitions_work() {
        let (db, repo, file_id) = fresh().await;
        let slots = repo.ensure_slots(file_id, 4).await.unwrap();
        repo.mark_done(slots[0].id).await.unwrap();
        repo.mark_failed(slots[1].id, "boom").await.unwrap();
        repo.mark_cancelled(slots[2].id).await.unwrap();
        repo.mark_paused(slots[3].id).await.unwrap();
        assert_eq!(worker_status(&db, slots[0].id).await, "done");
        assert_eq!(worker_status(&db, slots[1].id).await, "failed");
        assert_eq!(worker_status(&db, slots[2].id).await, "cancelled");
        assert_eq!(worker_status(&db, slots[3].id).await, "paused");
    }

    #[tokio::test]
    async fn pause_all_running_globally_covers_crash_recovery() {
        let (db, repo, file_id) = fresh().await;
        let slots = repo.ensure_slots(file_id, 3).await.unwrap();
        // Имитируем «демон упал» — строки остались в running.
        repo.pause_all_running_globally().await.unwrap();
        for s in &slots {
            assert_eq!(worker_status(&db, s.id).await, "paused");
        }
    }
}
