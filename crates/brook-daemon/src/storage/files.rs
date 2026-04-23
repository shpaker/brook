//! Репозиторий загрузок в `./brook.db` поверх общего [`SharedDb`].
//!
//! Реализует [`TQueueStore`]: отдаёт доменные методы, SQL и
//! `rusqlite::Connection` не покидают модуль (см. CLAUDE.md — репозиторная
//! граница).
//!
//! ## Раскладка по таблицам
//!
//! Одна доменная `File` ложится в три таблицы (миграция — в
//! [`super::db::SharedDb`]):
//!
//! - `files` — «шапка»: `id`, `url`, `target_dir`, `filename`,
//!   текущее `status_id`, `created_at`.
//! - `file_settings` (1:1) — inspect-поля (`total_size`, `piece_size`,
//!   `etag`, `last_modified`; NULL до stage 6, когда их пишет фабрика).
//! - `status_changes` — полный аудит переходов: `reason_code_id` +
//!   `reason_message` живут только тут; колонки в `files` их не дублируют.
//!
//! Собственной миграции у репозитория нет — схема + сиды справочников
//! приходят из [`SharedDb::finish_open`] (stage 1).
//!
//! ## Конкурентность
//!
//! Все публичные async-методы уходят в
//! [`SharedDb::with_conn`] → `spawn_blocking`: `rusqlite` синхронный,
//! держать его на async-потоке нельзя.
//!
//! [`SharedDb`]: super::db::SharedDb
//! [`SharedDb::with_conn`]: super::db::SharedDb::with_conn
//! [`SharedDb::finish_open`]: super::db::SharedDb

use std::path::{
    Path,
    PathBuf,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use brook_core::{
    Error,
    FailureReason,
    File,
    FileId,
    FileSpec,
    FileStatus,
    Result as CoreResult,
    TQueueStore,
};
use rusqlite::{
    Connection,
    OptionalExtension,
    Transaction,
    params,
};
use thiserror::Error;
use uuid::Uuid;

use super::db::SharedDb;

#[derive(Debug, Error)]
pub enum FilesError {
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
    /// Переход в `failed` обязан нести [`FailureReason`] — инвариант
    /// схемы. Нарушение ловим до SQL.
    #[error("failed transition requires reason")]
    MissingFailureReason,
}

impl From<FilesError> for Error {
    fn from(e: FilesError) -> Self {
        match e {
            FilesError::NotFound => Error::NotFound,
            other => Error::Other(other.to_string()),
        }
    }
}

pub type FilesResult<T> = std::result::Result<T, FilesError>;

/// Inspect-поля одной загрузки — DTO между фабрикой и репозиторием.
///
/// Живёт в адаптерном слое: доменному ядру ни форма таблицы
/// `file_settings`, ни этот тип не нужны.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectFields {
    /// `None`, если сервер не прислал `Content-Length` (streaming-режим).
    pub total_size: Option<u64>,
    /// `None` — сопутствует `total_size = None` (piece-нарезки нет).
    pub piece_size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// URL после цепочки редиректов — для воркеров (§3 плана).
    pub effective_url: Option<String>,
    /// Поддерживает ли источник `Range` (из первого inspect'а). Для
    /// streaming-режима (`total_size = None`) смысла не имеет; хранится,
    /// чтобы `prepare()` не передёргивал HEAD ради одного бита.
    pub accepts_ranges: bool,
}

/// SQLite-репозиторий загрузок.
///
/// Клонирование дешёвое: всё состояние под `Arc` внутри [`SharedDb`].
#[derive(Clone)]
pub struct SqliteFileRepository {
    db: SharedDb,
}

impl SqliteFileRepository {
    pub fn new(db: SharedDb) -> Self {
        Self { db }
    }

    /// Записать результат `inspect` в `file_settings` (+ `files.filename`).
    /// Вызывает фабрика при `resolve()`. 0 строк ⇒ `NotFound`.
    ///
    /// `filename` уходит в `files.filename` — это resolved-имя после
    /// `Content-Disposition` / `spec.filename` / URL-tail, которое менеджер
    /// потом использует во всех API-ответах (иначе TUI падает на URL-хвост,
    /// который для CDN-архивов бывает UUID'ом).
    #[allow(clippy::too_many_arguments)]
    pub async fn set_inspect_fields(
        &self,
        id: FileId,
        total_size: Option<u64>,
        piece_size: Option<u64>,
        etag: Option<String>,
        last_modified: Option<String>,
        effective_url: Option<String>,
        accepts_ranges: bool,
        filename: String,
    ) -> CoreResult<()> {
        run(&self.db, move |c| {
            set_inspect_fields_impl(
                c,
                id,
                total_size,
                piece_size,
                etag,
                last_modified,
                effective_url,
                accepts_ranges,
                filename,
            )
        })
        .await
    }

    /// Прочитать inspect-поля. `Ok(None)` — файл есть, но поля ещё
    /// не заполнены (до первого `prepare`). `Err(NotFound)` — файла нет.
    pub async fn get_inspect_fields(&self, id: FileId) -> CoreResult<Option<InspectFields>> {
        run(&self.db, move |c| get_inspect_fields_impl(c, id)).await
    }
}

// ─── TQueueStore ────────────────────────────────────────────────────────

impl TQueueStore for SqliteFileRepository {
    fn load_all(&self) -> impl std::future::Future<Output = CoreResult<Vec<File>>> + Send {
        let db = self.db.clone();
        async move { run(&db, load_all_impl).await }
    }

    fn insert(&self, download: &File) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        let d = download.clone();
        async move { run(&db, move |c| insert_impl(c, &d)).await }
    }

    fn update_status(
        &self,
        id: FileId,
        state: FileStatus,
        reason: Option<FailureReason>,
    ) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move {
            if matches!(state, FileStatus::Failed) && reason.is_none() {
                return Err(FilesError::MissingFailureReason.into());
            }
            run(&db, move |c| update_status_impl(c, id, state, reason)).await
        }
    }

    fn remove(&self, id: FileId) -> impl std::future::Future<Output = CoreResult<()>> + Send {
        let db = self.db.clone();
        async move { run(&db, move |c| remove_impl(c, id)).await }
    }
}

/// Хелпер: прогоняем замыкание через `SharedDb::with_conn`, снимаем
/// двойную обёртку ошибок (DB-уровень + domain-уровень) и конвертируем
/// в `brook_core::Error`.
async fn run<F, T>(db: &SharedDb, f: F) -> CoreResult<T>
where
    F: FnOnce(&mut Connection) -> FilesResult<T> + Send + 'static,
    T: Send + 'static,
{
    let inner: FilesResult<T> = db
        .with_conn(|c| Ok(f(c)))
        .await
        .map_err(|e| Error::Other(format!("db: {e}")))?;
    inner.map_err(Into::into)
}

// ─── Sync implementations (spawn_blocking) ──────────────────────────────

fn insert_impl(conn: &mut Connection, d: &File) -> FilesResult<()> {
    let target_dir = path_to_str(&d.spec.target_dir)?;
    let created_at = unix_secs(d.created_at);

    let tx = conn.transaction()?;
    let res = tx.execute(
        "INSERT INTO files
           (id, url, target_dir, filename, status_id, created_at)
         VALUES (?,?,?,?,?,?)",
        params![
            d.id.to_string(),
            d.spec.url,
            target_dir,
            d.spec.filename,
            d.status.as_str(),
            created_at,
        ],
    );
    match res {
        Ok(_) => {}
        Err(rusqlite::Error::SqliteFailure(e, _))
            if e.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            return Err(FilesError::Duplicate);
        }
        Err(e) => return Err(e.into()),
    }

    tx.execute(
        "INSERT INTO file_settings
           (file_id, total_size, piece_size, etag, last_modified, effective_url)
         VALUES (?, NULL, NULL, NULL, NULL, NULL)",
        params![d.id.to_string()],
    )?;

    insert_status_change(&tx, d.id, d.status, None, created_at)?;

    tx.commit()?;
    Ok(())
}

fn update_status_impl(
    conn: &mut Connection,
    id: FileId,
    state: FileStatus,
    reason: Option<FailureReason>,
) -> FilesResult<()> {
    let now = unix_secs(SystemTime::now());
    let tx = conn.transaction()?;
    let n = tx.execute(
        "UPDATE files SET status_id = ? WHERE id = ?",
        params![state.as_str(), id.to_string()],
    )?;
    if n == 0 {
        // Drop of `tx` откатит (пустой UPDATE всё равно ничего бы не поменял,
        // но формально — rollback).
        return Err(FilesError::NotFound);
    }
    insert_status_change(&tx, id, state, reason.as_ref(), now)?;
    tx.commit()?;
    Ok(())
}

fn insert_status_change(
    tx: &Transaction<'_>,
    id: FileId,
    state: FileStatus,
    reason: Option<&FailureReason>,
    at: i64,
) -> FilesResult<()> {
    let reason_code = reason.map(|r| r.code.as_str());
    let reason_message = reason.and_then(|r| r.message.as_deref());
    tx.execute(
        "INSERT INTO status_changes
           (id, file_id, status_id, reason_code_id, reason_message, created_at)
         VALUES (?,?,?,?,?,?)",
        params![
            Uuid::new_v4().to_string(),
            id.to_string(),
            state.as_str(),
            reason_code,
            reason_message,
            at,
        ],
    )?;
    Ok(())
}

fn remove_impl(conn: &mut Connection, id: FileId) -> FilesResult<()> {
    let n = conn.execute("DELETE FROM files WHERE id = ?", params![id.to_string()])?;
    if n == 0 {
        return Err(FilesError::NotFound);
    }
    Ok(())
}

fn load_all_impl(conn: &mut Connection) -> FilesResult<Vec<File>> {
    let mut stmt = conn.prepare(
        "SELECT f.id, f.url, f.target_dir, f.filename, f.status_id, f.created_at
         FROM files f
         JOIN file_settings s ON s.file_id = f.id
         ORDER BY f.created_at",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(RawRow {
                id: row.get(0)?,
                url: row.get(1)?,
                target_dir: row.get(2)?,
                filename: row.get(3)?,
                status: row.get(4)?,
                created_at: row.get::<_, i64>(5)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Второй запрос — последняя строка `status_changes` по каждому файлу.
    // Нужен, чтобы восстановить `updated_at` и (для failed) `error`.
    // Для небольшой очереди (единицы-сотни строк) отдельный проход дешевле
    // жонглирования сложным JOIN'ом с оконной функцией.
    let mut last_stmt = conn.prepare(
        "SELECT created_at, reason_message
         FROM status_changes
         WHERE file_id = ?
         ORDER BY created_at DESC, rowid DESC
         LIMIT 1",
    )?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let id_str = r.id.clone();
        let (updated_at, last_reason) = last_stmt
            .query_row(params![id_str], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .optional()?
            .unwrap_or((r.created_at, None));
        out.push(row_to_download(r, updated_at, last_reason)?);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn set_inspect_fields_impl(
    conn: &mut Connection,
    id: FileId,
    total_size: Option<u64>,
    piece_size: Option<u64>,
    etag: Option<String>,
    last_modified: Option<String>,
    effective_url: Option<String>,
    accepts_ranges: bool,
    filename: String,
) -> FilesResult<()> {
    let id_str = id.to_string();
    let tx = conn.transaction()?;
    let n = tx.execute(
        "UPDATE file_settings
           SET total_size = ?, piece_size = ?, etag = ?, last_modified = ?,
               effective_url = ?, accepts_ranges = ?
         WHERE file_id = ?",
        params![
            total_size.map(|v| v as i64),
            piece_size.map(|v| v as i64),
            etag,
            last_modified,
            effective_url,
            accepts_ranges as i64,
            id_str,
        ],
    )?;
    if n == 0 {
        return Err(FilesError::NotFound);
    }
    tx.execute(
        "UPDATE files SET filename = ? WHERE id = ?",
        params![filename, id_str],
    )?;
    tx.commit()?;
    Ok(())
}

fn get_inspect_fields_impl(
    conn: &mut Connection,
    id: FileId,
) -> FilesResult<Option<InspectFields>> {
    // Разделяем «файла нет» и «inspect ещё не заполнен»:
    // отсутствие строки в `file_settings` — NotFound; NULL в total_size —
    // Ok(None).
    type InspectRow = (
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
    );
    let row: Option<InspectRow> = conn
        .query_row(
            "SELECT total_size, piece_size, etag, last_modified, effective_url, accepts_ranges
             FROM file_settings WHERE file_id = ?",
            params![id.to_string()],
            |r| {
                Ok((
                    r.get::<_, Option<i64>>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((total, piece, etag, last_modified, effective_url, accepts_ranges)) = row else {
        return Err(FilesError::NotFound);
    };
    // `accepts_ranges IS NOT NULL` — маркер «inspect уже записан»: колонка
    // заполняется только из `set_inspect_fields`. До неё все остальные
    // поля могут быть NULL сами по себе (streaming без etag), поэтому
    // полагаться на их суммарное «filled» ненадёжно.
    let Some(accepts_ranges) = accepts_ranges else {
        return Ok(None);
    };
    Ok(Some(InspectFields {
        total_size: total.map(|v| v as u64),
        piece_size: piece.map(|v| v as u64),
        etag,
        last_modified,
        effective_url,
        accepts_ranges: accepts_ranges != 0,
    }))
}

// ─── Row ↔ Domain ───────────────────────────────────────────────────────

struct RawRow {
    id: String,
    url: String,
    target_dir: String,
    filename: Option<String>,
    status: String,
    created_at: i64,
}

fn row_to_download(
    r: RawRow,
    updated_at: i64,
    last_reason_message: Option<String>,
) -> FilesResult<File> {
    let id: FileId =
        r.id.parse()
            .map_err(|e| FilesError::Corrupt(format!("id: {e}")))?;
    let status: FileStatus = r
        .status
        .parse()
        .map_err(|e| FilesError::Corrupt(format!("status: {e}")))?;
    let spec = FileSpec {
        url: r.url,
        target_dir: PathBuf::from(r.target_dir),
        filename: r.filename,
    };
    let error = if matches!(status, FileStatus::Failed) {
        last_reason_message
    } else {
        None
    };
    Ok(File {
        id,
        spec,
        status,
        attempt: 0,
        error,
        created_at: from_unix_secs(r.created_at),
        updated_at: from_unix_secs(updated_at),
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

fn path_to_str(p: &Path) -> FilesResult<String> {
    p.to_str()
        .map(str::to_owned)
        .ok_or_else(|| FilesError::Corrupt(format!("non-utf8 path: {}", p.display())))
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use brook_core::{
        FailureReason,
        File,
        FileId,
        FileSpec,
        FileStatus,
        ReasonCode,
        TQueueStore,
    };

    use super::*;

    fn sample_download() -> File {
        let spec = FileSpec {
            url: "https://example.com/file.bin".into(),
            target_dir: PathBuf::from("/tmp/brook"),
            filename: Some("file.bin".into()),
        };
        File::new(FileId::new(), spec)
    }

    fn fresh_repo() -> (SharedDb, SqliteFileRepository) {
        let db = SharedDb::open_in_memory().unwrap();
        let repo = SqliteFileRepository::new(db.clone());
        (db, repo)
    }

    #[tokio::test]
    async fn roundtrip_insert_load_update_remove() {
        let (_db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;

        repo.insert(&d).await.unwrap();
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        let got = &loaded[0];
        assert_eq!(got.id, id);
        assert_eq!(got.spec, d.spec);
        assert_eq!(got.status, FileStatus::Pending);
        assert_eq!(got.attempt, 0);
        assert!(got.error.is_none());

        repo.update_status(id, FileStatus::Running, None)
            .await
            .unwrap();
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded[0].status, FileStatus::Running);
        assert!(loaded[0].updated_at >= loaded[0].created_at);

        repo.remove(id).await.unwrap();
        assert!(repo.load_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn insert_writes_initial_status_change() {
        let (db, repo) = fresh_repo();
        let d = sample_download();
        repo.insert(&d).await.unwrap();

        let id_str = d.id.to_string();
        let (cnt, status_id, reason): (i64, String, Option<String>) = db
            .with_conn(move |c| {
                let cnt: i64 = c.query_row(
                    "SELECT COUNT(*) FROM status_changes WHERE file_id = ?",
                    params![id_str],
                    |r| r.get(0),
                )?;
                let id_str2 = d.id.to_string();
                let (status_id, reason): (String, Option<String>) = c.query_row(
                    "SELECT status_id, reason_code_id FROM status_changes WHERE file_id = ?",
                    params![id_str2],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                Ok((cnt, status_id, reason))
            })
            .await
            .unwrap();
        assert_eq!(cnt, 1);
        assert_eq!(status_id, "pending");
        assert!(reason.is_none());
    }

    #[tokio::test]
    async fn update_status_failed_writes_reason_atomically() {
        let (db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;
        repo.insert(&d).await.unwrap();

        let reason = FailureReason::with_message(ReasonCode::Timeout, "deadline 30s");
        repo.update_status(id, FileStatus::Failed, Some(reason))
            .await
            .unwrap();

        let id_str = id.to_string();
        let (files_status, reason_code, reason_msg): (String, String, Option<String>) = db
            .with_conn(move |c| {
                let files_status: String = c.query_row(
                    "SELECT status_id FROM files WHERE id = ?",
                    params![id_str.clone()],
                    |r| r.get(0),
                )?;
                let (code, msg): (String, Option<String>) = c.query_row(
                    "SELECT reason_code_id, reason_message
                       FROM status_changes
                      WHERE file_id = ?
                      ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    params![id_str],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                Ok((files_status, code, msg))
            })
            .await
            .unwrap();
        assert_eq!(files_status, "failed");
        assert_eq!(reason_code, "timeout");
        assert_eq!(reason_msg.as_deref(), Some("deadline 30s"));

        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded[0].status, FileStatus::Failed);
        assert_eq!(loaded[0].error.as_deref(), Some("deadline 30s"));
    }

    #[tokio::test]
    async fn update_status_failed_without_reason_errors() {
        let (_db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;
        repo.insert(&d).await.unwrap();

        let err = repo
            .update_status(id, FileStatus::Failed, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed transition"), "{err}");

        // Состояние не изменилось.
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded[0].status, FileStatus::Pending);
    }

    #[tokio::test]
    async fn update_status_rolls_back_on_status_changes_failure() {
        // Валим INSERT `status_changes` через FK: удаляем строку 'retrying'
        // из справочника `states`; UPDATE `files` пройдёт (там state
        // валидный), а INSERT в историю с тем же state упадёт на FK.
        let (db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;
        repo.insert(&d).await.unwrap();

        db.with_conn(|c| {
            c.execute("DELETE FROM statuses WHERE name = 'retrying'", [])
                .map_err(Into::into)
                .map(|_| ())
        })
        .await
        .unwrap();

        let err = repo
            .update_status(id, FileStatus::Retrying, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("FOREIGN KEY") || err.to_string().contains("constraint"),
            "unexpected error: {err}"
        );

        // `files.status_id` не сдвинулся — UPDATE откатился вместе с транзакцией.
        let id_str = id.to_string();
        let status: String = db
            .with_conn(move |c| {
                c.query_row(
                    "SELECT status_id FROM files WHERE id = ?",
                    params![id_str],
                    |r| r.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(status, "pending");
    }

    #[tokio::test]
    async fn update_status_missing_is_not_found() {
        let (_db, repo) = fresh_repo();
        let err = repo
            .update_status(FileId::new(), FileStatus::Paused, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn remove_missing_is_not_found() {
        let (_db, repo) = fresh_repo();
        let err = repo.remove(FileId::new()).await.unwrap_err();
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn remove_cascades_to_children() {
        let (db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;
        repo.insert(&d).await.unwrap();

        // Добавим лишнюю строку в pieces и ещё одну историю.
        let id_str = id.to_string();
        db.with_conn(move |c| {
            c.execute(
                "INSERT INTO pieces (id, file_id, number, status_id, created_at)
                 VALUES (?, ?, 0, 'pending', 0)",
                params![Uuid::new_v4().to_string(), id_str],
            )
            .map_err(Into::into)
            .map(|_| ())
        })
        .await
        .unwrap();

        repo.update_status(id, FileStatus::Paused, None)
            .await
            .unwrap();
        repo.remove(id).await.unwrap();

        let id_str = id.to_string();
        let (files, settings, changes, pieces): (i64, i64, i64, i64) = db
            .with_conn(move |c| {
                let q = |sql: &str| -> rusqlite::Result<i64> {
                    c.query_row(sql, params![id_str], |r| r.get::<_, i64>(0))
                };
                Ok((
                    q("SELECT COUNT(*) FROM files WHERE id = ?")?,
                    q("SELECT COUNT(*) FROM file_settings WHERE file_id = ?")?,
                    q("SELECT COUNT(*) FROM status_changes WHERE file_id = ?")?,
                    q("SELECT COUNT(*) FROM pieces WHERE file_id = ?")?,
                ))
            })
            .await
            .unwrap();
        assert_eq!((files, settings, changes, pieces), (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn set_get_inspect_fields_roundtrip() {
        let (_db, repo) = fresh_repo();
        let d = sample_download();
        let id = d.id;
        repo.insert(&d).await.unwrap();

        // До первого set — поля NULL.
        assert!(repo.get_inspect_fields(id).await.unwrap().is_none());

        repo.set_inspect_fields(
            id,
            Some(123_456_789),
            Some(8 * 1024 * 1024),
            Some("\"abc\"".into()),
            Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            Some("https://cdn.example/resolved".into()),
            true,
            "resolved.bin".into(),
        )
        .await
        .unwrap();

        let got = repo.get_inspect_fields(id).await.unwrap().unwrap();
        assert_eq!(got.total_size, Some(123_456_789));
        assert_eq!(got.piece_size, Some(8 * 1024 * 1024));
        assert_eq!(got.etag.as_deref(), Some("\"abc\""));
        assert_eq!(
            got.last_modified.as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(
            got.effective_url.as_deref(),
            Some("https://cdn.example/resolved")
        );
        assert!(got.accepts_ranges);

        // `files.filename` тоже обновился.
        let loaded = repo.load_all().await.unwrap();
        let row = loaded.iter().find(|d| d.id == id).unwrap();
        assert_eq!(row.spec.filename.as_deref(), Some("resolved.bin"));

        // Повторный set перезаписывает.
        repo.set_inspect_fields(id, Some(1), Some(1), None, None, None, false, "x".into())
            .await
            .unwrap();
        let got = repo.get_inspect_fields(id).await.unwrap().unwrap();
        assert_eq!(got.total_size, Some(1));
        assert!(got.etag.is_none());
        assert!(!got.accepts_ranges);
    }

    #[tokio::test]
    async fn load_all_orders_by_created_at() {
        let (_db, repo) = fresh_repo();
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

    #[tokio::test]
    async fn reopen_preserves_spec_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("brook.db");
        let d = sample_download();
        let id = d.id;
        {
            let db = SharedDb::open(&path).unwrap();
            let repo = SqliteFileRepository::new(db);
            repo.insert(&d).await.unwrap();
        }
        let db = SharedDb::open(&path).unwrap();
        let repo = SqliteFileRepository::new(db);
        let loaded = repo.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        let got = &loaded[0];
        assert_eq!(got.id, id);
        assert_eq!(got.spec.filename.as_deref(), Some("file.bin"));
    }
}
