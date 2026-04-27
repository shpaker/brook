//! Общее SQLite-соединение к `./brook.db`.
//!
//! Один файл, одно соединение, одна точка применения миграций и PRAGMA.
//! Репозитории (`SqliteFileRepository`, `SqlitePieceRepository`,
//! `SqliteWorkerRepository`, `SqlitePieceAttemptRepository`) берут
//! `SharedDb` и гоняют SQL-замыкания через [`SharedDb::with_conn`].
//!
//! ## Конкурентность
//!
//! `Arc<Mutex<Connection>>` — ровно как в `queue.rs`: `rusqlite`
//! синхронный и его нельзя держать на async-потоке, поэтому любой
//! публичный async-метод уходит в `tokio::task::spawn_blocking`.
//!
//! ## Миграция
//!
//! Продукт ещё не запущен, обратной совместимости нет: пустая БД
//! получает `user_version = 1` и полную схему в одной транзакции.
//! Справочники `statuses` и `reason_codes` заполняются natural-key'ами
//! (имя = PK), чтобы не тащить бессмысленные суррогатные UUID'ы.

use std::path::Path;
use std::sync::{
    Arc,
    Mutex,
};

use rusqlite::Connection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type DbResult<T> = std::result::Result<T, DbError>;

/// Разделяемое соединение к `brook.db`.
///
/// Клонирование дешёвое: всё состояние под `Arc<Mutex<_>>`.
#[derive(Clone)]
pub struct SharedDb {
    inner: Arc<Mutex<Connection>>,
}

impl SharedDb {
    /// Открыть файл БД (создав при необходимости), применить PRAGMA
    /// и миграции.
    pub fn open(path: &Path) -> DbResult<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::finish_open(conn)
    }

    /// Открыть in-memory БД (для тестов).
    pub fn open_in_memory() -> DbResult<Self> {
        let conn = Connection::open_in_memory()?;
        Self::finish_open(conn)
    }

    fn finish_open(mut conn: Connection) -> DbResult<Self> {
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        migrate(&mut conn)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Выполнить замыкание под локом на отдельной blocking-таске.
    pub async fn with_conn<F, T>(&self, f: F) -> DbResult<T>
    where
        F: FnOnce(&mut Connection) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let arc = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = arc.lock().expect("shared db mutex poisoned");
            f(&mut guard)
        })
        .await?
    }
}

// ─── Схема и миграция ──────────────────────────────────────────────────

const SCHEMA_V1: &str = "\
CREATE TABLE IF NOT EXISTS statuses (
    name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS reason_codes (
    code TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS files (
    id               TEXT    PRIMARY KEY,
    url              TEXT    NOT NULL,
    target_dir       TEXT    NOT NULL,
    filename         TEXT,
    status_id        TEXT    NOT NULL REFERENCES statuses(name)
                             CHECK (status_id IN
                                 ('pending','running','paused','retrying',
                                  'done','failed','cancelled')),
    created_at       INTEGER NOT NULL,
    -- High-watermark последней активности по файлу: max(created_at,
    -- status_changes.created_at, pieces.created_at|finished_at,
    -- piece_attempts.started_at|finished_at). Поддерживается триггерами
    -- ниже (trg_bump_activity_*). Используется для GetRecently
    -- (фильтрация главного экрана TUI по «recently»).
    last_activity_at INTEGER NOT NULL,
    -- inspect-поля (заполняются позже фабрикой через set_inspect_fields):
    total_size       INTEGER,
    piece_size       INTEGER,
    etag             TEXT,
    last_modified    TEXT,
    effective_url    TEXT,
    accepts_ranges   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_files_last_activity
    ON files (last_activity_at DESC);

CREATE TABLE IF NOT EXISTS status_changes (
    id             TEXT    PRIMARY KEY,
    file_id        TEXT    NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    status_id      TEXT    NOT NULL REFERENCES statuses(name),
    reason_code_id TEXT    REFERENCES reason_codes(code),
    reason_message TEXT,
    created_at     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_status_changes_file_time
    ON status_changes (file_id, created_at);

CREATE TABLE IF NOT EXISTS pieces (
    id          TEXT    PRIMARY KEY,
    file_id     TEXT    NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    number      INTEGER NOT NULL,
    status_id   TEXT    NOT NULL REFERENCES statuses(name)
                        CHECK (status_id IN ('pending','running','done')),
    created_at  INTEGER NOT NULL,
    finished_at INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_pieces_file_number
    ON pieces (file_id, number);

CREATE TABLE IF NOT EXISTS workers (
    id          TEXT    PRIMARY KEY,
    file_id     TEXT    NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    slot_index  INTEGER NOT NULL,
    status_id   TEXT    NOT NULL REFERENCES statuses(name)
                        CHECK (status_id IN
                            ('running','paused','done','failed','cancelled')),
    started_at  INTEGER NOT NULL,
    finished_at INTEGER,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_workers_file_slot
    ON workers (file_id, slot_index);

CREATE TABLE IF NOT EXISTS piece_attempts (
    id          TEXT    PRIMARY KEY,
    piece_id    TEXT    NOT NULL REFERENCES pieces(id) ON DELETE CASCADE,
    worker_id   TEXT    NOT NULL REFERENCES workers(id),
    status_id   TEXT    NOT NULL REFERENCES statuses(name)
                        CHECK (status_id IN
                            ('running','paused','done','failed','cancelled')),
    started_at  INTEGER NOT NULL,
    finished_at INTEGER,
    bytes       INTEGER NOT NULL DEFAULT 0,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_piece_attempts_piece_time
    ON piece_attempts (piece_id, started_at);

CREATE INDEX IF NOT EXISTS idx_piece_attempts_worker_time
    ON piece_attempts (worker_id, started_at);

-- Триггеры на bump `files.last_activity_at`. `AND last_activity_at <
-- NEW.<ts>` гарантирует монотонность watermark'а (не двигаемся назад,
-- если событие пришло из прошлого).
CREATE TRIGGER IF NOT EXISTS trg_bump_activity_status_changes
AFTER INSERT ON status_changes
BEGIN
    UPDATE files SET last_activity_at = NEW.created_at
    WHERE id = NEW.file_id AND last_activity_at < NEW.created_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_bump_activity_piece_insert
AFTER INSERT ON pieces
BEGIN
    UPDATE files SET last_activity_at = NEW.created_at
    WHERE id = NEW.file_id AND last_activity_at < NEW.created_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_bump_activity_piece_finish
AFTER UPDATE OF finished_at ON pieces WHEN NEW.finished_at IS NOT NULL
BEGIN
    UPDATE files SET last_activity_at = NEW.finished_at
    WHERE id = NEW.file_id AND last_activity_at < NEW.finished_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_bump_activity_attempt_insert
AFTER INSERT ON piece_attempts
BEGIN
    UPDATE files SET last_activity_at = NEW.started_at
    WHERE id = (SELECT file_id FROM pieces WHERE id = NEW.piece_id)
      AND last_activity_at < NEW.started_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_bump_activity_attempt_finish
AFTER UPDATE OF finished_at ON piece_attempts WHEN NEW.finished_at IS NOT NULL
BEGIN
    UPDATE files SET last_activity_at = NEW.finished_at
    WHERE id = (SELECT file_id FROM pieces WHERE id = NEW.piece_id)
      AND last_activity_at < NEW.finished_at;
END;
";

/// Единый словарь статусов (natural key). Семь значений — `pending`,
/// `running`, `paused`, `retrying`, `done`, `failed`, `cancelled`.
/// Подмножества по сущностям выбираются через `CHECK (status_id IN ...)`
/// на соответствующих колонках.
const STATUSES: &[&str] = &[
    "pending",
    "running",
    "paused",
    "retrying",
    "done",
    "failed",
    "cancelled",
];

/// Коды причин отказа (natural key).
const REASON_CODES: &[&str] = &[
    "network",
    "timeout",
    "http_4xx",
    "http_5xx",
    "source_mutated",
    "disk_full",
    "invalid_response",
    "cancelled_by_user",
    "unknown",
];

fn migrate(conn: &mut Connection) -> DbResult<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;

    if current < 1 {
        let tx = conn.transaction()?;
        tx.execute_batch(SCHEMA_V1)?;

        {
            let mut stmt = tx.prepare("INSERT OR IGNORE INTO statuses (name) VALUES (?1)")?;
            for name in STATUSES {
                stmt.execute([name])?;
            }
        }
        {
            let mut stmt = tx.prepare("INSERT OR IGNORE INTO reason_codes (code) VALUES (?1)")?;
            for code in REASON_CODES {
                stmt.execute([code])?;
            }
        }

        tx.pragma_update(None, "user_version", 1i64)?;
        tx.commit()?;
    }

    Ok(())
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn table_names(conn: &Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap()
    }

    #[test]
    fn open_in_memory_smoke() {
        let db = SharedDb::open_in_memory().unwrap();
        let conn = db.inner.lock().unwrap();

        let version: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(version, 1);

        let tables = table_names(&conn);
        for t in [
            "statuses",
            "reason_codes",
            "files",
            "status_changes",
            "pieces",
            "workers",
            "piece_attempts",
        ] {
            assert!(tables.iter().any(|n| n == t), "missing table {t}");
        }
        // `file_settings` смержена в `files` — отдельной таблицы быть не должно.
        assert!(
            !tables.iter().any(|n| n == "file_settings"),
            "file_settings table must not exist after merge"
        );

        assert_eq!(count(&conn, "SELECT COUNT(*) FROM statuses"), 7);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM reason_codes"), 9);
    }

    #[test]
    fn foreign_keys_are_on() {
        let db = SharedDb::open_in_memory().unwrap();
        let conn = db.inner.lock().unwrap();
        let fk: i64 = conn
            .pragma_query_value(None, "foreign_keys", |r| r.get(0))
            .unwrap();
        assert_eq!(fk, 1);
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("brook.db");

        {
            let _db = SharedDb::open(&path).unwrap();
        }
        {
            let db = SharedDb::open(&path).unwrap();
            let conn = db.inner.lock().unwrap();

            let version: i64 = conn
                .pragma_query_value(None, "user_version", |r| r.get(0))
                .unwrap();
            assert_eq!(version, 1);
            assert_eq!(count(&conn, "SELECT COUNT(*) FROM statuses"), 7);
            assert_eq!(count(&conn, "SELECT COUNT(*) FROM reason_codes"), 9);
        }
    }

    #[test]
    fn cascade_delete_drops_children() {
        let db = SharedDb::open_in_memory().unwrap();
        let conn = db.inner.lock().unwrap();

        conn.execute(
            "INSERT INTO files
                 (id, url, target_dir, filename, status_id, created_at, last_activity_at)
             VALUES ('f1', 'http://x', '/tmp', 'x.bin', 'pending', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pieces (id, file_id, number, status_id, created_at)
             VALUES ('p1', 'f1', 0, 'pending', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO status_changes (id, file_id, status_id, created_at)
             VALUES ('sc1', 'f1', 'pending', 0)",
            [],
        )
        .unwrap();

        conn.execute("DELETE FROM files WHERE id = 'f1'", [])
            .unwrap();

        assert_eq!(count(&conn, "SELECT COUNT(*) FROM pieces"), 0);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM status_changes"), 0);
    }

    #[tokio::test]
    async fn with_conn_runs_closure() {
        let db = SharedDb::open_in_memory().unwrap();
        let v: i64 = db
            .with_conn(|c| {
                c.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))
                    .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(v, 1);
    }
}
