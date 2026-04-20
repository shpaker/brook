//! Индекс piece'ов одной загрузки на SQLite (`<name>.index.brook`).
//!
//! Репозиторий — единственное место во всём крейте, где живут SQL-строки
//! и `rusqlite::Connection`. Наружу торчат только доменные методы:
//! ничто выше `storage::index` не знает, как устроена таблица `pieces`,
//! какой в ней `CHECK`, и что вообще используется SQLite.
//!
//! ## Схема
//!
//! ```sql
//! CREATE TABLE pieces (
//!     idx    INTEGER PRIMARY KEY,
//!     offset INTEGER NOT NULL,
//!     size   INTEGER NOT NULL,
//!     status TEXT    NOT NULL CHECK (status IN ('pending', 'done'))
//! );
//! CREATE TABLE meta (
//!     key   TEXT PRIMARY KEY,
//!     value TEXT NOT NULL
//! );
//! ```
//!
//! ## Режим SQLite
//!
//! `journal_mode=WAL` — writers не блокируют readers, важно когда
//! воркеры параллельно коммитят готовые piece'ы, а в это же время
//! наблюдатель спрашивает `pending_pieces`.
//!
//! `synchronous=NORMAL` — fsync WAL на checkpoint, но не на каждый
//! commit. Даёт ×5–10 по скорости записи при сохранении «commit ⇒ persisted»
//! после следующего checkpoint. Для нас это приемлемо: инвариант
//! `TPieceStorage::commit_batch` требует fsync на `.data.brook`
//! (сами байты), а индекс может отстать на один WAL-checkpoint —
//! в худшем случае после краша перекачаем уже записанные куски.

use std::path::Path;

use rusqlite::{
    Connection,
    OptionalExtension,
    params,
};
use thiserror::Error;

use super::plan::PieceLayout;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    /// `init` вызван на уже инициализированном индексе.
    #[error("index already initialized")]
    AlreadyInitialized,
}

pub type IndexResult<T> = Result<T, IndexError>;

/// Статус одного piece'а.
///
/// Сериализуется в TEXT ('pending'|'done') — так читаемее при ручном
/// просмотре `.index.brook` в DB Browser, и CHECK-constraint отсекает
/// мусор на уровне SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceStatus {
    Pending,
    Done,
}

impl PieceStatus {
    fn as_str(self) -> &'static str {
        match self {
            PieceStatus::Pending => "pending",
            PieceStatus::Done => "done",
        }
    }
}

pub struct PieceIndexRepository {
    conn: Connection,
}

impl PieceIndexRepository {
    /// Открыть (создав при необходимости) индекс по пути.
    ///
    /// Применяет миграцию и включает WAL. Идемпотентно: повторный `open`
    /// того же файла не ломает данные.
    pub fn open(path: &Path) -> IndexResult<Self> {
        let conn = Connection::open(path)?;
        // Параметры sqlite — `PRAGMA`, а не `SET`; их применяем на каждом
        // открытии соединения (они привязаны к соединению, не к файлу).
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pieces (
                idx    INTEGER PRIMARY KEY,
                offset INTEGER NOT NULL,
                size   INTEGER NOT NULL,
                status TEXT    NOT NULL CHECK (status IN ('pending', 'done'))
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        Ok(Self { conn })
    }

    /// Был ли этот индекс уже проинициализирован (есть piece-строки).
    ///
    /// Нужен вызывающему (`LocalPieceStorage::new` в 1.7), чтобы отличить
    /// «первый запуск» от «рестарт с докачкой».
    pub fn is_initialized(&self) -> IndexResult<bool> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM pieces", [], |r| r.get(0))?;
        Ok(count > 0)
    }

    /// Заполнить индекс раскладкой piece'ов и записать базовую мету
    /// (`url`, `total_size`, `piece_size`). Всё в одной транзакции.
    ///
    /// Если индекс уже заполнен — возвращает `AlreadyInitialized`
    /// (перезапись считаем ошибкой программы, а не штатным кейсом).
    pub fn init(
        &mut self,
        url: &str,
        total_size: u64,
        piece_size: u64,
        pieces: &[PieceLayout],
    ) -> IndexResult<()> {
        if self.is_initialized()? {
            return Err(IndexError::AlreadyInitialized);
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt =
                tx.prepare("INSERT INTO pieces (idx, offset, size, status) VALUES (?, ?, ?, ?)")?;
            for p in pieces {
                stmt.execute(params![
                    p.idx as i64,
                    p.offset as i64,
                    p.size as i64,
                    PieceStatus::Pending.as_str(),
                ])?;
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('url', ?)",
            params![url],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('total_size', ?)",
            params![total_size.to_string()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('piece_size', ?)",
            params![piece_size.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Piece'ы, которые ещё не скачаны — упорядочены по `idx`.
    pub fn pending_pieces(&self) -> IndexResult<Vec<PieceLayout>> {
        let mut stmt = self.conn.prepare(
            "SELECT idx, offset, size FROM pieces WHERE status = 'pending' ORDER BY idx",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PieceLayout {
                idx: row.get::<_, i64>(0)? as u32,
                offset: row.get::<_, i64>(1)? as u64,
                size: row.get::<_, i64>(2)? as u64,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Пометить набор piece'ов как `done` одной транзакцией.
    ///
    /// Неизвестные `idx` просто не обновятся — это нормальный кейс
    /// (воркер мог быть отменён, а его batch уже не актуален).
    pub fn commit_done_batch(&mut self, indices: &[u32]) -> IndexResult<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE pieces SET status = 'done' WHERE idx = ?")?;
            for &i in indices {
                stmt.execute(params![i as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Получить значение метаданных по ключу.
    pub fn meta_get(&self, key: &str) -> IndexResult<Option<String>> {
        let v: Option<String> = self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?", params![key], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(v)
    }

    /// Установить значение метаданных (upsert).
    pub fn meta_set(&mut self, key: &str, value: &str) -> IndexResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            params![key, value],
        )?;
        Ok(())
    }

    /// Удалить всё содержимое индекса (используется при `abort`).
    ///
    /// Сам файл `.index.brook` не удаляется — это ответственность
    /// `LocalPieceStorage::abort`. Здесь только очистка строк на случай,
    /// если индекс переиспользуется.
    pub fn delete_all(&mut self) -> IndexResult<()> {
        self.conn.execute_batch(
            "DELETE FROM pieces;
             DELETE FROM meta;",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn sample_pieces() -> Vec<PieceLayout> {
        vec![
            PieceLayout {
                idx: 0,
                offset: 0,
                size: 100,
            },
            PieceLayout {
                idx: 1,
                offset: 100,
                size: 100,
            },
            PieceLayout {
                idx: 2,
                offset: 200,
                size: 50,
            },
        ]
    }

    fn open_fresh() -> (tempfile::TempDir, PieceIndexRepository) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.index.brook");
        let repo = PieceIndexRepository::open(&path).unwrap();
        (dir, repo)
    }

    #[test]
    fn open_creates_empty_schema() {
        let (_d, repo) = open_fresh();
        assert!(!repo.is_initialized().unwrap());
        assert_eq!(repo.pending_pieces().unwrap(), vec![]);
    }

    #[test]
    fn open_reopens_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.index.brook");
        {
            let mut repo = PieceIndexRepository::open(&path).unwrap();
            repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        }
        let repo2 = PieceIndexRepository::open(&path).unwrap();
        assert!(repo2.is_initialized().unwrap());
        assert_eq!(repo2.pending_pieces().unwrap().len(), 3);
        assert_eq!(repo2.meta_get("url").unwrap().as_deref(), Some("https://x"));
    }

    #[test]
    fn init_populates_pieces_and_meta() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x/f", 250, 100, &sample_pieces())
            .unwrap();
        assert!(repo.is_initialized().unwrap());
        assert_eq!(repo.pending_pieces().unwrap(), sample_pieces());
        assert_eq!(
            repo.meta_get("url").unwrap().as_deref(),
            Some("https://x/f")
        );
        assert_eq!(repo.meta_get("total_size").unwrap().as_deref(), Some("250"));
        assert_eq!(repo.meta_get("piece_size").unwrap().as_deref(), Some("100"));
    }

    #[test]
    fn init_twice_errors() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        let err = repo.init("https://y", 1, 1, &[]).unwrap_err();
        assert!(matches!(err, IndexError::AlreadyInitialized));
    }

    #[test]
    fn commit_done_batch_marks_pieces() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        repo.commit_done_batch(&[0, 2]).unwrap();
        let pending = repo.pending_pieces().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].idx, 1);
    }

    #[test]
    fn commit_done_batch_unknown_idx_is_noop() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        repo.commit_done_batch(&[42]).unwrap();
        assert_eq!(repo.pending_pieces().unwrap().len(), 3);
    }

    #[test]
    fn commit_empty_batch_is_noop() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        repo.commit_done_batch(&[]).unwrap();
        assert_eq!(repo.pending_pieces().unwrap().len(), 3);
    }

    #[test]
    fn meta_set_and_get_roundtrip() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        assert_eq!(repo.meta_get("etag").unwrap(), None);
        repo.meta_set("etag", "\"abc123\"").unwrap();
        assert_eq!(
            repo.meta_get("etag").unwrap().as_deref(),
            Some("\"abc123\"")
        );
        // upsert
        repo.meta_set("etag", "\"def456\"").unwrap();
        assert_eq!(
            repo.meta_get("etag").unwrap().as_deref(),
            Some("\"def456\"")
        );
    }

    #[test]
    fn delete_all_clears_tables() {
        let (_d, mut repo) = open_fresh();
        repo.init("https://x", 250, 100, &sample_pieces()).unwrap();
        repo.delete_all().unwrap();
        assert!(!repo.is_initialized().unwrap());
        assert_eq!(repo.meta_get("url").unwrap(), None);
    }

    #[test]
    fn check_constraint_rejects_bad_status() {
        let (_d, repo) = open_fresh();
        let err = repo
            .conn
            .execute(
                "INSERT INTO pieces (idx, offset, size, status) VALUES (0, 0, 1, 'garbage')",
                [],
            )
            .unwrap_err();
        // SQLite вернёт constraint error — для нас важно, что оно не прошло.
        let msg = err.to_string();
        assert!(msg.contains("CHECK") || msg.contains("constraint"), "{msg}");
    }
}
