//! [`LocalPieceStorage`] — реальная реализация [`TPieceStorage`] поверх
//! файловой системы и SQLite-индекса.
//!
//! Собирает вместе кирпичи этапа 1.6:
//! - [`super::fs`] — pwrite/preallocate/available_space;
//! - [`super::plan`] — `PiecePlan` (раскладка приходит извне);
//! - [`super::paths`] — `validate_filename`/`resolve_target`;
//! - [`super::index::PieceIndexRepository`] — состояние «какие piece'ы уже
//!   зафиксированы на диске».
//!
//! ## Файлы на диске
//!
//! Для загрузки с `filename = "foo.iso"` в `target_dir = "/dl"` адаптер
//! держит ровно три пути:
//! - `"/dl/foo.iso.data.brook"` — преаллоцированный на `total_size`
//!   контейнер, куда воркеры пишут байты по offset'ам (`pwrite`).
//! - `"/dl/foo.iso.index.brook"` — SQLite с картой piece'ов.
//! - `"/dl/foo.iso"` — целевой файл, появляется только после `finalize`
//!   (атомарный `rename` из `.data.brook`).
//!
//! ## Почему `spawn_blocking` во всех методах
//!
//! `File::write_all_at` и `rusqlite::Connection` — **блокирующие** API.
//! Трейт [`TPieceStorage`] обещает `-> impl Future + Send`, а tokio-рантайм
//! не переносит долгие блокирующие операции на worker-поток сам. Чтобы не
//! «сожрать» reactor'ный поток многомегабайтной записью или fsync'ом,
//! каждая операция уезжает в `tokio::task::spawn_blocking`.
//!
//! Под замком [`std::sync::Mutex`] лежит `Inner` с `File` и `Connection`:
//! обычный блокирующий mutex здесь ок, так как все критсекции уже
//! внутри blocking-потока, а значит `.await` под локом физически невозможен.

use std::collections::HashMap;
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
    DownloadSpec,
    Error,
    Result,
    TPieceStorage,
};

use super::fs::{
    available_space,
    preallocate,
    pwrite_full,
};
use super::index::{
    IndexError,
    PieceIndexRepository,
};
use super::paths::resolve_target;
use super::plan::{
    PieceLayout,
    PiecePlan,
};

/// Локальное хранилище piece'ов одной загрузки.
///
/// Создаётся через [`LocalPieceStorage::open`]. Конструктор делает либо
/// «свежий» init (преаллокация + init индекса), либо resume (открытие
/// существующих файлов и валидация их меты). Битые/неконсистентные файлы
/// не считаются ошибкой — адаптер стартует с нуля (см. todo 1.7).
pub struct LocalPieceStorage {
    inner: Arc<Mutex<Inner>>,
    data_path: PathBuf,
    index_path: PathBuf,
    target_path: PathBuf,
    /// `piece_index → абсолютный offset в .data.brook`.
    piece_offsets: Arc<HashMap<u32, u64>>,
    /// `piece_index → размер куска в байтах` (для валидации границ записи).
    piece_sizes: Arc<HashMap<u32, u64>>,
}

struct Inner {
    /// Handle на `.data.brook`. `None` после `finalize`/`abort`.
    data: Option<File>,
    /// SQLite-соединение с индексом. `None` после `finalize`/`abort`.
    index: Option<PieceIndexRepository>,
    finalized: bool,
    aborted: bool,
}

impl LocalPieceStorage {
    /// Открыть хранилище для загрузки `filename` в `target_dir` размером
    /// `total_size` с нарезкой `plan` (источник — `url`).
    ///
    /// Поведение по состоянию рядом лежащих файлов:
    /// - оба `.data.brook`/`.index.brook` отсутствуют или битые → init с нуля;
    /// - оба существуют и мета-данные совпадают → resume: карта piece'ов
    ///   восстанавливается из индекса, уже закоммиченные piece'ы в `pending`
    ///   не попадут;
    /// - любая инконсистентность (url/total_size/piece_size в meta не совпадают,
    ///   индекс без init, `.data.brook` отсутствует при живом индексе и т.п.)
    ///   → стираем служебные файлы и стартуем заново.
    pub async fn open(
        target_dir: &Path,
        filename: &str,
        url: &str,
        total_size: u64,
        plan: &PiecePlan,
    ) -> Result<Self> {
        let target_path = resolve_target(target_dir, filename)
            .map_err(|e| Error::Other(format!("invalid filename: {e}")))?;
        let data_path = with_suffix(&target_path, ".data.brook");
        let index_path = with_suffix(&target_path, ".index.brook");
        let target_dir = target_dir.to_path_buf();
        let url = url.to_owned();
        let plan = plan.clone();

        // Всё открытие — блокирующее (statvfs, open, preallocate, SQLite).
        tokio::task::spawn_blocking(move || {
            open_blocking(
                target_dir,
                target_path,
                data_path,
                index_path,
                url,
                total_size,
                plan,
            )
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }
}

fn open_blocking(
    target_dir: PathBuf,
    target_path: PathBuf,
    data_path: PathBuf,
    index_path: PathBuf,
    url: String,
    total_size: u64,
    plan: PiecePlan,
) -> Result<LocalPieceStorage> {
    // Свободное место проверяем по родительской директории. Если её
    // ещё нет — это ошибка конфигурации, пусть вызывающий её увидит.
    let free = available_space(&target_dir)?;
    if free < total_size {
        return Err(Error::Io(io::Error::other(format!(
            "not enough free space: have {free}, need {total_size}"
        ))));
    }

    let mut index = PieceIndexRepository::open(&index_path).map_err(index_err)?;
    let use_resume = if index.is_initialized().map_err(index_err)? {
        meta_matches(&index, &url, total_size, plan.piece_size) && data_path.exists()
    } else {
        false
    };

    let (pieces, data) = if use_resume {
        // Resume: используем существующую раскладку из индекса и открываем
        // уже преаллоцированный `.data.brook` без truncate.
        let pieces = index.all_pieces().map_err(index_err)?;
        let data = OpenOptions::new().read(true).write(true).open(&data_path)?;
        (pieces, data)
    } else {
        // Fresh start: стираем возможные остатки, пересоздаём индекс и
        // преаллоцируем `.data.brook`.
        if index.is_initialized().map_err(index_err)? {
            index.delete_all().map_err(index_err)?;
        }
        // Отпускаем соединение, удаляем файл целиком (включая -wal/-shm),
        // чтобы не тащить чужие meta-строки, и открываем заново.
        drop(index);
        remove_index_files(&index_path);
        if data_path.exists() {
            std::fs::remove_file(&data_path)?;
        }

        let data = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&data_path)?;
        preallocate(&data, total_size)?;

        let mut fresh = PieceIndexRepository::open(&index_path).map_err(index_err)?;
        fresh
            .init(&url, total_size, plan.piece_size, &plan.pieces)
            .map_err(index_err)?;
        index = fresh;
        (plan.pieces.clone(), data)
    };

    let (piece_offsets, piece_sizes) = build_maps(&pieces);

    Ok(LocalPieceStorage {
        inner: Arc::new(Mutex::new(Inner {
            data: Some(data),
            index: Some(index),
            finalized: false,
            aborted: false,
        })),
        data_path,
        index_path,
        target_path,
        piece_offsets: Arc::new(piece_offsets),
        piece_sizes: Arc::new(piece_sizes),
    })
}

fn meta_matches(index: &PieceIndexRepository, url: &str, total_size: u64, piece_size: u64) -> bool {
    let m_url = index.meta_get("url").ok().flatten();
    let m_total = index.meta_get("total_size").ok().flatten();
    let m_piece = index.meta_get("piece_size").ok().flatten();
    m_url.as_deref() == Some(url)
        && m_total.as_deref() == Some(total_size.to_string().as_str())
        && m_piece.as_deref() == Some(piece_size.to_string().as_str())
}

fn build_maps(pieces: &[PieceLayout]) -> (HashMap<u32, u64>, HashMap<u32, u64>) {
    let mut offs = HashMap::with_capacity(pieces.len());
    let mut sizes = HashMap::with_capacity(pieces.len());
    for p in pieces {
        offs.insert(p.idx, p.offset);
        sizes.insert(p.idx, p.size);
    }
    (offs, sizes)
}

/// Добавить суффикс к пути без потери расширения.
///
/// `PathBuf::with_extension` заменяет расширение — нам наоборот нужно
/// приклеить `.data.brook`/`.index.brook` к полному имени.
fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s: OsString = p.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

fn index_err(e: IndexError) -> Error {
    Error::Other(format!("index: {e}"))
}

/// Удалить `.index.brook` и все WAL-спутники (`-wal`, `-shm`). Best-effort.
fn remove_index_files(index_path: &Path) {
    let _ = std::fs::remove_file(index_path);
    for suffix in ["-wal", "-shm"] {
        let mut with = index_path.as_os_str().to_os_string();
        with.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(with));
    }
}

impl TPieceStorage for LocalPieceStorage {
    async fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let base = match self.piece_offsets.get(&piece_index) {
            Some(o) => *o,
            None => {
                return Err(Error::Other(format!(
                    "piece_index {piece_index} out of range"
                )));
            }
        };
        let piece_size = self
            .piece_sizes
            .get(&piece_index)
            .copied()
            .expect("piece_sizes and piece_offsets share keys");
        let end = offset_in_piece
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::Other("offset overflow".into()))?;
        if end > piece_size {
            return Err(Error::Other(format!(
                "write past piece end: piece {piece_index}, end {end} vs size {piece_size}"
            )));
        }
        let abs = base + offset_in_piece;
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

    async fn commit_batch(&self, piece_indices: &[u32]) -> Result<()> {
        let indices = piece_indices.to_vec();
        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.finalized || guard.aborted {
                return Err(Error::Other("commit after finalize/abort".into()));
            }
            // Сначала fsync самих байт — иначе после краша индекс будет
            // врать, что piece готов, а данных на диске нет. Инвариант
            // «commit ⇒ persisted» держится именно этим порядком.
            guard
                .data
                .as_ref()
                .expect("data handle present")
                .sync_data()?;
            guard
                .index
                .as_mut()
                .expect("index present")
                .commit_done_batch(&indices)
                .map_err(index_err)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn pending_pieces(&self) -> Result<Vec<u32>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || -> Result<Vec<u32>> {
            let guard = inner.lock().expect("mutex poisoned");
            let layouts = guard
                .index
                .as_ref()
                .expect("index present")
                .pending_pieces()
                .map_err(index_err)?;
            Ok(layouts.into_iter().map(|p| p.idx).collect())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn finalize(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let index_path = self.index_path.clone();
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
            guard.index = None;
            std::fs::rename(&data_path, &target_path)?;
            remove_index_files(&index_path);
            guard.finalized = true;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }

    async fn abort(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let index_path = self.index_path.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.data = None;
            guard.index = None;
            let _ = std::fs::remove_file(&data_path);
            remove_index_files(&index_path);
            guard.aborted = true;
            Ok(())
        })
        .await
        .map_err(|e| Error::Other(format!("join: {e}")))?
    }
}

/// Заглушка: трейт-фабрику `TPieceStorageFactory` реализуем позже (этап 4.2),
/// когда `DownloadManager` сможет дотянуться до `InspectReport` и `PiecePlan`.
/// Тип здесь только чтобы не забыть о связи с `DownloadSpec`.
#[doc(hidden)]
pub fn _spec_compile_check(_s: &DownloadSpec) {}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::tempdir;

    use super::*;
    use crate::storage::plan::{
        PieceLayout,
        PiecePlan,
    };

    /// Маленький plan для тестов: 3 piece'а по 8/8/4 байт, total = 20.
    fn tiny_plan() -> (u64, PiecePlan) {
        let pieces = vec![
            PieceLayout {
                idx: 0,
                offset: 0,
                size: 8,
            },
            PieceLayout {
                idx: 1,
                offset: 8,
                size: 8,
            },
            PieceLayout {
                idx: 2,
                offset: 16,
                size: 4,
            },
        ];
        (
            20,
            PiecePlan {
                piece_size: 8,
                pieces,
            },
        )
    }

    fn read_file(path: &Path) -> Vec<u8> {
        let mut f = File::open(path).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        out
    }

    #[tokio::test]
    async fn round_trip_write_commit_finalize() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();

        let s = LocalPieceStorage::open(dir.path(), "out.bin", "https://x", total, &plan)
            .await
            .unwrap();

        // Стартовое состояние: все piece'ы pending.
        assert_eq!(s.pending_pieces().await.unwrap(), vec![0, 1, 2]);

        s.write_piece_bytes(0, 0, b"AAAA").await.unwrap();
        s.write_piece_bytes(0, 4, b"BBBB").await.unwrap();
        s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();

        s.commit_batch(&[0, 1, 2]).await.unwrap();
        assert!(s.pending_pieces().await.unwrap().is_empty());

        s.finalize().await.unwrap();

        let target = dir.path().join("out.bin");
        assert!(target.exists());
        assert!(!dir.path().join("out.bin.data.brook").exists());
        assert!(!dir.path().join("out.bin.index.brook").exists());
        let bytes = read_file(&target);
        assert_eq!(bytes, b"AAAABBBBCCCCDDDDEEEE".to_vec());
    }

    #[tokio::test]
    async fn resume_after_crash_continues_where_left() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();

        {
            let s = LocalPieceStorage::open(dir.path(), "r.bin", "https://x", total, &plan)
                .await
                .unwrap();
            s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
            s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
            s.commit_batch(&[0, 1]).await.unwrap();
            // Имитируем падение: просто дропаем storage без finalize.
        }

        let s = LocalPieceStorage::open(dir.path(), "r.bin", "https://x", total, &plan)
            .await
            .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![2]);

        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();
        s.commit_batch(&[2]).await.unwrap();
        s.finalize().await.unwrap();

        assert_eq!(
            read_file(&dir.path().join("r.bin")),
            b"AAAABBBBCCCCDDDDEEEE".to_vec()
        );
    }

    #[tokio::test]
    async fn abort_removes_sidecar_files_and_blocks_operations() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();
        let s = LocalPieceStorage::open(dir.path(), "a.bin", "https://x", total, &plan)
            .await
            .unwrap();

        s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
        s.commit_batch(&[0]).await.unwrap();
        s.abort().await.unwrap();

        assert!(!dir.path().join("a.bin.data.brook").exists());
        assert!(!dir.path().join("a.bin.index.brook").exists());
        assert!(!dir.path().join("a.bin").exists());

        assert!(s.write_piece_bytes(1, 0, b"CC").await.is_err());
        assert!(s.commit_batch(&[1]).await.is_err());
    }

    #[tokio::test]
    async fn mismatched_meta_restarts_from_scratch() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();

        {
            let s = LocalPieceStorage::open(dir.path(), "m.bin", "https://old", total, &plan)
                .await
                .unwrap();
            s.write_piece_bytes(0, 0, b"XXXXYYYY").await.unwrap();
            s.commit_batch(&[0]).await.unwrap();
        }

        // Тот же файл, но другой url в meta → адаптер должен стартовать с нуля.
        let s = LocalPieceStorage::open(dir.path(), "m.bin", "https://new", total, &plan)
            .await
            .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn write_past_piece_end_is_error() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();
        let s = LocalPieceStorage::open(dir.path(), "b.bin", "https://x", total, &plan)
            .await
            .unwrap();

        // Piece 2 размером 4 байта — запись 5 байт вылезает.
        assert!(s.write_piece_bytes(2, 0, b"EEEEE").await.is_err());
        // Несуществующий piece.
        assert!(s.write_piece_bytes(99, 0, b"Z").await.is_err());
    }

    #[tokio::test]
    async fn commit_persists_across_reopen_without_finalize() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();

        {
            let s = LocalPieceStorage::open(dir.path(), "p.bin", "https://x", total, &plan)
                .await
                .unwrap();
            s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
            s.commit_batch(&[0]).await.unwrap();
        }

        let s = LocalPieceStorage::open(dir.path(), "p.bin", "https://x", total, &plan)
            .await
            .unwrap();
        assert_eq!(s.pending_pieces().await.unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn write_after_finalize_is_rejected() {
        let dir = tempdir().unwrap();
        let (total, plan) = tiny_plan();
        let s = LocalPieceStorage::open(dir.path(), "f.bin", "https://x", total, &plan)
            .await
            .unwrap();

        s.write_piece_bytes(0, 0, b"AAAABBBB").await.unwrap();
        s.write_piece_bytes(1, 0, b"CCCCDDDD").await.unwrap();
        s.write_piece_bytes(2, 0, b"EEEE").await.unwrap();
        s.commit_batch(&[0, 1, 2]).await.unwrap();
        s.finalize().await.unwrap();

        assert!(s.write_piece_bytes(0, 0, b"ZZZZ").await.is_err());
        assert!(s.commit_batch(&[0]).await.is_err());
    }
}
