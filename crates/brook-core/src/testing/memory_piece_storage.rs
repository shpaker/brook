//! In-memory реализация [`TPieceStorage`] для тестов.
//!
//! Идея: вместо `<name>.data.brook` на диске — `Vec<u8>` в памяти, вместо
//! SQLite-индекса — `HashSet<u32>` с множеством закоммиченных piece'ов.
//! Этого хватает, чтобы проверить поведение верхних слоёв (engine, manager)
//! без файловых syscalls и fsync'ов.
//!
//! ### Почему `std::sync::Mutex`, а не `tokio::sync::Mutex`
//! Критические секции короткие и чисто CPU-шные (копирование байт в Vec,
//! вставка в HashSet). В таких случаях блокирующий `std::sync::Mutex` быстрее
//! и проще: он не требует `await`. `tokio::sync::Mutex` нужен, когда под
//! замком есть `.await` — здесь такого нет.

use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::Mutex;

use crate::domain::DownloadSpec;
use crate::error::{
    Error,
    Result,
};
use crate::ports::{
    TPieceStorage,
    TPieceStorageFactory,
};

/// In-memory хранилище piece'ов одной загрузки.
///
/// Порядок работы типичного теста:
/// 1. `MemoryPieceStorage::new(piece_count, piece_size)` — «преаллокация».
/// 2. `write_piece_bytes(...)` — воркеры складывают байты.
/// 3. `commit_batch(...)` — помечаем готовыми.
/// 4. `finalize()` или `abort()` — завершаем.
/// 5. Ассерты через [`MemoryPieceStorage::snapshot`].
pub struct MemoryPieceStorage {
    inner: Mutex<Inner>,
}

struct Inner {
    /// Всего piece'ов у этой загрузки.
    piece_count: u32,
    /// Размер одного piece'а в байтах. Последний piece может быть короче,
    /// но для тестов равного размера этого достаточно.
    piece_size: u64,
    /// Накопленные байты по piece'ам — `piece_index -> буфер размера piece_size`.
    /// Буфер создаётся лениво при первой записи в piece.
    buffers: HashMap<u32, Vec<u8>>,
    /// Piece'ы, для которых был вызван `commit_batch`.
    committed: HashSet<u32>,
    /// Стал ли `finalize`d — после этого операции записи запрещены.
    finalized: bool,
    /// Был ли вызван `abort` — сбрасывает всё состояние.
    aborted: bool,
}

/// Срез состояния хранилища — удобно для ассертов в тестах.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPieceStorageSnapshot {
    pub piece_count: u32,
    pub committed: Vec<u32>,
    pub finalized: bool,
    pub aborted: bool,
}

impl MemoryPieceStorage {
    /// Создать хранилище под фиксированное число piece'ов.
    pub fn new(piece_count: u32, piece_size: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                piece_count,
                piece_size,
                buffers: HashMap::new(),
                committed: HashSet::new(),
                finalized: false,
                aborted: false,
            }),
        }
    }

    /// Собрать полные байты по piece'ам в порядке индексов.
    ///
    /// Нужно тестам, которые хотят проверить «что мы записали сложилось в
    /// правильный файл».
    pub fn assembled_bytes(&self) -> Vec<u8> {
        let inner = self.inner.lock().expect("mutex poisoned");
        let mut out = Vec::new();
        for i in 0..inner.piece_count {
            if let Some(buf) = inner.buffers.get(&i) {
                out.extend_from_slice(buf);
            }
        }
        out
    }

    /// Снэпшот для ассертов (отсортированный список закоммиченных).
    pub fn snapshot(&self) -> MemoryPieceStorageSnapshot {
        let inner = self.inner.lock().expect("mutex poisoned");
        let mut committed: Vec<u32> = inner.committed.iter().copied().collect();
        committed.sort_unstable();
        MemoryPieceStorageSnapshot {
            piece_count: inner.piece_count,
            committed,
            finalized: inner.finalized,
            aborted: inner.aborted,
        }
    }
}

impl TPieceStorage for MemoryPieceStorage {
    async fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        if inner.finalized {
            return Err(Error::Other("write after finalize".into()));
        }
        if inner.aborted {
            return Err(Error::Other("write after abort".into()));
        }
        if piece_index >= inner.piece_count {
            return Err(Error::Other(format!(
                "piece_index {piece_index} out of range (count = {})",
                inner.piece_count
            )));
        }
        let piece_size = inner.piece_size as usize;
        let buf = inner
            .buffers
            .entry(piece_index)
            .or_insert_with(|| vec![0u8; piece_size]);
        let start = offset_in_piece as usize;
        let end = start + bytes.len();
        if end > buf.len() {
            return Err(Error::Other(format!(
                "write past piece end: piece {piece_index}, [{start}..{end}) vs size {}",
                buf.len()
            )));
        }
        buf[start..end].copy_from_slice(bytes);
        Ok(())
    }

    async fn commit_batch(&self, piece_indices: &[u32]) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        if inner.finalized || inner.aborted {
            return Err(Error::Other("commit after finalize/abort".into()));
        }
        for idx in piece_indices {
            if *idx >= inner.piece_count {
                return Err(Error::Other(format!(
                    "commit: piece_index {idx} out of range (count = {})",
                    inner.piece_count
                )));
            }
            inner.committed.insert(*idx);
        }
        Ok(())
    }

    async fn pending_pieces(&self) -> Result<Vec<u32>> {
        let inner = self.inner.lock().expect("mutex poisoned");
        let mut out: Vec<u32> = (0..inner.piece_count)
            .filter(|i| !inner.committed.contains(i))
            .collect();
        out.sort_unstable();
        Ok(out)
    }

    async fn finalize(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        if inner.aborted {
            return Err(Error::Other("finalize after abort".into()));
        }
        let missing: Vec<u32> = (0..inner.piece_count)
            .filter(|i| !inner.committed.contains(i))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Other(format!(
                "finalize with pending pieces: {missing:?}"
            )));
        }
        inner.finalized = true;
        Ok(())
    }

    async fn abort(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        inner.buffers.clear();
        inner.committed.clear();
        inner.aborted = true;
        Ok(())
    }
}

/// Фабрика in-memory хранилищ.
///
/// `piece_count`/`piece_size` задаются на уровне фабрики: реальный расчёт
/// нарезки появится только в 1.7 (`LocalPieceStorage`), а сейчас для тестов
/// удобнее явно сказать «у каждой создаваемой загрузки будет N piece'ов».
pub struct MemoryPieceStorageFactory {
    piece_count: u32,
    piece_size: u64,
}

impl MemoryPieceStorageFactory {
    pub fn new(piece_count: u32, piece_size: u64) -> Self {
        Self {
            piece_count,
            piece_size,
        }
    }
}

impl TPieceStorageFactory for MemoryPieceStorageFactory {
    type Storage = MemoryPieceStorage;

    async fn create(&self, _spec: &DownloadSpec) -> Result<Self::Storage> {
        Ok(MemoryPieceStorage::new(self.piece_count, self.piece_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_write_commit_finalize() {
        // 3 куска по 4 байта = 12 байт всего.
        let storage = MemoryPieceStorage::new(3, 4);

        // До записи — все piece'ы в pending.
        assert_eq!(storage.pending_pieces().await.unwrap(), vec![0, 1, 2]);

        // Пишем байты во все три куска в два захода (проверяем offset_in_piece).
        storage
            .write_piece_bytes(0, 0, &[0xAA, 0xBB])
            .await
            .unwrap();
        storage
            .write_piece_bytes(0, 2, &[0xCC, 0xDD])
            .await
            .unwrap();
        storage
            .write_piece_bytes(1, 0, &[1, 2, 3, 4])
            .await
            .unwrap();
        storage
            .write_piece_bytes(2, 0, &[9, 9, 9, 9])
            .await
            .unwrap();

        // Коммит — батчем на два куска.
        storage.commit_batch(&[0, 1]).await.unwrap();
        assert_eq!(storage.pending_pieces().await.unwrap(), vec![2]);

        // Финализация раньше времени — ошибка, piece 2 ещё не закоммичен.
        assert!(storage.finalize().await.is_err());

        storage.commit_batch(&[2]).await.unwrap();
        assert!(storage.pending_pieces().await.unwrap().is_empty());

        // Теперь finalize проходит.
        storage.finalize().await.unwrap();

        // Проверяем собранные байты и снэпшот.
        assert_eq!(
            storage.assembled_bytes(),
            vec![0xAA, 0xBB, 0xCC, 0xDD, 1, 2, 3, 4, 9, 9, 9, 9]
        );
        let snap = storage.snapshot();
        assert_eq!(snap.committed, vec![0, 1, 2]);
        assert!(snap.finalized);
        assert!(!snap.aborted);
    }

    #[tokio::test]
    async fn write_after_finalize_is_rejected() {
        let s = MemoryPieceStorage::new(1, 2);
        s.write_piece_bytes(0, 0, &[1, 2]).await.unwrap();
        s.commit_batch(&[0]).await.unwrap();
        s.finalize().await.unwrap();
        assert!(s.write_piece_bytes(0, 0, &[3, 4]).await.is_err());
    }

    #[tokio::test]
    async fn abort_wipes_state_and_blocks_commit() {
        let s = MemoryPieceStorage::new(2, 4);
        s.write_piece_bytes(0, 0, &[1, 2, 3, 4]).await.unwrap();
        s.commit_batch(&[0]).await.unwrap();
        s.abort().await.unwrap();

        let snap = s.snapshot();
        assert!(snap.aborted);
        assert!(snap.committed.is_empty());
        assert_eq!(s.assembled_bytes(), Vec::<u8>::new());
        assert!(s.commit_batch(&[1]).await.is_err());
    }

    #[tokio::test]
    async fn write_out_of_bounds_errors() {
        let s = MemoryPieceStorage::new(1, 4);
        // Несуществующий piece_index.
        assert!(s.write_piece_bytes(5, 0, &[0]).await.is_err());
        // За пределы piece_size.
        assert!(s.write_piece_bytes(0, 3, &[0, 0]).await.is_err());
    }

    #[tokio::test]
    async fn factory_creates_independent_storages() {
        let factory = MemoryPieceStorageFactory::new(2, 3);
        let spec = DownloadSpec::new("https://example.com/a", "/tmp");
        let a = factory.create(&spec).await.unwrap();
        let b = factory.create(&spec).await.unwrap();

        a.write_piece_bytes(0, 0, &[1, 2, 3]).await.unwrap();
        a.commit_batch(&[0]).await.unwrap();

        // У `b` ничего не закоммичено — инстансы независимы.
        assert_eq!(a.pending_pieces().await.unwrap(), vec![1]);
        assert_eq!(b.pending_pieces().await.unwrap(), vec![0, 1]);
    }
}
