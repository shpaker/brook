//! [`LocalStreamStorage`] — реализация [`TStreamStorage`] для unknown-size
//! загрузок (сервер не прислал `Content-Length`).
//!
//! В отличие от [`super::piece::LocalPieceStorage`]: один append-файл,
//! без преаллокации, без piece-строк в БД. Resume не поддерживается —
//! при повторном открытии `.data.brook` truncate'ится.

use std::fs::{
    File,
    OpenOptions,
};
use std::io::Write;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    Arc,
    Mutex,
};

use brook_core::{
    Result as CoreResult,
    TStreamStorage,
};

use super::layout::with_suffix;
use super::verify::{
    fd_dev_ino,
    verify_data_file_present,
};
use crate::storage::error::{
    StorageError,
    StorageResult,
};
use crate::storage::paths::resolve_target;

pub struct LocalStreamStorage {
    inner: Arc<Mutex<StreamInner>>,
    data_path: PathBuf,
    target_path: PathBuf,
}

struct StreamInner {
    data: Option<File>,
    finalized: bool,
    aborted: bool,
    /// `(dev, ino)` файла на момент `open` — для детекта удаления
    /// `.data.brook` пользователем посреди стрима. См. [`super::verify`].
    expected_dev: u64,
    expected_ino: u64,
}

impl LocalStreamStorage {
    /// Открыть append-хранилище. Любые старые байты в `.data.brook`
    /// отбрасываются (streaming не умеет resume — при перезапуске
    /// начинаем с нуля; это не regression, обычный HTTP-поток без Range
    /// всё равно не переиспользуем).
    pub async fn open_streaming(target_dir: &Path, filename: &str) -> CoreResult<Self> {
        Ok(Self::open_inner(target_dir, filename).await?)
    }

    async fn open_inner(target_dir: &Path, filename: &str) -> StorageResult<Self> {
        let target_path = resolve_target(target_dir, filename)?;
        let data_path = with_suffix(&target_path, ".data.brook");
        let data_path_for_blocking = data_path.clone();
        let data = tokio::task::spawn_blocking(move || -> StorageResult<File> {
            // truncate=true — streaming-mode не делает resume.
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&data_path_for_blocking)?;
            Ok(f)
        })
        .await??;
        let (expected_dev, expected_ino) = fd_dev_ino(&data)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(StreamInner {
                data: Some(data),
                finalized: false,
                aborted: false,
                expected_dev,
                expected_ino,
            })),
            data_path,
            target_path,
        })
    }
}

impl TStreamStorage for LocalStreamStorage {
    async fn append_chunk(&self, bytes: &[u8]) -> CoreResult<()> {
        Ok(self.append_chunk_inner(bytes).await?)
    }

    async fn finalize(&self) -> CoreResult<()> {
        Ok(self.finalize_inner().await?)
    }

    async fn abort(&self) -> CoreResult<()> {
        Ok(self.abort_inner().await?)
    }
}

impl LocalStreamStorage {
    async fn append_chunk_inner(&self, bytes: &[u8]) -> StorageResult<()> {
        let bytes = bytes.to_vec();
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.finalized {
                return Err(StorageError::AfterFinalize { op: "append" });
            }
            if guard.aborted {
                return Err(StorageError::AfterAbort { op: "append" });
            }
            verify_data_file_present(&data_path, guard.expected_dev, guard.expected_ino)?;
            let file = guard
                .data
                .as_mut()
                .expect("data handle present while !finalized && !aborted");
            file.write_all(&bytes)?;
            Ok(())
        })
        .await?
    }

    async fn finalize_inner(&self) -> StorageResult<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        let target_path = self.target_path.clone();
        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            if guard.aborted {
                return Err(StorageError::AfterAbort { op: "finalize" });
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
        .await?
    }

    async fn abort_inner(&self) -> StorageResult<()> {
        let inner = Arc::clone(&self.inner);
        let data_path = self.data_path.clone();
        tokio::task::spawn_blocking(move || -> StorageResult<()> {
            let mut guard = inner.lock().expect("mutex poisoned");
            guard.data = None;
            let _ = std::fs::remove_file(&data_path);
            guard.aborted = true;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use brook_core::TStreamStorage;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    #[cfg(unix)]
    async fn deleting_data_file_makes_append_fail() {
        let dir = tempdir().unwrap();
        let s = LocalStreamStorage::open_streaming(dir.path(), "s.bin")
            .await
            .unwrap();

        s.append_chunk(b"hello ").await.unwrap();

        std::fs::remove_file(dir.path().join("s.bin.data.brook")).unwrap();

        let err = s.append_chunk(b"world").await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("data file missing"), "unexpected: {msg}");
        assert!(msg.contains("s.bin.data.brook"), "missing path: {msg}");
    }
}
