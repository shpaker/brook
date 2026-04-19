//! In-memory реализация [`TQueueStore`] для тестов.
//!
//! Боевая реализация (1.8, `SqliteQueueRepository`) будет хранить очередь
//! в `brook.db`. На этапах 1.x, где SQLite ещё не подключён, этой «очереди
//! в памяти» достаточно, чтобы тестировать `DownloadManager`: вставка,
//! смена состояний, удаление, восстановление при «рестарте» (через новый
//! инстанс нельзя — он пустой; это ожидаемо, персистентность — для SQLite).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use crate::domain::{Download, DownloadId, DownloadState};
use crate::error::{Error, Result};
use crate::ports::TQueueStore;

/// In-memory очередь. Клонирует `Download` при каждой операции, чтобы
/// вызывающий не получал доступ к внутренним структурам под замком.
#[derive(Default)]
pub struct MemoryTQueueStore {
    inner: Mutex<HashMap<DownloadId, Download>>,
}

impl MemoryTQueueStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Число записей — удобно в тестах.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TQueueStore for MemoryTQueueStore {
    async fn load_all(&self) -> Result<Vec<Download>> {
        let inner = self.inner.lock().expect("mutex poisoned");
        // Сортируем по `created_at` — даёт стабильный порядок в ассертах,
        // и совпадает с тем, как позже будет вести себя SQL-реализация
        // (`ORDER BY created_at`).
        let mut all: Vec<Download> = inner.values().cloned().collect();
        all.sort_by_key(|d| d.created_at);
        Ok(all)
    }

    async fn insert(&self, download: &Download) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        if inner.contains_key(&download.id) {
            return Err(Error::Other(format!("duplicate id: {}", download.id)));
        }
        inner.insert(download.id, download.clone());
        Ok(())
    }

    async fn update_state(&self, id: DownloadId, state: DownloadState) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        let entry = inner.get_mut(&id).ok_or(Error::NotFound)?;
        entry.state = state;
        entry.updated_at = SystemTime::now();
        Ok(())
    }

    async fn remove(&self, id: DownloadId) -> Result<()> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        inner.remove(&id).ok_or(Error::NotFound)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DownloadSpec;

    fn make(url: &str) -> Download {
        Download::new(DownloadId::new(), DownloadSpec::new(url, "/tmp"))
    }

    #[tokio::test]
    async fn round_trip_insert_load_update_remove() {
        let store = MemoryTQueueStore::new();
        assert!(store.load_all().await.unwrap().is_empty());

        let a = make("https://example.com/a");
        let b = make("https://example.com/b");
        store.insert(&a).await.unwrap();
        store.insert(&b).await.unwrap();

        // load_all возвращает обе записи, отсортированные по created_at.
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 2);
        let loaded_ids: Vec<_> = loaded.iter().map(|d| d.id).collect();
        assert!(loaded_ids.contains(&a.id) && loaded_ids.contains(&b.id));

        // update_state меняет state и updated_at.
        let before = loaded.iter().find(|d| d.id == a.id).unwrap().updated_at;
        // SystemTime::now() на macOS имеет разрешение не хуже микросекунды,
        // спать не нужно — разные вызовы now() гарантированно различаются.
        store
            .update_state(a.id, DownloadState::Running)
            .await
            .unwrap();
        let after_list = store.load_all().await.unwrap();
        let after_a = after_list.iter().find(|d| d.id == a.id).unwrap();
        assert_eq!(after_a.state, DownloadState::Running);
        assert!(after_a.updated_at >= before);

        // remove — запись пропадает.
        store.remove(a.id).await.unwrap();
        let after_remove = store.load_all().await.unwrap();
        assert_eq!(after_remove.len(), 1);
        assert_eq!(after_remove[0].id, b.id);
    }

    #[tokio::test]
    async fn insert_duplicate_errors() {
        let store = MemoryTQueueStore::new();
        let d = make("https://example.com/x");
        store.insert(&d).await.unwrap();
        assert!(store.insert(&d).await.is_err());
    }

    #[tokio::test]
    async fn update_missing_errors() {
        let store = MemoryTQueueStore::new();
        let missing = DownloadId::new();
        assert!(matches!(
            store.update_state(missing, DownloadState::Paused).await,
            Err(Error::NotFound)
        ));
    }

    #[tokio::test]
    async fn remove_missing_errors() {
        let store = MemoryTQueueStore::new();
        let missing = DownloadId::new();
        assert!(matches!(store.remove(missing).await, Err(Error::NotFound)));
    }
}
