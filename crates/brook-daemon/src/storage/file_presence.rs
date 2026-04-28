//! Адаптер [`TFilePresenceCheck`] поверх локальной FS.
//!
//! Тонкая обёртка над `tokio::fs::metadata`: нужен только бинарный
//! ответ. Любой I/O-сбой (permission denied, размонтированный том)
//! трактуется как «не существует» — у манагера на этот случай уже есть
//! правильное поведение (перевести запись в Failed).

use std::path::Path;

use async_trait::async_trait;
use brook_core::TFilePresenceCheck;

pub struct LocalFilePresence;

#[async_trait]
impl TFilePresenceCheck for LocalFilePresence {
    async fn exists(&self, path: &Path) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }
}
