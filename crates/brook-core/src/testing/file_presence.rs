//! Тестовая реализация [`TFilePresenceCheck`].
//!
//! `AlwaysPresent` отвечает «файл есть» на любой путь — для большинства
//! тестов это естественный default: они не оперируют физическими
//! файлами, и Done-записи у них «полные». Тесты, которым нужен fake
//! «файл удалён», могут собрать собственную реализацию (см. unit-тесты
//! `manager::audit_done_presence`).

use std::path::Path;

use async_trait::async_trait;

use crate::ports::TFilePresenceCheck;

/// Считать любой путь существующим. Подходит для тестов, которые не
/// дёргают аудит явно: Done-записи остаются Done.
pub struct AlwaysPresent;

#[async_trait]
impl TFilePresenceCheck for AlwaysPresent {
    async fn exists(&self, _path: &Path) -> bool {
        true
    }
}
