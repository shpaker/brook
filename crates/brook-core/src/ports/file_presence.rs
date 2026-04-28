//! Outbound-порт «файл существует на диске».
//!
//! Используется `manager::list_recently` / `manager::list_files` для
//! ленивого аудита Done-записей: если фактического файла нет, запись
//! переводится в `Failed` с reason'ом `FileMissing`. Порт намеренно
//! минимальный — никаких `metadata`, `mtime`, `size`-полей: ядру нужен
//! только бинарный ответ «лежит / не лежит».
//!
//! Реализация в `brook-daemon` (`LocalFilePresence`) — тонкая обёртка
//! над `tokio::fs::metadata`. В тестах подменяется fake-имплементацией,
//! которая возвращает контролируемый ответ по списку «отсутствующих»
//! путей (см. unit-тесты `manager::audit_done_presence`).
//!
//! `#[async_trait]` нужен для `Arc<dyn TFilePresenceCheck>` — без него
//! `async fn` в трейте не объект-безопасна, а менеджеру удобнее держать
//! проверку как dyn (порт с одним коротким методом не стоит ещё одного
//! generic-параметра в подписи `DownloadManager`).

use std::path::Path;

use async_trait::async_trait;

/// Бинарная проверка существования файла по пути. Любой I/O-сбой
/// (permission denied, broken FS, размонтированный том) трактуется как
/// «не существует» — для целей UX это единственное безопасное
/// поведение: если мы не можем подтвердить наличие файла, значит
/// открыть его пользователь тоже не сможет.
#[async_trait]
pub trait TFilePresenceCheck: Send + Sync {
    async fn exists(&self, path: &Path) -> bool;
}
