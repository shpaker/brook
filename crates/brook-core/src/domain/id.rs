//! Идентификатор загрузки.
//!
//! **Newtype pattern** — обёртка с нулевой стоимостью времени выполнения
//! (`struct DownloadId(Uuid)` в памяти занимает ровно столько же, сколько `Uuid`).
//! Смысл: `fn remove(id: DownloadId)` нельзя случайно вызвать с `WorkerId`,
//! даже если оба внутри — `Uuid`. Компилятор заставит распаковать и упаковать явно.
//!
//! В `todo.md` стоит пометка `(xid)` — короткий sortable-id. Пока стартуем на
//! `uuid::Uuid` (уже в workspace deps); миграция на xid тривиальна —
//! поменять тип внутри newtype'а, публичный API не поменяется.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// Глобально-уникальный идентификатор загрузки.
///
/// Derive-макросы:
/// - `Debug`  — автоматический `{:?}` для логов.
/// - `Clone, Copy` — `Uuid` — это 16 байт, дёшево копируется, owner-ship не нужен.
/// - `PartialEq, Eq` — сравнение по значению.
/// - `Hash` — можно класть в `HashMap`/`HashSet`.
/// - `PartialOrd, Ord` — сортировка (для стабильного вывода в списках).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DownloadId(Uuid);

impl DownloadId {
    /// Новый случайный id (UUIDv4).
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Доступ к внутреннему `Uuid` — например, чтобы положить в БД как BLOB.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// `Default::default()` возвращает свежий id. Полезно для тестов и builder'ов.
impl Default for DownloadId {
    fn default() -> Self {
        Self::new()
    }
}

/// `Display` — «человеческое» представление (через `{}` и `.to_string()`).
/// Делегируем `Display` у `Uuid`.
impl fmt::Display for DownloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Парсинг из строки (`"550e8400-...".parse::<DownloadId>()?`).
/// `type Err = uuid::Error` — заявляем, какую ошибку возвращает парсер.
impl FromStr for DownloadId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_are_unique() {
        // На практике — астрономически маленькая вероятность совпадения
        // двух UUIDv4. Если упало — лотерея.
        assert_ne!(DownloadId::new(), DownloadId::new());
    }

    #[test]
    fn display_parse_roundtrip() {
        let id = DownloadId::new();
        let s = id.to_string();
        let parsed: DownloadId = s.parse().expect("valid uuid string");
        assert_eq!(id, parsed);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!("not-a-uuid".parse::<DownloadId>().is_err());
    }
}
