//! Идентификатор файла.
//!
//! **Newtype pattern** — обёртка с нулевой стоимостью времени выполнения
//! (`struct FileId(Uuid)` в памяти занимает ровно столько же, сколько `Uuid`).
//! Смысл: `fn remove(id: FileId)` нельзя случайно вызвать с `WorkerId`,
//! даже если оба внутри — `Uuid`. Компилятор заставит распаковать и упаковать явно.
//!
//! В `todo.md` стоит пометка `(xid)` — короткий sortable-id. Пока стартуем на
//! `uuid::Uuid` (уже в workspace deps); миграция на xid тривиальна —
//! поменять тип внутри newtype'а, публичный API не поменяется.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// Глобально-уникальный идентификатор файла.
///
/// Derive-макросы:
/// - `Debug`  — автоматический `{:?}` для логов.
/// - `Clone, Copy` — `Uuid` — это 16 байт, дёшево копируется, owner-ship не нужен.
/// - `PartialEq, Eq` — сравнение по значению.
/// - `Hash` — можно класть в `HashMap`/`HashSet`.
/// - `PartialOrd, Ord` — сортировка (для стабильного вывода в списках).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId(Uuid);

impl FileId {
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
impl Default for FileId {
    fn default() -> Self {
        Self::new()
    }
}

/// `Display` — «человеческое» представление (через `{}` и `.to_string()`).
/// Делегируем `Display` у `Uuid`.
impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Парсинг из строки (`"550e8400-...".parse::<FileId>()?`).
/// `type Err = uuid::Error` — заявляем, какую ошибку возвращает парсер.
impl FromStr for FileId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Идентификатор одного воркера в рамках одной engine-сессии.
///
/// Пересоздаётся на каждый старт движка: после паузы/рестарта предыдущий
/// `WorkerId` уходит в историю со статусом `paused`, а новые воркеры
/// получают свежие идентификаторы. Это даёт стабильный ключ для
/// аналитики по попыткам конкретного slot-а.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for WorkerId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Идентификатор одной попытки (attempt) скачать конкретный piece
/// конкретным воркером.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(Uuid);

impl AttemptId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for AttemptId {
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
        assert_ne!(FileId::new(), FileId::new());
    }

    #[test]
    fn display_parse_roundtrip() {
        let id = FileId::new();
        let s = id.to_string();
        let parsed: FileId = s.parse().expect("valid uuid string");
        assert_eq!(id, parsed);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!("not-a-uuid".parse::<FileId>().is_err());
    }
}
