//! Порты ядра: трейты, через которые ядро обращается наружу.
//!
//! Это outbound-порты в смысле Hexagonal Architecture: ядро — клиент
//! (оно *вызывает* эти трейты), а реализация-адаптер — сервер (SQLite,
//! файловая система, S3, in-memory для тестов). Адаптеры живут **вне**
//! `brook-core` — в `brookd` или в отдельных крейтах; ядро не знает про
//! их существование.
//!
//! Соседи по слою:
//! - `piece_storage` — запись и индексирование кусков одной загрузки.
//! - `queue_store` — персистентность глобальной очереди.
//! - `http` — осмотр URL и потоковое получение байт (в т.ч. Range).
//!
//! Всё, что здесь живёт, — это только `trait` и сопутствующие типы.
//! Никаких `impl for ...` конкретных бэкендов в ядре нет и быть не должно.

pub mod http;
pub mod piece_storage;
pub mod queue_store;

pub use http::{
    ByteStream,
    InspectError,
    InspectReport,
    RangeError,
    RangeGuard,
    THttpInspect,
    TRangeFetch,
};
pub use piece_storage::{
    PreparedDownload,
    TPieceStorage,
    TPieceStorageFactory,
};
pub use queue_store::TQueueStore;
