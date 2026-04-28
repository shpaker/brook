//! In-memory реализации портов для тестов.
//!
//! Модуль виден только под feature `test-utils` — боевой код ядра и бинарей
//! от него не зависит. Реализации здесь сознательно простые: они не про
//! производительность и не про реализм I/O, а про то, чтобы поверх чистой
//! логики ядра (engine, manager) можно было написать быстрые юнит-тесты
//! без файлов, SQLite и сети.

pub mod file_presence;
pub mod memory_piece_storage;
pub mod memory_queue_store;
pub mod memory_tracking;
pub mod mock_fetch;

pub use file_presence::AlwaysPresent;
pub use memory_piece_storage::{
    MemoryPieceStorage,
    MemoryPieceStorageFactory,
    MemoryStreamStorage,
};
pub use memory_queue_store::MemoryTQueueStore;
pub use memory_tracking::{
    MemoryAttemptRepo,
    MemoryAttemptRow,
    MemoryWorkerRepo,
    MemoryWorkerRow,
};
pub use mock_fetch::{
    FetchOutcome,
    MockRangeFetch,
    sequential_bytes,
};
