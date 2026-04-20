//! Application-services ядра — координаторы между доменными типами и портами.
//!
//! Сейчас здесь живёт только чистая доменная логика без I/O. Большие сервисы
//! (`DownloadEngine`, `DownloadManager`) появятся на этапах 1.10–1.13.

mod engine;
mod manager;
mod retry;

pub use engine::{
    DownloadEngine,
    EngineConfig,
    EngineHandle,
    EngineInputs,
};
pub use manager::{
    DownloadManager,
    ManagerConfig,
};
pub use retry::{
    CrashLoopGuard,
    CrashLoopState,
    RetryDecision,
    RetryPolicy,
};
