//! `brook-core` — ядро менеджера загрузок.
//!
//! Ядро следует паттерну **Hexagonal Architecture** (Ports & Adapters):
//! - [`domain`] — сущности и value-объекты (чистые типы, без I/O).
//! - [`ports`] — outbound-трейты, через которые ядро обращается наружу
//!   (хранилище piece'ов, персистентность очереди, позднее — HTTP-клиент).
//! - *Services* (этап 1.3+) — application-координаторы `DownloadManager`
//!   и `DownloadEngine`. Появятся в `src/service/`.
//! - *Adapters* — реализации `ports` — живут **вне** `brook-core`:
//!   SQLite/HTTP — в `brookd`, gRPC — в `brook-api`.
//!
//! Публичный API намеренно плоский: `brook_core::DownloadId`,
//! `brook_core::TPieceStorage` и т.п. Внутренняя раскладка по папкам —
//! чтобы слои не смешивались; перекроить модули без breaking change легко.

mod domain;
mod error;
mod ports;
mod service;

/// In-memory реализации портов для юнит-тестов. Видны только под feature
/// `test-utils`, чтобы боевые бинари не тянули тестовый код.
#[cfg(feature = "test-utils")]
pub mod testing;

pub use domain::{
    AttemptId,
    AttemptStatus,
    Download,
    DownloadCommand,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    FailureReason,
    FileStatus,
    OnFileExistsOverride,
    PieceStatus,
    Progress,
    ReasonCode,
    WorkerId,
    WorkerStatus,
    default_workers,
};
pub use error::{
    Error,
    Result,
};
pub use ports::{
    AttemptRecord,
    ByteStream,
    InspectError,
    InspectReport,
    NoopAttemptRepo,
    NoopWorkerRepo,
    PreparedDownload,
    RangeError,
    RangeGuard,
    THttpInspect,
    TPieceAttemptRepo,
    TPieceStorage,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TWorkerRepo,
    WorkerRecord,
};
pub use service::{
    CrashLoopGuard,
    CrashLoopState,
    DownloadEngine,
    DownloadManager,
    EngineConfig,
    EngineHandle,
    EngineInputs,
    ManagerConfig,
    RetryDecision,
    RetryPolicy,
};
