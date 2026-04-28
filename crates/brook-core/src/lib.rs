//! `brook-core` — ядро менеджера загрузок.
//!
//! Ядро следует паттерну **Hexagonal Architecture** (Ports & Adapters):
//! - [`domain`] — сущности и value-объекты (чистые типы, без I/O).
//! - [`ports`] — outbound-трейты, через которые ядро обращается наружу
//!   (хранилище piece'ов, персистентность очереди, позднее — HTTP-клиент).
//! - *Services* (этап 1.3+) — application-координаторы `DownloadManager`
//!   и `DownloadEngine`. Появятся в `src/service/`.
//! - *Adapters* — реализации `ports` — живут **вне** `brook-core`:
//!   SQLite — в `brook-daemon`, HTTP — в `brook-http`, gRPC — в `brook-api`.
//!
//! Публичный API намеренно плоский: `brook_core::FileId`,
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
    FailureReason,
    File,
    FileCommand,
    FileId,
    FileLifecycleEvent,
    FileSpec,
    FileStatus,
    MAX_WORKERS,
    PieceStatus,
    Progress,
    ProgressEvent,
    ReasonCode,
    WorkerId,
    WorkerStatus,
    compute_workers,
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
    PreparedMode,
    RangeError,
    RangeGuard,
    TFilePresenceCheck,
    THttpInspect,
    TPathPolicy,
    TPieceAttemptRepo,
    TPieceStorage,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TStreamStorage,
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
    EngineSubscriptions,
    ManagerConfig,
    RetryDecision,
    RetryPolicy,
    StreamingEngineInputs,
};
