//! `brook-core` — ядро менеджера загрузок.
//!
//! Слой без сетевого и дискового I/O по умолчанию: здесь живут
//! **доменные типы** (этап 1.1) и **трейты абстракций** (этап 1.2).
//! Конкретные реализации (HTTP-probe, engine, SQLite-хранилища)
//! приходят в следующих этапах roadmap'а.
//!
//! Публичный API намеренно плоский: `brook_core::DownloadId`,
//! `brook_core::TPieceStorage` и т.п. Внутренняя раскладка по файлам —
//! просто для читабельности; перекроить модули без breaking change легко.

mod command;
mod download;
mod error;
mod event;
mod id;
mod piece_storage;
mod progress;
mod queue_store;
mod spec;
mod state;

pub use command::DownloadCommand;
pub use download::Download;
pub use error::{Error, Result};
pub use event::DownloadEvent;
pub use id::DownloadId;
pub use piece_storage::{TPieceStorage, TPieceStorageFactory};
pub use progress::Progress;
pub use queue_store::TQueueStore;
pub use spec::{DownloadSpec, HeaderPair, default_workers};
pub use state::DownloadState;
