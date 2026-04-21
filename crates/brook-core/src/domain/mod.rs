//! Доменный слой ядра: сущности и value-объекты.
//!
//! Здесь живут чистые типы — без сетевого и дискового I/O, без зависимостей
//! на внешний мир. Всё, что внутри `domain/`, можно сконструировать и
//! проверить без tokio-рантайма, файлов и сокетов.
//!
//! Соседи по слою:
//! - `id` — уникальные идентификаторы (newtypes над UUID): `DownloadId`,
//!   `WorkerId`, `AttemptId`.
//! - `spec` — неизменяемое описание задачи (url, target_dir, workers).
//! - `status` — единый словарь статусов для файлов / воркеров / piece'ов /
//!   attempt'ов.
//! - `progress` — снэпшот прогресса (байты, скорость, ETA).
//! - `download` — агрегат: spec + status + progress + метаданные.
//! - `event` — события, которые engine эмитит наружу.
//! - `command` — команды, которые engine принимает снаружи.
//!
//! В паттерне Hexagonal это — «центр шестиугольника»: ничего не знает ни
//! про порты, ни про адаптеры; его импортируют все, он — никого снаружи
//! своего слоя (кроме `error`, сквозного для всего крейта).

pub mod command;
pub mod download;
pub mod event;
pub mod id;
pub mod progress;
pub mod reason;
pub mod spec;
pub mod status;

pub use command::DownloadCommand;
pub use download::Download;
pub use event::DownloadEvent;
pub use id::{
    AttemptId,
    DownloadId,
    WorkerId,
};
pub use progress::Progress;
pub use reason::{
    FailureReason,
    ReasonCode,
};
pub use spec::{
    DownloadSpec,
    OnFileExistsOverride,
    default_workers,
};
pub use status::{
    AttemptStatus,
    FileStatus,
    PieceStatus,
    WorkerStatus,
};
