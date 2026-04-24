//! Доменный слой ядра: сущности и value-объекты.
//!
//! Здесь живут чистые типы — без сетевого и дискового I/O, без зависимостей
//! на внешний мир. Всё, что внутри `domain/`, можно сконструировать и
//! проверить без tokio-рантайма, файлов и сокетов.
//!
//! Соседи по слою:
//! - `id` — уникальные идентификаторы (newtypes над UUID): `FileId`,
//!   `WorkerId`, `AttemptId`.
//! - `spec` — неизменяемое описание задачи (url, target_dir, workers).
//! - `status` — единый словарь статусов для файлов / воркеров / piece'ов /
//!   attempt'ов.
//! - `progress` — снэпшот прогресса (байты, скорость, ETA).
//! - `file` — агрегат: spec + status + метаданные.
//! - `event` — события, которые engine эмитит наружу.
//! - `command` — команды, которые engine принимает снаружи.
//!
//! В паттерне Hexagonal это — «центр шестиугольника»: ничего не знает ни
//! про порты, ни про адаптеры; его импортируют все, он — никого снаружи
//! своего слоя (кроме `error`, сквозного для всего крейта).

pub mod command;
pub mod event;
pub mod file;
pub mod id;
pub mod progress;
pub mod reason;
pub mod spec;
pub mod status;

pub use command::FileCommand;
pub use event::{
    FileLifecycleEvent,
    ProgressEvent,
};
pub use file::File;
pub use id::{
    AttemptId,
    FileId,
    WorkerId,
};
pub use progress::{
    BAR_SEGMENTS,
    BarState,
    Progress,
};
pub use reason::{
    FailureReason,
    ReasonCode,
};
pub use spec::{
    FileSpec,
    MAX_WORKERS,
    compute_workers,
};
pub use status::{
    AttemptStatus,
    FileStatus,
    PieceStatus,
    WorkerStatus,
};
