//! Доменный слой ядра: сущности и value-объекты.
//!
//! Здесь живут чистые типы — без сетевого и дискового I/O, без зависимостей
//! на внешний мир. Всё, что внутри `domain/`, можно сконструировать и
//! проверить без tokio-рантайма, файлов и сокетов.
//!
//! Соседи по слою:
//! - `id` — уникальный идентификатор загрузки (newtype над UUID).
//! - `spec` — неизменяемое описание задачи (url, target_dir, workers).
//! - `state` — конечный автомат состояний загрузки.
//! - `progress` — снэпшот прогресса (байты, скорость, ETA).
//! - `download` — агрегат: spec + state + progress + метаданные.
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
pub mod spec;
pub mod state;

pub use command::DownloadCommand;
pub use download::Download;
pub use event::DownloadEvent;
pub use id::DownloadId;
pub use progress::Progress;
pub use spec::{
    DownloadSpec,
    default_workers,
};
pub use state::DownloadState;
