//! Локальные реализации [`TPieceStorage`] и [`TStreamStorage`].
//!
//! Два несвязанных адаптера жили в одном файле — теперь разнесены:
//! - [`LocalPieceStorage`] — random-access с преаллокацией и piece-картой
//!   в `brook.db` (классический download-менеджер).
//! - [`LocalStreamStorage`] — append-only, для unknown-size (`Content-Length`
//!   отсутствует); resume не поддерживается.
//!
//! Вспомогательные модули:
//! - [`layout`] — арифметика piece'ов (`piece_count`, `offset_for`,
//!   `size_for`), хелпер `with_suffix` для `.data.brook`.
//! - [`preallocate`] — `spawn_blocking`-обёртка над free-space check +
//!   `open` + `truncate` + `preallocate` (возвращает готовый `File`).
//!
//! [`TPieceStorage`]: brook_core::TPieceStorage
//! [`TStreamStorage`]: brook_core::TStreamStorage

mod layout;
mod piece;
mod preallocate;
mod stream;
mod verify;

pub use piece::LocalPieceStorage;
pub use stream::LocalStreamStorage;
