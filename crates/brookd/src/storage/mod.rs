//! Локальное хранилище загрузок: файловые примитивы, раскладка piece'ов,
//! индекс кусков на SQLite.
//!
//! Этап 1.6 — только кирпичи (низкоуровневые функции и репозиторий).
//! Сборка в `TPieceStorage` будет в 1.7.

pub mod db;
pub mod error;
pub mod factory;
pub mod files;
pub mod fs;
pub mod local;
pub mod paths;
pub mod piece_attempts;
pub mod pieces;
pub mod plan;
pub mod workers;

pub use error::{
    StorageError,
    StorageResult,
};
