//! Локальное хранилище загрузок: файловые примитивы, раскладка piece'ов,
//! индекс кусков на SQLite.
//!
//! Этап 1.6 — только кирпичи (низкоуровневые функции и репозиторий).
//! Сборка в `TPieceStorage` будет в 1.7.

pub mod fs;
pub mod index;
pub mod local;
pub mod paths;
pub mod plan;
