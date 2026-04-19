//! Абстракция над хранилищем piece'ов одной загрузки.
//!
//! Концепция: каждая загрузка пишется в два файла рядом с целевым:
//! - `<name>.data.brook` — преаллокация на полный размер, в неё воркеры
//!   пишут байты по offset'ам (`pwrite`).
//! - `<name>.index.brook` — SQLite (WAL) с картой piece'ов: какой готов,
//!   какой нет. Нужен для докачки после падения демона.
//!
//! Контракт этого трейта — **отдельный от** его реализаций. В 1.2 у нас
//! только контракт; конкретные реализации появятся в 1.9–1.10.
//!
//! ### Почему `-> impl Future + Send`, а не `async fn`
//! В edition 2024 `async fn` в трейте компилируется, но **не добавляет
//! автоматически `Send`** к возвращаемому Future. В многопоточном tokio
//! (что у нас) это быстро выстреливает в ногу: `tokio::spawn` требует
//! `Send`. Явная форма `fn ... -> impl Future<Output = _> + Send` —
//! это ровно то, во что async fn «разворачивается», плюс мы фиксируем
//! `Send` как часть контракта трейта.

use std::future::Future;

use crate::domain::DownloadSpec;
use crate::error::Result;

/// Хранилище piece'ов **одной конкретной** загрузки.
///
/// `&self` во всех методах: внутри реализации будут `Mutex`/`Connection`
/// и файловые handles, но вызывающему это не видно — он просто шлёт
/// команды. Такая обёртка называется «interior mutability».
pub trait TPieceStorage: Send + Sync {
    /// Записать байты одного piece по смещению внутри piece'а.
    ///
    /// **Инвариант**: вызов НЕ фиксирует piece как готовый и НЕ делает
    /// `fsync`. После рестарта эти байты могут пропасть — это ожидаемо,
    /// они будут перекачаны.
    fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: &[u8],
    ) -> impl Future<Output = Result<()>> + Send;

    /// Пометить набор piece'ов как полностью завершённые и зафиксировать на диске.
    ///
    /// **Инвариант (commit ⇒ persisted)**: после успешного `Ok(())` эти
    /// piece'ы гарантированно переживут kill -9 / обесточивание;
    /// скачивать их повторно не нужно.
    ///
    /// Батч (а не по одному) — чтобы амортизировать `fsync`: один fsync
    /// на десятки piece'ов, а не на каждый.
    fn commit_batch(&self, piece_indices: &[u32]) -> impl Future<Output = Result<()>> + Send;

    /// Piece'ы, которые ещё не завершены. Используется при старте, чтобы
    /// понять, что нужно докачать.
    fn pending_pieces(&self) -> impl Future<Output = Result<Vec<u32>>> + Send;

    /// Финализация успешной загрузки: переименовать `<name>.data.brook → <name>`,
    /// удалить `<name>.index.brook`.
    fn finalize(&self) -> impl Future<Output = Result<()>> + Send;

    /// Отмена: удалить `*.data.brook` и `*.index.brook`. Целевой файл, если
    /// уже существовал до старта, не трогается.
    fn abort(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Фабрика `TPieceStorage` — создаёт хранилище под конкретный spec.
///
/// **Почему `type Storage`, а не `Box<dyn TPieceStorage>`**: `async fn` /
/// `impl Future` в трейте несовместимы с `dyn` (объектами-трейтами).
/// Ассоциированный тип + `-> Self::Storage` — обходной путь: фабрика
/// статически параметризуется, `dyn` не нужен. Для dyn-случаев есть
/// крейт `async-trait`, но лишнюю зависимость добавлять не хочется.
pub trait TPieceStorageFactory: Send + Sync {
    type Storage: TPieceStorage;

    fn create(&self, spec: &DownloadSpec) -> impl Future<Output = Result<Self::Storage>> + Send;
}
