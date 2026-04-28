//! Абстракция над хранилищем piece'ов одной загрузки.
//!
//! Концепция: рядом с целевым именем живёт `<name>.data.brook` —
//! преаллоцированный контейнер на полный размер, в который воркеры пишут
//! байты по offset'ам (`pwrite`). Карта готовых piece'ов и вся
//! persisted-метаинформация лежат в общей `./brook.db` (см.
//! [`crate::ports::TQueueStore`] и адаптерный слой в `brook-daemon`); трейт
//! про это знать не обязан — он оперирует номерами piece'ов.
//!
//! Контракт этого трейта — **отдельный от** его реализаций. Конкретные
//! реализации (`LocalPieceStorage` и его фабрика) живут в `brook-daemon`.
//!
//! ### Почему `-> impl Future + Send`, а не `async fn`
//! В edition 2024 `async fn` в трейте компилируется, но **не добавляет
//! автоматически `Send`** к возвращаемому Future. В многопоточном tokio
//! (что у нас) это быстро выстреливает в ногу: `tokio::spawn` требует
//! `Send`. Явная форма `fn ... -> impl Future<Output = _> + Send` —
//! это ровно то, во что async fn «разворачивается», плюс мы фиксируем
//! `Send` как часть контракта трейта.

use std::future::Future;

use bytes::Bytes;

use crate::domain::{
    FileId,
    FileSpec,
};
use crate::error::Result;
use crate::ports::RangeGuard;

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
    ///
    /// Принимаем `Bytes` по владению (а не `&[u8]`), чтобы адаптеры
    /// могли переехать в `spawn_blocking` без `.to_vec()`: `Bytes` —
    /// refcounted zero-copy буфер, клонирование/передача по владению
    /// не копирует байты. Сеть (reqwest) уже отдаёт `Bytes`, так что
    /// путь «сокет → pwrite» становится по-настоящему zero-copy
    /// после финального пакетирования в engine.
    fn write_piece_bytes(
        &self,
        piece_index: u32,
        offset_in_piece: u64,
        bytes: Bytes,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Пометить piece как полностью завершённый и зафиксировать на диске.
    ///
    /// **Инвариант (commit ⇒ persisted)**: после успешного `Ok(())` piece
    /// гарантированно переживёт kill -9 / обесточивание; скачивать его
    /// повторно не нужно.
    fn commit_done(&self, piece_index: u32) -> impl Future<Output = Result<()>> + Send;

    /// Piece'ы, которые ещё не завершены. Используется при старте, чтобы
    /// понять, что нужно докачать.
    fn pending_pieces(&self) -> impl Future<Output = Result<Vec<u32>>> + Send;

    /// Финализация успешной загрузки: переименовать `<name>.data.brook → <name>`,
    /// очистить piece-строки загрузки в `brook.db`.
    fn finalize(&self) -> impl Future<Output = Result<()>> + Send;

    /// Отмена: удалить `<name>.data.brook` и стереть piece-строки загрузки
    /// в `brook.db`. Целевой файл, если уже существовал до старта, не трогается.
    fn abort(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Хранилище потоковой загрузки (unknown-size / no-Range).
///
/// Используется, когда сервер не сообщил `Content-Length` и/или не
/// поддерживает Range. Один append-файл, без преаллокации и piece-строк
/// в БД. Resume невозможен — при перезапуске хранилище truncate'ится.
pub trait TStreamStorage: Send + Sync {
    /// Добавить очередной кусок байт в конец файла.
    fn append_chunk(&self, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Финализация: fsync + rename `<name>.data.brook → <name>`.
    fn finalize(&self) -> impl Future<Output = Result<()>> + Send;

    /// Удалить `<name>.data.brook` — аналог `TPieceStorage::abort`.
    fn abort(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Режим подготовленной загрузки: известный размер (piece-based) или
/// стриминговый (append-only).
#[derive(Debug)]
pub enum PreparedMode {
    /// Known-size: классическая Range-раскладка по piece'ам.
    Known {
        /// Общий размер файла (из `Content-Length` / Content-Range).
        total_size: u64,
        /// Размер «обычного» piece'а (последний может быть короче).
        piece_size: u64,
        /// Поддерживает ли источник Range-запросы.
        accepts_ranges: bool,
        /// ETag/Last-Modified для защиты piece'ов от мутации источника.
        guard: Option<RangeGuard>,
    },
    /// Streaming: `Content-Length` неизвестен. Один воркер, append-mode,
    /// indeterminate-progress, без resume.
    Streaming,
}

/// Результат «предстарта» загрузки: всё, что менеджеру нужно узнать об
/// источнике и нарезке, плюс готовое хранилище piece'ов или стрима.
///
/// Фабрика делает inspect источника, считает раскладку и открывает
/// storage одной операцией — `DownloadManager` видит этот бандл как
/// единый шаг, без прямой зависимости от `THttpInspect` или `plan_pieces`.
#[derive(Debug)]
pub struct PreparedDownload<S, SS>
where
    S: TPieceStorage,
    SS: TStreamStorage,
{
    /// Режим и сопутствующие параметры (Known/Streaming).
    pub mode: PreparedMode,
    /// Piece-хранилище для Known-режима. В Streaming — `None`.
    pub piece_storage: Option<S>,
    /// Streaming-хранилище для Streaming-режима. В Known — `None`.
    pub stream_storage: Option<SS>,
    /// Имя файла, которое фабрика резолвила (из spec / Content-Disposition / URL).
    pub resolved_filename: String,
    /// URL после цепочки редиректов (`None` — редиректов не было).
    /// Воркеры шлют range-GET'ы именно на этот URL, что экономит RTT
    /// на повторном резолве подписанных CDN-ссылок.
    pub effective_url: Option<String>,
}

/// Фабрика `TPieceStorage` — инкапсулирует всё, что нужно сделать до
/// старта движка: inspect URL, расчёт нарезки, открытие хранилища.
///
/// **Почему `type Storage`, а не `Box<dyn TPieceStorage>`**: `async fn` /
/// `impl Future` в трейте несовместимы с `dyn` (объектами-трейтами).
/// Ассоциированный тип + `-> Self::Storage` — обходной путь: фабрика
/// статически параметризуется, `dyn` не нужен. Для dyn-случаев есть
/// крейт `async-trait`, но лишнюю зависимость добавлять не хочется.
///
/// **Зачем `prepare`, а не `create(spec) + отдельный inspect`**: движку
/// и хранилищу нужны одни и те же метаданные источника (`total_size`,
/// `piece_size`, `accepts_ranges`). Вычислять их дважды — лишние HEAD-ы
/// и риск рассогласования; держать inspect-порт в ядре рядом с
/// хранилищем — смешение ответственностей. Один метод, который отдаёт
/// всё нужное для запуска, — самый дешёвый инвариант для менеджера.
pub trait TPieceStorageFactory: Send + Sync {
    type Storage: TPieceStorage;
    type StreamStorage: TStreamStorage;

    /// Синхронная фаза «предстарта», которую `DownloadManager` вызывает
    /// из `add()` **до** вставки записи в очередь через `TQueueStore`.
    ///
    /// Делает один HEAD к источнику, резолвит имя файла
    /// (`spec.filename` → `Content-Disposition` → URL-tail), валидирует
    /// его и проверяет, что целевой файл ещё не существует. Все
    /// inspect-поля (size, etag, last_modified, effective_url,
    /// accepts_ranges) плюс resolved-имя персистятся здесь же: дальше
    /// `prepare()` читает их из БД и больше в сеть не ходит.
    ///
    /// Ошибки:
    /// * [`Error::FileExists`] — в `target_dir` уже лежит файл с
    ///   таким именем; клиент получает `AlreadyExists` из RPC `Add`
    ///   и открывает rename-модалку.
    /// * прочие сетевые / валидационные — маппятся менеджером в
    ///   обычные RPC-ошибки.
    ///
    /// Возвращает итоговое имя файла (то же, что потом попадает в
    /// `spec.filename` записи в очереди).
    fn resolve(&self, id: FileId, spec: &FileSpec) -> impl Future<Output = Result<String>> + Send;

    /// Подготовить piece-хранилище.
    ///
    /// `id` нужен фабрике, чтобы связать persisted-state загрузки
    /// в `brook.db` (строку в `files`, а начиная со stage 4 —
    /// и piece-таблицу) с конкретным открытым хранилищем. Без id
    /// фабрика не смогла бы писать inspect-колонки `files` или
    /// делать resume через общий `SharedDb`.
    ///
    /// Предполагает, что `resolve()` уже был вызван для этого `id`:
    /// `spec.filename` заполнено, inspect-поля лежат в inspect-колонках
    /// `files`. В обычном пути `prepare()` не делает сетевых запросов.
    fn prepare(
        &self,
        id: FileId,
        spec: &FileSpec,
    ) -> impl Future<Output = Result<PreparedDownload<Self::Storage, Self::StreamStorage>>> + Send;

    /// Удалить рядом-лежащие артефакты загрузки (`.data.brook` и т.п.)
    /// для пары `(target_dir, filename)` без открытия хранилища.
    /// Идемпотентно: отсутствие файла — Ok. Вызывается `manager::remove`
    /// для inactive-записей (Failed/Cancelled/Pending в waiting), чтобы
    /// частичник не оставался на диске. Для активных engine
    /// `.data.brook` уже сносит штатный `abort`-путь.
    fn wipe_artifacts(&self, spec: &FileSpec) -> impl Future<Output = Result<()>> + Send;
}
