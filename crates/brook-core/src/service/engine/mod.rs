//! `DownloadEngine` — движок одной загрузки.
//!
//! Один engine владеет ровно одной загрузкой: держит фиксированный набор
//! воркеров, тянет piece'ы через [`TRangeFetch`], пишет их в [`TPieceStorage`]
//! и эмитит события во внешний мир через `broadcast`-канал.
//!
//! ## Структура модуля
//!
//! Публичный фасад ([`EngineHandle`], [`DownloadEngine`], [`EngineInputs`]
//! и друзья) и геометрические хелперы живут здесь. Логика разнесена:
//!
//! - [`supervisor`] — главный цикл range/no-range движка (`run_engine`):
//!   команды, worker-таски, агрегация прогресса, финализация.
//! - [`range`] — воркер Range-режима (`worker_range`), retry одиночного
//!   piece'а, дрейн стрима в хранилище.
//! - [`full`] — воркер no-Range режима (`worker_full`), `fetch_full`
//!   + разрезание тела по piece'ам согласно раскладке.
//! - [`streaming`] — движок unknown-size (`run_streaming_engine`),
//!   append-only загрузка без piece-учёта.
//!
//! ## Взаимодействие с внешним миром
//!
//! - Вход: [`FileCommand`] через mpsc (`EngineHandle::pause/resume/cancel`).
//! - Выход: два broadcast-канала — [`FileLifecycleEvent`] (смены статусов,
//!   `Completed`, `Failed`) и [`ProgressEvent`] (прогресс-тики). Каналы
//!   разделены, чтобы высокочастотный прогресс не конкурировал по
//!   capacity с редкими lifecycle-событиями. `broadcast` (а не `mpsc`) —
//!   одно событие видят все подписчики, без backpressure на engine.
//!
//! ## Воркеры и work-stealing
//!
//! Общая очередь `Arc<Mutex<VecDeque<u32>>>` с индексами pending-piece'ов:
//! воркеры конкурируют за неё, ровно тот, кто первым взял `lock`, берёт piece.
//! Это проще atomic-счётчика, когда очередь динамическая (piece может
//! вернуться обратно после обрыва середины).
//!
//! ## No-Range режим
//!
//! Если `inputs.accepts_ranges = false`, spawn'ится **один** воркер, который
//! зовёт [`TRangeFetch::fetch_full`] и пишет байты последовательно по piece'ам
//! согласно раскладке. Это единственный корректный путь: без Range сервер
//! отдаёт тело один раз от 0 до EOF, параллелизма на нём нет.
//!
//! ## Агрегация `Progress`
//!
//! Раз в `progress_interval` (по умолчанию 200 мс) супервизор читает
//! атомарный `bytes_done` и отправляет [`ProgressEvent::Tick`]. Частота
//! эмита ≤ `1 / progress_interval`, тем самым Watch-клиенты не захлёбываются.
//!
//! ## Retry
//!
//! Каждый воркер самостоятельно применяет [`RetryPolicy`] к транзиентным
//! ошибкам (`is_transient()`), усыпляясь через `tokio::time::sleep` на
//! рассчитанную задержку. Не-транзиентные или исчерпанный лимит попыток —
//! permanent failure: воркер шлёт супервизору сигнал, движок переходит в
//! `Failed` и останавливает всех.
//!
//! [`RetryPolicy`]: crate::service::retry::RetryPolicy

mod full;
mod range;
mod streaming;
mod supervisor;

#[cfg(all(test, feature = "test-utils"))]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{
    broadcast,
    mpsc,
};
use tokio::task::JoinHandle;

use crate::domain::{
    FileCommand,
    FileId,
    FileLifecycleEvent,
    FileSpec,
    ProgressEvent,
};
use crate::ports::{
    RangeGuard,
    TPieceAttemptRepo,
    TPieceStorage,
    TRangeFetch,
    TStreamStorage,
    TWorkerRepo,
};
use crate::service::retry::RetryPolicy;

/// Данные, которые собирает `DownloadManager` до старта движка.
///
/// Движку достаточно знать общий размер файла и размер одного piece'а —
/// всё остальное (offset и размер конкретного куска) выводится:
/// `offset(idx) = idx * piece_size`, `size(idx) = min(piece_size,
/// total_size - offset(idx))`. Явная раскладка (`Vec<PieceLayout>`)
/// живёт в адаптере ([`brookd::storage::plan`]) — ему она нужна для
/// преаллокации и SQLite-индекса.
#[derive(Debug, Clone)]
pub struct EngineInputs {
    pub spec: FileSpec,
    pub total_size: u64,
    /// Размер «обычного» piece'а. Последний piece может быть короче, если
    /// `total_size` не кратен `piece_size` — это считается помощником
    /// [`piece_size_at`].
    pub piece_size: u64,
    pub accepts_ranges: bool,
    pub guard: Option<RangeGuard>,
    /// URL после цепочки редиректов, если фабрика его резолвила. Воркеры
    /// шлют range-GET'ы именно сюда — экономим RTT на повторном резолве
    /// подписанных CDN-ссылок. `None` → используем `spec.url` напрямую.
    pub effective_url: Option<String>,
}

/// Входные данные стриминг-движка (unknown-size / no-Range).
#[derive(Debug, Clone)]
pub struct StreamingEngineInputs {
    pub spec: FileSpec,
    /// URL после цепочки редиректов (см. [`EngineInputs::effective_url`]).
    pub effective_url: Option<String>,
}

/// Абсолютный offset piece'а в файле.
#[inline]
pub(super) fn piece_offset(idx: u32, piece_size: u64) -> u64 {
    idx as u64 * piece_size
}

/// Фактический размер piece'а с индексом `idx`. Последний может быть меньше
/// `piece_size`, если общий размер не кратен.
#[inline]
pub(super) fn piece_size_at(idx: u32, piece_size: u64, total_size: u64) -> u64 {
    let start = piece_offset(idx, piece_size);
    let end = start.saturating_add(piece_size).min(total_size);
    end.saturating_sub(start)
}

/// Сколько всего piece'ов будет у файла такого размера.
#[inline]
pub(super) fn pieces_total(total_size: u64, piece_size: u64) -> u32 {
    if piece_size == 0 {
        return 0;
    }
    total_size.div_ceil(piece_size) as u32
}

/// Настройки движка. Выделены отдельно, чтобы тесты могли подставлять быстрые
/// значения (короткий retry, маленький буфер), не меняя сам engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Размер потокового буфера при записи в storage: копия из ByteStream
    /// копится, пока не перевалит за этот порог, затем — `write_piece_bytes`.
    /// Разумный диапазон 64–256 KiB.
    pub write_buffer: usize,
    /// Интервал агрегата `Progress`.
    pub progress_interval: Duration,
    /// Политика повторов для воркеров.
    pub retry: RetryPolicy,
    /// Ёмкость broadcast-канала событий. 1024 — стандарт для `Watch`.
    pub events_capacity: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            write_buffer: 128 * 1024,
            progress_interval: Duration::from_millis(200),
            retry: RetryPolicy::default(),
            events_capacity: 1024,
        }
    }
}

/// Публичный handle движка: позволяет слать команды и дождаться завершения.
///
/// Дроп handle'а **не** останавливает задачу автоматически — менеджер обязан
/// явно прислать `Cancel`, иначе engine продолжит работу до успеха / fail.
pub struct EngineHandle {
    id: FileId,
    cmd_tx: mpsc::UnboundedSender<FileCommand>,
    join: JoinHandle<()>,
}

impl EngineHandle {
    pub fn id(&self) -> FileId {
        self.id
    }

    /// Послать команду движку. Возвращает `false`, если задача уже завершилась
    /// и канал закрыт (команду применять некому).
    pub fn send(&self, cmd: FileCommand) -> bool {
        self.cmd_tx.send(cmd).is_ok()
    }

    pub fn pause(&self) -> bool {
        self.send(FileCommand::Pause)
    }
    pub fn resume(&self) -> bool {
        self.send(FileCommand::Resume)
    }
    pub fn cancel(&self) -> bool {
        self.send(FileCommand::Cancel)
    }

    /// Дождаться завершения задачи движка.
    pub async fn join(self) {
        let _ = self.join.await;
    }
}

/// Фасад старта движка.
pub struct DownloadEngine;

/// Пара broadcast-receiver'ов, которые отдаёт `DownloadEngine::spawn` —
/// один для lifecycle-событий, второй для прогресс-тиков. Менеджер
/// разливает их в соответствующие broadcast-шины наружу.
pub struct EngineSubscriptions {
    pub lifecycle: broadcast::Receiver<FileLifecycleEvent>,
    pub progress: broadcast::Receiver<ProgressEvent>,
}

impl DownloadEngine {
    /// Запустить движок для указанного файла.
    ///
    /// Возвращает [`EngineHandle`] для управления и [`EngineSubscriptions`]
    /// с двумя broadcast-receiver'ами (lifecycle / progress).
    /// Дополнительных подписчиков можно получать через `.resubscribe()`.
    pub fn spawn<S, F, WR, AR>(
        id: FileId,
        inputs: EngineInputs,
        config: EngineConfig,
        storage: Arc<S>,
        fetch: Arc<F>,
        workers_repo: Arc<WR>,
        attempts_repo: Arc<AR>,
    ) -> (EngineHandle, EngineSubscriptions)
    where
        S: TPieceStorage + Send + Sync + 'static,
        F: TRangeFetch + Send + Sync + 'static,
        WR: TWorkerRepo + Send + Sync + 'static,
        AR: TPieceAttemptRepo + Send + Sync + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (lifecycle_tx, lifecycle_rx) = broadcast::channel(config.events_capacity);
        let (progress_tx, progress_rx) = broadcast::channel(config.events_capacity);

        let join = tokio::spawn(supervisor::run_engine(
            id,
            inputs,
            config,
            storage,
            fetch,
            workers_repo,
            attempts_repo,
            cmd_rx,
            lifecycle_tx,
            progress_tx,
        ));

        (
            EngineHandle { id, cmd_tx, join },
            EngineSubscriptions {
                lifecycle: lifecycle_rx,
                progress: progress_rx,
            },
        )
    }

    /// Запустить стриминг-движок для загрузки с неизвестным `Content-Length`.
    ///
    /// Отличия от [`DownloadEngine::spawn`]:
    /// - один воркер (Range всё равно недоступен в unknown-size сценарии);
    /// - нет piece-раскладки и persisted-state воркеров/попыток в БД —
    ///   resume невозможен, логировать отдельные попытки нет смысла;
    /// - `Progress.bytes_total = 0` как сигнал «размер неизвестен»;
    ///   TUI рендерит indeterminate-gauge.
    pub fn spawn_streaming<SS, F, WR, AR>(
        id: FileId,
        inputs: StreamingEngineInputs,
        config: EngineConfig,
        stream: Arc<SS>,
        fetch: Arc<F>,
        workers_repo: Arc<WR>,
        attempts_repo: Arc<AR>,
    ) -> (EngineHandle, EngineSubscriptions)
    where
        SS: TStreamStorage + Send + Sync + 'static,
        F: TRangeFetch + Send + Sync + 'static,
        WR: TWorkerRepo + Send + Sync + 'static,
        AR: TPieceAttemptRepo + Send + Sync + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (lifecycle_tx, lifecycle_rx) = broadcast::channel(config.events_capacity);
        let (progress_tx, progress_rx) = broadcast::channel(config.events_capacity);

        let join = tokio::spawn(streaming::run_streaming_engine(
            id,
            inputs,
            config,
            stream,
            fetch,
            workers_repo,
            attempts_repo,
            cmd_rx,
            lifecycle_tx,
            progress_tx,
        ));

        (
            EngineHandle { id, cmd_tx, join },
            EngineSubscriptions {
                lifecycle: lifecycle_rx,
                progress: progress_rx,
            },
        )
    }
}
