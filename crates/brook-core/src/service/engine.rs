//! `DownloadEngine` — движок одной загрузки.
//!
//! Один engine владеет ровно одной загрузкой: держит фиксированный набор
//! воркеров, тянет piece'ы через [`TRangeFetch`], пишет их в [`TPieceStorage`]
//! и эмитит события во внешний мир через `broadcast`-канал.
//!
//! ## Взаимодействие с внешним миром
//!
//! - Вход: [`DownloadCommand`] через mpsc (`EngineHandle::pause/resume/cancel`).
//! - Выход: [`DownloadEvent`] через broadcast — `Progress`, `StateChanged`,
//!   `Completed`, `Failed`. Канал — `broadcast` (а не `mpsc`), чтобы одно
//!   событие видели все подписчики (gRPC `Watch` потребителей может быть
//!   несколько), и без backpressure (engine не должен останавливаться, если
//!   клиент тупит).
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
//! атомарный `bytes_done` и отправляет [`DownloadEvent::Progress`]. Частота
//! эмита ≤ `1 / progress_interval`, тем самым Watch-клиенты не захлёбываются.
//!
//! ## Retry
//!
//! Каждый воркер самостоятельно применяет [`RetryPolicy`] к транзиентным
//! ошибкам (`is_transient()`), усыпляясь через `tokio::time::sleep` на
//! рассчитанную задержку. Не-транзиентные или исчерпанный лимит попыток —
//! permanent failure: воркер шлёт супервизору сигнал, движок переходит в
//! `Failed` и останавливает всех.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{
    Mutex,
    broadcast,
    mpsc,
    watch,
};
use tokio::task::JoinHandle;
use tracing::{
    debug,
    info,
    warn,
};

use crate::domain::{
    DownloadCommand,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    FileStatus,
    Progress,
};
use crate::ports::{
    ByteStream,
    RangeError,
    RangeGuard,
    TPieceStorage,
    TRangeFetch,
};
use crate::service::retry::{
    RetryDecision,
    RetryPolicy,
};

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
    pub spec: DownloadSpec,
    pub total_size: u64,
    /// Размер «обычного» piece'а. Последний piece может быть короче, если
    /// `total_size` не кратен `piece_size` — это считается помощником
    /// [`piece_size_at`].
    pub piece_size: u64,
    pub accepts_ranges: bool,
    pub guard: Option<RangeGuard>,
}

/// Абсолютный offset piece'а в файле.
#[inline]
fn piece_offset(idx: u32, piece_size: u64) -> u64 {
    idx as u64 * piece_size
}

/// Фактический размер piece'а с индексом `idx`. Последний может быть меньше
/// `piece_size`, если общий размер не кратен.
#[inline]
fn piece_size_at(idx: u32, piece_size: u64, total_size: u64) -> u64 {
    let start = piece_offset(idx, piece_size);
    let end = start.saturating_add(piece_size).min(total_size);
    end.saturating_sub(start)
}

/// Сколько всего piece'ов будет у файла такого размера.
#[inline]
fn pieces_total(total_size: u64, piece_size: u64) -> u32 {
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
    /// Через сколько закоммиченных piece'ов делать `commit_batch`.
    pub commit_every: usize,
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
            commit_every: 16,
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
    id: DownloadId,
    cmd_tx: mpsc::UnboundedSender<DownloadCommand>,
    join: JoinHandle<()>,
}

impl EngineHandle {
    pub fn id(&self) -> DownloadId {
        self.id
    }

    /// Послать команду движку. Возвращает `false`, если задача уже завершилась
    /// и канал закрыт (команду применять некому).
    pub fn send(&self, cmd: DownloadCommand) -> bool {
        self.cmd_tx.send(cmd).is_ok()
    }

    pub fn pause(&self) -> bool {
        self.send(DownloadCommand::Pause)
    }
    pub fn resume(&self) -> bool {
        self.send(DownloadCommand::Resume)
    }
    pub fn cancel(&self) -> bool {
        self.send(DownloadCommand::Cancel)
    }

    /// Дождаться завершения задачи движка.
    pub async fn join(self) {
        let _ = self.join.await;
    }
}

/// Внутреннее состояние супервизора. Передаётся воркерам через `watch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunState {
    Running,
    Paused,
    Stopping,
}

/// Сообщения от воркеров к супервизору.
enum WorkerMsg {
    /// Piece полностью записан и готов к commit'у.
    PieceDone(u32),
    /// Permanent failure: ретраи исчерпаны или не-транзиентная ошибка.
    Failed(String),
}

/// Фасад старта движка.
pub struct DownloadEngine;

impl DownloadEngine {
    /// Запустить движок для указанной загрузки.
    ///
    /// Возвращает [`EngineHandle`] для управления и подписчика-receiver для
    /// событий. Дополнительных подписчиков можно получать через
    /// `events.resubscribe()`.
    pub fn spawn<S, F>(
        id: DownloadId,
        inputs: EngineInputs,
        config: EngineConfig,
        storage: Arc<S>,
        fetch: Arc<F>,
    ) -> (EngineHandle, broadcast::Receiver<DownloadEvent>)
    where
        S: TPieceStorage + Send + Sync + 'static,
        F: TRangeFetch + Send + Sync + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = broadcast::channel(config.events_capacity);

        let join = tokio::spawn(run_engine(
            id, inputs, config, storage, fetch, cmd_rx, events_tx,
        ));

        (EngineHandle { id, cmd_tx, join }, events_rx)
    }
}

async fn run_engine<S, F>(
    id: DownloadId,
    inputs: EngineInputs,
    config: EngineConfig,
    storage: Arc<S>,
    fetch: Arc<F>,
    mut cmd_rx: mpsc::UnboundedReceiver<DownloadCommand>,
    events_tx: broadcast::Sender<DownloadEvent>,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    info!(%id, url = %inputs.spec.url, "engine starting");

    // Стартовый снимок `pending`: spec говорит, что нужно скачать всё;
    // storage подскажет, чего ещё не хватает (resume-кейс).
    let pending: Vec<u32> = match storage.pending_pieces().await {
        Ok(p) => p,
        Err(e) => {
            warn!(%id, error = %e, "failed to read pending pieces");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(DownloadEvent::Failed {
                id,
                error: format!("pending_pieces: {e}"),
            });
            return;
        }
    };

    // Быстрый выход, если качать нечего — сразу финализируем и Completed.
    if pending.is_empty() {
        info!(%id, "no pending pieces — finalizing immediately");
        emit_status(&events_tx, id, FileStatus::Running);
        match storage.finalize().await {
            Ok(()) => {
                emit_status(&events_tx, id, FileStatus::Done);
                let _ = events_tx.send(DownloadEvent::Completed { id });
            }
            Err(e) => {
                warn!(%id, error = %e, "finalize failed");
                emit_status(&events_tx, id, FileStatus::Failed);
                let _ = events_tx.send(DownloadEvent::Failed {
                    id,
                    error: format!("finalize: {e}"),
                });
            }
        }
        return;
    }

    // Доменный переход: Queued → Running.
    emit_status(&events_tx, id, FileStatus::Running);

    // Общий стейт между воркерами и супервизором.
    let total_pieces = pieces_total(inputs.total_size, inputs.piece_size);
    let total_pieces_expected = pending.len();
    let bytes_in_pending: u64 = pending
        .iter()
        .map(|idx| piece_size_at(*idx, inputs.piece_size, inputs.total_size))
        .sum();
    let pending_arc: Arc<Mutex<VecDeque<u32>>> = Arc::new(Mutex::new(pending.into()));
    let (state_tx, state_rx) = watch::channel(RunState::Running);
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<WorkerMsg>();
    // Бейзлайн для прогресса: сколько уже на диске = total - pending.
    let bytes_done = Arc::new(AtomicU64::new(
        inputs.total_size.saturating_sub(bytes_in_pending),
    ));

    // Решаем, сколько воркеров пустить. No-Range — строго один.
    let worker_count = if inputs.accepts_ranges {
        inputs.spec.workers.max(1) as usize
    } else {
        1
    };
    info!(%id, pieces = total_pieces_expected, workers = worker_count, accepts_ranges = inputs.accepts_ranges, "spawning workers");

    let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
    if inputs.accepts_ranges {
        for _ in 0..worker_count {
            let h = tokio::spawn(worker_range(
                inputs.spec.url.clone(),
                inputs.guard.clone(),
                inputs.piece_size,
                inputs.total_size,
                Arc::clone(&pending_arc),
                Arc::clone(&storage),
                Arc::clone(&fetch),
                state_rx.clone(),
                worker_tx.clone(),
                Arc::clone(&bytes_done),
                config.clone(),
            ));
            worker_handles.push(h);
        }
    } else {
        let h = tokio::spawn(worker_full(
            inputs.spec.url.clone(),
            inputs.piece_size,
            inputs.total_size,
            Arc::clone(&storage),
            Arc::clone(&fetch),
            state_rx.clone(),
            worker_tx.clone(),
            Arc::clone(&bytes_done),
            config.clone(),
        ));
        worker_handles.push(h);
    }
    // Сбрасываем свою копию sender'а — иначе канал никогда не закроется.
    drop(worker_tx);

    // Главный цикл супервизора.
    let mut commit_buffer: Vec<u32> = Vec::with_capacity(config.commit_every);
    let mut progress_tick = tokio::time::interval(config.progress_interval);
    progress_tick.tick().await; // первый tick — мгновенный, пропускаем.
    let mut pieces_committed: usize = 0;
    let mut final_outcome: Option<Outcome> = None;

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    DownloadCommand::Pause => {
                        if *state_rx.borrow() == RunState::Running {
                            info!(%id, "pausing");
                            let _ = state_tx.send(RunState::Paused);
                            emit_status(&events_tx, id, FileStatus::Paused);
                        }
                    }
                    DownloadCommand::Resume => {
                        if *state_rx.borrow() == RunState::Paused {
                            info!(%id, "resuming");
                            let _ = state_tx.send(RunState::Running);
                            emit_status(&events_tx, id, FileStatus::Running);
                        }
                    }
                    DownloadCommand::Cancel => {
                        info!(%id, "cancelling");
                        let _ = state_tx.send(RunState::Stopping);
                        final_outcome = Some(Outcome::Cancelled);
                        break;
                    }
                }
            }
            Some(msg) = worker_rx.recv() => {
                match msg {
                    WorkerMsg::PieceDone(idx) => {
                        commit_buffer.push(idx);
                        if commit_buffer.len() >= config.commit_every {
                            if let Err(e) = storage.commit_batch(&commit_buffer).await {
                                final_outcome = Some(Outcome::Failed(format!("commit: {e}")));
                                let _ = state_tx.send(RunState::Stopping);
                                break;
                            }
                            pieces_committed += commit_buffer.len();
                            commit_buffer.clear();
                            emit_progress(&events_tx, id, &bytes_done, &inputs,
                                          pieces_committed as u32, total_pieces);
                        }
                    }
                    WorkerMsg::Failed(err) => {
                        warn!(%id, error = %err, "worker reported failure");
                        final_outcome = Some(Outcome::Failed(err));
                        let _ = state_tx.send(RunState::Stopping);
                        break;
                    }
                }
            }
            _ = progress_tick.tick() => {
                emit_progress(&events_tx, id, &bytes_done, &inputs,
                              pieces_committed as u32, total_pieces);
            }
            else => {
                // Все воркеры и command-канал закрылись — работа окончена.
                break;
            }
        }

        // Воркеры могли отчитаться о завершении очереди, канал закрылся,
        // но `select!` не выйдет, пока cmd-канал открыт. Проверяем явно:
        // если все worker-handles завершены и pending пуст — done.
        if worker_rx.is_closed() {
            break;
        }
    }

    // Ждём, пока воркеры корректно дожмут текущий piece и выйдут.
    for h in worker_handles {
        let _ = h.await;
    }
    // Сливаем оставшиеся сообщения от воркеров (могли долететь PieceDone
    // после отправки сигнала Stopping).
    while let Ok(msg) = worker_rx.try_recv() {
        if let WorkerMsg::PieceDone(idx) = msg {
            commit_buffer.push(idx);
        }
    }

    if final_outcome.is_none() {
        // Нормальный сход: финальный commit + finalize.
        if !commit_buffer.is_empty() {
            if let Err(e) = storage.commit_batch(&commit_buffer).await {
                final_outcome = Some(Outcome::Failed(format!("commit: {e}")));
            } else {
                pieces_committed += commit_buffer.len();
                commit_buffer.clear();
            }
        }
        if final_outcome.is_none() {
            match storage.pending_pieces().await {
                Ok(p) if p.is_empty() => final_outcome = Some(Outcome::Completed),
                Ok(_) if total_pieces_expected > 0 && pieces_committed == 0 => {
                    // Воркеры завершились, ничего не скачав — это отказ.
                    final_outcome = Some(Outcome::Failed("workers exited without progress".into()));
                }
                Ok(_) => {
                    // Остались pending, но воркеры молча вышли — тоже fail.
                    final_outcome =
                        Some(Outcome::Failed("workers exited before completion".into()));
                }
                Err(e) => final_outcome = Some(Outcome::Failed(format!("pending: {e}"))),
            }
        }
    } else {
        // При Cancel/Failed делаем best-effort commit того, что успели.
        if !commit_buffer.is_empty() {
            let _ = storage.commit_batch(&commit_buffer).await;
        }
    }

    match final_outcome.unwrap() {
        Outcome::Completed => {
            match storage.finalize().await {
                Ok(()) => {
                    info!(%id, "download completed");
                    // Финальный Progress — «100%».
                    emit_progress(
                        &events_tx,
                        id,
                        &bytes_done,
                        &inputs,
                        total_pieces,
                        total_pieces,
                    );
                    emit_status(&events_tx, id, FileStatus::Done);
                    let _ = events_tx.send(DownloadEvent::Completed { id });
                }
                Err(e) => {
                    warn!(%id, error = %e, "finalize failed");
                    emit_status(&events_tx, id, FileStatus::Failed);
                    let _ = events_tx.send(DownloadEvent::Failed {
                        id,
                        error: format!("finalize: {e}"),
                    });
                }
            }
        }
        Outcome::Failed(err) => {
            warn!(%id, error = %err, "download failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(DownloadEvent::Failed { id, error: err });
        }
        Outcome::Cancelled => {
            info!(%id, "download cancelled — aborting storage");
            let _ = storage.abort().await;
            emit_status(&events_tx, id, FileStatus::Cancelled);
        }
    }
}

enum Outcome {
    Completed,
    Failed(String),
    Cancelled,
}

fn emit_status(tx: &broadcast::Sender<DownloadEvent>, id: DownloadId, status: FileStatus) {
    let _ = tx.send(DownloadEvent::StatusChanged { id, status });
}

fn emit_progress(
    tx: &broadcast::Sender<DownloadEvent>,
    id: DownloadId,
    bytes_done: &AtomicU64,
    inputs: &EngineInputs,
    pieces_done: u32,
    pieces_total: u32,
) {
    let done = bytes_done.load(Ordering::Relaxed);
    let progress = Progress {
        bytes_done: done,
        bytes_total: inputs.total_size,
        pieces_done,
        pieces_total,
        speed_bps: 0.0,
        eta_secs: None,
    };
    let _ = tx.send(DownloadEvent::Progress { id, progress });
}

/// Достать следующий piece из очереди с учётом паузы/стопа. Возвращает
/// `None` только когда очередь пуста или движок останавливается.
async fn next_piece(
    pending: &Mutex<VecDeque<u32>>,
    state_rx: &mut watch::Receiver<RunState>,
) -> Option<u32> {
    loop {
        let current = *state_rx.borrow();
        match current {
            RunState::Stopping => return None,
            RunState::Paused => {
                if state_rx.changed().await.is_err() {
                    return None;
                }
                continue;
            }
            RunState::Running => {}
        }
        let mut q = pending.lock().await;
        if let Some(idx) = q.pop_front() {
            return Some(idx);
        }
        return None;
    }
}

#[allow(clippy::too_many_arguments)]
async fn worker_range<S, F>(
    url: String,
    guard: Option<RangeGuard>,
    piece_size: u64,
    total_size: u64,
    pending: Arc<Mutex<VecDeque<u32>>>,
    storage: Arc<S>,
    fetch: Arc<F>,
    mut state_rx: watch::Receiver<RunState>,
    worker_tx: mpsc::UnboundedSender<WorkerMsg>,
    bytes_done: Arc<AtomicU64>,
    config: EngineConfig,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    while let Some(idx) = next_piece(&pending, &mut state_rx).await {
        let offset = piece_offset(idx, piece_size);
        let size = piece_size_at(idx, piece_size, total_size);
        match run_piece_with_retry(
            &url,
            guard.as_ref(),
            idx,
            offset,
            size,
            storage.as_ref(),
            fetch.as_ref(),
            &bytes_done,
            &config,
        )
        .await
        {
            Ok(()) => {
                debug!(piece = idx, "piece done");
                if worker_tx.send(WorkerMsg::PieceDone(idx)).is_err() {
                    return;
                }
            }
            Err(PieceError::Transient(msg)) | Err(PieceError::Permanent(msg)) => {
                warn!(piece = idx, error = %msg, "piece failed — stopping worker");
                let _ = worker_tx.send(WorkerMsg::Failed(msg));
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_piece_with_retry<S, F>(
    url: &str,
    guard: Option<&RangeGuard>,
    idx: u32,
    offset: u64,
    size: u64,
    storage: &S,
    fetch: &F,
    bytes_done: &AtomicU64,
    config: &EngineConfig,
) -> Result<(), PieceError>
where
    S: TPieceStorage + Send + Sync,
    F: TRangeFetch + Send + Sync,
{
    let mut attempt: u32 = 0;
    loop {
        // На каждой попытке пишем с нуля по offset в piece. Байты из
        // прошлой попытки лежат в `.data.brook`, но не закоммичены — их
        // затрут новые.
        let mut written: u64 = 0;
        let res = fetch_and_write_piece(
            url,
            guard,
            idx,
            offset,
            size,
            storage,
            fetch,
            &mut written,
            config,
        )
        .await;
        match res {
            Ok(()) => {
                bytes_done.fetch_add(size, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) => {
                let transient = matches!(&e, PieceError::Transient(_));
                // Байты частичной попытки не добавляем в bytes_done — иначе
                // счётчик уедет выше реального прогресса.
                let _ = written;
                match config.retry.classify(attempt, transient) {
                    RetryDecision::Retry(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    RetryDecision::GiveUp => return Err(e),
                }
            }
        }
    }
}

enum PieceError {
    Transient(String),
    Permanent(String),
}

#[allow(clippy::too_many_arguments)]
async fn fetch_and_write_piece<S, F>(
    url: &str,
    guard: Option<&RangeGuard>,
    idx: u32,
    offset: u64,
    size: u64,
    storage: &S,
    fetch: &F,
    written: &mut u64,
    config: &EngineConfig,
) -> Result<(), PieceError>
where
    S: TPieceStorage + Send + Sync,
    F: TRangeFetch + Send + Sync,
{
    let stream = fetch
        .fetch_range(url, offset, size, guard)
        .await
        .map_err(range_err_to_piece)?;
    drain_stream_into_piece(stream, idx, size, storage, written, config.write_buffer).await
}

async fn drain_stream_into_piece<S>(
    mut stream: ByteStream,
    idx: u32,
    size: u64,
    storage: &S,
    written: &mut u64,
    buf_cap: usize,
) -> Result<(), PieceError>
where
    S: TPieceStorage + Send + Sync,
{
    let mut buf: Vec<u8> = Vec::with_capacity(buf_cap);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(range_err_to_piece)?;
        let mut slice: &[u8] = &chunk;
        while !slice.is_empty() {
            let space = buf_cap.saturating_sub(buf.len()).max(1);
            let take = slice.len().min(space);
            buf.extend_from_slice(&slice[..take]);
            slice = &slice[take..];
            if buf.len() >= buf_cap {
                let total = *written + buf.len() as u64;
                if total > size {
                    return Err(PieceError::Transient("piece overflow".into()));
                }
                storage
                    .write_piece_bytes(idx, *written, &buf)
                    .await
                    .map_err(|e| PieceError::Permanent(format!("write: {e}")))?;
                *written += buf.len() as u64;
                buf.clear();
            }
        }
    }
    if !buf.is_empty() {
        let total = *written + buf.len() as u64;
        if total > size {
            return Err(PieceError::Transient("piece overflow".into()));
        }
        storage
            .write_piece_bytes(idx, *written, &buf)
            .await
            .map_err(|e| PieceError::Permanent(format!("write: {e}")))?;
        *written += buf.len() as u64;
    }
    if *written != size {
        // Усечённый поток — транзиентный сбой, будет ретрай.
        return Err(PieceError::Transient(format!(
            "truncated: wrote {written} of {size}"
        )));
    }
    Ok(())
}

fn range_err_to_piece(e: RangeError) -> PieceError {
    if e.is_transient() {
        PieceError::Transient(e.to_string())
    } else {
        PieceError::Permanent(e.to_string())
    }
}

/// Воркер no-Range режима: один стрим с начала до EOF, раскладывается по
/// piece'ам последовательно.
#[allow(clippy::too_many_arguments)]
async fn worker_full<S, F>(
    url: String,
    piece_size: u64,
    total_size: u64,
    storage: Arc<S>,
    fetch: Arc<F>,
    mut state_rx: watch::Receiver<RunState>,
    worker_tx: mpsc::UnboundedSender<WorkerMsg>,
    bytes_done: Arc<AtomicU64>,
    config: EngineConfig,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    let mut attempt: u32 = 0;
    loop {
        // Уважать пользовательскую паузу до начала попытки.
        loop {
            let current = *state_rx.borrow();
            match current {
                RunState::Stopping => return,
                RunState::Paused => {
                    if state_rx.changed().await.is_err() {
                        return;
                    }
                }
                RunState::Running => break,
            }
        }

        let result = tokio::select! {
            biased;
            // Cancellation mid-stream: drop the future to abort the in-flight request.
            _ = state_rx.wait_for(|s| matches!(s, RunState::Stopping)) => return,
            r = fetch_full_and_dispatch(
                &url,
                piece_size,
                total_size,
                storage.as_ref(),
                fetch.as_ref(),
                &bytes_done,
            ) => r,
        };
        match result {
            Ok(pieces_done) => {
                for idx in pieces_done {
                    if worker_tx.send(WorkerMsg::PieceDone(idx)).is_err() {
                        return;
                    }
                }
                return;
            }
            Err(e) => {
                let transient = matches!(&e, PieceError::Transient(_));
                match config.retry.classify(attempt, transient) {
                    RetryDecision::Retry(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    RetryDecision::GiveUp => {
                        let msg = match e {
                            PieceError::Transient(s) | PieceError::Permanent(s) => s,
                        };
                        let _ = worker_tx.send(WorkerMsg::Failed(msg));
                        return;
                    }
                }
            }
        }
    }
}

async fn fetch_full_and_dispatch<S, F>(
    url: &str,
    piece_size: u64,
    total_size: u64,
    storage: &S,
    fetch: &F,
    bytes_done: &AtomicU64,
) -> Result<Vec<u32>, PieceError>
where
    S: TPieceStorage + Send + Sync,
    F: TRangeFetch + Send + Sync,
{
    let mut stream = fetch.fetch_full(url).await.map_err(range_err_to_piece)?;
    let mut absolute: u64 = 0;
    let mut completed: Vec<u32> = Vec::new();
    let total_pieces = pieces_total(total_size, piece_size);
    let mut cursor: u32 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(range_err_to_piece)?;
        let mut slice: &[u8] = &chunk;
        while !slice.is_empty() {
            if cursor >= total_pieces {
                return Err(PieceError::Transient("server overshoots total_size".into()));
            }
            let piece_start = piece_offset(cursor, piece_size);
            let piece_end = piece_start + piece_size_at(cursor, piece_size, total_size);
            let offset_in_piece = absolute.saturating_sub(piece_start);
            let room = (piece_end - absolute) as usize;
            let take = slice.len().min(room);
            storage
                .write_piece_bytes(cursor, offset_in_piece, &slice[..take])
                .await
                .map_err(|e| PieceError::Permanent(format!("write: {e}")))?;
            absolute += take as u64;
            bytes_done.fetch_add(take as u64, Ordering::Relaxed);
            slice = &slice[take..];
            if absolute == piece_end {
                completed.push(cursor);
                cursor += 1;
            }
        }
    }

    // EOF до конца запланированного — транзиентно (можно ретраить).
    if cursor != total_pieces {
        return Err(PieceError::Transient(format!(
            "truncated: wrote {absolute} bytes"
        )));
    }
    Ok(completed)
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream;

    use super::*;
    use crate::domain::{
        DownloadEvent,
        DownloadId,
        DownloadSpec,
    };
    use crate::ports::{
        InspectError,
        InspectReport,
        THttpInspect,
    };
    use crate::testing::MemoryPieceStorage;

    /// Простая «загрузка»: равные piece'ы размера `piece_size`, всего `count`
    /// штук — итоговый `total_size` = `piece_size * count`.
    #[derive(Clone, Copy)]
    struct TestPlan {
        piece_size: u64,
        count: u32,
    }

    impl TestPlan {
        fn total(self) -> u64 {
            self.piece_size * self.count as u64
        }
    }

    fn bytes_for(total: u64) -> Vec<u8> {
        (0..total).map(|i| (i % 251) as u8).collect()
    }

    /// Моковый fetch с программируемым «планом» попыток.
    /// Для каждого piece_idx хранится очередь исходов.
    enum Outcome {
        Ok,
        Transient500,
        Truncated(usize), // отдаёт первые N байт и обрывается
    }

    struct MockFetch {
        full_bytes: Vec<u8>,
        plans: std::sync::Mutex<HashMap<u32, VecDeque<Outcome>>>,
        default_ok: bool,
        plan: TestPlan,
        full_plan: std::sync::Mutex<VecDeque<Outcome>>,
        calls: AtomicUsize,
        /// Искусственная задержка перед отдачей ответа — даёт тестам время
        /// на отправку Pause/Cancel до завершения загрузки.
        delay: Duration,
    }

    impl MockFetch {
        fn always_ok(plan: TestPlan) -> Self {
            Self {
                full_bytes: bytes_for(plan.total()),
                plans: Default::default(),
                default_ok: true,
                plan,
                full_plan: Default::default(),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn set_piece_plan(&self, idx: u32, outcomes: Vec<Outcome>) {
            let mut p = self.plans.lock().unwrap();
            p.insert(idx, outcomes.into());
        }
    }

    #[async_trait]
    impl TRangeFetch for MockFetch {
        async fn fetch_range(
            &self,
            _url: &str,
            offset: u64,
            len: u64,
            _guard: Option<&RangeGuard>,
        ) -> Result<ByteStream, RangeError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            assert_eq!(
                offset % self.plan.piece_size,
                0,
                "test plan expects offsets aligned to piece_size"
            );
            let idx = (offset / self.plan.piece_size) as u32;
            let expected = piece_size_at(idx, self.plan.piece_size, self.plan.total());
            assert_eq!(len, expected, "unexpected Range length");

            let outcome = {
                let mut p = self.plans.lock().unwrap();
                match p.get_mut(&idx).and_then(|q| q.pop_front()) {
                    Some(o) => o,
                    None if self.default_ok => Outcome::Ok,
                    None => Outcome::Transient500,
                }
            };

            match outcome {
                Outcome::Ok => {
                    let start = offset as usize;
                    let end = start + len as usize;
                    let bytes = Bytes::copy_from_slice(&self.full_bytes[start..end]);
                    let s = stream::iter(vec![Ok(bytes)]);
                    Ok(Box::pin(s))
                }
                Outcome::Transient500 => Err(RangeError::UnexpectedStatus { code: 503 }),
                Outcome::Truncated(n) => {
                    let start = offset as usize;
                    let slice = &self.full_bytes[start..start + n];
                    let bytes = Bytes::copy_from_slice(slice);
                    let s = stream::iter(vec![Ok(bytes)]);
                    Ok(Box::pin(s))
                }
            }
        }

        async fn fetch_full(&self, _url: &str) -> Result<ByteStream, RangeError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let outcome = {
                let mut p = self.full_plan.lock().unwrap();
                p.pop_front().unwrap_or(Outcome::Ok)
            };
            match outcome {
                Outcome::Ok => {
                    let bytes = Bytes::copy_from_slice(&self.full_bytes);
                    // Разобьём на пару чанков, чтобы покрыть путь «несколько chunk'ов».
                    let mid = self.full_bytes.len() / 2;
                    let s = stream::iter(vec![
                        Ok(Bytes::copy_from_slice(&self.full_bytes[..mid])),
                        Ok(Bytes::copy_from_slice(&self.full_bytes[mid..])),
                    ]);
                    let _ = bytes;
                    Ok(Box::pin(s))
                }
                Outcome::Transient500 => Err(RangeError::UnexpectedStatus { code: 503 }),
                Outcome::Truncated(n) => {
                    let bytes = Bytes::copy_from_slice(&self.full_bytes[..n]);
                    let s = stream::iter(vec![Ok(bytes)]);
                    Ok(Box::pin(s))
                }
            }
        }
    }

    /// Простейший inspect не используется движком, но нужен для компиляции
    /// совместимости — оставлен как заглушка.
    struct _NoInspect;
    #[async_trait]
    impl THttpInspect for _NoInspect {
        async fn inspect(&self, _url: &str) -> Result<InspectReport, InspectError> {
            Err(InspectError::Network("not used in engine tests".into()))
        }
    }

    fn fast_config() -> EngineConfig {
        EngineConfig {
            write_buffer: 16,
            commit_every: 2,
            progress_interval: Duration::from_millis(40),
            retry: RetryPolicy {
                base: Duration::from_millis(5),
                max_delay: Duration::from_millis(20),
                max_attempts: 5,
                jitter_ratio: 0.0,
            },
            events_capacity: 64,
        }
    }

    fn inputs_range(plan: TestPlan) -> EngineInputs {
        EngineInputs {
            spec: DownloadSpec {
                url: "https://test/f".into(),
                target_dir: "/tmp".into(),
                filename: Some("f".into()),
                workers: 2,
                piece_target_count: None,
                piece_size_min: None,
                piece_size_max: None,
                on_file_exists_override: Default::default(),
            },
            total_size: plan.total(),
            piece_size: plan.piece_size,
            accepts_ranges: true,
            guard: None,
        }
    }

    async fn collect_events(mut rx: broadcast::Receiver<DownloadEvent>) -> Vec<DownloadEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.recv().await {
            let terminal = matches!(
                ev,
                DownloadEvent::Completed { .. }
                    | DownloadEvent::Failed { .. }
                    | DownloadEvent::StatusChanged {
                        status: FileStatus::Cancelled,
                        ..
                    }
            );
            out.push(ev);
            if terminal {
                break;
            }
        }
        out
    }

    #[tokio::test]
    async fn happy_path_range_completes() {
        let plan = TestPlan {
            piece_size: 20,
            count: 3,
        };
        let storage = Arc::new(MemoryPieceStorage::new(3, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        let (handle, rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            fast_config(),
            storage.clone(),
            fetch,
        );
        let events = collect_events(rx).await;
        handle.join.await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DownloadEvent::Completed { .. }))
        );
        let snap = storage.snapshot();
        assert!(snap.finalized);
        assert_eq!(snap.committed, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn transient_500_is_retried_and_succeeds() {
        let plan = TestPlan {
            piece_size: 20,
            count: 2,
        };
        let storage = Arc::new(MemoryPieceStorage::new(2, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        fetch.set_piece_plan(0, vec![Outcome::Transient500, Outcome::Transient500]);
        let (handle, rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            fast_config(),
            storage.clone(),
            fetch,
        );
        let events = collect_events(rx).await;
        handle.join.await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DownloadEvent::Completed { .. }))
        );
        assert!(storage.snapshot().finalized);
    }

    #[tokio::test]
    async fn truncated_piece_retries_until_success() {
        let plan = TestPlan {
            piece_size: 32,
            count: 1,
        };
        let storage = Arc::new(MemoryPieceStorage::new(1, 32));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        // Первая попытка — отдали 10 байт и отключились; вторая — полный piece.
        fetch.set_piece_plan(0, vec![Outcome::Truncated(10)]);
        let (handle, rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            fast_config(),
            storage.clone(),
            fetch,
        );
        let _ = collect_events(rx).await;
        handle.join.await.unwrap();
        assert!(storage.snapshot().finalized);
    }

    #[tokio::test]
    async fn pause_then_resume_emits_states_and_completes() {
        let plan = TestPlan {
            piece_size: 20,
            count: 6,
        };
        let storage = Arc::new(MemoryPieceStorage::new(6, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(30)));
        let (handle, mut rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            fast_config(),
            storage.clone(),
            fetch,
        );
        // Дождёмся первого StateChanged(Running).
        let _ = rx.recv().await;
        assert!(handle.pause());
        // Небольшая пауза, чтобы supervisor успел обработать команду.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(handle.resume());
        // Дожидаемся Completed.
        let mut saw_paused = false;
        let mut saw_completed = false;
        while let Ok(ev) = rx.recv().await {
            match ev {
                DownloadEvent::StatusChanged {
                    status: FileStatus::Paused,
                    ..
                } => {
                    saw_paused = true;
                }
                DownloadEvent::Completed { .. } => {
                    saw_completed = true;
                    break;
                }
                _ => {}
            }
        }
        handle.join.await.unwrap();
        assert!(saw_paused, "expected Paused StateChanged event");
        assert!(saw_completed);
    }

    #[tokio::test]
    async fn cancel_aborts_storage() {
        let plan = TestPlan {
            piece_size: 20,
            count: 6,
        };
        let storage = Arc::new(MemoryPieceStorage::new(6, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(30)));
        let (handle, mut rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            fast_config(),
            storage.clone(),
            fetch,
        );
        let _ = rx.recv().await;
        assert!(handle.cancel());
        // Dren events.
        while let Ok(ev) = rx.recv().await {
            if matches!(
                ev,
                DownloadEvent::StatusChanged {
                    status: FileStatus::Cancelled,
                    ..
                }
            ) {
                break;
            }
        }
        handle.join.await.unwrap();
        assert!(storage.snapshot().aborted);
    }

    #[tokio::test]
    async fn progress_events_throttled_to_interval() {
        let plan = TestPlan {
            piece_size: 20,
            count: 8,
        };
        let storage = Arc::new(MemoryPieceStorage::new(8, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        // Ставим большой интервал — чтобы Progress эмитился редко.
        let mut cfg = fast_config();
        cfg.progress_interval = Duration::from_millis(200);
        let start = std::time::Instant::now();
        let (handle, mut rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs_range(plan),
            cfg,
            storage.clone(),
            fetch,
        );
        let mut progress_count = 0;
        while let Ok(ev) = rx.recv().await {
            if matches!(ev, DownloadEvent::Progress { .. }) {
                progress_count += 1;
            }
            if matches!(ev, DownloadEvent::Completed { .. }) {
                break;
            }
        }
        handle.join.await.unwrap();
        let elapsed = start.elapsed();
        // За elapsed-время при 5 Hz максимум allowed = elapsed/200ms + 2.
        let max_allowed = (elapsed.as_millis() / 200) as usize + 3;
        assert!(
            progress_count <= max_allowed,
            "progress emitted {progress_count} in {elapsed:?}, allowed <= {max_allowed}"
        );
    }

    #[tokio::test]
    async fn no_range_mode_uses_single_worker_and_completes() {
        let plan = TestPlan {
            piece_size: 10,
            count: 4,
        };
        let storage = Arc::new(MemoryPieceStorage::new(4, 10));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        let mut inputs = inputs_range(plan);
        inputs.accepts_ranges = false;
        inputs.spec.workers = 4;
        let (handle, rx) = DownloadEngine::spawn(
            DownloadId::new(),
            inputs,
            fast_config(),
            storage.clone(),
            fetch.clone(),
        );
        let events = collect_events(rx).await;
        handle.join.await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DownloadEvent::Completed { .. }))
        );
        // Всего один fetch_full вызов (без ретраев).
        assert_eq!(fetch.calls.load(Ordering::Relaxed), 1);
        assert!(storage.snapshot().finalized);
    }
}
