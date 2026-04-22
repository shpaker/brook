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
    AttemptId,
    DownloadCommand,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    FileStatus,
    Progress,
    WorkerId,
};
use crate::ports::{
    ByteStream,
    RangeError,
    RangeGuard,
    TPieceAttemptRepo,
    TPieceStorage,
    TRangeFetch,
    TStreamStorage,
    TWorkerRepo,
    WorkerRecord,
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
    /// URL после цепочки редиректов, если фабрика его резолвила. Воркеры
    /// шлют range-GET'ы именно сюда — экономим RTT на повторном резолве
    /// подписанных CDN-ссылок. `None` → используем `spec.url` напрямую.
    pub effective_url: Option<String>,
}

/// Входные данные стриминг-движка (unknown-size / no-Range).
#[derive(Debug, Clone)]
pub struct StreamingEngineInputs {
    pub spec: DownloadSpec,
    /// URL после цепочки редиректов (см. [`EngineInputs::effective_url`]).
    pub effective_url: Option<String>,
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
///
/// Attempt-lifecycle-события (`AttemptStarted` / `AttemptFinished` /
/// `AttemptFailed` / `AttemptPaused`) шлются тем же каналом, чтобы все
/// DB-writes происходили в одном месте — в select-цикле супервизора.
/// Воркерам не приходится дергать репозиторий из горячего пути;
/// супервизор гарантирует, что терминальное сообщение attempt'а
/// (`Finished` / `Failed` / `Paused`) обработано до того, как engine
/// перейдёт к финализации (дренаж `worker_rx.try_recv()` после join'а).
enum WorkerMsg {
    /// Piece полностью записан и готов к commit'у.
    PieceDone { piece: u32 },
    /// Permanent failure: ретраи исчерпаны или не-транзиентная ошибка.
    Failed(String),
    /// Воркер начал попытку скачать piece.
    AttemptStarted { worker_id: WorkerId, piece: u32 },
    /// Попытка закрылась успешно (hash ok / принято).
    AttemptFinished {
        worker_id: WorkerId,
        piece: u32,
        bytes: u64,
    },
    /// Попытка закрылась ошибкой (транзиентной или permanent).
    AttemptFailed {
        worker_id: WorkerId,
        piece: u32,
        error: String,
    },
}

/// Фасад старта движка.
pub struct DownloadEngine;

impl DownloadEngine {
    /// Запустить движок для указанной загрузки.
    ///
    /// Возвращает [`EngineHandle`] для управления и подписчика-receiver для
    /// событий. Дополнительных подписчиков можно получать через
    /// `events.resubscribe()`.
    pub fn spawn<S, F, WR, AR>(
        id: DownloadId,
        inputs: EngineInputs,
        config: EngineConfig,
        storage: Arc<S>,
        fetch: Arc<F>,
        workers_repo: Arc<WR>,
        attempts_repo: Arc<AR>,
    ) -> (EngineHandle, broadcast::Receiver<DownloadEvent>)
    where
        S: TPieceStorage + Send + Sync + 'static,
        F: TRangeFetch + Send + Sync + 'static,
        WR: TWorkerRepo + Send + Sync + 'static,
        AR: TPieceAttemptRepo + Send + Sync + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = broadcast::channel(config.events_capacity);

        let join = tokio::spawn(run_engine(
            id,
            inputs,
            config,
            storage,
            fetch,
            workers_repo,
            attempts_repo,
            cmd_rx,
            events_tx,
        ));

        (EngineHandle { id, cmd_tx, join }, events_rx)
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
        id: DownloadId,
        inputs: StreamingEngineInputs,
        config: EngineConfig,
        stream: Arc<SS>,
        fetch: Arc<F>,
        workers_repo: Arc<WR>,
        attempts_repo: Arc<AR>,
    ) -> (EngineHandle, broadcast::Receiver<DownloadEvent>)
    where
        SS: TStreamStorage + Send + Sync + 'static,
        F: TRangeFetch + Send + Sync + 'static,
        WR: TWorkerRepo + Send + Sync + 'static,
        AR: TPieceAttemptRepo + Send + Sync + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = broadcast::channel(config.events_capacity);

        let join = tokio::spawn(run_streaming_engine(
            id,
            inputs,
            config,
            stream,
            fetch,
            workers_repo,
            attempts_repo,
            cmd_rx,
            events_tx,
        ));

        (EngineHandle { id, cmd_tx, join }, events_rx)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_engine<S, F, WR, AR>(
    id: DownloadId,
    inputs: EngineInputs,
    config: EngineConfig,
    storage: Arc<S>,
    fetch: Arc<F>,
    workers_repo: Arc<WR>,
    attempts_repo: Arc<AR>,
    mut cmd_rx: mpsc::UnboundedReceiver<DownloadCommand>,
    events_tx: broadcast::Sender<DownloadEvent>,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
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

    // Заводим фиксированный набор worker-строк под эту engine-сессию.
    // `ensure_slots` защитно сбрасывает все running-воркеры этого файла
    // в `paused` — если предыдущая сессия не успела этого сделать сама.
    let worker_records = match workers_repo.ensure_slots(id, worker_count).await {
        Ok(r) => r,
        Err(e) => {
            warn!(%id, error = %e, "ensure_slots failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(DownloadEvent::Failed {
                id,
                error: format!("ensure_slots: {e}"),
            });
            return;
        }
    };

    let request_url = inputs
        .effective_url
        .clone()
        .unwrap_or_else(|| inputs.spec.url.clone());
    let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
    if inputs.accepts_ranges {
        for i in 0..worker_count {
            let record = worker_records.get(i).cloned();
            let h = tokio::spawn(worker_range(
                request_url.clone(),
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
                record,
            ));
            worker_handles.push(h);
        }
    } else {
        let record = worker_records.first().cloned();
        let h = tokio::spawn(worker_full(
            request_url.clone(),
            inputs.piece_size,
            inputs.total_size,
            Arc::clone(&storage),
            Arc::clone(&fetch),
            state_rx.clone(),
            worker_tx.clone(),
            Arc::clone(&bytes_done),
            config.clone(),
            record,
        ));
        worker_handles.push(h);
    }
    // Сбрасываем свою копию sender'а — иначе канал никогда не закроется.
    drop(worker_tx);

    // Главный цикл супервизора.
    let mut progress_tick = tokio::time::interval(config.progress_interval);
    progress_tick.tick().await; // первый tick — мгновенный, пропускаем.
    let mut pieces_committed: usize = 0;
    let mut final_outcome: Option<Outcome> = None;
    // Маппинг (worker_id, piece_number) → AttemptId для открытых попыток.
    // При `AttemptStarted` супервизор вызывает `attempts_repo.start`
    // и запоминает `attempt_id`; при терминальном сообщении достаёт его.
    // На один (worker, piece) в любой момент жива не более чем одна
    // попытка (воркер либо качает piece, либо закрыл и идёт за следующим).
    use std::collections::HashMap;
    let mut open_attempts: HashMap<(WorkerId, u32), AttemptId> = HashMap::new();

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
                    WorkerMsg::PieceDone { piece: idx, .. } => {
                        if let Err(e) = storage.commit_done(idx).await {
                            final_outcome = Some(Outcome::Failed(format!("commit: {e}")));
                            let _ = state_tx.send(RunState::Stopping);
                            break;
                        }
                        pieces_committed += 1;
                        emit_progress(&events_tx, id, &bytes_done, &inputs,
                                      pieces_committed as u32, total_pieces);
                    }
                    WorkerMsg::Failed(err) => {
                        warn!(%id, error = %err, "worker reported failure");
                        final_outcome = Some(Outcome::Failed(err));
                        let _ = state_tx.send(RunState::Stopping);
                        break;
                    }
                    WorkerMsg::AttemptStarted { worker_id, piece } => {
                        match attempts_repo.start(id, piece, worker_id).await {
                            Ok(rec) => {
                                open_attempts.insert((worker_id, piece), rec.id);
                            }
                            Err(e) => {
                                warn!(%id, %worker_id, piece, error = %e,
                                      "attempt start persistence failed");
                            }
                        }
                    }
                    WorkerMsg::AttemptFinished { worker_id, piece, bytes } => {
                        if let Some(attempt_id) = open_attempts.remove(&(worker_id, piece))
                            && let Err(e) = attempts_repo.finish(attempt_id, bytes).await {
                                warn!(%id, %attempt_id, error = %e,
                                      "attempt finish persistence failed");
                        }
                    }
                    WorkerMsg::AttemptFailed { worker_id, piece, error } => {
                        if let Some(attempt_id) = open_attempts.remove(&(worker_id, piece))
                            && let Err(e) = attempts_repo.fail(attempt_id, &error).await {
                                warn!(%id, %attempt_id, error = %e,
                                      "attempt fail persistence failed");
                        }
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
    // Сливаем оставшиеся сообщения от воркеров (PieceDone после Stopping,
    // а также хвост attempt-событий — их обработать обязательно, иначе в БД
    // останутся «висящие» running-attempt'ы этой сессии). Так мы
    // гарантируем, что терминальное сообщение каждой попытки дойдёт до
    // репозитория до engine-финализации.
    while let Ok(msg) = worker_rx.try_recv() {
        match msg {
            WorkerMsg::PieceDone { piece: idx, .. } => {
                if let Err(e) = storage.commit_done(idx).await {
                    warn!(%id, piece = idx, error = %e, "commit on drain failed");
                } else {
                    pieces_committed += 1;
                }
            }
            WorkerMsg::AttemptStarted { worker_id, piece } => {
                match attempts_repo.start(id, piece, worker_id).await {
                    Ok(rec) => {
                        open_attempts.insert((worker_id, piece), rec.id);
                    }
                    Err(e) => warn!(%id, error = %e, "attempt start persistence failed"),
                }
            }
            WorkerMsg::AttemptFinished {
                worker_id,
                piece,
                bytes,
            } => {
                if let Some(aid) = open_attempts.remove(&(worker_id, piece))
                    && let Err(e) = attempts_repo.finish(aid, bytes).await
                {
                    warn!(%id, error = %e, "attempt finish persistence failed");
                }
            }
            WorkerMsg::AttemptFailed {
                worker_id,
                piece,
                error,
            } => {
                if let Some(aid) = open_attempts.remove(&(worker_id, piece))
                    && let Err(e) = attempts_repo.fail(aid, &error).await
                {
                    warn!(%id, error = %e, "attempt fail persistence failed");
                }
            }
            WorkerMsg::Failed(_) => {}
        }
    }

    if final_outcome.is_none() {
        // Нормальный сход: finalize.
        match storage.pending_pieces().await {
            Ok(p) if p.is_empty() => final_outcome = Some(Outcome::Completed),
            Ok(_) if total_pieces_expected > 0 && pieces_committed == 0 => {
                // Воркеры завершились, ничего не скачав — это отказ.
                final_outcome = Some(Outcome::Failed("workers exited without progress".into()));
            }
            Ok(_) => {
                // Остались pending, но воркеры молча вышли — тоже fail.
                final_outcome = Some(Outcome::Failed("workers exited before completion".into()));
            }
            Err(e) => final_outcome = Some(Outcome::Failed(format!("pending: {e}"))),
        }
    }

    // Переводим worker-строки в терминальный статус, парный исходу engine.
    // Делаем это ДО emit'а финального события, чтобы внешний наблюдатель
    // (например, fan-in менеджера, который на Completed пойдёт спаунить
    // следующего в очереди) видел консистентную БД. Best-effort: ошибки
    // персистенции не меняют исход engine.
    let outcome = final_outcome.unwrap();
    match &outcome {
        Outcome::Completed => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_done(rec.id).await {
                    warn!(%id, worker = %rec.id, error = %e, "mark_done failed");
                }
            }
        }
        Outcome::Failed(err) => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_failed(rec.id, err).await {
                    warn!(%id, worker = %rec.id, error = %e, "mark_failed failed");
                }
            }
        }
        Outcome::Cancelled => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_cancelled(rec.id).await {
                    warn!(%id, worker = %rec.id, error = %e, "mark_cancelled failed");
                }
            }
            if let Err(e) = attempts_repo.pause_all_running_for_file(id).await {
                warn!(%id, error = %e, "attempts cleanup on cancel failed");
            }
        }
    }

    match outcome {
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
    record: Option<WorkerRecord>,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    let span = record
        .as_ref()
        .map(|r| tracing::info_span!("worker", id = %r.id, slot = r.slot_index));
    let _guard_span = span.as_ref().map(|s| s.enter());
    let worker_id = record.as_ref().map(|r| r.id);

    while let Some(idx) = next_piece(&pending, &mut state_rx).await {
        let offset = piece_offset(idx, piece_size);
        let size = piece_size_at(idx, piece_size, total_size);
        // Отметить старт попытки — супервизор зарегистрирует attempt в БД.
        if let Some(wid) = worker_id {
            let _ = worker_tx.send(WorkerMsg::AttemptStarted {
                worker_id: wid,
                piece: idx,
            });
        }
        let mut attempt_bytes: u64 = 0;
        let res = run_piece_with_retry(
            &url,
            guard.as_ref(),
            idx,
            offset,
            size,
            storage.as_ref(),
            fetch.as_ref(),
            &bytes_done,
            &config,
            &mut attempt_bytes,
            worker_id,
            &worker_tx,
        )
        .await;
        match res {
            Ok(()) => {
                debug!(piece = idx, "piece done");
                if let Some(wid) = worker_id {
                    let _ = worker_tx.send(WorkerMsg::AttemptFinished {
                        worker_id: wid,
                        piece: idx,
                        bytes: size,
                    });
                }
                if worker_tx.send(WorkerMsg::PieceDone { piece: idx }).is_err() {
                    return;
                }
            }
            Err(PieceError::Transient(msg)) | Err(PieceError::Permanent(msg)) => {
                warn!(piece = idx, error = %msg, "piece failed — stopping worker");
                if let Some(wid) = worker_id {
                    let _ = worker_tx.send(WorkerMsg::AttemptFailed {
                        worker_id: wid,
                        piece: idx,
                        error: msg.clone(),
                    });
                }
                let _ = worker_tx.send(WorkerMsg::Failed(msg));
                return;
            }
        }
    }

    // Выход по пустой очереди или сигналу Stopping. Если мы уходим
    // в паузу с открытой попыткой — сообщить супервизору, чтобы он
    // закрыл строку `paused` в БД.
    if let (Some(wid), RunState::Paused) = (worker_id, *state_rx.borrow()) {
        // Открытой попытки на этом моменте у worker_range нет
        // (next_piece возвращает None до взятия следующего piece),
        // но если бы была — пометили бы её здесь.
        let _ = wid;
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
    attempt_bytes: &mut u64,
    worker_id: Option<WorkerId>,
    worker_tx: &mpsc::UnboundedSender<WorkerMsg>,
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
        *attempt_bytes = written;
        match res {
            Ok(()) => {
                bytes_done.fetch_add(size, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) => {
                let transient = matches!(&e, PieceError::Transient(_));
                let _ = written;
                match config.retry.classify(attempt, transient) {
                    RetryDecision::Retry(delay) => {
                        // Закрываем текущую попытку как failed и сразу
                        // открываем новую — журнал `piece_attempts`
                        // видит каждую сетевую попытку отдельной строкой.
                        if let Some(wid) = worker_id {
                            let msg = match &e {
                                PieceError::Transient(s) | PieceError::Permanent(s) => s.clone(),
                            };
                            let _ = worker_tx.send(WorkerMsg::AttemptFailed {
                                worker_id: wid,
                                piece: idx,
                                error: msg,
                            });
                        }
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        if let Some(wid) = worker_id {
                            let _ = worker_tx.send(WorkerMsg::AttemptStarted {
                                worker_id: wid,
                                piece: idx,
                            });
                        }
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
    record: Option<WorkerRecord>,
) where
    S: TPieceStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    let span = record
        .as_ref()
        .map(|r| tracing::info_span!("worker", id = %r.id, slot = r.slot_index));
    let _guard_span = span.as_ref().map(|s| s.enter());
    let worker_id = record.as_ref().map(|r| r.id);

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

        // В no-Range режиме «попытка» — это одна загрузка всего тела
        // целиком. Для журналирования привязываем её к piece 0 (условный
        // якорь: fetch_full стартует с начала файла, первый piece всегда
        // участвует). Если нужна раздельная статистика по piece'ам — это
        // задача будущей Range-реализации.
        if let Some(wid) = worker_id {
            let _ = worker_tx.send(WorkerMsg::AttemptStarted {
                worker_id: wid,
                piece: 0,
            });
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
                if let Some(wid) = worker_id {
                    let bytes_attempted = pieces_done
                        .iter()
                        .map(|i| piece_size_at(*i, piece_size, total_size))
                        .sum();
                    let _ = worker_tx.send(WorkerMsg::AttemptFinished {
                        worker_id: wid,
                        piece: 0,
                        bytes: bytes_attempted,
                    });
                }
                for idx in pieces_done {
                    if worker_tx.send(WorkerMsg::PieceDone { piece: idx }).is_err() {
                        return;
                    }
                }
                return;
            }
            Err(e) => {
                let transient = matches!(&e, PieceError::Transient(_));
                let msg = match &e {
                    PieceError::Transient(s) | PieceError::Permanent(s) => s.clone(),
                };
                if let Some(wid) = worker_id {
                    let _ = worker_tx.send(WorkerMsg::AttemptFailed {
                        worker_id: wid,
                        piece: 0,
                        error: msg.clone(),
                    });
                }
                match config.retry.classify(attempt, transient) {
                    RetryDecision::Retry(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    RetryDecision::GiveUp => {
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

/// Стриминг-движок: один append-поток, без piece'ов и resume.
///
/// Супервизор читает `fetch_full` одним стримом, передаёт байты в
/// [`TStreamStorage::append_chunk`], эмитит [`Progress`] с
/// `bytes_total = 0` (TUI понимает это как «размер неизвестен»).
/// `Pause`/`Resume` реализуются через break-out of read-loop и повтор
/// `fetch_full`, но так как сервер отдаёт тело без Range, повторная
/// выдача с начала обесценит уже загруженные байты — поэтому пауза
/// в streaming-режиме эквивалентна отмене с точки зрения прогресса;
/// мы всё же поддерживаем её как «не принимать новых байт», а после
/// Resume стартуем заново (storage труцируется).
#[allow(clippy::too_many_arguments)]
async fn run_streaming_engine<SS, F, WR, AR>(
    id: DownloadId,
    inputs: StreamingEngineInputs,
    config: EngineConfig,
    stream: Arc<SS>,
    fetch: Arc<F>,
    workers_repo: Arc<WR>,
    _attempts_repo: Arc<AR>,
    mut cmd_rx: mpsc::UnboundedReceiver<DownloadCommand>,
    events_tx: broadcast::Sender<DownloadEvent>,
) where
    SS: TStreamStorage + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    info!(%id, url = %inputs.spec.url, "streaming engine starting");
    let url = inputs
        .effective_url
        .clone()
        .unwrap_or_else(|| inputs.spec.url.clone());

    // Заводим одну worker-строку, чтобы UI видел «один активный воркер».
    let worker_records = match workers_repo.ensure_slots(id, 1).await {
        Ok(r) => r,
        Err(e) => {
            warn!(%id, error = %e, "ensure_slots failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(DownloadEvent::Failed {
                id,
                error: format!("ensure_slots: {e}"),
            });
            return;
        }
    };

    emit_status(&events_tx, id, FileStatus::Running);

    let bytes_done = Arc::new(AtomicU64::new(0));
    let (state_tx, mut state_rx) = watch::channel(RunState::Running);

    // Основной цикл качания. На Pause — прерываем стрим и ждём Resume;
    // на Resume — стартуем заново (truncate через abort+open не делаем:
    // реализация хранилища владеет файлом и решает).
    let mut final_outcome: Option<Outcome> = None;
    let mut progress_tick = tokio::time::interval(config.progress_interval);
    progress_tick.tick().await;

    'outer: loop {
        // Уважить паузу: ждём Running/Stopping.
        loop {
            let current = *state_rx.borrow();
            match current {
                RunState::Stopping => break 'outer,
                RunState::Paused => {
                    tokio::select! {
                        Some(cmd) = cmd_rx.recv() => {
                            match cmd {
                                DownloadCommand::Resume => {
                                    let _ = state_tx.send(RunState::Running);
                                    emit_status(&events_tx, id, FileStatus::Running);
                                }
                                DownloadCommand::Cancel => {
                                    let _ = state_tx.send(RunState::Stopping);
                                    final_outcome = Some(Outcome::Cancelled);
                                    break 'outer;
                                }
                                DownloadCommand::Pause => {}
                            }
                        }
                        _ = state_rx.changed() => {}
                    }
                    continue;
                }
                RunState::Running => break,
            }
        }

        // Открыть стрим.
        let stream_res = fetch.fetch_full(&url).await;
        let mut byte_stream = match stream_res {
            Ok(s) => s,
            Err(e) => {
                let transient = e.is_transient();
                let msg = e.to_string();
                match config.retry.classify(0, transient) {
                    RetryDecision::Retry(delay) => {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    RetryDecision::GiveUp => {
                        final_outcome = Some(Outcome::Failed(msg));
                        break 'outer;
                    }
                }
            }
        };

        // Качаем поток до EOF или до Pause/Cancel. Выходим через `break`
        // (EOF → Completed), через `continue 'outer` (retry/Pause) или через
        // `break 'outer` (Cancel/permanent). Если внутренний `loop` сошёл
        // `break`'ом без изменения `final_outcome` — это нормальный EOF.
        loop {
            tokio::select! {
                biased;
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        DownloadCommand::Pause => {
                            let _ = state_tx.send(RunState::Paused);
                            emit_status(&events_tx, id, FileStatus::Paused);
                            // Уронить стрим — соединение закроется.
                            drop(byte_stream);
                            continue 'outer;
                        }
                        DownloadCommand::Resume => {}
                        DownloadCommand::Cancel => {
                            let _ = state_tx.send(RunState::Stopping);
                            final_outcome = Some(Outcome::Cancelled);
                            break 'outer;
                        }
                    }
                }
                _ = progress_tick.tick() => {
                    emit_progress_streaming(&events_tx, id, &bytes_done);
                }
                chunk = byte_stream.next() => {
                    match chunk {
                        None => break,
                        Some(Err(e)) => {
                            let transient = e.is_transient();
                            let msg = e.to_string();
                            match config.retry.classify(0, transient) {
                                RetryDecision::Retry(delay) => {
                                    tokio::time::sleep(delay).await;
                                    continue 'outer;
                                }
                                RetryDecision::GiveUp => {
                                    final_outcome = Some(Outcome::Failed(msg));
                                    break 'outer;
                                }
                            }
                        }
                        Some(Ok(bytes)) => {
                            if let Err(e) = stream.append_chunk(&bytes).await {
                                final_outcome = Some(Outcome::Failed(format!("append: {e}")));
                                break 'outer;
                            }
                            bytes_done.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        // EOF: внутренний loop завершился break'ом без установки outcome.
        final_outcome = Some(Outcome::Completed);
        break;
    }

    let outcome = final_outcome.unwrap_or(Outcome::Failed(
        "streaming engine exit without outcome".into(),
    ));

    // Обновить worker-строку в соответствии с исходом.
    match &outcome {
        Outcome::Completed => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_done(rec.id).await {
                    warn!(%id, error = %e, "mark_done failed");
                }
            }
        }
        Outcome::Failed(err) => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_failed(rec.id, err).await {
                    warn!(%id, error = %e, "mark_failed failed");
                }
            }
        }
        Outcome::Cancelled => {
            for rec in &worker_records {
                if let Err(e) = workers_repo.mark_cancelled(rec.id).await {
                    warn!(%id, error = %e, "mark_cancelled failed");
                }
            }
        }
    }

    match outcome {
        Outcome::Completed => match stream.finalize().await {
            Ok(()) => {
                info!(%id, "streaming download completed");
                emit_progress_streaming(&events_tx, id, &bytes_done);
                emit_status(&events_tx, id, FileStatus::Done);
                let _ = events_tx.send(DownloadEvent::Completed { id });
            }
            Err(e) => {
                warn!(%id, error = %e, "streaming finalize failed");
                emit_status(&events_tx, id, FileStatus::Failed);
                let _ = events_tx.send(DownloadEvent::Failed {
                    id,
                    error: format!("finalize: {e}"),
                });
            }
        },
        Outcome::Failed(err) => {
            warn!(%id, error = %err, "streaming download failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(DownloadEvent::Failed { id, error: err });
        }
        Outcome::Cancelled => {
            info!(%id, "streaming download cancelled");
            let _ = stream.abort().await;
            emit_status(&events_tx, id, FileStatus::Cancelled);
        }
    }
}

fn emit_progress_streaming(
    tx: &broadcast::Sender<DownloadEvent>,
    id: DownloadId,
    bytes_done: &AtomicU64,
) {
    let done = bytes_done.load(Ordering::Relaxed);
    let progress = Progress {
        bytes_done: done,
        // 0 = «размер неизвестен». TUI понимает это как indeterminate-gauge.
        bytes_total: 0,
        pieces_done: 0,
        pieces_total: 0,
        speed_bps: 0.0,
        eta_secs: None,
    };
    let _ = tx.send(DownloadEvent::Progress { id, progress });
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

    use crate::ports::{
        NoopAttemptRepo,
        NoopWorkerRepo,
    };

    fn noop_repos() -> (Arc<NoopWorkerRepo>, Arc<NoopAttemptRepo>) {
        (Arc::new(NoopWorkerRepo), Arc::new(NoopAttemptRepo))
    }

    fn fast_config() -> EngineConfig {
        EngineConfig {
            write_buffer: 16,
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
            effective_url: None,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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
            noop_repos().0,
            noop_repos().1,
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

    // ─── Worker / attempt tracking tests ────────────────────────────────

    use crate::domain::{
        AttemptStatus,
        WorkerStatus,
    };
    use crate::testing::{
        MemoryAttemptRepo,
        MemoryWorkerRepo,
    };

    /// Две engine-сессии на один файл. Первая «падает» на полпути
    /// (handle-task дропается до того, как воркеры успели добежать
    /// до терминала) — её строки остаются `running`. Вторая сессия
    /// в `ensure_slots` делает защитный sweep: старые → `paused`,
    /// новые → свежие UUID'ы со статусом `running`/`done`.
    #[tokio::test]
    async fn two_sessions_produce_disjoint_worker_sets() {
        let plan = TestPlan {
            piece_size: 20,
            count: 4,
        };
        let workers = Arc::new(MemoryWorkerRepo::new());
        let attempts = Arc::new(MemoryAttemptRepo::new());
        let file_id = DownloadId::new();

        // Сессия 1: медленный fetch — снимаем её до финализации
        // (drop handle на половине работы).
        let storage1 = Arc::new(MemoryPieceStorage::new(4, 20));
        let fetch1 = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(200)));
        let (handle1, _rx1) = DownloadEngine::spawn(
            file_id,
            inputs_range(plan),
            fast_config(),
            storage1,
            fetch1,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Имитация «демон упал»: дропаем engine-task, не дожидаясь
        // terminal-outcome.
        handle1.join.abort();
        let _ = handle1.join.await;
        let first_ids: Vec<_> = workers.by_file(file_id).iter().map(|w| w.id).collect();
        assert_eq!(first_ids.len(), 2);
        // Строки всё ещё `running` — engine не успел их перевести.
        for w in workers.by_file(file_id) {
            assert_eq!(w.status, WorkerStatus::Running);
        }

        // Сессия 2: свежий storage и быстрый fetch.
        let storage2 = Arc::new(MemoryPieceStorage::new(4, 20));
        let fetch2 = Arc::new(MockFetch::always_ok(plan));
        let (handle2, rx2) = DownloadEngine::spawn(
            file_id,
            inputs_range(plan),
            fast_config(),
            storage2,
            fetch2,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = collect_events(rx2).await;
        handle2.join.await.unwrap();

        let all = workers.by_file(file_id);
        assert_eq!(all.len(), 4);
        let second_ids: Vec<_> = all
            .iter()
            .filter(|w| !first_ids.contains(&w.id))
            .map(|w| w.id)
            .collect();
        assert_eq!(second_ids.len(), 2);
        for w in all {
            if first_ids.contains(&w.id) {
                assert_eq!(w.status, WorkerStatus::Paused);
            } else {
                assert_eq!(w.status, WorkerStatus::Done);
            }
        }
    }

    /// Имитация «демон упал»: предыдущие строки остались `running`,
    /// глобальный sweep их закрывает, затем `ensure_slots` выдаёт свежие.
    #[tokio::test]
    async fn crash_recovery_sweep_paves_the_way_for_next_session() {
        let workers = Arc::new(MemoryWorkerRepo::new());
        let attempts = Arc::new(MemoryAttemptRepo::new());
        let file_id = DownloadId::new();

        // Эмулируем 3 «залипших» running-воркера.
        let stale = workers.ensure_slots(file_id, 3).await.unwrap();
        assert!(
            workers
                .by_file(file_id)
                .iter()
                .all(|w| w.status == WorkerStatus::Running)
        );

        // Daemon startup recovery.
        workers.pause_all_running_globally().await.unwrap();
        attempts.pause_all_running_globally().await.unwrap();
        for w in workers.by_file(file_id) {
            assert_eq!(w.status, WorkerStatus::Paused);
        }

        // Реальный старт новой сессии.
        let plan = TestPlan {
            piece_size: 20,
            count: 1,
        };
        let storage = Arc::new(MemoryPieceStorage::new(1, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        let (handle, rx) = DownloadEngine::spawn(
            file_id,
            inputs_range(plan),
            fast_config(),
            storage,
            fetch,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = collect_events(rx).await;
        handle.join.await.unwrap();

        let all = workers.by_file(file_id);
        // 3 старых paused + новый набор (fast_config, spec.workers=2).
        let paused: Vec<_> = all
            .iter()
            .filter(|w| stale.iter().any(|s| s.id == w.id))
            .collect();
        assert_eq!(paused.len(), 3);
        for w in &paused {
            assert_eq!(w.status, WorkerStatus::Paused);
        }
        let fresh: Vec<_> = all
            .iter()
            .filter(|w| !stale.iter().any(|s| s.id == w.id))
            .collect();
        assert!(!fresh.is_empty());
        for w in fresh {
            assert_eq!(w.status, WorkerStatus::Done);
        }
    }

    /// Piece retry: первая попытка 503 → `failed`, вторая → `done`.
    /// В журнале остаются обе строки.
    #[tokio::test]
    async fn piece_retry_appends_new_attempt_row() {
        let plan = TestPlan {
            piece_size: 20,
            count: 1,
        };
        let storage = Arc::new(MemoryPieceStorage::new(1, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        fetch.set_piece_plan(0, vec![Outcome::Transient500]);

        let workers = Arc::new(MemoryWorkerRepo::new());
        let attempts = Arc::new(MemoryAttemptRepo::new());
        let file_id = DownloadId::new();

        let mut inputs = inputs_range(plan);
        inputs.spec.workers = 1;
        let (handle, rx) = DownloadEngine::spawn(
            file_id,
            inputs,
            fast_config(),
            storage,
            fetch,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = collect_events(rx).await;
        handle.join.await.unwrap();

        let rows = attempts.by_piece(file_id, 0);
        assert_eq!(
            rows.len(),
            2,
            "expected failed+done attempts, got {:?}",
            rows
        );
        let statuses: Vec<_> = rows.iter().map(|r| r.status).collect();
        assert!(statuses.contains(&AttemptStatus::Failed));
        assert!(statuses.contains(&AttemptStatus::Done));
    }

    /// Cancel посреди загрузки: активные воркеры → cancelled,
    /// running attempt'ы файла → paused (чистим хвост).
    #[tokio::test]
    async fn cancel_marks_workers_cancelled() {
        let plan = TestPlan {
            piece_size: 20,
            count: 6,
        };
        let storage = Arc::new(MemoryPieceStorage::new(6, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan).with_delay(Duration::from_millis(50)));
        let workers = Arc::new(MemoryWorkerRepo::new());
        let attempts = Arc::new(MemoryAttemptRepo::new());
        let file_id = DownloadId::new();

        let (handle, mut rx) = DownloadEngine::spawn(
            file_id,
            inputs_range(plan),
            fast_config(),
            storage,
            fetch,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = rx.recv().await;
        // Даём воркерам начать пилить piece'ы.
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(handle.cancel());
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

        for w in workers.by_file(file_id) {
            assert_eq!(
                w.status,
                WorkerStatus::Cancelled,
                "worker {:?} not cancelled",
                w
            );
        }
        // Любая открытая попытка закрыта (paused/failed).
        let still_running = attempts
            .snapshot()
            .into_iter()
            .filter(|r| r.file_id == file_id && r.status == AttemptStatus::Running)
            .count();
        assert_eq!(still_running, 0);
    }

    /// `max_workers` меняется между сессиями: 4 → 2. Первый набор — paused,
    /// второй — 2 свежих записи со `slot_index` 0, 1.
    #[tokio::test]
    async fn worker_count_change_between_sessions() {
        let plan = TestPlan {
            piece_size: 20,
            count: 2,
        };
        let storage1 = Arc::new(MemoryPieceStorage::new(2, 20));
        let fetch = Arc::new(MockFetch::always_ok(plan));
        let workers = Arc::new(MemoryWorkerRepo::new());
        let attempts = Arc::new(MemoryAttemptRepo::new());
        let file_id = DownloadId::new();

        let mut inputs1 = inputs_range(plan);
        inputs1.spec.workers = 4;
        let (h1, rx1) = DownloadEngine::spawn(
            file_id,
            inputs1,
            fast_config(),
            storage1,
            fetch.clone(),
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = collect_events(rx1).await;
        h1.join.await.unwrap();
        let first_ids: Vec<_> = workers.by_file(file_id).iter().map(|w| w.id).collect();
        assert_eq!(first_ids.len(), 4);

        let storage2 = Arc::new(MemoryPieceStorage::new(2, 20));
        let mut inputs2 = inputs_range(plan);
        inputs2.spec.workers = 2;
        let (h2, rx2) = DownloadEngine::spawn(
            file_id,
            inputs2,
            fast_config(),
            storage2,
            fetch,
            Arc::clone(&workers),
            Arc::clone(&attempts),
        );
        let _ = collect_events(rx2).await;
        h2.join.await.unwrap();

        let all = workers.by_file(file_id);
        assert_eq!(all.len(), 6, "4 старых + 2 свежих");
        let fresh: Vec<_> = all.iter().filter(|w| !first_ids.contains(&w.id)).collect();
        assert_eq!(fresh.len(), 2);
        let mut slots: Vec<_> = fresh.iter().map(|w| w.slot_index).collect();
        slots.sort_unstable();
        assert_eq!(slots, vec![0, 1]);
    }
}
