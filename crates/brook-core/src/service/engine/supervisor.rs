//! Главный цикл range/no-range движка — всё, что живёт в одном
//! `tokio::select!`: команды, обработка сообщений воркеров, прогресс-тики,
//! финализация. Геометрию piece'ов и фасад API см. в [`super`].

use std::collections::{
    HashMap,
    VecDeque,
};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::time::{
    Duration,
    Instant,
};

use tokio::sync::{
    Mutex,
    broadcast,
    mpsc,
    watch,
};
use tokio::task::JoinHandle;
use tracing::{
    info,
    warn,
};

use super::{
    EngineConfig,
    EngineInputs,
    piece_size_at,
    pieces_total,
};
use crate::domain::{
    AttemptId,
    FileCommand,
    FileId,
    FileLifecycleEvent,
    FileStatus,
    Progress,
    ProgressEvent,
    WorkerId,
};
use crate::ports::{
    TPieceAttemptRepo,
    TPieceStorage,
    TRangeFetch,
    TWorkerRepo,
};

/// Внутреннее состояние супервизора. Передаётся воркерам через `watch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunState {
    Running,
    Paused,
    Stopping,
}

/// Сообщения от воркеров к супервизору.
///
/// Attempt-lifecycle-события (`AttemptStarted` / `AttemptFinished` /
/// `AttemptFailed`) шлются тем же каналом, чтобы все DB-writes происходили
/// в одном месте — в select-цикле супервизора. Супервизор гарантирует, что
/// терминальное сообщение attempt'а обработано до того, как engine перейдёт
/// к финализации (дренаж `worker_rx.try_recv()` после join'а).
pub(super) enum WorkerMsg {
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

pub(super) enum Outcome {
    Completed,
    Failed(String),
    Cancelled,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_engine<S, F, WR, AR>(
    id: FileId,
    inputs: EngineInputs,
    config: EngineConfig,
    storage: Arc<S>,
    fetch: Arc<F>,
    workers_repo: Arc<WR>,
    attempts_repo: Arc<AR>,
    mut cmd_rx: mpsc::UnboundedReceiver<FileCommand>,
    events_tx: broadcast::Sender<FileLifecycleEvent>,
    progress_tx: broadcast::Sender<ProgressEvent>,
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
            let _ = events_tx.send(FileLifecycleEvent::Failed {
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
                let _ = events_tx.send(FileLifecycleEvent::Completed { id });
            }
            Err(e) => {
                warn!(%id, error = %e, "finalize failed");
                emit_status(&events_tx, id, FileStatus::Failed);
                let _ = events_tx.send(FileLifecycleEvent::Failed {
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

    // Решаем, сколько воркеров пустить. No-Range — строго один.
    let worker_count = if inputs.accepts_ranges {
        crate::compute_workers(inputs.total_size).max(1) as usize
    } else {
        1
    };

    let pending_arc: Arc<Mutex<VecDeque<u32>>> = Arc::new(Mutex::new(pending.into()));
    let (state_tx, state_rx) = watch::channel(RunState::Running);
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<WorkerMsg>();
    // Бейзлайн для прогресса: сколько уже на диске = total - pending.
    let bytes_done = Arc::new(AtomicU64::new(
        inputs.total_size.saturating_sub(bytes_in_pending),
    ));
    info!(%id, pieces = total_pieces_expected, workers = worker_count, accepts_ranges = inputs.accepts_ranges, "spawning workers");

    // Заводим фиксированный набор worker-строк под эту engine-сессию.
    // `ensure_slots` защитно сбрасывает все running-воркеры этого файла
    // в `paused` — если предыдущая сессия не успела этого сделать сама.
    let worker_records = match workers_repo.ensure_slots(id, worker_count).await {
        Ok(r) => r,
        Err(e) => {
            warn!(%id, error = %e, "ensure_slots failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(FileLifecycleEvent::Failed {
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
            let h = tokio::spawn(super::range::worker_range(
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
        let h = tokio::spawn(super::full::worker_full(
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
    let mut speed_meter = SpeedMeter::new(Duration::from_secs(3));
    let mut pieces_committed: usize = 0;
    let mut final_outcome: Option<Outcome> = None;
    // Маппинг (worker_id, piece_number) → AttemptId для открытых попыток.
    // На один (worker, piece) в любой момент жива не более чем одна
    // попытка (воркер либо качает piece, либо закрыл и идёт за следующим).
    let mut open_attempts: HashMap<(WorkerId, u32), AttemptId> = HashMap::new();

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    FileCommand::Pause => {
                        if *state_rx.borrow() == RunState::Running {
                            info!(%id, "pausing");
                            let _ = state_tx.send(RunState::Paused);
                            emit_status(&events_tx, id, FileStatus::Paused);
                            speed_meter.reset();
                        }
                    }
                    FileCommand::Resume => {
                        if *state_rx.borrow() == RunState::Paused {
                            info!(%id, "resuming");
                            let _ = state_tx.send(RunState::Running);
                            emit_status(&events_tx, id, FileStatus::Running);
                            speed_meter.reset();
                        }
                    }
                    FileCommand::Cancel => {
                        info!(%id, "cancelling");
                        let _ = state_tx.send(RunState::Stopping);
                        final_outcome = Some(Outcome::Cancelled);
                        break;
                    }
                }
            }
            Some(msg) = worker_rx.recv() => {
                match msg {
                    WorkerMsg::PieceDone { piece: idx } => {
                        if let Err(e) = storage.commit_done(idx).await {
                            final_outcome = Some(Outcome::Failed(format!("commit: {e}")));
                            let _ = state_tx.send(RunState::Stopping);
                            break;
                        }
                        pieces_committed += 1;
                        emit_progress(
                            &progress_tx,
                            id,
                            &bytes_done,
                            &inputs,
                            pieces_committed as u32,
                            total_pieces,
                            &mut speed_meter,
                        );
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
                emit_progress(
                    &progress_tx,
                    id,
                    &bytes_done,
                    &inputs,
                    pieces_committed as u32,
                    total_pieces,
                    &mut speed_meter,
                );
            }
            else => {
                // Все воркеры и command-канал закрылись — работа окончена.
                break;
            }
        }

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
    // останутся «висящие» running-attempt'ы этой сессии).
    while let Ok(msg) = worker_rx.try_recv() {
        match msg {
            WorkerMsg::PieceDone { piece: idx } => {
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
    // (например, fan-in менеджера) видел консистентную БД. Best-effort:
    // ошибки персистенции не меняют исход engine.
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
                        &progress_tx,
                        id,
                        &bytes_done,
                        &inputs,
                        total_pieces,
                        total_pieces,
                        &mut speed_meter,
                    );
                    emit_status(&events_tx, id, FileStatus::Done);
                    let _ = events_tx.send(FileLifecycleEvent::Completed { id });
                }
                Err(e) => {
                    warn!(%id, error = %e, "finalize failed");
                    emit_status(&events_tx, id, FileStatus::Failed);
                    let _ = events_tx.send(FileLifecycleEvent::Failed {
                        id,
                        error: format!("finalize: {e}"),
                    });
                }
            }
        }
        Outcome::Failed(err) => {
            warn!(%id, error = %err, "download failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(FileLifecycleEvent::Failed { id, error: err });
        }
        Outcome::Cancelled => {
            info!(%id, "download cancelled — aborting storage");
            let _ = storage.abort().await;
            emit_status(&events_tx, id, FileStatus::Cancelled);
        }
    }
}

pub(super) fn emit_status(
    tx: &broadcast::Sender<FileLifecycleEvent>,
    id: FileId,
    status: FileStatus,
) {
    let _ = tx.send(FileLifecycleEvent::StatusChanged { id, status });
}

pub(super) fn emit_progress(
    tx: &broadcast::Sender<ProgressEvent>,
    id: FileId,
    bytes_done: &AtomicU64,
    inputs: &EngineInputs,
    pieces_done: u32,
    pieces_total: u32,
    meter: &mut SpeedMeter,
) {
    let done = bytes_done.load(Ordering::Relaxed);
    meter.observe(Instant::now(), done);
    let remaining = inputs.total_size.saturating_sub(done);
    let progress = Progress {
        bytes_done: done,
        bytes_total: inputs.total_size,
        pieces_done,
        pieces_total,
        speed_bps: meter.speed_bps(),
        eta_secs: if inputs.total_size > 0 {
            meter.eta_secs(remaining)
        } else {
            None
        },
    };
    let _ = tx.send(ProgressEvent::Tick { id, progress });
}

/// Сглаженная скорость по экспоненциальному среднему (EMA).
///
/// Каждое наблюдение `(t, bytes_done)` превращается в мгновенную скорость
/// `Δbytes/Δt`, затем сливается в `smoothed` с весом `α = 1 - exp(-Δt/τ)`.
/// τ ≈ 3 с — компромисс: достаточно быстро реагирует на изменение,
/// но не прыгает от отдельных piece-коммитов.
///
/// `reset()` обнуляет историю — используется при Pause/Resume, чтобы
/// первое наблюдение после возобновления стало новой точкой отсчёта
/// (иначе дыра в Δt даст «мгновенную скорость = Δbytes / долгий Δt»,
/// т. е. резкое занижение).
pub(super) struct SpeedMeter {
    tau: Duration,
    last: Option<(Instant, u64)>,
    smoothed: Option<f64>,
}

impl SpeedMeter {
    pub(super) fn new(tau: Duration) -> Self {
        Self {
            tau,
            last: None,
            smoothed: None,
        }
    }

    pub(super) fn reset(&mut self) {
        self.last = None;
        self.smoothed = None;
    }

    pub(super) fn observe(&mut self, now: Instant, bytes: u64) {
        if let Some((t_prev, b_prev)) = self.last {
            let dt = now.saturating_duration_since(t_prev).as_secs_f64();
            if dt > 0.0 && bytes >= b_prev {
                let instant = (bytes - b_prev) as f64 / dt;
                let alpha = 1.0 - (-dt / self.tau.as_secs_f64()).exp();
                self.smoothed = Some(match self.smoothed {
                    Some(s) => alpha * instant + (1.0 - alpha) * s,
                    None => instant,
                });
            }
        }
        self.last = Some((now, bytes));
    }

    pub(super) fn speed_bps(&self) -> f64 {
        self.smoothed.unwrap_or(0.0)
    }

    pub(super) fn eta_secs(&self, remaining: u64) -> Option<u64> {
        let s = self.smoothed?;
        if s < 1.0 {
            return None;
        }
        Some((remaining as f64 / s).round() as u64)
    }
}

/// Достать следующий piece из очереди с учётом паузы/стопа. Возвращает
/// `None` только когда очередь пуста или движок останавливается.
pub(super) async fn next_piece(
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
