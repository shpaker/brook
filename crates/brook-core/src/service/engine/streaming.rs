//! Стриминг-движок для unknown-size загрузок (`Content-Length` отсутствует).
//!
//! Супервизор читает `fetch_full` одним стримом, передаёт байты в
//! [`TStreamStorage::append_chunk`], эмитит [`Progress`] с
//! `bytes_total = 0` (TUI понимает это как «размер неизвестен»).
//! `Pause`/`Resume` реализуются через break-out of read-loop и повтор
//! `fetch_full`, но так как сервер отдаёт тело без Range, повторная
//! выдача с начала обесценит уже загруженные байты — поэтому пауза
//! в streaming-режиме эквивалентна отмене с точки зрения прогресса;
//! мы всё же поддерживаем её как «не принимать новых байт», а после
//! Resume стартуем заново (storage труцируется).

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::time::{
    Duration,
    Instant,
};

use futures_util::StreamExt;
use tokio::sync::{
    broadcast,
    mpsc,
    watch,
};
use tracing::{
    info,
    warn,
};

use super::StreamingEngineInputs;
use super::supervisor::{
    Outcome,
    RunState,
    SpeedMeter,
    emit_status,
};
use crate::domain::{
    FileCommand,
    FileId,
    FileLifecycleEvent,
    FileStatus,
    Progress,
    ProgressEvent,
};
use crate::ports::{
    TPieceAttemptRepo,
    TRangeFetch,
    TStreamStorage,
    TWorkerRepo,
};
use crate::service::retry::RetryDecision;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_streaming_engine<SS, F, WR, AR>(
    id: FileId,
    inputs: StreamingEngineInputs,
    config: super::EngineConfig,
    stream: Arc<SS>,
    fetch: Arc<F>,
    workers_repo: Arc<WR>,
    _attempts_repo: Arc<AR>,
    mut cmd_rx: mpsc::UnboundedReceiver<FileCommand>,
    events_tx: broadcast::Sender<FileLifecycleEvent>,
    progress_tx: broadcast::Sender<ProgressEvent>,
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
            let _ = events_tx.send(FileLifecycleEvent::Failed {
                id,
                error: format!("ensure_slots: {e}"),
            });
            return;
        }
    };

    emit_status(&events_tx, id, FileStatus::Running);

    let bytes_done = Arc::new(AtomicU64::new(0));
    let (state_tx, mut state_rx) = watch::channel(RunState::Running);
    let mut speed_meter = SpeedMeter::new(Duration::from_secs(3));

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
                                FileCommand::Resume => {
                                    let _ = state_tx.send(RunState::Running);
                                    emit_status(&events_tx, id, FileStatus::Running);
                                    speed_meter.reset();
                                }
                                FileCommand::Cancel => {
                                    let _ = state_tx.send(RunState::Stopping);
                                    final_outcome = Some(Outcome::Cancelled);
                                    break 'outer;
                                }
                                FileCommand::Pause => {}
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
                        FileCommand::Pause => {
                            let _ = state_tx.send(RunState::Paused);
                            emit_status(&events_tx, id, FileStatus::Paused);
                            speed_meter.reset();
                            // Уронить стрим — соединение закроется.
                            drop(byte_stream);
                            continue 'outer;
                        }
                        FileCommand::Resume => {}
                        FileCommand::Cancel => {
                            let _ = state_tx.send(RunState::Stopping);
                            final_outcome = Some(Outcome::Cancelled);
                            break 'outer;
                        }
                    }
                }
                _ = progress_tick.tick() => {
                    emit_progress_streaming(&progress_tx, id, &bytes_done, &mut speed_meter);
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
                emit_progress_streaming(&progress_tx, id, &bytes_done, &mut speed_meter);
                emit_status(&events_tx, id, FileStatus::Done);
                let _ = events_tx.send(FileLifecycleEvent::Completed { id });
            }
            Err(e) => {
                warn!(%id, error = %e, "streaming finalize failed");
                emit_status(&events_tx, id, FileStatus::Failed);
                let _ = events_tx.send(FileLifecycleEvent::Failed {
                    id,
                    error: format!("finalize: {e}"),
                });
            }
        },
        Outcome::Failed(err) => {
            warn!(%id, error = %err, "streaming download failed");
            emit_status(&events_tx, id, FileStatus::Failed);
            let _ = events_tx.send(FileLifecycleEvent::Failed { id, error: err });
        }
        Outcome::Cancelled => {
            info!(%id, "streaming download cancelled");
            let _ = stream.abort().await;
            emit_status(&events_tx, id, FileStatus::Cancelled);
        }
    }
}

fn emit_progress_streaming(
    tx: &broadcast::Sender<ProgressEvent>,
    id: FileId,
    bytes_done: &AtomicU64,
    meter: &mut SpeedMeter,
) {
    let done = bytes_done.load(Ordering::Relaxed);
    meter.observe(Instant::now(), done);
    let progress = Progress {
        bytes_done: done,
        // 0 = «размер неизвестен». TUI понимает это как indeterminate-gauge.
        bytes_total: 0,
        pieces_done: 0,
        pieces_total: 0,
        speed_bps: meter.speed_bps(),
        // ETA непредставима без total_size — TUI покажет «unknown».
        eta_secs: None,
        // Streaming-движок всегда однопоточный (no-Range).
        workers_count: 1,
    };
    let _ = tx.send(ProgressEvent::Tick { id, progress });
}
