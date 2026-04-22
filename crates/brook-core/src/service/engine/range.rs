//! Range-воркер: берёт один piece из общей очереди, качает его через
//! `fetch_range` с ретраями, пишет в [`TPieceStorage`]. Работа нескольких
//! воркеров координируется через `Arc<Mutex<VecDeque<u32>>>`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use futures_util::StreamExt;
use tokio::sync::{
    Mutex,
    mpsc,
    watch,
};
use tracing::{
    debug,
    warn,
};

use super::supervisor::{
    RunState,
    WorkerMsg,
    next_piece,
};
use super::{
    EngineConfig,
    piece_offset,
    piece_size_at,
};
use crate::domain::WorkerId;
use crate::ports::{
    ByteStream,
    RangeError,
    RangeGuard,
    TPieceStorage,
    TRangeFetch,
    WorkerRecord,
};
use crate::service::retry::RetryDecision;

pub(super) enum PieceError {
    Transient(String),
    Permanent(String),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn worker_range<S, F>(
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

    // Выход по пустой очереди или сигналу Stopping.
    if let (Some(wid), RunState::Paused) = (worker_id, *state_rx.borrow()) {
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

pub(super) async fn drain_stream_into_piece<S>(
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

pub(super) fn range_err_to_piece(e: RangeError) -> PieceError {
    if e.is_transient() {
        PieceError::Transient(e.to_string())
    } else {
        PieceError::Permanent(e.to_string())
    }
}
