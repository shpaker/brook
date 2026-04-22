//! No-Range воркер: один стрим от 0 до EOF, раскладывается по piece'ам
//! последовательно. Используется, когда сервер не поддерживает Range —
//! распараллеливание невозможно, так что worker-строка всего одна.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use futures_util::StreamExt;
use tokio::sync::{
    mpsc,
    watch,
};

use super::range::{
    PieceError,
    range_err_to_piece,
};
use super::supervisor::{
    RunState,
    WorkerMsg,
};
use super::{
    EngineConfig,
    piece_offset,
    piece_size_at,
    pieces_total,
};
use crate::ports::{
    TPieceStorage,
    TRangeFetch,
    WorkerRecord,
};
use crate::service::retry::RetryDecision;

#[allow(clippy::too_many_arguments)]
pub(super) async fn worker_full<S, F>(
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
