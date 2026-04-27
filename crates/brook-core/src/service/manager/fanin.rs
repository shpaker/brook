//! Fan-in задачи: слушают broadcast каждого движка и форвардят события
//! в общий канал менеджера. `fan_in_lifecycle` дополнительно обновляет
//! `records`, персистит смены статуса и на терминальном событии двигает
//! очередь дальше.

use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::broadcast;
use tracing::{
    debug,
    warn,
};

use super::{
    DownloadManager,
    Shared,
};
use crate::domain::{
    FailureReason,
    FileId,
    FileLifecycleEvent,
    FileStatus,
    ProgressEvent,
    ReasonCode,
};
use crate::ports::{
    TPieceAttemptRepo,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TWorkerRepo,
};

pub(super) async fn fan_in_lifecycle<PF, QS, F, WR, AR>(
    shared: Arc<Shared<PF, QS, F, WR, AR>>,
    id: FileId,
    mut rx: broadcast::Receiver<FileLifecycleEvent>,
) where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    // Выходим из цикла только когда канал событий движка закрылся
    // (движок завершил таск и уронил свой `Sender`). До этого момента
    // форвардим все события — иначе можно пропустить финальный `Completed`
    // или `Failed`, идущий сразу после `StateChanged(Done/Failed)`.
    loop {
        let ev = match rx.recv().await {
            Ok(e) => e,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(%id, lagged = n, "engine lifecycle stream lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        let mut status_to_persist: Option<FileStatus> = None;
        let mut reason_to_persist: Option<FailureReason> = None;
        {
            let mut inner = shared.inner.lock().expect("mutex poisoned");
            if let Some(record) = inner.records.get_mut(&id) {
                if record.status != ev.status {
                    record.status = ev.status;
                    record.updated_at = SystemTime::now();
                    status_to_persist = Some(ev.status);
                }
                if matches!(ev.status, FileStatus::Failed) {
                    // У engine нет типизированного Error — только строка.
                    // Stage 3+ поднимет ReasonCode в FileLifecycleEvent;
                    // пока маппим свободный текст в Unknown и сохраняем его
                    // в сообщении, чтобы причина не терялась.
                    let msg = ev.description.clone().unwrap_or_default();
                    record.error = Some(msg.clone());
                    status_to_persist = Some(FileStatus::Failed);
                    reason_to_persist = Some(FailureReason::with_message(ReasonCode::Unknown, msg));
                }
            }
        }
        let _ = shared.lifecycle_tx.send(ev);
        if let Some(status) = status_to_persist
            && let Err(e) = shared
                .queue
                .update_status(id, status, reason_to_persist)
                .await
        {
            warn!(%id, %status, error = %e, "failed to persist status change");
        }
    }
    let handle = {
        let mut inner = shared.inner.lock().expect("mutex poisoned");
        inner.engines.remove(&id)
    };
    if let Some(h) = handle {
        h.join().await;
    }
    debug!(%id, "engine task terminated, advancing queue");
    advance_queue(Arc::clone(&shared)).await;
}

/// Фан-ин прогресс-тиков: просто форвардит `ProgressEvent` в общий
/// broadcast, без касания `records` и без влияния на lifecycle.
pub(super) async fn fan_in_progress<PF, QS, F, WR, AR>(
    shared: Arc<Shared<PF, QS, F, WR, AR>>,
    id: FileId,
    mut rx: broadcast::Receiver<ProgressEvent>,
) where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let _ = shared.progress_tx.send(ev);
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(%id, lagged = n, "engine progress stream lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Попытаться поднять следующий движок после завершения предыдущего.
/// Вынесено как свободная функция, чтобы не тянуть `DownloadManager` в
/// сигнатуру fan-in task (он бы создал больше генериков).
async fn advance_queue<PF, QS, F, WR, AR>(shared: Arc<Shared<PF, QS, F, WR, AR>>)
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    let manager = DownloadManager { shared };
    manager.try_spawn_next().await;
}
