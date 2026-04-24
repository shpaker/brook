//! Продвижение очереди и спавн движков.
//!
//! [`DownloadManager::try_spawn_next`] и [`spawn_engine_impl`] вынесены
//! из модуля фасада, чтобы публичный API в [`super`] был чище.

use std::sync::Arc;
use std::time::SystemTime;

use tracing::warn;

use super::{
    DownloadManager,
    Shared,
};
use crate::domain::{
    FailureReason,
    FileId,
    FileLifecycleEvent,
    FileStatus,
};
use crate::error::{
    Error,
    Result,
};
use crate::ports::{
    PreparedMode,
    TPieceAttemptRepo,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TWorkerRepo,
};
use crate::service::engine::{
    DownloadEngine,
    EngineInputs,
    EngineSubscriptions,
    StreamingEngineInputs,
};

impl<PF, QS, F, WR, AR> DownloadManager<PF, QS, F, WR, AR>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    /// Продвинуть очередь — спаунить движки для всех ожидающих по порядку.
    pub(super) async fn try_spawn_next(&self) {
        loop {
            let next = {
                let mut inner = self.shared.inner.lock().expect("mutex poisoned");
                // Перебираем `waiting` до первого подходящего (Queued);
                // Paused/Cancelled игнорируем и пропускаем.
                let mut picked: Option<FileId> = None;
                while let Some(id) = inner.waiting.pop_front() {
                    match inner.records.get(&id).map(|d| d.status) {
                        Some(FileStatus::Pending) | Some(FileStatus::Retrying) => {
                            picked = Some(id);
                            break;
                        }
                        Some(_) => continue, // не Queued — пропускаем
                        None => continue,
                    }
                }
                picked
            };
            let Some(id) = next else { return };
            if let Err(e) = self.spawn_engine(id).await {
                warn!(%id, error = %e, "failed to spawn engine");
                let reason = FailureReason::from_error(&e);
                {
                    let mut inner = self.shared.inner.lock().expect("mutex poisoned");
                    if let Some(record) = inner.records.get_mut(&id) {
                        record.status = FileStatus::Failed;
                        record.error = Some(e.to_string());
                        record.updated_at = SystemTime::now();
                    }
                }
                let _ = self
                    .shared
                    .queue
                    .update_status(id, FileStatus::Failed, Some(reason))
                    .await;
                let _ = self.shared.lifecycle_tx.send(FileLifecycleEvent::Failed {
                    id,
                    error: e.to_string(),
                });
            }
        }
    }

    /// Обёртка над [`spawn_engine_impl`] с явной аннотацией `+ Send` на
    /// возвращаемом типе. Нужна, потому что авто-вывод `Send` у Rust
    /// спотыкается на цикле вызовов
    /// `try_spawn_next → spawn_engine_impl → tokio::spawn(fan_in_lifecycle) →
    /// advance_queue → try_spawn_next`; явная граница разрывает цикл
    /// инференса.
    fn spawn_engine(&self, id: FileId) -> impl std::future::Future<Output = Result<()>> + Send {
        let shared = Arc::clone(&self.shared);
        async move { spawn_engine_impl(shared, id).await }
    }
}

/// Поднять движок для `id`. Вынесено как свободная функция: инстанс
/// менеджера для этого не нужен, а fan-in task всё равно работает от
/// `Arc<Shared>`.
pub(super) async fn spawn_engine_impl<PF, QS, F, WR, AR>(
    shared: Arc<Shared<PF, QS, F, WR, AR>>,
    id: FileId,
) -> Result<()>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    let maybe_spec = {
        let inner = shared.inner.lock().expect("mutex poisoned");
        inner.records.get(&id).map(|d| d.spec.clone())
    };
    let spec = maybe_spec.ok_or(Error::NotFound)?;
    let prepared = shared.factory.prepare(id, &spec).await?;
    let effective_url = prepared.effective_url.clone();
    let (
        handle,
        EngineSubscriptions {
            lifecycle: lifecycle_rx,
            progress: progress_rx,
        },
    ) = match prepared.mode {
        PreparedMode::Known {
            total_size,
            piece_size,
            accepts_ranges,
            guard,
        } => {
            let inputs = EngineInputs {
                spec,
                total_size,
                piece_size,
                accepts_ranges,
                guard,
                effective_url,
            };
            let storage = Arc::new(
                prepared
                    .piece_storage
                    .expect("Known mode must carry piece storage"),
            );
            DownloadEngine::spawn(
                id,
                inputs,
                shared.config.engine.clone(),
                storage,
                Arc::clone(&shared.fetch),
                Arc::clone(&shared.workers_repo),
                Arc::clone(&shared.attempts_repo),
            )
        }
        PreparedMode::Streaming => {
            let inputs = StreamingEngineInputs {
                spec,
                effective_url,
            };
            let stream = Arc::new(
                prepared
                    .stream_storage
                    .expect("Streaming mode must carry stream storage"),
            );
            DownloadEngine::spawn_streaming(
                id,
                inputs,
                shared.config.engine.clone(),
                stream,
                Arc::clone(&shared.fetch),
                Arc::clone(&shared.workers_repo),
                Arc::clone(&shared.attempts_repo),
            )
        }
    };
    {
        let mut inner = shared.inner.lock().expect("mutex poisoned");
        inner.engines.insert(id, handle);
    }
    // Отдельный таск для прогресса — он не влияет ни на state, ни
    // на продвижение очереди. Форвардит `ProgressEvent` в общий
    // broadcast, без обновления `records` (прогресс в домене File
    // теперь не живёт — это отдельный стрим).
    tokio::spawn(super::fanin::fan_in_progress(
        Arc::clone(&shared),
        id,
        progress_rx,
    ));
    // Основной таск — lifecycle: обновляет records, персистит
    // состояние и триггерит advance_queue на терминале.
    tokio::spawn(super::fanin::fan_in_lifecycle(
        Arc::clone(&shared),
        id,
        lifecycle_rx,
    ));
    Ok(())
}
