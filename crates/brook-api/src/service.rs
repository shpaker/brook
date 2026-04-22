//! `BrookService` — реализация proto-трейта `brook.v1.BrookService`.
//!
//! Обёртка максимально тонкая: все методы транслируют аргументы в
//! `DownloadManager` и мапят ответ/ошибку. Никакой персистентности,
//! никакой конкуренции — этим занимается ядро.

use std::pin::Pin;
use std::sync::Arc;

use brook_core::{
    DownloadManager,
    NoopAttemptRepo,
    NoopWorkerRepo,
    TPathPolicy,
    TPieceAttemptRepo,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TWorkerRepo,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::brook_service_server::BrookService as BrookServiceTrait;
use futures_core::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tonic::{
    Request,
    Response,
    Status,
};
use tracing::warn;

use crate::mapper;

/// tonic-обёртка над `DownloadManager`.
///
/// Generics совпадают с `DownloadManager` ради zero-cost: никакого
/// `dyn`-диспатча, конкретный тип фабрики/очереди/fetch'а известен в
/// месте сборки (в `brook-daemon` или в тестах).
pub struct BrookService<PF, QS, F, WR = NoopWorkerRepo, AR = NoopAttemptRepo>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    manager: Arc<DownloadManager<PF, QS, F, WR, AR>>,
    settings: ApiSettings,
    /// Sandbox-политика для `target_dir` в `Add`. Проверяется
    /// **синхронно** до enqueue — чтобы клиент сразу увидел
    /// `PermissionDenied`, а не висящую `Failed`-запись. Фабрика также
    /// проверит путь в `prepare()` (defense in depth).
    path_policy: Arc<dyn TPathPolicy>,
    // Сюда уходит «тик» от `Shutdown` RPC. Серверный стек получает его
    // через парный `Receiver` и встраивает в select с SIGTERM/SIGINT.
    // Broadcast выбран ради `Sender::send` без ownership'а — в сервисе
    // только клонируемый `Sender`, состояние канала остаётся у демона.
    shutdown_tx: broadcast::Sender<()>,
}

/// Рантайм-снимок `brook.yaml`, которым обслуживается `GetSettings`.
/// `brook-daemon` собирает его из `DaemonRuntime` на старте; в тестах
/// `default()` даёт разумные значения.
#[derive(Debug, Clone)]
pub struct ApiSettings {
    pub default_dir: String,
    pub max_concurrent: u32,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            default_dir: String::new(),
            max_concurrent: 3,
        }
    }
}

impl<PF, QS, F, WR, AR> BrookService<PF, QS, F, WR, AR>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    pub fn new(
        manager: Arc<DownloadManager<PF, QS, F, WR, AR>>,
        settings: ApiSettings,
        path_policy: Arc<dyn TPathPolicy>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            manager,
            settings,
            path_policy,
            shutdown_tx,
        }
    }
}

fn ok_status() -> Response<proto::StatusResponse> {
    Response::new(proto::StatusResponse {
        ok: true,
        message: String::new(),
    })
}

#[tonic::async_trait]
impl<PF, QS, F, WR, AR> BrookServiceTrait for BrookService<PF, QS, F, WR, AR>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    async fn add(
        &self,
        req: Request<proto::AddRequest>,
    ) -> Result<Response<proto::AddResponse>, Status> {
        let spec = req
            .into_inner()
            .spec
            .ok_or_else(|| Status::invalid_argument("spec is required"))?;
        let mut spec = mapper::spec_from_proto(spec)?;
        // Сразу клэмпим target_dir — клиент должен увидеть
        // PermissionDenied синхронно, а не через Failed-переход
        // после prepare(). Канонизированный путь уходит в очередь,
        // factory потом проверит его ещё раз как страховку.
        spec.target_dir = self
            .path_policy
            .check_target_dir(&spec.target_dir)
            .map_err(mapper::core_err_to_status)?;
        let id = self
            .manager
            .add(spec)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(Response::new(proto::AddResponse {
            id: Some(mapper::id_to_proto(id)),
        }))
    }

    async fn remove(
        &self,
        req: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let id = mapper::id_from_proto_opt(req.into_inner().id.as_ref())?;
        self.manager
            .remove(id)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(Response::new(proto::RemoveResponse {}))
    }

    async fn pause(
        &self,
        req: Request<proto::IdRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let id = mapper::id_from_proto_opt(req.into_inner().id.as_ref())?;
        self.manager
            .pause(id)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(ok_status())
    }

    async fn resume(
        &self,
        req: Request<proto::IdRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let id = mapper::id_from_proto_opt(req.into_inner().id.as_ref())?;
        self.manager
            .resume(id)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(ok_status())
    }

    async fn retry(
        &self,
        req: Request<proto::IdRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let id = mapper::id_from_proto_opt(req.into_inner().id.as_ref())?;
        self.manager
            .retry(id)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(ok_status())
    }

    async fn get_settings(
        &self,
        _req: Request<proto::GetSettingsRequest>,
    ) -> Result<Response<proto::GetSettingsResponse>, Status> {
        let s = &self.settings;
        Ok(Response::new(proto::GetSettingsResponse {
            default_dir: s.default_dir.clone(),
            max_concurrent: s.max_concurrent,
        }))
    }

    async fn shutdown(
        &self,
        _req: Request<proto::ShutdownRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        // Err от `send` значит, что ресиверов больше нет — демон уже в
        // процессе завершения. Для клиента это не ошибка: цель достигнута.
        let _ = self.shutdown_tx.send(());
        Ok(ok_status())
    }

    type WatchFileStream = Pin<Box<dyn Stream<Item = Result<proto::FileEvent, Status>> + Send>>;

    async fn watch_file(
        &self,
        _req: Request<proto::WatchFileRequest>,
    ) -> Result<Response<Self::WatchFileStream>, Status> {
        // Важно: сначала подписываемся, потом снимаем initial-snapshot.
        // Обратный порядок мог бы потерять событие, прилетевшее между
        // snapshot'ом и subscribe: сам broadcast не буферизует события
        // до создания ресивера.
        let rx = self.manager.subscribe_lifecycle();
        let initial = self.manager.snapshot();
        let manager = Arc::clone(&self.manager);

        let stream = async_stream::try_stream! {
            for d in &initial {
                yield mapper::snapshot_event(d);
            }

            let mut broadcast = BroadcastStream::new(rx);
            // Используем BroadcastStream, чтобы получить `Lagged(n)` как
            // Err-элемент стрима, а не тихий пропуск.
            use tokio_stream::StreamExt;
            while let Some(item) = broadcast.next().await {
                match item {
                    Ok(ev) => {
                        yield mapper::lifecycle_event_to_proto(&ev);
                    }
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        warn!(lagged = n, "watch_file stream lagged; reconciling");
                        // Реконсиляция: шлём свежий snapshot по активным
                        // файлам — терминальные клиент и так не обновляет.
                        for d in manager
                            .snapshot()
                            .iter()
                            .filter(|d| !d.status.is_terminal())
                        {
                            yield mapper::snapshot_event(d);
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    type WatchProgressStream =
        Pin<Box<dyn Stream<Item = Result<proto::ProgressTick, Status>> + Send>>;

    async fn watch_progress(
        &self,
        _req: Request<proto::WatchProgressRequest>,
    ) -> Result<Response<Self::WatchProgressStream>, Status> {
        // Progress — чисто поток тиков, без initial-sync: свежий snapshot
        // лежит в WatchFile (`SnapshotEvent`), клиент показывает пустой
        // прогресс до первого тика от активных загрузок.
        let rx = self.manager.subscribe_progress();

        let stream = async_stream::try_stream! {
            let mut broadcast = BroadcastStream::new(rx);
            use tokio_stream::StreamExt;
            while let Some(item) = broadcast.next().await {
                match item {
                    Ok(ev) => {
                        yield mapper::progress_event_to_proto(&ev);
                    }
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        // Прогресс — лоссовый по природе: пропущенные
                        // тики просто потеряны, следующий перепишет UI.
                        warn!(lagged = n, "watch_progress stream lagged");
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}
