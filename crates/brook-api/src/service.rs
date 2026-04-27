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
#[derive(Debug, Clone, Default)]
pub struct ApiSettings {
    pub default_dir: String,
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
        // Достаём свежий File сразу из in-memory кэша менеджера, чтобы
        // ответ Add сразу нёс полное состояние записи. Клиент вставит её
        // в свой view-model, не дожидаясь дельта-событий из WatchFile.
        let file = self
            .manager
            .get_file(&id)
            .ok_or_else(|| Status::internal("manager: just-inserted file vanished"))?;
        Ok(Response::new(proto::AddResponse {
            file: Some(mapper::file_to_proto(&file)),
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

    async fn get_recently(
        &self,
        req: Request<proto::GetRecentlyRequest>,
    ) -> Result<Response<proto::GetRecentlyResponse>, Status> {
        let since = mapper::proto_ts_to_systime(req.into_inner().since.as_ref())
            .ok_or_else(|| Status::invalid_argument("since is required"))?;
        let files = self
            .manager
            .list_recently(since)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(Response::new(proto::GetRecentlyResponse {
            files: files.iter().map(mapper::file_to_proto).collect(),
        }))
    }

    async fn get_files(
        &self,
        req: Request<proto::GetFilesRequest>,
    ) -> Result<Response<proto::GetFilesResponse>, Status> {
        const DEFAULT_LIMIT: u32 = 50;
        const MAX_LIMIT: u32 = 200;
        let r = req.into_inner();
        let effective_limit = if r.limit == 0 {
            DEFAULT_LIMIT
        } else {
            r.limit.min(MAX_LIMIT)
        };
        let files = self
            .manager
            .list_files(r.offset, effective_limit)
            .await
            .map_err(mapper::core_err_to_status)?;
        let returned = files.len() as u32;
        Ok(Response::new(proto::GetFilesResponse {
            files: files.iter().map(mapper::file_to_proto).collect(),
            has_more: returned == effective_limit,
        }))
    }

    async fn get_settings(
        &self,
        _req: Request<proto::GetSettingsRequest>,
    ) -> Result<Response<proto::GetSettingsResponse>, Status> {
        let s = &self.settings;
        Ok(Response::new(proto::GetSettingsResponse {
            default_dir: s.default_dir.clone(),
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

    type WatchStatusStream = Pin<Box<dyn Stream<Item = Result<proto::StatusEvent, Status>> + Send>>;

    async fn watch_status(
        &self,
        _req: Request<proto::WatchStatusRequest>,
    ) -> Result<Response<Self::WatchStatusStream>, Status> {
        // Initial-sync нет: стартовое состояние клиент берёт через
        // GetRecently/GetFiles. Здесь мы сразу подписываемся и форвардим
        // статусные переходы. Создание новых записей и физическое
        // удаление через стрим не уведомляются — соответствующие
        // клиенты узнают результат через ответы Add/Remove (или через
        // следующий GetRecently после reconnect).
        let rx = self.manager.subscribe_lifecycle();

        let stream = async_stream::try_stream! {
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
                        warn!(lagged = n, "watch_status stream lagged; closing with DataLoss");
                        Err(Status::data_loss("watch_status lagged"))?;
                        unreachable!()
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
