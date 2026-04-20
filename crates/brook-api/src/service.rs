//! `BrookService` — реализация proto-трейта `brook.v1.BrookService`.
//!
//! Обёртка максимально тонкая: все методы транслируют аргументы в
//! `DownloadManager` и мапят ответ/ошибку. Никакой персистентности,
//! никакой конкуренции — этим занимается ядро.

use std::pin::Pin;
use std::sync::Arc;

use brook_core::{
    DownloadManager,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::Event as ProtoEvent;
use brook_proto::brook::v1::brook_service_server::BrookService as BrookServiceTrait;
use futures_core::Stream;
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
/// месте сборки (в `brookd` или в тестах).
pub struct BrookService<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    manager: Arc<DownloadManager<PF, QS, F>>,
    settings: ApiSettings,
}

/// Рантайм-снимок `brook.yaml` + `DownloadDefaults`, которым обслуживается
/// `GetSettings`. `brookd` собирает его из `DaemonRuntime` на старте; в
/// тестах `default()` даёт разумные значения.
#[derive(Debug, Clone)]
pub struct ApiSettings {
    pub default_dir: String,
    pub default_workers: u32,
    pub max_workers: u32,
    pub max_concurrent: u32,
    pub piece_target_count: u32,
    pub piece_size_min: u64,
    pub piece_size_max: u64,
    pub on_duplicate_url: proto::OnDuplicateUrlPolicy,
    pub on_file_exists: proto::OnFileExistsPolicy,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            default_dir: String::new(),
            default_workers: 4,
            max_workers: 16,
            max_concurrent: 3,
            piece_target_count: 128,
            piece_size_min: 16 * 1024 * 1024,
            piece_size_max: 128 * 1024 * 1024,
            on_duplicate_url: proto::OnDuplicateUrlPolicy::Ask,
            on_file_exists: proto::OnFileExistsPolicy::Ask,
        }
    }
}

impl<PF, QS, F> BrookService<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    pub fn new(manager: Arc<DownloadManager<PF, QS, F>>, settings: ApiSettings) -> Self {
        Self { manager, settings }
    }
}

fn ok_status() -> Response<proto::StatusResponse> {
    Response::new(proto::StatusResponse {
        ok: true,
        message: String::new(),
    })
}

#[tonic::async_trait]
impl<PF, QS, F> BrookServiceTrait for BrookService<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    async fn list(
        &self,
        _req: Request<proto::ListRequest>,
    ) -> Result<Response<proto::ListResponse>, Status> {
        let downloads = self
            .manager
            .snapshot()
            .iter()
            .map(mapper::download_to_proto)
            .collect();
        Ok(Response::new(proto::ListResponse { downloads }))
    }

    async fn add(
        &self,
        req: Request<proto::AddRequest>,
    ) -> Result<Response<proto::AddResponse>, Status> {
        let spec = req
            .into_inner()
            .spec
            .ok_or_else(|| Status::invalid_argument("spec is required"))?;
        let spec = mapper::spec_from_proto(spec)?;
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

    async fn cancel(
        &self,
        req: Request<proto::IdRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let id = mapper::id_from_proto_opt(req.into_inner().id.as_ref())?;
        self.manager
            .cancel(id)
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(ok_status())
    }

    async fn pause_all(
        &self,
        _req: Request<proto::PauseAllRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        self.manager
            .pause_all()
            .await
            .map_err(mapper::core_err_to_status)?;
        Ok(ok_status())
    }

    async fn resume_all(
        &self,
        _req: Request<proto::ResumeAllRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        self.manager
            .resume_all()
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
            default_workers: s.default_workers,
            max_workers: s.max_workers,
            max_concurrent: s.max_concurrent,
            piece_target_count: s.piece_target_count,
            piece_size_min: s.piece_size_min,
            piece_size_max: s.piece_size_max,
            on_duplicate_url: s.on_duplicate_url as i32,
            on_file_exists: s.on_file_exists as i32,
        }))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<ProtoEvent, Status>> + Send>>;

    async fn watch(
        &self,
        _req: Request<proto::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // Важно: сначала подписываемся, потом снимаем initial-snapshot.
        // Обратный порядок мог бы потерять событие, прилетевшее между
        // snapshot'ом и subscribe: сам broadcast не буферизует события
        // до создания ресивера.
        let rx = self.manager.subscribe();
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
                        yield mapper::event_to_proto(&ev);
                    }
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        warn!(lagged = n, "watch stream lagged; reconciling");
                        // Реконсиляция: шлём свежий snapshot по активным
                        // загрузкам — терминальные клиент и так не обновляет.
                        for d in manager
                            .snapshot()
                            .iter()
                            .filter(|d| !d.state.is_terminal())
                        {
                            yield mapper::snapshot_event(d);
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}
