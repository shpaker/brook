//! Общие фикстуры для интеграционных тестов `brook-api`.
//!
//! Поднимает `DownloadManager` на мемори-адаптерах и tonic-сервер на
//! ephemeral-порту (`127.0.0.1:0`). Возвращает готовый клиент.
//!
//! Тесты из `tests/` компилируются каждый в свой бинарь — этот модуль
//! подключается как `mod common;` и живёт per-crate (не публичный).

use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::time::Duration;

use brook_api::{
    ApiSettings,
    BrookService,
    BrookServiceServer,
};
use brook_core::testing::{
    MemoryPieceStorageFactory,
    MemoryTQueueStore,
    MockRangeFetch,
    sequential_bytes,
};
use brook_core::{
    DownloadManager,
    EngineConfig,
    ManagerConfig,
    Result as CoreResult,
    RetryPolicy,
    TPathPolicy,
};
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{
    Channel,
    Server,
};

pub type TestManager =
    DownloadManager<MemoryPieceStorageFactory, MemoryTQueueStore, MockRangeFetch>;

/// No-op `TPathPolicy` для тестов — ядро-тесты не заморочены песочницей.
struct AllowAnyPath;

impl TPathPolicy for AllowAnyPath {
    fn check_target_dir(&self, target_dir: &Path) -> CoreResult<PathBuf> {
        Ok(target_dir.to_path_buf())
    }
}

#[allow(dead_code)] // поля трогаются из разных test-бинарей, не каждый использует всё.
pub struct TestHarness {
    pub client: BrookServiceClient<Channel>,
    pub manager: Arc<TestManager>,
    pub server_task: tokio::task::JoinHandle<()>,
}

pub fn fast_engine_config() -> EngineConfig {
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

pub struct HarnessBuilder {
    piece_count: u32,
    piece_size: u64,
    events_capacity: usize,
    fetch_delay: Option<Duration>,
}

impl Default for HarnessBuilder {
    fn default() -> Self {
        Self {
            piece_count: 2,
            piece_size: 10,
            events_capacity: 64,
            fetch_delay: None,
        }
    }
}

#[allow(dead_code)] // разные test-бинари используют разные билд-методы.
impl HarnessBuilder {
    pub fn events_capacity(mut self, n: usize) -> Self {
        self.events_capacity = n;
        self
    }

    pub fn fetch_delay(mut self, d: Duration) -> Self {
        self.fetch_delay = Some(d);
        self
    }

    pub async fn build(self) -> TestHarness {
        let factory = Arc::new(MemoryPieceStorageFactory::new(
            self.piece_count,
            self.piece_size,
        ));
        let queue = Arc::new(MemoryTQueueStore::new());
        let total = u64::from(self.piece_count) * self.piece_size;
        let mut fetch = MockRangeFetch::always_ok(sequential_bytes(total));
        if let Some(d) = self.fetch_delay {
            fetch = fetch.with_delay(d);
        }
        let fetch = Arc::new(fetch);
        let cfg = ManagerConfig {
            events_capacity: self.events_capacity,
            engine: fast_engine_config(),
        };
        let manager = Arc::new(DownloadManager::new(factory, queue, fetch, cfg));
        let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);
        let policy: Arc<dyn TPathPolicy> = Arc::new(AllowAnyPath);
        let service = BrookService::new(
            Arc::clone(&manager),
            ApiSettings::default(),
            policy,
            shutdown_tx,
        );
        let server = BrookServiceServer::new(service);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(server)
                .serve_with_incoming(incoming)
                .await;
        });

        // Клиент подключается к полученному адресу. Небольшой retry на
        // случай гонки между listen и первым connect'ом.
        let uri = format!("http://{addr}");
        let mut attempts = 0;
        let client = loop {
            match BrookServiceClient::connect(uri.clone()).await {
                Ok(c) => break c,
                Err(e) if attempts < 20 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = e;
                }
                Err(e) => panic!("connect failed: {e}"),
            }
        };

        TestHarness {
            client,
            manager,
            server_task: task,
        }
    }
}
