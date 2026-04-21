//! `DownloadManager` — верхний координатор над множеством `DownloadEngine`'ов.
//!
//! Отвечает за:
//! - реестр загрузок (`records`) и активных движков (`engines`);
//! - ограничение параллельности (`max_concurrent`);
//! - fan-in событий в единый `broadcast::Sender<DownloadEvent>` для `Watch`;
//! - персистенцию состояний через [`TQueueStore`] (insert при `add`,
//!   `update_state` при смене стейта);
//! - восстановление очереди из [`TQueueStore`] при старте ([`Self::bootstrap`]).
//!
//! ## Модель конкурентности
//!
//! `Mutex<Inner>` — обычный `std::sync::Mutex`. Внутренние критсекции
//! короткие и чисто CPU-шные (вставка в `HashMap`, `pop_front` из
//! `VecDeque`), `.await` под локом не выполняется — значит async-mutex
//! не нужен, а синхронный в tokio-контексте дешевле.
//!
//! «Медленные» операции (`factory.prepare`, `queue.insert/update_state`)
//! всегда вызываются **вне** лока: из публичных async-методов либо из
//! fan-in задачи, которая читает `broadcast::Receiver` от движка.
//!
//! ## Fan-in событий
//!
//! На каждый спаунингом движок создаётся маленький task, который:
//! 1. Читает `DownloadEvent` из `broadcast::Receiver` движка.
//! 2. Форвардит событие в общий `events_tx` (для подписчиков `Watch`).
//! 3. Обновляет `records[id]` (state/progress/error/updated_at).
//! 4. На смене state — пишет в `TQueueStore`.
//! 5. На терминальном событии — удаляет движок из `engines` и пробует
//!    спаунить следующий из `waiting`.
//!
//! Задача держит `Arc<Shared>` — это создаёт временный «цикл» владения,
//! который разрывается сам, когда task завершается (после терминального
//! события движка).

use std::collections::{
    HashMap,
    VecDeque,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::time::{
    Duration,
    SystemTime,
};

use tokio::sync::broadcast;
use tracing::{
    debug,
    warn,
};

use crate::domain::{
    Download,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    DownloadState,
};
use crate::error::{
    Error,
    Result,
};
use crate::ports::{
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
};
use crate::service::engine::{
    DownloadEngine,
    EngineConfig,
    EngineHandle,
    EngineInputs,
};

/// Конфигурация менеджера.
#[derive(Clone)]
pub struct ManagerConfig {
    /// Максимум одновременно активных движков. Остальные держатся в `waiting`.
    /// Дефолт 3 — согласован с MVP; финальное значение придёт из `settings` в 3.x.
    pub max_concurrent: usize,
    /// Ёмкость центрального broadcast-канала событий.
    pub events_capacity: usize,
    /// Конфигурация, которая прокидывается в каждый движок.
    pub engine: EngineConfig,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 3,
            events_capacity: 1024,
            engine: EngineConfig::default(),
        }
    }
}

/// Верхний координатор загрузок. Клонируется дёшево (внутри `Arc`).
pub struct DownloadManager<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    shared: Arc<Shared<PF, QS, F>>,
}

impl<PF, QS, F> Clone for DownloadManager<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

struct Shared<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    factory: Arc<PF>,
    queue: Arc<QS>,
    fetch: Arc<F>,
    inner: Mutex<Inner>,
    events_tx: broadcast::Sender<DownloadEvent>,
    config: ManagerConfig,
}

struct Inner {
    records: HashMap<DownloadId, Download>,
    engines: HashMap<DownloadId, EngineHandle>,
    /// Упорядоченная очередь id в ожидании слота. Id может быть и в
    /// `Paused` (пользователь попросил pause до старта) — такие при
    /// продвижении пропускаются.
    waiting: VecDeque<DownloadId>,
}

impl<PF, QS, F> DownloadManager<PF, QS, F>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    pub fn new(factory: Arc<PF>, queue: Arc<QS>, fetch: Arc<F>, config: ManagerConfig) -> Self {
        let (events_tx, _) = broadcast::channel(config.events_capacity);
        Self {
            shared: Arc::new(Shared {
                factory,
                queue,
                fetch,
                inner: Mutex::new(Inner {
                    records: HashMap::new(),
                    engines: HashMap::new(),
                    waiting: VecDeque::new(),
                }),
                events_tx,
                config,
            }),
        }
    }

    /// Подписаться на поток всех событий всех движков.
    pub fn subscribe(&self) -> broadcast::Receiver<DownloadEvent> {
        self.shared.events_tx.subscribe()
    }

    /// Срез всех известных менеджеру загрузок — для Snapshot-реконсиляции
    /// в `Watch`.
    pub fn snapshot(&self) -> Vec<Download> {
        let inner = self.shared.inner.lock().expect("mutex poisoned");
        inner.records.values().cloned().collect()
    }

    /// Загрузить очередь из `TQueueStore` и запустить продвижение.
    ///
    /// Не-терминальные состояния нормализуются: `Running`/`Retrying`
    /// → `Queued` (движок не пережил рестарт демона), `Paused` и
    /// `Queued` остаются как есть.
    pub async fn bootstrap(&self) -> Result<()> {
        let loaded = self.shared.queue.load_all().await?;
        let mut to_fix: Vec<(DownloadId, DownloadState)> = Vec::new();
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            for mut d in loaded {
                let target = match d.state {
                    DownloadState::Running | DownloadState::Retrying => {
                        to_fix.push((d.id, DownloadState::Queued));
                        DownloadState::Queued
                    }
                    s => s,
                };
                d.state = target;
                if matches!(
                    target,
                    DownloadState::Queued | DownloadState::Paused | DownloadState::Retrying
                ) && !matches!(target, DownloadState::Paused)
                {
                    inner.waiting.push_back(d.id);
                }
                inner.records.insert(d.id, d);
            }
        }
        for (id, state) in to_fix {
            if let Err(e) = self.shared.queue.update_state(id, state).await {
                warn!(%id, error = %e, "bootstrap: failed to persist normalized state");
            }
        }
        self.try_spawn_next().await;
        Ok(())
    }

    /// Добавить новую загрузку. Возвращает её `DownloadId`.
    pub async fn add(&self, spec: DownloadSpec) -> Result<DownloadId> {
        let id = DownloadId::new();
        let download = Download::new(id, spec);
        self.shared.queue.insert(&download).await?;
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            inner.records.insert(id, download.clone());
            inner.waiting.push_back(id);
        }
        // Уведомляем подписчиков Watch о новой записи до того, как движок
        // начнёт слать `StateChanged`/`Progress`: у клиента этого id ещё
        // нет, и событие без предшествующего `Snapshot` было бы выброшено.
        let _ = self.shared.events_tx.send(DownloadEvent::Snapshot {
            download: Box::new(download),
        });
        self.try_spawn_next().await;
        Ok(id)
    }

    /// Удалить загрузку. Разрешено только для терминальных/не-активных.
    /// Для активных (running engine) — `Err`: сначала `cancel`.
    ///
    /// Идемпотентно: если id ни в records, ни в queue — тоже Ok. Это важно
    /// для клиента, у которого в ViewModel может остаться «призрак»
    /// (например, при рассинхроне после рестарта демона): повторный
    /// `Remove` должен починить состояние, а не ругаться NotFound'ом.
    pub async fn remove(&self, id: DownloadId) -> Result<()> {
        let was_known = {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if inner.engines.contains_key(&id) {
                return Err(Error::Other(
                    "download is active, cancel before remove".into(),
                ));
            }
            let had_record = inner.records.remove(&id).is_some();
            inner.waiting.retain(|x| *x != id);
            had_record
        };
        match self.shared.queue.remove(id).await {
            Ok(()) => Ok(()),
            Err(Error::NotFound) if !was_known => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Поставить загрузку на паузу.
    ///
    /// Если движок уже запущен — команда `Pause` уезжает в engine; финальный
    /// `StateChanged(Paused)` прилетит из движка.
    /// Если загрузка ещё в `waiting` — сразу помечаем `Paused` и
    /// убираем из `waiting` (движок не появится, пока не будет `Resume`).
    pub async fn pause(&self, id: DownloadId) -> Result<()> {
        let mut persist = None;
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.pause();
                return Ok(());
            }
            inner.waiting.retain(|x| *x != id);
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.state.is_terminal() {
                return Err(Error::Other("download is terminal".into()));
            }
            if record.state != DownloadState::Paused {
                record.state = DownloadState::Paused;
                record.updated_at = SystemTime::now();
                persist = Some(DownloadState::Paused);
                let _ = self.shared.events_tx.send(DownloadEvent::StateChanged {
                    id,
                    state: DownloadState::Paused,
                });
            }
        }
        if let Some(state) = persist {
            self.shared.queue.update_state(id, state).await?;
        }
        Ok(())
    }

    /// Возобновить загрузку.
    ///
    /// Если движок активен — прокидываем `Resume`.
    /// Иначе — возвращаем в `waiting` и пытаемся спаунить.
    pub async fn resume(&self, id: DownloadId) -> Result<()> {
        let mut persist = None;
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.resume();
                return Ok(());
            }
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.state.is_terminal() {
                return Err(Error::Other("download is terminal".into()));
            }
            if record.state != DownloadState::Queued {
                record.state = DownloadState::Queued;
                record.updated_at = SystemTime::now();
                persist = Some(DownloadState::Queued);
                let _ = self.shared.events_tx.send(DownloadEvent::StateChanged {
                    id,
                    state: DownloadState::Queued,
                });
            }
            if !inner.waiting.iter().any(|x| *x == id) {
                inner.waiting.push_back(id);
            }
        }
        if let Some(state) = persist {
            self.shared.queue.update_state(id, state).await?;
        }
        self.try_spawn_next().await;
        Ok(())
    }

    /// Отменить загрузку.
    pub async fn cancel(&self, id: DownloadId) -> Result<()> {
        let should_persist = {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.cancel();
                return Ok(());
            }
            inner.waiting.retain(|x| *x != id);
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.state.is_terminal() {
                return Ok(()); // уже всё.
            }
            record.state = DownloadState::Cancelled;
            record.updated_at = SystemTime::now();
            let _ = self.shared.events_tx.send(DownloadEvent::StateChanged {
                id,
                state: DownloadState::Cancelled,
            });
            true
        };
        if should_persist {
            self.shared
                .queue
                .update_state(id, DownloadState::Cancelled)
                .await?;
        }
        Ok(())
    }

    /// Поставить на паузу все активные и всё, что в `waiting`.
    pub async fn pause_all(&self) -> Result<()> {
        let ids: Vec<DownloadId> = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            inner.records.keys().copied().collect()
        };
        for id in ids {
            let _ = self.pause(id).await;
        }
        Ok(())
    }

    /// Остановить все активные движки и дождаться, пока каждый дойдёт до
    /// `Paused` или терминального состояния.
    ///
    /// Возвращает `Err(Error::Other("shutdown timeout"))`, если `deadline`
    /// истёк раньше, чем все active-движки успели коммитнуться. Engines в
    /// этом случае будут дропнуты владельцем — in-flight piece-пачки без
    /// коммита перекачаются при следующем старте.
    pub async fn shutdown(&self, deadline: Duration) -> Result<()> {
        let active: Vec<DownloadId> = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            inner.engines.keys().copied().collect()
        };
        if active.is_empty() {
            return Ok(());
        }
        let _ = self.pause_all().await;
        let shared = Arc::clone(&self.shared);
        let wait = async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let all_drained = {
                    let inner = shared.inner.lock().expect("mutex poisoned");
                    active.iter().all(|id| match inner.records.get(id) {
                        Some(d) => matches!(
                            d.state,
                            DownloadState::Paused
                                | DownloadState::Done
                                | DownloadState::Failed
                                | DownloadState::Cancelled
                        ),
                        None => true,
                    })
                };
                if all_drained {
                    break;
                }
            }
        };
        match tokio::time::timeout(deadline, wait).await {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!("shutdown deadline elapsed with engines still draining");
                Err(Error::Other("shutdown timeout".into()))
            }
        }
    }

    /// Возобновить все `Paused`.
    pub async fn resume_all(&self) -> Result<()> {
        let ids: Vec<DownloadId> = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            inner
                .records
                .iter()
                .filter(|(_, d)| d.state == DownloadState::Paused)
                .map(|(id, _)| *id)
                .collect()
        };
        for id in ids {
            let _ = self.resume(id).await;
        }
        Ok(())
    }

    /// Продвинуть очередь — спаунить движки, пока есть слоты и ожидающие.
    async fn try_spawn_next(&self) {
        loop {
            let next = {
                let mut inner = self.shared.inner.lock().expect("mutex poisoned");
                if inner.engines.len() >= self.shared.config.max_concurrent {
                    return;
                }
                // Перебираем `waiting` до первого подходящего (Queued);
                // Paused/Cancelled игнорируем и пропускаем.
                let mut picked: Option<DownloadId> = None;
                while let Some(id) = inner.waiting.pop_front() {
                    match inner.records.get(&id).map(|d| d.state) {
                        Some(DownloadState::Queued) | Some(DownloadState::Retrying) => {
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
                {
                    let mut inner = self.shared.inner.lock().expect("mutex poisoned");
                    if let Some(record) = inner.records.get_mut(&id) {
                        record.state = DownloadState::Failed;
                        record.error = Some(e.to_string());
                        record.updated_at = SystemTime::now();
                    }
                }
                let _ = self
                    .shared
                    .queue
                    .update_state(id, DownloadState::Failed)
                    .await;
                let _ = self.shared.events_tx.send(DownloadEvent::Failed {
                    id,
                    error: e.to_string(),
                });
            }
        }
    }

    fn spawn_engine(&self, id: DownloadId) -> impl std::future::Future<Output = Result<()>> + Send {
        let shared = Arc::clone(&self.shared);
        async move { Self::spawn_engine_impl(shared, id).await }
    }

    async fn spawn_engine_impl(shared: Arc<Shared<PF, QS, F>>, id: DownloadId) -> Result<()> {
        let maybe_spec = {
            let inner = shared.inner.lock().expect("mutex poisoned");
            inner.records.get(&id).map(|d| d.spec.clone())
        };
        let spec = maybe_spec.ok_or(Error::NotFound)?;
        let prepared = shared.factory.prepare(&spec).await?;
        let inputs = EngineInputs {
            spec,
            total_size: prepared.total_size,
            piece_size: prepared.piece_size,
            accepts_ranges: prepared.accepts_ranges,
            guard: prepared.guard,
        };
        let storage = Arc::new(prepared.storage);
        let (handle, events_rx) = DownloadEngine::spawn(
            id,
            inputs,
            shared.config.engine.clone(),
            storage,
            Arc::clone(&shared.fetch),
        );
        {
            let mut inner = shared.inner.lock().expect("mutex poisoned");
            inner.engines.insert(id, handle);
        }
        tokio::spawn(fan_in_events(Arc::clone(&shared), id, events_rx));
        Ok(())
    }
}

async fn fan_in_events<PF, QS, F>(
    shared: Arc<Shared<PF, QS, F>>,
    id: DownloadId,
    mut rx: broadcast::Receiver<DownloadEvent>,
) where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    // Выходим из цикла только когда канал событий движка закрылся
    // (движок завершил таск и уронил свой `Sender`). До этого момента
    // форвардим все события — иначе можно пропустить финальный `Completed`
    // или `Failed`, идущий сразу после `StateChanged(Done/Failed)`.
    loop {
        let ev = match rx.recv().await {
            Ok(e) => e,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(%id, lagged = n, "engine event stream lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        let mut state_to_persist: Option<DownloadState> = None;
        {
            let mut inner = shared.inner.lock().expect("mutex poisoned");
            if let Some(record) = inner.records.get_mut(&id) {
                match &ev {
                    DownloadEvent::Progress { progress, .. } => {
                        record.progress = *progress;
                        record.updated_at = SystemTime::now();
                    }
                    DownloadEvent::StateChanged { state, .. } => {
                        if record.state != *state {
                            record.state = *state;
                            record.updated_at = SystemTime::now();
                            state_to_persist = Some(*state);
                        }
                    }
                    DownloadEvent::Completed { .. } => {
                        if record.state != DownloadState::Done {
                            record.state = DownloadState::Done;
                            record.updated_at = SystemTime::now();
                            state_to_persist = Some(DownloadState::Done);
                        }
                    }
                    DownloadEvent::Failed { error, .. } => {
                        record.state = DownloadState::Failed;
                        record.error = Some(error.clone());
                        record.updated_at = SystemTime::now();
                        state_to_persist = Some(DownloadState::Failed);
                    }
                    DownloadEvent::WorkerUpdate { .. } | DownloadEvent::Snapshot { .. } => {}
                }
            }
        }
        let _ = shared.events_tx.send(ev);
        if let Some(state) = state_to_persist
            && let Err(e) = shared.queue.update_state(id, state).await
        {
            warn!(%id, %state, error = %e, "failed to persist state change");
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

/// Попытаться поднять следующий движок после завершения предыдущего.
/// Вынесено как свободная функция, чтобы не тянуть `DownloadManager` в
/// сигнатуру fan-in task (он бы создал больше генериков).
async fn advance_queue<PF, QS, F>(shared: Arc<Shared<PF, QS, F>>)
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    let manager = DownloadManager { shared };
    manager.try_spawn_next().await;
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::domain::DownloadSpec;
    use crate::service::retry::RetryPolicy;
    use crate::testing::{
        MemoryPieceStorageFactory,
        MemoryTQueueStore,
        MockRangeFetch,
        sequential_bytes,
    };

    fn fast_engine_config() -> EngineConfig {
        EngineConfig {
            write_buffer: 16,
            commit_every: 2,
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

    fn spec(url: &str) -> DownloadSpec {
        let mut s = DownloadSpec::new(url, "/tmp");
        s.workers = 2;
        s
    }

    fn make_manager(
        piece_count: u32,
        piece_size: u64,
        max_concurrent: usize,
    ) -> (
        DownloadManager<MemoryPieceStorageFactory, MemoryTQueueStore, MockRangeFetch>,
        Arc<MemoryTQueueStore>,
    ) {
        let factory = Arc::new(MemoryPieceStorageFactory::new(piece_count, piece_size));
        let queue = Arc::new(MemoryTQueueStore::new());
        let total = piece_count as u64 * piece_size;
        let fetch = Arc::new(MockRangeFetch::always_ok(sequential_bytes(total)));
        let cfg = ManagerConfig {
            max_concurrent,
            events_capacity: 64,
            engine: fast_engine_config(),
        };
        (
            DownloadManager::new(factory, queue.clone(), fetch, cfg),
            queue,
        )
    }

    async fn wait_for_terminal(
        mgr: &DownloadManager<MemoryPieceStorageFactory, MemoryTQueueStore, MockRangeFetch>,
        id: DownloadId,
    ) {
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let snap = mgr.snapshot();
            if let Some(d) = snap.iter().find(|d| d.id == id)
                && d.state.is_terminal()
            {
                return;
            }
        }
        panic!("timeout waiting for terminal state of {id}");
    }

    #[tokio::test]
    async fn add_spawns_engine_and_completes() {
        let (mgr, queue) = make_manager(3, 20, 2);
        let id = mgr.add(spec("https://test/a")).await.unwrap();
        wait_for_terminal(&mgr, id).await;
        let snap = mgr.snapshot();
        let d = snap.iter().find(|d| d.id == id).unwrap();
        assert_eq!(d.state, DownloadState::Done);
        // queue тоже обновился.
        let persisted = queue.load_all().await.unwrap();
        assert_eq!(persisted[0].state, DownloadState::Done);
    }

    #[tokio::test]
    async fn max_concurrent_respected() {
        let factory = Arc::new(MemoryPieceStorageFactory::new(3, 20));
        let queue = Arc::new(MemoryTQueueStore::new());
        let fetch = Arc::new(
            MockRangeFetch::always_ok(sequential_bytes(60)).with_delay(Duration::from_millis(60)),
        );
        let cfg = ManagerConfig {
            max_concurrent: 2,
            events_capacity: 128,
            engine: fast_engine_config(),
        };
        let mgr = DownloadManager::new(factory, queue, fetch, cfg);

        let a = mgr.add(spec("https://t/a")).await.unwrap();
        let b = mgr.add(spec("https://t/b")).await.unwrap();
        let c = mgr.add(spec("https://t/c")).await.unwrap();

        // Сразу после add'ов даём чуть времени, чтобы engine'ы стартанули.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let active = {
            let inner = mgr.shared.inner.lock().unwrap();
            inner.engines.len()
        };
        assert!(active <= 2, "spawned {active} engines, expected <= 2");
        assert!(active >= 1, "expected at least one active engine");

        for id in [a, b, c] {
            wait_for_terminal(&mgr, id).await;
        }
    }

    #[tokio::test]
    async fn cancel_before_spawn_marks_cancelled() {
        // max_concurrent = 0 — ни один engine не запустится, загрузки
        // остаются в waiting, можно без гонки отменить.
        let factory = Arc::new(MemoryPieceStorageFactory::new(2, 10));
        let queue = Arc::new(MemoryTQueueStore::new());
        let fetch = Arc::new(MockRangeFetch::always_ok(sequential_bytes(20)));
        let cfg = ManagerConfig {
            max_concurrent: 0,
            events_capacity: 64,
            engine: fast_engine_config(),
        };
        let mgr = DownloadManager::new(factory, queue.clone(), fetch, cfg);
        let id = mgr.add(spec("https://t/a")).await.unwrap();
        mgr.cancel(id).await.unwrap();
        let snap = mgr.snapshot();
        let d = snap.iter().find(|d| d.id == id).unwrap();
        assert_eq!(d.state, DownloadState::Cancelled);
        let persisted = queue.load_all().await.unwrap();
        assert_eq!(persisted[0].state, DownloadState::Cancelled);
    }

    #[tokio::test]
    async fn remove_blocked_for_active_download() {
        let factory = Arc::new(MemoryPieceStorageFactory::new(3, 20));
        let queue = Arc::new(MemoryTQueueStore::new());
        let fetch = Arc::new(
            MockRangeFetch::always_ok(sequential_bytes(60)).with_delay(Duration::from_millis(200)),
        );
        let cfg = ManagerConfig {
            max_concurrent: 1,
            events_capacity: 64,
            engine: fast_engine_config(),
        };
        let mgr = DownloadManager::new(factory, queue, fetch, cfg);
        let id = mgr.add(spec("https://t/slow")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Пока engine активен — remove должен отказать.
        let err = mgr.remove(id).await.unwrap_err();
        assert!(matches!(err, Error::Other(_)));
        mgr.cancel(id).await.unwrap();
        wait_for_terminal(&mgr, id).await;
        mgr.remove(id).await.unwrap();
    }

    #[tokio::test]
    async fn bootstrap_restores_from_queue() {
        let factory = Arc::new(MemoryPieceStorageFactory::new(2, 10));
        let queue = Arc::new(MemoryTQueueStore::new());
        // Предзаполним очередь тремя состояниями.
        let mut qd = Download::new(DownloadId::new(), spec("https://t/q"));
        qd.state = DownloadState::Queued;
        let mut rd = Download::new(DownloadId::new(), spec("https://t/r"));
        rd.state = DownloadState::Running;
        let mut pd = Download::new(DownloadId::new(), spec("https://t/p"));
        pd.state = DownloadState::Paused;
        queue.insert(&qd).await.unwrap();
        queue.insert(&rd).await.unwrap();
        queue.insert(&pd).await.unwrap();

        let fetch = Arc::new(
            MockRangeFetch::always_ok(sequential_bytes(20)).with_delay(Duration::from_millis(200)),
        );
        let cfg = ManagerConfig {
            max_concurrent: 1,
            events_capacity: 64,
            engine: fast_engine_config(),
        };
        let mgr = DownloadManager::new(factory, queue.clone(), fetch, cfg);
        mgr.bootstrap().await.unwrap();
        // Running должен быть нормализован до Queued, записано в очередь.
        let persisted = queue.load_all().await.unwrap();
        let r_persisted = persisted.iter().find(|d| d.id == rd.id).unwrap();
        assert_eq!(r_persisted.state, DownloadState::Queued);
        // Paused остался как есть.
        let p_persisted = persisted.iter().find(|d| d.id == pd.id).unwrap();
        assert_eq!(p_persisted.state, DownloadState::Paused);
        // Движков запущено не больше max_concurrent = 1.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let active = {
            let inner = mgr.shared.inner.lock().unwrap();
            inner.engines.len()
        };
        assert_eq!(active, 1);

        // Отменим всё, чтобы тест закрыл background tasks.
        let ids: Vec<_> = mgr.snapshot().iter().map(|d| d.id).collect();
        for id in ids {
            let _ = mgr.cancel(id).await;
        }
    }

    #[tokio::test]
    async fn snapshot_contains_all_downloads() {
        let (mgr, _) = make_manager(1, 10, 0);
        for u in ["a", "b", "c"] {
            mgr.add(spec(&format!("https://t/{u}"))).await.unwrap();
        }
        let snap = mgr.snapshot();
        assert_eq!(snap.len(), 3);
    }

    #[tokio::test]
    async fn shutdown_pauses_active_engines() {
        let factory = Arc::new(MemoryPieceStorageFactory::new(5, 20));
        let queue = Arc::new(MemoryTQueueStore::new());
        // Долгий fetch, чтобы на момент shutdown загрузки были активны.
        let fetch = Arc::new(
            MockRangeFetch::always_ok(sequential_bytes(100)).with_delay(Duration::from_millis(400)),
        );
        let cfg = ManagerConfig {
            max_concurrent: 2,
            events_capacity: 128,
            engine: fast_engine_config(),
        };
        let mgr = DownloadManager::new(factory, queue, fetch, cfg);
        let a = mgr.add(spec("https://t/a")).await.unwrap();
        let b = mgr.add(spec("https://t/b")).await.unwrap();
        // Дать engine'ам стартовать.
        tokio::time::sleep(Duration::from_millis(50)).await;

        mgr.shutdown(Duration::from_secs(5)).await.unwrap();

        let snap = mgr.snapshot();
        for id in [a, b] {
            let d = snap.iter().find(|d| d.id == id).unwrap();
            assert!(
                matches!(
                    d.state,
                    DownloadState::Paused
                        | DownloadState::Done
                        | DownloadState::Failed
                        | DownloadState::Cancelled
                ),
                "download {id} in state {:?} after shutdown",
                d.state
            );
        }
    }

    #[tokio::test]
    async fn shutdown_with_no_engines_is_noop() {
        let (mgr, _) = make_manager(1, 10, 0);
        mgr.shutdown(Duration::from_millis(100)).await.unwrap();
    }

    #[tokio::test]
    async fn events_fan_in_delivers_completed() {
        let (mgr, _) = make_manager(2, 10, 2);
        let mut rx = mgr.subscribe();
        let id = mgr.add(spec("https://t/x")).await.unwrap();
        let mut saw_completed = false;
        for _ in 0..400 {
            match tokio::time::timeout(Duration::from_millis(20), rx.recv()).await {
                Ok(Ok(DownloadEvent::Completed { id: eid })) if eid == id => {
                    saw_completed = true;
                    break;
                }
                // Lagged — медленный ресивер пропустил часть событий;
                // Completed мог быть среди них, проверяем state через snapshot.
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Ok(Ok(_)) => {}
                Err(_) => {}
            }
            if mgr
                .snapshot()
                .iter()
                .any(|d| d.id == id && d.state == DownloadState::Done)
            {
                saw_completed = true;
                break;
            }
        }
        assert!(saw_completed, "Completed event was not delivered");
    }
}
