//! `DownloadManager` — верхний координатор над множеством `DownloadEngine`'ов.
//!
//! Отвечает за:
//! - реестр загрузок (`records`) и активных движков (`engines`);
//! - fan-in событий в два broadcast-канала —
//!   `broadcast::Sender<FileLifecycleEvent>` (для `WatchFile`) и
//!   `broadcast::Sender<ProgressEvent>` (для `WatchProgress`);
//! - персистенцию состояний через [`TQueueStore`] (insert при `add`,
//!   `update_state` при смене стейта);
//! - восстановление очереди из [`TQueueStore`] при старте ([`DownloadManager::bootstrap`]).
//!
//! ## Структура модуля
//!
//! Публичный фасад (`DownloadManager`, `ManagerConfig`) и пользовательские
//! команды (`add`/`remove`/`pause`/...) живут здесь. Внутренняя механика
//! разнесена:
//!
//! - [`queue`] — продвижение очереди (`try_spawn_next`) и сам спавн движка
//!   (`spawn_engine_impl`).
//! - [`fanin`] — задачи-подписчики, слушающие broadcast каждого движка и
//!   форвардящие события в общий канал менеджера (`fan_in_lifecycle` /
//!   `fan_in_progress`).
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
//! 1. Читает lifecycle-события из `broadcast::Receiver` движка.
//! 2. Форвардит их в общий `lifecycle_tx` (для подписчиков `WatchFile`).
//! 3. Обновляет `records[id]` (state/progress/error/updated_at).
//! 4. На смене state — пишет в `TQueueStore`.
//! 5. На терминальном событии — удаляет движок из `engines` и пробует
//!    спаунить следующий из `waiting`.
//!
//! Задача держит `Arc<Shared>` — это создаёт временный «цикл» владения,
//! который разрывается сам, когда task завершается.

mod fanin;
mod queue;

#[cfg(all(test, feature = "test-utils"))]
mod tests;

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
use tracing::warn;

use crate::domain::{
    FailureReason,
    File,
    FileId,
    FileLifecycleEvent,
    FileSpec,
    FileStatus,
    ProgressEvent,
    ReasonCode,
};
use crate::error::{
    Error,
    Result,
};
use crate::ports::{
    NoopAttemptRepo,
    NoopWorkerRepo,
    TFilePresenceCheck,
    TPieceAttemptRepo,
    TPieceStorageFactory,
    TQueueStore,
    TRangeFetch,
    TWorkerRepo,
};
use crate::service::engine::{
    EngineConfig,
    EngineHandle,
};

/// Конфигурация менеджера.
#[derive(Clone)]
pub struct ManagerConfig {
    /// Ёмкость центрального broadcast-канала событий.
    pub events_capacity: usize,
    /// Конфигурация, которая прокидывается в каждый движок.
    pub engine: EngineConfig,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            events_capacity: 1024,
            engine: EngineConfig::default(),
        }
    }
}

/// Верхний координатор загрузок. Клонируется дёшево (внутри `Arc`).
///
/// `WR`/`AR` — репозитории воркеров и попыток piece'ов. Дефолты
/// ([`NoopWorkerRepo`] / [`NoopAttemptRepo`]) — для тестов и кейсов,
/// где аналитика журнала не нужна; prod-подключение (brook-daemon)
/// подставляет SQLite-адаптеры явно через [`DownloadManager::with_tracking`].
pub struct DownloadManager<PF, QS, F, WR = NoopWorkerRepo, AR = NoopAttemptRepo>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    pub(super) shared: Arc<Shared<PF, QS, F, WR, AR>>,
}

impl<PF, QS, F, WR, AR> Clone for DownloadManager<PF, QS, F, WR, AR>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

pub(super) struct Shared<PF, QS, F, WR, AR>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
    WR: TWorkerRepo + Send + Sync + 'static,
    AR: TPieceAttemptRepo + Send + Sync + 'static,
{
    pub(super) factory: Arc<PF>,
    pub(super) queue: Arc<QS>,
    pub(super) fetch: Arc<F>,
    pub(super) workers_repo: Arc<WR>,
    pub(super) attempts_repo: Arc<AR>,
    /// Проверяет, что Done-файл всё ещё лежит на диске. Используется
    /// `list_recently` / `list_files` для ленивого аудита и переноса
    /// записи в Failed при удалённом файле.
    pub(super) file_presence: Arc<dyn TFilePresenceCheck>,
    pub(super) inner: Mutex<Inner>,
    pub(super) lifecycle_tx: broadcast::Sender<FileLifecycleEvent>,
    pub(super) progress_tx: broadcast::Sender<ProgressEvent>,
    pub(super) config: ManagerConfig,
}

pub(super) struct Inner {
    pub(super) records: HashMap<FileId, File>,
    pub(super) engines: HashMap<FileId, EngineHandle>,
    /// Упорядоченная очередь id для спавна движков. Id может быть и в
    /// `Paused` (пользователь попросил pause до старта) — такие при
    /// продвижении пропускаются.
    pub(super) waiting: VecDeque<FileId>,
}

impl<PF, QS, F> DownloadManager<PF, QS, F, NoopWorkerRepo, NoopAttemptRepo>
where
    PF: TPieceStorageFactory + Send + Sync + 'static,
    PF::Storage: Send + Sync + 'static,
    PF::StreamStorage: Send + Sync + 'static,
    QS: TQueueStore + Send + Sync + 'static,
    F: TRangeFetch + Send + Sync + 'static,
{
    /// Собрать менеджер без аналитики воркеров/попыток (no-op репозитории).
    /// Используется в юнит-тестах ядра и in-memory harness'ах.
    pub fn new(
        factory: Arc<PF>,
        queue: Arc<QS>,
        fetch: Arc<F>,
        file_presence: Arc<dyn TFilePresenceCheck>,
        config: ManagerConfig,
    ) -> Self {
        Self::with_tracking(
            factory,
            queue,
            fetch,
            Arc::new(NoopWorkerRepo),
            Arc::new(NoopAttemptRepo),
            file_presence,
            config,
        )
    }
}

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
    /// Собрать менеджер с явными репозиториями воркеров и попыток —
    /// prod-путь brook-daemon, где `workers` и `piece_attempts` пишутся
    /// в общую `brook.db`.
    pub fn with_tracking(
        factory: Arc<PF>,
        queue: Arc<QS>,
        fetch: Arc<F>,
        workers_repo: Arc<WR>,
        attempts_repo: Arc<AR>,
        file_presence: Arc<dyn TFilePresenceCheck>,
        config: ManagerConfig,
    ) -> Self {
        let (lifecycle_tx, _) = broadcast::channel(config.events_capacity);
        let (progress_tx, _) = broadcast::channel(config.events_capacity);
        Self {
            shared: Arc::new(Shared {
                factory,
                queue,
                fetch,
                workers_repo,
                attempts_repo,
                file_presence,
                inner: Mutex::new(Inner {
                    records: HashMap::new(),
                    engines: HashMap::new(),
                    waiting: VecDeque::new(),
                }),
                lifecycle_tx,
                progress_tx,
                config,
            }),
        }
    }

    /// Подписаться на поток lifecycle-событий (`WatchFile`).
    pub fn subscribe_lifecycle(&self) -> broadcast::Receiver<FileLifecycleEvent> {
        self.shared.lifecycle_tx.subscribe()
    }

    /// Подписаться на поток прогресс-тиков (`WatchProgress`).
    pub fn subscribe_progress(&self) -> broadcast::Receiver<ProgressEvent> {
        self.shared.progress_tx.subscribe()
    }

    /// Срез всех известных менеджеру загрузок (read-only).
    pub fn snapshot(&self) -> Vec<File> {
        let inner = self.shared.inner.lock().expect("mutex poisoned");
        inner.records.values().cloned().collect()
    }

    /// Одна запись по id из in-memory кэша. Нужно API-handler'у `Add`,
    /// чтобы вернуть свежий `File` сразу после persist'а — без лишнего
    /// похода в БД.
    pub fn get_file(&self, id: &FileId) -> Option<File> {
        let inner = self.shared.inner.lock().expect("mutex poisoned");
        inner.records.get(id).cloned()
    }

    /// Файлы с активностью >= `since`. Источник — репо
    /// (`TQueueStore::list_recently`), потому что `last_activity_at`
    /// поддерживается триггерами в БД и in-memory не дублируется.
    /// На выходе прогоняется ленивый аудит существования файлов:
    /// каждая `Done`-запись stat'ается, пропавшие переезжают в
    /// `Failed`/`FileMissing` (и в БД, и в возвращаемом снимке).
    pub async fn list_recently(&self, since: SystemTime) -> Result<Vec<File>> {
        let mut files = self.shared.queue.list_recently(since).await?;
        self.audit_done_presence(&mut files).await;
        Ok(files)
    }

    /// Пагинированный список всех файлов (`TQueueStore::list_paginated`).
    /// Аудит существования Done-файлов — как в [`Self::list_recently`].
    pub async fn list_files(&self, offset: u32, limit: u32) -> Result<Vec<File>> {
        let mut files = self.shared.queue.list_paginated(offset, limit).await?;
        self.audit_done_presence(&mut files).await;
        Ok(files)
    }

    /// Сообщение, которое записываем в `error` (и в `reason_message`
    /// строки `status_changes`) при переводе пропавшего Done-файла в
    /// Failed. UI рендерит его в meta-строке карточки.
    const MISSING_FILE_MSG: &'static str = "file no longer exists on disk";

    /// Прогон по списку: каждый `Done`-файл проверяем на наличие на
    /// диске и при отсутствии переводим в `Failed` (с `FileMissing`
    /// reason'ом). Запись в БД, in-memory снимок и возвращаемый
    /// `files`-вектор синхронно патчатся; broadcast lifecycle-event
    /// шлётся, чтобы подключённые наблюдатели увидели транзицию без
    /// нового `GetRecently`.
    async fn audit_done_presence(&self, files: &mut [File]) {
        for f in files.iter_mut() {
            if f.status != FileStatus::Done {
                continue;
            }
            let Some(filename) = f.spec.filename.as_deref() else {
                // Без resolved-имени проверять нечего; такой Done быть
                // не должен (resolve обязателен для Add), но защищаемся.
                continue;
            };
            let path = f.spec.target_dir.join(filename);
            if self.shared.file_presence.exists(&path).await {
                continue;
            }
            self.flip_done_to_failed_missing(f.id).await;
            f.status = FileStatus::Failed;
            f.error = Some(Self::MISSING_FILE_MSG.into());
            f.updated_at = SystemTime::now();
        }
    }

    /// Hot-path транзиции «Done → Failed (FileMissing)». Пишем в БД,
    /// in-memory и broadcast — ровно по тому же шаблону, что и
    /// engine-driven переходы (см. fanin.rs). Гонки с другим аудитом
    /// безопасны: `update_status` идемпотентна на уровне SQLite, второй
    /// проход просто допишет ещё один `status_changes`-row.
    async fn flip_done_to_failed_missing(&self, id: FileId) {
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(record) = inner.records.get_mut(&id) {
                // Если кто-то уже перевёл запись (другой аудит, retry)
                // — не перетираем. Транзиция нужна только из Done.
                if record.status != FileStatus::Done {
                    return;
                }
                record.status = FileStatus::Failed;
                record.error = Some(Self::MISSING_FILE_MSG.into());
                record.updated_at = SystemTime::now();
            }
            // Если в in-memory записи нет (бывает: bootstrap ещё не
            // прогрузился, или запись только что remove'нули) — делаем
            // только DB-запись, чтобы следующий `GetRecently` уже видел
            // Failed. Broadcast в этом случае слать некому.
        }
        let reason = FailureReason::with_message(ReasonCode::FileMissing, Self::MISSING_FILE_MSG);
        if let Err(e) = self
            .shared
            .queue
            .update_status(id, FileStatus::Failed, Some(reason))
            .await
        {
            warn!(%id, error = %e, "audit: failed to persist Done→Failed transition");
        }
        let _ = self
            .shared
            .lifecycle_tx
            .send(FileLifecycleEvent::failed(id, Self::MISSING_FILE_MSG));
    }

    /// Загрузить очередь из `TQueueStore` и запустить продвижение.
    ///
    /// Не-терминальные состояния нормализуются: `Running`/`Retrying`
    /// → `Queued` (движок не пережил рестарт демона), `Paused` и
    /// `Queued` остаются как есть.
    pub async fn bootstrap(&self) -> Result<()> {
        let loaded = self.shared.queue.load_all().await?;
        let mut to_fix: Vec<(FileId, FileStatus)> = Vec::new();
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            for mut d in loaded {
                let target = match d.status {
                    FileStatus::Running | FileStatus::Retrying => {
                        to_fix.push((d.id, FileStatus::Pending));
                        FileStatus::Pending
                    }
                    s => s,
                };
                d.status = target;
                if matches!(
                    target,
                    FileStatus::Pending | FileStatus::Paused | FileStatus::Retrying
                ) && !matches!(target, FileStatus::Paused)
                {
                    inner.waiting.push_back(d.id);
                }
                inner.records.insert(d.id, d);
            }
        }
        for (id, status) in to_fix {
            if let Err(e) = self.shared.queue.update_status(id, status, None).await {
                warn!(%id, error = %e, "bootstrap: failed to persist normalized state");
            }
        }
        self.try_spawn_next().await;
        Ok(())
    }

    /// Добавить новый файл. Возвращает его `FileId`.
    ///
    /// Синхронно вызывает `factory.resolve()` — один HEAD к источнику,
    /// резолв имени и exists-чек. Если имя уже занято, клиент получает
    /// `AlreadyExists` прямо в ответе на RPC `Add` (rename-модалка в TUI
    /// открывается моментально, а не по Failed-переходу). На ошибке
    /// резолва откатываем уже вставленную строку: `queue.remove` каскадом
    /// снимет и `pieces`/`status_changes`.
    pub async fn add(&self, spec: FileSpec) -> Result<FileId> {
        let id = FileId::new();
        let mut file = File::new(id, spec.clone());
        self.shared.queue.insert(&file).await?;

        let filename = match self.shared.factory.resolve(id, &spec).await {
            Ok(name) => name,
            Err(e) => {
                if let Err(remove_err) = self.shared.queue.remove(id).await {
                    warn!(%id, error = %remove_err, "failed to roll back queue row after resolve error");
                }
                return Err(e);
            }
        };
        file.spec.filename = Some(filename);
        // После inspect демон уже знает `total_size`. Перечитываем
        // одну строку из БД, чтобы AddResponse сразу нёс полный размер
        // (иначе клиент рисует «—» до первого progress-tick).
        if let Ok(Some(persisted)) = self.shared.queue.get(id).await {
            file.total_size = persisted.total_size;
        }

        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            inner.records.insert(id, file);
            inner.waiting.push_back(id);
        }
        // WatchStatus отдаёт только статусные переходы. Создающая
        // сторона видит новую запись через `AddResponse.file`; других
        // клиентов о новой записи в стриме не уведомляем — они узнают
        // о ней через следующий `GetRecently`/reconnect.
        self.try_spawn_next().await;
        Ok(id)
    }

    /// Удалить загрузку. Работает в любом состоянии: если engine активен
    /// (running/paused) — сначала шлём `Cancel` и ждём, пока fan-in task
    /// снимет запись из `engines`, затем чистим records/waiting/queue.
    ///
    /// Идемпотентно: если id ни в records, ни в queue — тоже Ok. Это важно
    /// для клиента, у которого в ViewModel может остаться «призрак»
    /// (например, при рассинхроне после рестарта демона): повторный
    /// `Remove` должен починить состояние, а не ругаться NotFound'ом.
    pub async fn remove(&self, id: FileId) -> Result<()> {
        let was_active = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.cancel();
                true
            } else {
                false
            }
        };
        if was_active {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let inner = self.shared.inner.lock().expect("mutex poisoned");
                if !inner.engines.contains_key(&id) {
                    break;
                }
            }
        }
        // Снимаем spec до выкидывания записи: для inactive id `.data.brook`
        // никем больше не сносится, и без spec'а его не найти. Активные
        // engine удаляют свой `.data.brook` сами через `abort` — для них
        // ниже `wipe_artifacts` будет no-op.
        let spec = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            inner.records.get(&id).map(|f| f.spec.clone())
        };
        let was_known = {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            let had_record = inner.records.remove(&id).is_some();
            inner.waiting.retain(|x| *x != id);
            had_record
        };
        let queue_result = match self.shared.queue.remove(id).await {
            Ok(()) => Ok(()),
            // Запись могла быть только в памяти (рассинхрон с БД) или
            // вообще не существовать — оба случая считаем успехом
            // (Remove идемпотентен; призраков у наблюдателей лечит
            // ghost-режим TUI на следующей команде).
            Err(Error::NotFound) if !was_known => Ok(()),
            Err(e) => Err(e),
        };
        // Best-effort покос `.data.brook`. Делаем после queue.remove:
        // если SQL упал, на диске ещё лежит файл, при ретрае Remove
        // дойдём сюда снова. Ошибки FS логируем и игнорируем — Remove
        // идемпотентен и не должен падать из-за стейл-файла.
        if let Some(spec) = spec
            && let Err(e) = self.shared.factory.wipe_artifacts(&spec).await
        {
            warn!(%id, error = %e, "remove: wipe_artifacts failed; ignoring");
        }
        queue_result
    }

    /// Поставить загрузку на паузу.
    ///
    /// Если движок уже запущен — команда `Pause` уезжает в engine; финальный
    /// `StateChanged(Paused)` прилетит из движка.
    /// Если загрузка ещё в `waiting` — сразу помечаем `Paused` и
    /// убираем из `waiting` (движок не появится, пока не будет `Resume`).
    pub async fn pause(&self, id: FileId) -> Result<()> {
        let mut persist = None;
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.pause();
                return Ok(());
            }
            inner.waiting.retain(|x| *x != id);
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.status.is_terminal() {
                return Err(Error::Other("download is terminal".into()));
            }
            if record.status != FileStatus::Paused {
                record.status = FileStatus::Paused;
                record.updated_at = SystemTime::now();
                persist = Some(FileStatus::Paused);
                let _ = self
                    .shared
                    .lifecycle_tx
                    .send(FileLifecycleEvent::status(id, FileStatus::Paused));
            }
        }
        if let Some(status) = persist {
            self.shared.queue.update_status(id, status, None).await?;
        }
        Ok(())
    }

    /// Возобновить загрузку.
    ///
    /// Если движок активен — прокидываем `Resume`.
    /// Иначе — возвращаем в `waiting` и пытаемся спаунить.
    pub async fn resume(&self, id: FileId) -> Result<()> {
        let mut persist = None;
        {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.resume();
                return Ok(());
            }
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.status.is_terminal() {
                return Err(Error::Other("download is terminal".into()));
            }
            if record.status != FileStatus::Pending {
                record.status = FileStatus::Pending;
                record.updated_at = SystemTime::now();
                persist = Some(FileStatus::Pending);
                let _ = self
                    .shared
                    .lifecycle_tx
                    .send(FileLifecycleEvent::status(id, FileStatus::Pending));
            }
            if !inner.waiting.iter().any(|x| *x == id) {
                inner.waiting.push_back(id);
            }
        }
        if let Some(status) = persist {
            self.shared.queue.update_status(id, status, None).await?;
        }
        self.try_spawn_next().await;
        Ok(())
    }

    /// Отменить загрузку.
    pub async fn cancel(&self, id: FileId) -> Result<()> {
        let should_persist = {
            let mut inner = self.shared.inner.lock().expect("mutex poisoned");
            if let Some(handle) = inner.engines.get(&id) {
                handle.cancel();
                return Ok(());
            }
            inner.waiting.retain(|x| *x != id);
            let record = inner.records.get_mut(&id).ok_or(Error::NotFound)?;
            if record.status.is_terminal() {
                return Ok(()); // уже всё.
            }
            record.status = FileStatus::Cancelled;
            record.updated_at = SystemTime::now();
            let _ = self
                .shared
                .lifecycle_tx
                .send(FileLifecycleEvent::status(id, FileStatus::Cancelled));
            true
        };
        if should_persist {
            self.shared
                .queue
                .update_status(
                    id,
                    FileStatus::Cancelled,
                    Some(FailureReason::new(ReasonCode::CancelledByUser)),
                )
                .await?;
        }
        Ok(())
    }

    /// Поставить на паузу все активные и всё, что в `waiting`.
    pub async fn pause_all(&self) -> Result<()> {
        let ids: Vec<FileId> = {
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
        let active: Vec<FileId> = {
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
                            d.status,
                            FileStatus::Paused
                                | FileStatus::Done
                                | FileStatus::Failed
                                | FileStatus::Cancelled
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

    /// Перезапустить упавшую загрузку. Hard restart: запись со старым
    /// `id` уходит со всем каскадом (`pieces`, `status_changes`,
    /// `workers`, `piece_attempts`) и `.data.brook` через
    /// [`Self::remove`], затем для того же `FileSpec` создаётся новая
    /// запись через [`Self::add`] — со свежим `FileId`, повторным
    /// resolve / inspect / новой piece-раскладкой. Возвращает снимок
    /// созданной записи (по аналогии с `add` → `AddResponse.file`).
    ///
    /// На не-`Failed` записи — ошибка: retry это явный пользовательский
    /// жест после терминального падения, на Running/Done и т.п. он не
    /// предлагается в UI и не должен срабатывать через API.
    pub async fn retry(&self, id: FileId) -> Result<File> {
        let spec = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            let record = inner.records.get(&id).ok_or(Error::NotFound)?;
            if record.status != FileStatus::Failed {
                return Err(Error::Other("download is not in failed state".into()));
            }
            record.spec.clone()
        };
        // Каскадный снос старой записи + `.data.brook`.
        self.remove(id).await?;
        // Та же дорога, что и при ручном Add: factory.resolve (HEAD),
        // inspect-поля, новые pieces, push в waiting, try_spawn_next.
        let new_id = self.add(spec).await?;
        self.get_file(&new_id).ok_or(Error::NotFound)
    }

    /// Возобновить все `Paused`.
    pub async fn resume_all(&self) -> Result<()> {
        let ids: Vec<FileId> = {
            let inner = self.shared.inner.lock().expect("mutex poisoned");
            inner
                .records
                .iter()
                .filter(|(_, d)| d.status == FileStatus::Paused)
                .map(|(id, _)| *id)
                .collect()
        };
        for id in ids {
            let _ = self.resume(id).await;
        }
        Ok(())
    }
}
