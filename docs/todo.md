# TODO — MVP

Порядок фиксирован ([roadmap.md](roadmap.md)). Не идти дальше, пока текущий этап не закрыт.

Каждый пункт — один атомарный коммитабельный шаг.

## 0. Workspace
- [x] Cargo workspace + 4 крейта: `brook-proto`, `brook-core`, `brook-api`, `brook`
- [x] Переименовать крейт `brook` → `brook-tui`; бинарь `brook` (`[[bin]] name = "brook"`)
- [x] Добавить крейт `brookd` (демон, bin) — итого 5 крейтов
- [x] Editorconfig для rust/markdown/toml
- [x] prek config
- [x] `proto/brook/v1/brook.proto` — скелет из [api.md](api.md)
- [x] `brook-proto/build.rs` → `tonic-build`
- [x] CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`

## 1. `brook-core`

### 1.1 Доменные типы (без I/O)
- [x] Тип `DownloadId` (newtype над `uuid::Uuid`; миграция на `xid` тривиальна внутри newtype)
- [x] Enum `DownloadState` (`Queued`, `Running`, `Paused`, `Retrying`, `Done`, `Failed`, `Cancelled`)
- [x] Структура `DownloadSpec` (url, target_dir, filename, workers)
- [x] Структура `Progress` (bytes_done, bytes_total, pieces_done, pieces_total, speed_bps, eta_secs)
- [x] Структура `Download` (id, spec, state, progress, attempt, error, timestamps)
- [x] Enum `DownloadCommand` (`Pause`, `Resume`, `Cancel`)
- [x] Enum `DownloadEvent` (`Progress`, `StateChanged`, `WorkerUpdate`, `Completed`, `Failed`, `Snapshot`)
- [x] Юнит-тесты: сериализация/дефолты/конверсии

### 1.2 Трейты абстракций (без реализаций)
- [x] `TPieceStorage`: `write_piece_bytes`, `commit_batch`, `pending_pieces`, `finalize`, `abort`
- [x] `TPieceStorageFactory`: `prepare(spec) -> PreparedDownload { storage, total_size, piece_size, accepts_ranges, guard, resolved_filename }` (расширено в 1.10: фабрика инкапсулирует inspect+plan+open, менеджеру не нужен отдельный `THttpInspect`-порт)
- [x] `TQueueStore`: `load_all`, `insert`, `update_state`, `remove`
- [x] Документирующие doc-комментарии инвариантов (commit ⇒ persisted)

### 1.3 In-memory реализации для тестов
- [x] `MemoryPieceStorage` (test-utils, feature `test-utils`)
- [x] `MemoryTQueueStore` (test-utils)
- [x] Юнит-тест: round-trip через `MemoryPieceStorage`
- [x] Юнит-тест: round-trip через `MemoryTQueueStore`

### 1.4 HTTP-слой (порт в `brook-core` + адаптер `brook-http`)
- [x] Новый крейт `brook-http`; `brook-core` содержит только порты и не тянет `reqwest`
- [x] Порты в `brook-core`: `THttpInspect::inspect`, `TRangeFetch::{fetch_range, fetch_full}`; domain-типы `InspectReport`, `RangeGuard`, `ByteStream`; enum'ы `InspectError`, `RangeError`
- [x] Метод `is_transient()` на `InspectError` и `RangeError` — для будущего `RetryPolicy` (HTTP-слой сам retry не делает)
- [x] Правило: реализации называются `*Client` (`HttpInspectClient`, `RangeFetchClient`); `reqwest::{Client,Response,RequestBuilder,Error}` и `reqwest_middleware::Error` не утекают за пределы `brook-http`
- [x] `HttpClientBuilder` — единая точка сборки `reqwest::Client`: rustls, connect timeout 10 s, read (idle) 30 s, pool idle 90 s, User-Agent `brook/<version>`
- [x] Автокомпрессия выключена (`no_gzip`, `no_brotli`, `no_deflate`) — байт-точность `Content-Length` и Range
- [x] Политика редиректов: `redirect::Policy::limited(10)`
- [x] Middleware `RequestResponseLoggingMiddleware` (`reqwest-middleware`): method/URL/статус/длительность/`Content-Length`; тело не логируется; корреляция `download_id`/`request_id` из `tracing::Span::current()`
- [x] Валидация URL на границе адаптера: только `http`/`https`, иначе `InvalidScheme` до сетевого вызова
- [x] `HttpInspectClient::inspect`: `HEAD` → на 4xx/5xx fallback `GET Range: bytes=0-0`; парсинг `Content-Length`, `Accept-Ranges`, `ETag`, `Last-Modified`, `Content-Disposition`
- [x] Имя файла: `Content-Disposition filename*=` (RFC 5987) → `filename=` → последний сегмент URL-пути
- [x] `RangeFetchClient::fetch_range`: `Range: bytes=OFFSET-END`, guard `If-Match`/`If-Unmodified-Since`; `206`+`Content-Range` валидация, `200`→`RangeNotSupported`, `412`→`SourceMutated`, усечённое тело → `TruncatedResponse`
- [x] `RangeFetchClient::fetch_full`: полный стрим до EOF — для no-Range fallback на уровне engine
- [x] Cancellation: дроп `ByteStream` отменяет in-flight запрос
- [x] Тесты `wiremock`: inspect (HEAD-ok, HEAD-fail+GET-range, no-Range, Content-Disposition: filename*/filename/URL-fallback), range (`206`-ok, `200`-на-Range, `412`, усечённое тело, невалидный Content-Range), fetch_full до EOF, invalid scheme без сетевого вызова
- [x] Юнит-тесты на `is_transient()` для `InspectError` и `RangeError`
- [x] Гарантия hex-arch: `cargo tree -p brook-core | grep reqwest` — пусто

### 1.5 Retry-политика
- [x] `RetryPolicy`: экспо-бэкофф `1s × 2^attempt` + jitter ±20 %, max delay 60 s, max 10 попыток
- [x] Классификация через `is_transient()` из 1.4 (транзиентные → ретрай, остальные → fail fast)
- [x] Crash-loop guard: 5 одинаковых ошибок подряд → `FAILED`
- [x] Юнит-тесты на расчёт задержек и trigger crash-loop

### 1.6 FS-примитивы, нарезка и piece-index
Подготовить всё, на чём будет собран `LocalPieceStorage`: низкоуровневые файловые операции, чистая арифметика нарезки и персистентный индекс кусков. В этом этапе ещё нет реализации `TPieceStorage` — только кирпичи.
- [x] `pwrite_full` / `read_full` — тонкие обёртки над `FileExt::write_all_at`/`read_exact_at` (std сам обрабатывает EINTR и частичные записи); юнит-тесты round-trip и чтение за EOF
- [x] Проверка свободного места на целевой ФС (`fs4::available_space`)
- [x] Пре-аллокация `<filename>.data.brook` (`fs4::FileExt::allocate` — на macOS это `F_PREALLOCATE`, на Linux `fallocate`, на Windows `SetFileInformationByHandle`)
- [x] Path-traversal защита: `validate_filename` + `resolve_target`; filename должен быть одним `Component::Normal`
- [x] Чистая функция `plan_pieces(size, cfg) -> PiecePlan { piece_size, pieces }`: `clamp(next_pow2(size / piece_target_count), piece_size_min, piece_size_max)`, дефолты 128 / 16 MiB / 128 MiB; границы hard-coded (TODO для 3.x — читать из `settings`)
- [x] Юнит-тесты: границы clamp, округление, последний кусок меньше `piece_size`, offset'ы непрерывны
- [x] Интеграционный тест: пре-аллокация 100 MiB файла в tmpdir + проверка раскладки кусков
- [x] `PieceIndexRepository`: миграция `pieces(idx INTEGER PK, offset, size, status TEXT CHECK)` + `meta(key, value)`
- [x] SQLite WAL + `synchronous=NORMAL` при открытии `.index.brook`
- [x] `PieceIndexRepository::open(path) -> Self` (создаёт/открывает)
- [x] `PieceIndexRepository::init(url, total_size, piece_size, pieces)` — первая запись; `piece_size` в `meta`
- [x] `PieceIndexRepository::pending_pieces() -> Vec<PieceLayout>`
- [x] `PieceIndexRepository::commit_done_batch(ids)` — UPDATE + транзакция
- [x] `PieceIndexRepository::meta_get/set` (url, etag, total_size, piece_size)
- [x] `PieceIndexRepository::delete_all` (для abort)
- [x] SQL-строки и `rusqlite::Connection` живут только внутри этого модуля
- [x] Юнит-тесты на каждый метод репозитория

### 1.7 `LocalPieceStorage` поверх примитивов
- [x] `LocalPieceStorage::open(target_dir, filename, url, total_size, plan)` открывает `.data.brook` (`pwrite`-handle) + `PieceIndexRepository` (подпись шире буквального `new(spec)` — фабрика с одним только `DownloadSpec` не считает нарезку; реализация `TPieceStorageFactory` — в 4.2)
- [x] `write_piece_bytes`: `pwrite_full` по абсолютному offset'у куска (карта `piece_index → offset`)
- [x] `commit_batch`: `sync_data(.data)` → `PieceIndexRepository::commit_done_batch`
- [x] `pending_pieces`: делегирует в репозиторий
- [x] `finalize`: `sync_all` → `rename .data.brook → <filename>` → удалить `.index.brook` (+ `-wal`/`-shm`)
- [x] `abort`: удалить `.data.brook` и `.index.brook` (+ `-wal`/`-shm`)
- [x] Сбои: `.index` или `.data` отсутствует/битый (или не совпадает meta url/total_size/piece_size) → стартовать с нуля
- [x] `PieceIndexRepository::all_pieces()` — для восстановления offset-карты при resume
- [x] Все блокирующие операции (pwrite, fsync, rusqlite) в `tokio::task::spawn_blocking`
- [x] Интеграционный тест на полный цикл init → write → commit → restart → resume → finalize

### 1.8 `DownloadEngine` — скелет
- [x] Структура `DownloadEngine` с mpsc команд и broadcast событий
- [x] `DownloadEngine::spawn(id, inputs, config, storage, fetch) -> (EngineHandle, events_rx)`
- [x] Обработка `Pause` / `Resume` / `Cancel`
- [x] Юнит-тест: команды меняют state, эмитят `StateChanged`

### 1.9 `DownloadEngine` — воркеры и события
- [x] Общая shared-очередь pending-piece'ов (`Arc<Mutex<VecDeque<u32>>>`) — work-stealing
- [x] Спаун N воркеров по `spec.workers`
- [x] Воркер: берёт piece → `TRangeFetch::fetch_range` → потоковый `write_piece_bytes` буфером 64–256 KB
- [x] Проверка полноты куска → `PieceDone` либо усечение = транзиентная ошибка (ретрай с нуля)
- [x] Батч-коммит каждые 16 кусков → `commit_batch`
- [x] Финальный коммит на `pause` / `shutdown` / перед `finalize`
- [x] No-Range режим (`InspectReport.accepts_ranges=false`) → один воркер, `TRangeFetch::fetch_full`, до EOF
- [x] Агрегация счётчиков в таймере 200 ms → один `Progress` за окно
- [x] State-changes — мгновенный эмит, без таймера
- [x] `Completed` после успешного `finalize`
- [x] `Failed(reason)` на терминальной ошибке
- [x] Юнит-тесты на mock-`TRangeFetch`: нормальная загрузка, обрыв посреди куска, 500+retry, no-Range режим
- [x] Юнит-тест: частота `Progress` ≤ 5 Hz

### 1.10 `DownloadManager`
- [x] Структура `DownloadManager` с реестром engines по `DownloadId`
- [x] Принимает `TPieceStorageFactory` + `TQueueStore` + `TRangeFetch` в конструкторе
- [x] `add(spec)` — insert в queue-store, спаун engine при наличии слота
- [x] `pause(id)` / `resume(id)` / `cancel(id)` / `remove(id)` — роутинг в нужный engine
- [x] `pause_all` / `resume_all`
- [x] `max_concurrent` — не спаунить engine сверх лимита, держать в `Queued`
- [x] Центральный `broadcast::Sender<Event>` ring 1024 — fan-in от всех engines
- [x] При старте: `bootstrap()` — `TQueueStore::load_all` → восстановить реестр (Running/Retrying → Queued), запустить по лимиту
- [x] Snapshot по запросу (для Watch-реконсиляции)
- [x] Юнит-тесты: 3 engines с `max_concurrent=2`, bootstrap-восстановление, snapshot, fan-in, cancel до спавна, блокировка remove для активных

### 1.11 Тесты `brook-core`
- [x] Fault-injection (через `brook-http` + `wiremock`): обрыв на полуслове (`TruncatedResponse` → ретрай), `500`×2 с ретраем, смена `ETag` (`412` → `SourceMutated` → `Failed`) — `crates/brook-core/tests/fault_injection.rs`
- [x] Отсутствие Range-поддержки (`accepts_ranges=false`) → `fetch_full` fallback, загрузка завершается
- [x] Пиковый RSS ≤ 150 MB при 10 параллельных engines (отдельный perf-тест, `#[ignore]`, запуск через `cargo test --test fault_injection -- --ignored`)

## 2. `brook-proto` + `brook-api`

### 2.1 Proto-контракт
- [x] Сервис и сообщения: `List`, `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `Watch`
- [x] Событие `Event` с oneof (`progress`, `state_changed`, `worker_update`, `completed`, `failed`, `snapshot`)
- [x] `protolint` чистый (рецепт `just lint-proto`, конфиг `.protolint.yaml`)
- [x] `cargo build -p brook-proto` генерирует без warning'ов

### 2.2 Тонкая обёртка API
- [x] `BrookService` — реализация tonic-сервиса поверх `DownloadManager`
- [x] Мапперы `proto ↔ core` в отдельном модуле (без бизнес-логики)
- [x] Unary: `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `List`
- [x] Юнит-тесты на мапперы

### 2.3 Watch-стрим
- [x] Per-client fanout-задача: подписка на центральный `broadcast` + tonic tx
- [x] Initial snapshots при коннекте — по одному `Event::Snapshot` на каждую известную загрузку
- [x] `tx.send().await` — транспортный backpressure
- [x] `RecvError::Lagged(n)` → запросить у `DownloadManager` snapshot'ы активных и дослать
- [x] Интеграционный тест: fast producer + slow consumer → клиент догоняется

### 2.4 Транспорт и трейсинг
- [x] Bind `127.0.0.1:<port из settings>` — контракт фиксирован; сам bind выполняет `brookd` (§4.2). `brook-api` экспортирует `BrookServiceServer`, `BrookService::new`, `trace_interceptor`.
- [x] Metadata: `session_id`, `request_id`, при наличии — `download_id` — интерцептор `trace::trace_interceptor` кладёт `CorrelationIds` в `Request::extensions`.
- [x] Tracing: root-span на запрос, наследование в core
- [x] Интеграционный тест: client ↔ server в одном процессе, все методы

## 3. Конфигурация (YAML, `./brook.yaml`)

### 3.1 Типы и парсер
- [x] `crates/brookd/src/config.rs`: структуры `Settings`, `DownloadSection`, `ApiSection`, `LogSection` с `serde(default)`
- [x] Enum'ы `OnDuplicateUrl` / `OnFileExists` (`ask`/`skip`/`add`, `ask`/`rename`/`overwrite`), `serde(rename_all = "lowercase")`
- [x] `Settings::load(path) -> Result<Settings, ConfigError>` — serde_yaml + валидация
- [x] `Settings::write_default(path)` — YAML-шаблон с комментариями
- [x] `Settings::load_or_init(path)` — создать файл при первом старте, прочитать на втором
- [x] `deny_unknown_fields` — неизвестный ключ = ошибка парса (опечатки не проходят молча)
- [x] Валидация: `pow2` для piece-границ, `min ≤ max`, `default_workers ≤ max_workers`, `api.bind` как валидный `IpAddr`
- [x] Раскрытие `~` в путях через `directories::BaseDirs::home_dir`
- [x] Юнит-тесты: дефолты валидны, partial-секции fill'ятся дефолтами, неизвестный ключ отклоняется, битое значение → `ConfigError` с именем ключа, запись дефолтного файла + обратный парс, `load_or_init` идемпотентен

### 3.2 Проекции и override'ы нарезки
- [x] `DaemonRuntime` (global-only) и `DownloadDefaults` (per-download defaults) как отдельные проекции
- [x] `DaemonRuntime::from_settings` — раскрытие `~` и перевод MiB → байты
- [x] `DownloadSpec` (proto + core): optional `piece_target_count`, `piece_size_min`, `piece_size_max`
- [x] Маппер `brook-api`: round-trip новых полей
- [x] `effective_plan_config(spec, defaults) -> Result<PiecePlanConfig, PlanConfigError>` в `storage::plan` — применение override'ов и валидация
- [x] Юнит-тесты: override побеждает default, невалидный override → ошибка, отсутствие override → fallback на defaults

## 4. `brookd` (демон)

### 4.1 `SqliteQueueRepository` (реализация `TQueueStore`)
Единственное место во всём крейте, где живут SQL и `rusqlite::Connection` по очереди загрузок (аналогично [index.rs](../crates/brookd/src/storage/index.rs)).
- [x] Схема `downloads`: `id TEXT PK`, `url`, `target_dir`, `filename NULL`, `workers INTEGER`, `piece_target_count NULL`, `piece_size_min NULL`, `piece_size_max NULL`, `state TEXT CHECK`, `attempt INTEGER`, `error NULL`, `created_at INTEGER`, `updated_at INTEGER` (unix-секунды) — все override-поля 3.2 персистятся, иначе после рестарта теряются
- [x] Прогресс (`bytes_done`, `pieces_done`, …) **не** хранится в `brook.db` — восстанавливается из `.index.brook` при старте engine
- [x] Версионирование схемы через `PRAGMA user_version` (как в piece-index)
- [x] Открытие БД: `journal_mode=WAL` + `synchronous=NORMAL`
- [x] `SqliteQueueRepository::open(path) -> Self` (создаёт/открывает + миграции)
- [x] `load_all` / `insert(download)` / `update_state(id, state)` / `remove(id)` — реализации трейта
- [x] `update_state` сам обновляет `updated_at = now()`
- [x] Все вызовы `rusqlite` через `tokio::task::spawn_blocking`
- [x] SQL-строки и `Connection` не покидают модуль — наружу только доменные методы `TQueueStore`
- [x] Юнит-тесты на каждый метод + round-trip insert → update_state → load_all → remove

### 4.2 `LocalPieceStorageFactory` (реализация `TPieceStorageFactory`)
Недостающее звено: `DownloadManager` требует фабрику, но сейчас есть только `LocalPieceStorage`.
- [x] `brookd/src/storage/factory.rs`: `LocalPieceStorageFactory { inspect: Arc<HttpInspectClient>, defaults: DownloadDefaults }`
- [x] `prepare(spec)`: `inspect.inspect(url)` → `effective_plan_config(spec, defaults)` → `plan_pieces` → `resolve_target` → `LocalPieceStorage::open` → `PreparedDownload`
- [x] Имя файла: приоритет `spec.filename` → `InspectReport.filename` → fallback из URL (логика уже в [HttpInspectClient](../crates/brook-http/src))
- [x] Политика `on_file_exists` (rename/overwrite/ask) в MVP — не здесь: фабрика ошибается `FileExists`, решение принимает слой выше (TUI-модалка, §6.6)
- [x] Юнит-тест: мок `THttpInspect` → проверка, что overrides из spec побеждают defaults
- [x] Интеграционный тест на `wiremock`: полный `prepare` с реальным `HttpInspectClient`

### 4.3 Бинарь `brookd`
- [x] `#[tokio::main]`; порядок старта: `.brook.lock` → tracing (stderr) → `brook.yaml` → `brook.db` + миграции → shared `reqwest::Client` (`HttpClientBuilder`) → `HttpInspectClient` + `RangeFetchClient` → `LocalPieceStorageFactory` → `DownloadManager::new` + `bootstrap()` → tonic `Server` с `BrookServiceServer` + `trace_interceptor`
- [x] Single-instance: `fs4::FileExt::try_lock_exclusive` на `.brook.lock` в CWD; вторая копия → exit 1 с понятным сообщением; `File` держится в `main` до exit
- [x] `Settings::load_or_init("./brook.yaml")` → `DaemonRuntime` + `DownloadDefaults`
- [x] `ManagerConfig.max_concurrent` = `runtime.max_concurrent`
- [x] Bind gRPC на `SocketAddr::new(runtime.api_bind, runtime.api_port)`
- [x] `session_id = xid::new()` → root-span `info_span!("brookd", %session_id)` оборачивает всё содержимое `main`
- [x] Tracing в этом этапе — минимальный stderr (JSON-форматтер + файловый sink вынесены в §7)

### 4.4 Graceful shutdown
- [x] Ожидание `SIGTERM` / `SIGINT` через `tokio::signal` + `select!` с `server.await`
- [x] `tonic::transport::Server::serve_with_shutdown(addr, shutdown_signal)` — перестаём принимать новые RPC при получении сигнала
- [x] `DownloadManager::shutdown(deadline: Duration)`: `pause_all` + дождаться `StateChanged(Paused)` / `Completed` / `Failed` / `Cancelled` от каждого активного engine через `subscribe()`; при таймауте (30 s) — drop handle'ов, логировать `warn`
- [x] После возврата `shutdown()`: engines сами делают финальный `commit_batch` перед эмитом `Paused` (инвариант из 1.9)
- [x] Закрыть `brook.db` (drop `SqliteQueueRepository`), затем flush tracing
- [x] `.brook.lock` освобождается автоматически при drop `File` на выходе из `main`
- [x] Интеграционный тест: старт brookd в tokio-тесте → `Add` живой загрузки (wiremock-источник) → shutdown-сигнал → рестарт → `load_all` возвращает запись в `Queued`, ресюм докачивает остаток

## 5. gRPC smoke-прогон через grpcurl
Ручная проверка контракта после того, как `brookd` начал слушать порт.
Бэкенд — реальные адаптеры (`SqliteQueueRepository` + `LocalPieceStorageFactory` + `brook-http`), источник — `https://httpbin.org/bytes/<N>`.
Рефлексия не включена — везде `-proto proto/brook/v1/brook.proto -import-path proto`.

Happy-path lifecycle:
- [x] `List` на пустом состоянии → `{}` (пустой `downloads`)
- [x] `Add` с валидным `spec` (url=httpbin, target_dir=/tmp, workers=2) → возвращает `DownloadId`
- [x] `List` после `Add` → элемент в состоянии `QUEUED`/`RUNNING`
- [x] `Watch` параллельно → initial `Snapshot` + `Progress`/`StateChanged`/`Completed`
- [x] `Pause` активного → `StatusResponse{ok:true}`, в `Watch` — `StateChanged(PAUSED)`
- [x] `Resume` → `StateChanged(RUNNING)`
- [x] `Cancel` → `StateChanged(CANCELLED)`, файл очищен
- [x] `Remove` после терминального состояния → успех, `List` снова пуст

Валидация и ошибки (маппинг в [mapper.rs](../crates/brook-api/src/mapper.rs)):
- [x] `Add` с пустым `url` → `InvalidArgument`
- [x] `Add` с пустым `target_dir` → `InvalidArgument`
- [x] `Pause`/`Resume`/`Cancel`/`Remove` без `id` → `InvalidArgument`
- [x] те же RPC с невалидным UUID (`"not-a-uuid"`) → `InvalidArgument`
- [x] `Pause` несуществующего UUID → `NotFound`
- [x] `Remove` активной загрузки → `FailedPrecondition` (сообщение про `active`)

Bulk-операции:
- [x] Запустить 3 загрузки → `PauseAll` → все уходят в `PAUSED`, `Watch` шлёт 3× `StateChanged`
- [x] `ResumeAll` → все возвращаются в `RUNNING`/`QUEUED`
- [x] `List` после bulk-операций → состояния консистентны

## 6. `brook-tui` (ratatui-клиент, бинарь `brook`)

### 6.1 Каркас клиента
- [ ] `main`: парсинг `--port` (дефолт 7090)
- [ ] `tonic::Channel` на `127.0.0.1:<port>`; при недоступности — сообщение + exit 1
- [ ] UI-loop `crossterm` + `ratatui`
- [ ] `q` закрывает клиент; демон не трогается

### 6.2 View-model и подписка
- [ ] Структура `ViewModel` с списком загрузок
- [ ] Фоновая задача: `Watch` → применяет события к `ViewModel` через канал
- [ ] Мутация `ViewModel` — только из этой задачи
- [ ] Сортировка: `RUNNING` → `RETRYING` → `QUEUED` → `PAUSED` → `DONE` → `FAILED` → `CANCELLED`, внутри — по `updated_at`

### 6.3 Рендер списка
- [ ] Колонки: имя, прогресс-бар, скорость, ETA, иконки `▶` / `❚❚` / `✓` / `✕`
- [ ] Статус-бар сверху: активные / очередь / суммарная скорость
- [ ] Хинт-бар снизу
- [ ] Форматирование байт (`1.5 MB`, `532 KB`)
- [ ] Форматирование ETA (`2h 15m`, `15m 30s`, `<1s`)
- [ ] `NO_COLOR` — рендер без ANSI-цветов
- [ ] Минимальный терминал 60×15 + заглушка ниже

### 6.4 Навигация и выбор
- [ ] `↑↓` и `jk` — курсор
- [ ] `gG` — в начало/конец
- [ ] `Enter` — разворот карточки (детали загрузки)
- [ ] `Space` — toggle select
- [ ] `Shift+↑↓` / `Shift+JK` — расширение выбора

### 6.5 Команды
- [ ] `a` — модалка добавления (prefill из clipboard)
- [ ] `p` — pause выбранных
- [ ] `r` — resume выбранных
- [ ] `c` — cancel с confirm-модалкой
- [ ] `o` — open в Finder
- [ ] `/` — фильтр по имени
- [ ] `Esc` — закрыть модалки/фильтр
- [ ] `?` — help overlay на один экран

### 6.6 Модалки конфликтов
- [ ] URL-дубликат в очереди (`ask` / `skip` / `add` по политике)
- [ ] Файл уже существует (`ask` / `rename` / `overwrite` по политике)
- [ ] Мышь игнорируется

## 7. Наблюдаемость
- [ ] `tracing-subscriber` с JSON-форматтером во всех крейтах
- [ ] `session_id` xid в root-span процесса
- [ ] `download_id` в span'ах core-операций над загрузкой
- [ ] `request_id` в span'ах gRPC-запросов
- [ ] File sink: `~/Library/Logs/brook/brook-<session_id>.jsonl`
- [ ] Stderr sink при запуске из терминала
- [ ] Ротация: 10 файлов × 50 MB, по размеру

## 8. Quality gate (ручная прогонка)
Сценарии из [open-questions.md](open-questions.md):
- [ ] Файл >1 GB + пауза + рестарт `brookd` → ресюм
- [ ] TUI переподключается к работающему демону после `q` и повторного запуска — состояние совпадает
- [ ] Отмена с очисткой частичного файла
- [ ] 10 задач одновременно — лимит параллельности соблюдается
- [ ] Потеря сети → автоматический ретрай → восстановление
- [ ] `brook` + `grpcurl` одновременно видят одно состояние
- [ ] Сервер без Range → fallback на 1 соединение
- [ ] `kill -9` посреди загрузки → рестарт → корректный ресюм
- [ ] Пиковый RSS при 10 параллельных укладывается в ≤ 150 MB
