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
- [x] Структура `DownloadSpec` (url, target_dir, filename, headers, workers)
- [x] Структура `Progress` (bytes_done, bytes_total, pieces_done, pieces_total, speed_bps, eta_secs)
- [x] Структура `Download` (id, spec, state, progress, attempt, error, timestamps)
- [x] Enum `DownloadCommand` (`Pause`, `Resume`, `Cancel`)
- [x] Enum `DownloadEvent` (`Progress`, `StateChanged`, `WorkerUpdate`, `Completed`, `Failed`, `Snapshot`)
- [x] Юнит-тесты: сериализация/дефолты/конверсии

### 1.2 Трейты абстракций (без реализаций)
- [x] `TPieceStorage`: `write_piece_bytes`, `commit_batch`, `pending_pieces`, `finalize`, `abort`
- [x] `TPieceStorageFactory`: `create(spec) -> impl TPieceStorage`
- [x] `TQueueStore`: `load_all`, `insert`, `update_state`, `remove`
- [x] Документирующие doc-комментарии инвариантов (commit ⇒ persisted)

### 1.3 In-memory реализации для тестов
- [ ] `MemoryPieceStorage` (test-utils, feature `test-utils`)
- [ ] `MemoryTQueueStore` (test-utils)
- [ ] Юнит-тест: round-trip через `MemoryPieceStorage`
- [ ] Юнит-тест: round-trip через `MemoryTQueueStore`

### 1.4 HTTP-контракт (`HttpProbe`)
- [ ] Подключить `reqwest` + `rustls` в `brook-core`
- [ ] `HttpProbe::head`: парсинг `Content-Length`, `Accept-Ranges`, `ETag`, `Last-Modified`, `Content-Disposition`
- [ ] Fallback: `HEAD` → 4xx/5xx → `GET Range: bytes=0-0`, размер из `Content-Range`
- [ ] Имя файла: `Content-Disposition filename*=` → `filename=` → последний сегмент URL
- [ ] Connect timeout 10 s
- [ ] Read (idle) timeout 30 s без новых байт в теле
- [ ] Тесты (`wiremock`): HEAD-ok, HEAD-fail+GET-range-ok, no-Range, Content-Disposition

### 1.5 Range-запрос и валидация
- [ ] `HttpFetcher::fetch_range(url, offset, len, guard) -> stream`
- [ ] Валидация: код `206` + `Content-Range: bytes X-Y/TOTAL` совпадает с запрошенным
- [ ] `200 OK` на Range-запрос → сигнал «Range-неспособен» для fallback
- [ ] Guard мутации: `If-Match: <etag>` (или `If-Unmodified-Since`) в каждом Range-запросе
- [ ] `412 Precondition Failed` → типизированная ошибка `SourceMutated`
- [ ] Проверка: принято ровно `piece_size` байт, иначе `TruncatedResponse`
- [ ] Тесты: `206`-ok, `200`-на-Range, `412`, усечённое тело

### 1.6 Retry-политика
- [ ] `RetryPolicy`: экспо-бэкофф `1s × 2^attempt` + jitter ±20 %, max delay 60 s, max 10 попыток
- [ ] Crash-loop guard: 5 одинаковых ошибок подряд → `FAILED`
- [ ] Юнит-тесты на расчёт задержек и trigger crash-loop

### 1.7 Пре-аллокация и нарезка (`LocalPieceStorage`)
- [ ] Крейт-локация: `LocalPieceStorage` в `brookd` (реализация `TPieceStorage`)
- [ ] `statvfs`-проверка свободного места на целевой ФС
- [ ] `F_PREALLOCATE` + `ftruncate` для `<filename>.data.brook`
- [ ] Path-traversal защита: целевой путь под `default_dir` или абсолютный
- [ ] Выбор `piece_size` 1–4 MB по общему размеру файла
- [ ] Расчёт offset'ов и числа кусков
- [ ] Тест: пре-аллокация 100 MB файла, проверка размера

### 1.8 `pwrite`/`read` обёртки
- [ ] `pwrite_full`: loop до полного слива, `EINTR`-safe
- [ ] `read_full`: аналогично
- [ ] Прокидывает только реальные ошибки (`ENOSPC`, `EIO`)
- [ ] Юнит-тест на частичную запись (мок)

### 1.9 Piece index — `PieceIndexRepository`
- [ ] Миграция: `pieces(idx INTEGER PK, offset, size, status TEXT CHECK)` + `meta(key, value)`
- [ ] SQLite WAL + `synchronous=NORMAL` при открытии `.index.brook`
- [ ] `PieceIndexRepository::open(path) -> Self` (создаёт/открывает)
- [ ] `PieceIndexRepository::init(spec, pieces)` — первая запись
- [ ] `PieceIndexRepository::pending_pieces() -> Vec<PieceRef>`
- [ ] `PieceIndexRepository::commit_done_batch(ids)` — UPDATE + транзакция
- [ ] `PieceIndexRepository::meta_get/set` (url, etag, total_size, piece_size)
- [ ] `PieceIndexRepository::delete_all` (для abort)
- [ ] SQL-строки и `rusqlite::Connection` живут только внутри этого модуля
- [ ] Юнит-тесты на каждый метод

### 1.10 `LocalPieceStorage` поверх репозитория
- [ ] `LocalPieceStorage::new(spec)` открывает `.data.brook` (`pwrite`-handle) + `PieceIndexRepository`
- [ ] `write_piece_bytes`: `pwrite_full` по offset'у куска
- [ ] `commit_batch`: `fsync(.data)` → `PieceIndexRepository::commit_done_batch`
- [ ] `pending_pieces`: делегирует в репозиторий
- [ ] `finalize`: `fsync` → `rename .data.brook → <filename>` → удалить `.index.brook`
- [ ] `abort`: удалить `.data.brook` и `.index.brook`
- [ ] Сбои: `.index` или `.data` отсутствует/битый → стартовать с нуля
- [ ] Интеграционный тест на полный цикл init → write → commit → finalize

### 1.11 `DownloadEngine` — скелет
- [ ] Структура `DownloadEngine<S: TPieceStorage>` с mpsc команд и broadcast событий
- [ ] `spawn(spec, storage) -> (handle, events_rx)`
- [ ] Обработка `Pause` / `Resume` / `Cancel`
- [ ] Юнит-тест: команды меняют state, эмитят `StateChanged`

### 1.12 `DownloadEngine` — воркеры
- [ ] Общий atomic-счётчик «следующий `pending` piece» (work-stealing)
- [ ] Спаун N воркеров по `spec.workers`
- [ ] Воркер: берёт piece → Range-запрос → потоковый `write_piece_bytes` буфером 64–256 KB
- [ ] Проверка полноты куска → либо `done`, либо обратно в `pending`
- [ ] Батч-коммит каждые 16 кусков → `commit_batch`
- [ ] Коммит на `pause` / `shutdown` / перед `finalize`
- [ ] Fallback: сервер без Range → один воркер без кусков, до EOF
- [ ] Тесты с `wiremock`: нормальная загрузка, обрыв посреди куска, 500+retry

### 1.13 `DownloadEngine` — события
- [ ] Агрегация счётчиков в таймере 200 ms → один `Progress` за окно
- [ ] State-changes — мгновенный эмит, без таймера
- [ ] `Completed` после успешного `finalize`
- [ ] `Failed(reason)` на терминальной ошибке
- [ ] Юнит-тест: частота `Progress` ≤ 5 Hz

### 1.14 `DownloadManager`
- [ ] Структура `DownloadManager` с реестром engines по `DownloadId`
- [ ] Принимает `TPieceStorageFactory` + `TQueueStore` в конструкторе
- [ ] `add(spec)` — insert в queue-store, спаун engine при наличии слота
- [ ] `pause(id)` / `resume(id)` / `cancel(id)` / `remove(id)` — роутинг в нужный engine
- [ ] `pause_all` / `resume_all`
- [ ] `max_concurrent` — не спаунить engine сверх лимита, держать в `QUEUED`
- [ ] Центральный `broadcast::Sender<Event>` ring 1024 — fan-in от всех engines
- [ ] При старте: `TQueueStore::load_all` → восстановить реестр, запустить по лимиту
- [ ] Snapshot по запросу (для Watch-реконсиляции)
- [ ] Интеграционный тест: 3 engines, `max_concurrent=2`, очередь соблюдается

### 1.15 Тесты `brook-core`
- [ ] Fault-injection: обрыв на полуслове, `500` с ретраем, смена `ETag` → `FAILED`
- [ ] Отсутствие `Content-Length` → fallback-режим
- [ ] Пиковый RSS ≤ 150 MB при 10 параллельных engines (отдельный perf-тест, `ignored`)

## 2. `brook-proto` + `brook-api`

### 2.1 Proto-контракт
- [ ] Сервис и сообщения: `List`, `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `Watch`
- [ ] Событие `Event` с oneof (`progress`, `state_changed`, `worker_update`, `completed`, `failed`, `snapshot`)
- [ ] `protolint` чистый
- [ ] `cargo build -p brook-proto` генерирует без warning'ов

### 2.2 Тонкая обёртка API
- [ ] `BrookService` — реализация tonic-сервиса поверх `DownloadManager`
- [ ] Мапперы `proto ↔ core` в отдельном модуле (без бизнес-логики)
- [ ] Unary: `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `List`
- [ ] Юнит-тесты на мапперы

### 2.3 Watch-стрим
- [ ] Per-client fanout-задача: подписка на центральный `broadcast` + tonic tx
- [ ] Initial snapshots при коннекте — по одному `Event::Snapshot` на каждую известную загрузку
- [ ] `tx.send().await` — транспортный backpressure
- [ ] `RecvError::Lagged(n)` → запросить у `DownloadManager` snapshot'ы активных и дослать
- [ ] Интеграционный тест: fast producer + slow consumer → клиент догоняется

### 2.4 Транспорт и трейсинг
- [ ] Bind `127.0.0.1:<port из settings>`
- [ ] Metadata: `session_id`, `request_id`, при наличии — `download_id`
- [ ] Tracing: root-span на запрос, наследование в core
- [ ] Интеграционный тест: client ↔ server в одном процессе, все методы

## 3. Конфигурация (`settings` в `brook.db`)

### 3.1 `SettingsRepository`
- [ ] Миграция: `CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`
- [ ] `SettingsRepository::open(conn)` — open/migrate
- [ ] `SettingsRepository::seed_defaults` — вставка дефолтов при пустой таблице
- [ ] `SettingsRepository::load_all -> HashMap<String, String>`
- [ ] SQL-строки и `Connection` не покидают модуль
- [ ] Юнит-тесты на миграцию, seed, load

### 3.2 Типизация и применение
- [ ] Структура `Settings` с типизированными полями (ключи — [architecture.md#конфигурация](architecture.md#конфигурация))
- [ ] Парсер: `HashMap<String, String> → Result<Settings, InvalidKeyError>`
- [ ] Invalid value → ошибка старта `brookd` с именем ключа в сообщении
- [ ] Unknown key → `tracing::warn!` и игнор
- [ ] `Settings` передаётся в `DownloadManager` и `brook-api` при старте
- [ ] Юнит-тест: все дефолты парсятся валидно

## 4. `brookd` (демон)

### 4.1 `SqliteQueueRepository` (реализация `TQueueStore`)
- [ ] Миграция: `downloads(id, url, target_dir, filename, state, created_at, updated_at, ...)`
- [ ] `SqliteQueueRepository::load_all`
- [ ] `SqliteQueueRepository::insert(download)`
- [ ] `SqliteQueueRepository::update_state(id, state, updated_at)`
- [ ] `SqliteQueueRepository::remove(id)`
- [ ] SQL-строки и `Connection` не покидают модуль
- [ ] Юнит-тесты на каждый метод

### 4.2 Бинарь
- [ ] `tokio::main`; порядок: lock → БД → миграции → settings → core → api
- [ ] Single-instance: `flock(LOCK_EX | LOCK_NB)` на `.brook.lock` в CWD; вторая копия → exit 1 с сообщением
- [ ] Открытие `./brook.db` + прогон миграций (settings, downloads)
- [ ] `brookd` передаёт `SqliteQueueRepository` и `LocalPieceStorageFactory` в `DownloadManager`
- [ ] Запуск `DownloadManager` + gRPC-сервера как двух `tokio::spawn`-задач
- [ ] `session_id` xid — root-span

### 4.3 Graceful shutdown
- [ ] Сигналы: `SIGTERM`, `SIGINT`
- [ ] Шаг 1: перестать принимать новые gRPC-запросы
- [ ] Шаг 2: `pause_all` engines — каждый доводит in-flight до batch-границы
- [ ] Шаг 3: финальный `commit_batch` для активных загрузок
- [ ] Шаг 4: закрыть gRPC-сервер, `brook.db`, лог-writer
- [ ] Шаг 5: отпустить `.brook.lock`, exit 0
- [ ] Интеграционный тест: SIGTERM на живой загрузке → ресюм после рестарта

## 5. `brook-tui` (ratatui-клиент, бинарь `brook`)

### 5.1 Каркас клиента
- [ ] `main`: парсинг `--port` (дефолт 7090)
- [ ] `tonic::Channel` на `127.0.0.1:<port>`; при недоступности — сообщение + exit 1
- [ ] UI-loop `crossterm` + `ratatui`
- [ ] `q` закрывает клиент; демон не трогается

### 5.2 View-model и подписка
- [ ] Структура `ViewModel` с списком загрузок
- [ ] Фоновая задача: `Watch` → применяет события к `ViewModel` через канал
- [ ] Мутация `ViewModel` — только из этой задачи
- [ ] Сортировка: `RUNNING` → `RETRYING` → `QUEUED` → `PAUSED` → `DONE` → `FAILED` → `CANCELLED`, внутри — по `updated_at`

### 5.3 Рендер списка
- [ ] Колонки: имя, прогресс-бар, скорость, ETA, иконки `▶` / `❚❚` / `✓` / `✕`
- [ ] Статус-бар сверху: активные / очередь / суммарная скорость
- [ ] Хинт-бар снизу
- [ ] Форматирование байт (`1.5 MB`, `532 KB`)
- [ ] Форматирование ETA (`2h 15m`, `15m 30s`, `<1s`)
- [ ] `NO_COLOR` — рендер без ANSI-цветов
- [ ] Минимальный терминал 60×15 + заглушка ниже

### 5.4 Навигация и выбор
- [ ] `↑↓` и `jk` — курсор
- [ ] `gG` — в начало/конец
- [ ] `Enter` — разворот карточки (детали загрузки)
- [ ] `Space` — toggle select
- [ ] `Shift+↑↓` / `Shift+JK` — расширение выбора

### 5.5 Команды
- [ ] `a` — модалка добавления (prefill из clipboard)
- [ ] `p` — pause выбранных
- [ ] `r` — resume выбранных
- [ ] `c` — cancel с confirm-модалкой
- [ ] `o` — open в Finder
- [ ] `/` — фильтр по имени
- [ ] `Esc` — закрыть модалки/фильтр
- [ ] `?` — help overlay на один экран

### 5.6 Модалки конфликтов
- [ ] URL-дубликат в очереди (`ask` / `skip` / `add` по политике)
- [ ] Файл уже существует (`ask` / `rename` / `overwrite` по политике)
- [ ] Мышь игнорируется

## 6. Наблюдаемость
- [ ] `tracing-subscriber` с JSON-форматтером во всех крейтах
- [ ] `session_id` xid в root-span процесса
- [ ] `download_id` в span'ах core-операций над загрузкой
- [ ] `request_id` в span'ах gRPC-запросов
- [ ] File sink: `~/Library/Logs/brook/brook-<session_id>.jsonl`
- [ ] Stderr sink при запуске из терминала
- [ ] Ротация: 10 файлов × 50 MB, по размеру

## 7. Quality gate (ручная прогонка)
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
