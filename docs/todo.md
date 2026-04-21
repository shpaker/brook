# TODO — миграция на единую `brook.db`

Переводим всё персистентное состояние загрузок в `./brook.db` и выбрасываем
sidecar-индекс `<name>.index.brook`. Рядом с таргетом остаётся только
`<name>.data.brook` (после `finalize` → `<name>`).

Финальная схема — [schema.dbml](schema.dbml). Продукт ещё не запущен,
обратную совместимость не держим: `user_version = 1` создаёт всю схему
с нуля и сидит справочники `states` / `reason_codes`.

## Цели

- Единственный файл БД (`./brook.db`).
- Рядом с таргетом — только `<name>.data.brook`.
- Схема пригодна для будущей статистики (время в каждом state без учёта
  пауз, скорость по `pieces.finished_at`, причины отказов без парсинга
  строк).
- `brook-core` по-прежнему видит piece-слой только через `TPieceStorage`;
  SQL не утекает из `brookd`.

## Инварианты приложения (SQLite `CHECK`-ом не выразить)

- `state_changes.reason_code_id IS NOT NULL`, когда переход ведёт
  в `failed`.
- `file_settings.total_size` и `piece_size` — NOT NULL до перехода
  файла в `running` (ставятся в `prepare`).
- `in_progress` — runtime-состояние engine, на диск не персистится.
  При рестарте любой piece не в `done` трактуется как `pending`.

## Этапы

### 1. `SharedDb` — общее соединение
- [x] Новый модуль `crates/brookd/src/storage/db.rs`: `SharedDb`
      с `open(path)`, `open_in_memory()`, `with_conn(f)` (через
      `spawn_blocking`)
- [x] `open` применяет PRAGMA `journal_mode=WAL`,
      `synchronous=NORMAL`, `foreign_keys=ON`
- [x] Миграция `user_version = 1` создаёт всю схему
      из [schema.dbml](schema.dbml) в одной транзакции
- [x] Сид `states` и `reason_codes` через `INSERT OR IGNORE`
      (natural-key: `states(name)`, `reason_codes(code)` —
      суррогатные UUID'ы выкинули как лишний уровень косвенности;
      [schema.dbml](schema.dbml) обновлён)
- [x] Юнит-тесты: `open` идемпотентен, сид не дублируется,
      `foreign_keys=ON`, каскадное удаление, `with_conn`
      выполняет замыкание

### 2. Расширение портов `brook-core`
- [x] `TQueueStore::update_state(id, state, reason: Option<FailureReason>)` —
      reason обязателен при переходе в `failed`
- [x] Доменный тип `FailureReason { code: ReasonCode, message: Option<String> }`
      + enum `ReasonCode` (по sideset'у из `reason_codes`;
      natural-key строки совпадают с PK справочника в `brook.db`)
- [x] `TPieceStorageFactory::prepare(id: DownloadId, spec: &DownloadSpec)` —
      фабрика получает идентификатор, чтобы связать persisted state
      с piece-слоем (используется в stage 4; stage 2 пока прокидывает
      как `_id`)
- [x] Обновить in-memory реализации (`MemoryTQueueStore`,
      mock-фабрики) под новые сигнатуры
- [x] Обновить все call-site'ы `update_state` в `service/manager.rs`
      и `service/engine.rs` — пробрасывать `FailureReason` на переходах
      в `failed` (engine не персистит сам — reason строится в
      `fan_in_events` из `DownloadEvent::Failed`; `cancel` → `CancelledByUser`)

### 3. `SqliteFileRepository` (переименование `queue.rs` → `files.rs`)
- [x] Репозиторий живёт на `SharedDb`
- [x] `insert(&Download)`: в одной транзакции INSERT `files`
      (`state_id` = начальный) + INSERT `file_settings` (inspect-поля NULL)
      + INSERT `state_changes` (initial transition)
- [x] `update_state(id, state, reason?)`: транзакция UPDATE `files.state_id`
      + INSERT `state_changes` (с `reason_code_id`/`reason_message`, если
      есть). Откат при любой ошибке. Инвариант «failed ⇒ reason» проверяется
      до SQL и возвращает `Error::Other`.
- [x] `set_inspect_fields(id, total_size, piece_size, etag, last_modified)` —
      UPDATE `file_settings`. Внутренний (не в трейте).
- [x] `get_inspect_fields(id)` — SELECT. Внутренний. Возвращает `InspectFields`
      (DTO адаптерного слоя).
- [x] `load_all`: JOIN `files` + `file_settings` + последний
      `state_changes` per file → `Download` (`updated_at` из истории,
      `error` — `reason_message` последней строки при `state = failed`)
- [x] `remove(id)`: DELETE `files` — каскад в `file_settings`,
      `state_changes`, `pieces`
- [x] Юнит-тесты: roundtrip insert/load/update_state/remove,
      `update_state(failed, reason)` атомарно пишет state + reason,
      cascade-delete, `set/get_inspect_fields`, rollback при FK-ошибке
      в `state_changes`, reopen сохраняет override-поля

### 4. `SqlitePieceRepository` (новый модуль `pieces.rs`; `index.rs` уйдёт в stage 5 вместе с sidecar-веткой `LocalPieceStorage`)
- [x] API по номерам (`u32`), а не по `PieceLayout`:
      `init(file_id, count)`, `pending_numbers(file_id)`,
      `commit_done_batch(file_id, numbers)`, `is_initialized(file_id)`,
      `delete_all(file_id)`
- [x] Статус в БД — `pending` / `done`; `in_progress` не персистится
- [x] Юнит-тесты: init + pending + commit + resume на in-memory
      `SharedDb`; две загрузки не пересекаются
      (`pending_numbers(a)` не видит piece'ов `b`)

### 5. `LocalPieceStorage` — без sidecar
- [x] Убрать `index_path`, `remove_index_files`, `Inner.index`;
      файл `storage/index.rs` удалён, `storage/mod.rs` обновлён
- [x] Держит `Arc<SqlitePieceRepository>`, `Arc<SqliteFileRepository>`,
      `file_id`, `piece_size`, `total_size`
- [x] `offset_for(number)` / `size_for(number)` — локальные хелперы
      (геометрия арифметикой), `piece_count = total_size.div_ceil(piece_size)`
- [x] `open(id, ...)`: resume, если inspect-поля совпадают
      + **число строк в `pieces` равно ожидаемому `piece_count`**
      + `.data.brook` существует; иначе fresh (`delete_all(id)`,
      пересоздать `.data.brook`, `preallocate`, `init(count)`).
      `set_inspect_fields` теперь вызывает фабрика (stage 6) до
      `open` — самому хранилищу нет смысла дублировать. Сторож
      «count == expected» нужен потому, что фабрика пишет inspect
      ДО `open`: иначе старые piece-строки совпадали бы с уже
      обновлёнными inspect-полями и резюмировали по чужой нарезке
- [x] `commit_done(number)`: `sync_data()` **до**
      `commit_done(id, number)` в репозитории
- [x] `finalize`: `sync_all` → `rename .data.brook → target`
      → `delete_all(id)`; переход в `done` делает engine
      через `TQueueStore::update_state`
- [x] `abort`: `delete_all(id)` + `remove_file(.data.brook)`
- [x] Юнит-тесты переписаны под новые конструкторы и shared db
      (round-trip, resume, abort, mismatched inspect → fresh,
      write past piece end, reopen без finalize, write after finalize)
- [x] Добавлен `SqlitePieceRepository::count(file_id)` — нужен
      сторожу нарезки в `open`

### 6. `LocalPieceStorageFactory`
- [x] `new` принимает дополнительно `Arc<SqlitePieceRepository>`
      и `Arc<SqliteFileRepository>`
- [x] `prepare(id, spec)`: после `inspect` пишет inspect-поля
      в `file_settings` (`set_inspect_fields`) и затем открывает
      `LocalPieceStorage`. `plan_pieces` остался только ради `piece_size`
      (раскладка-в-векторе больше никому не нужна, дропаем)
- [x] `app::build_runtime` собирает `pieces_repo` рядом с `files_repo`
      и пробрасывает оба в фабрику
- [x] Юнит-тесты: `prepare_persists_inspect_fields` (etag/last_modified
      доезжают до `file_settings`), все прежние сценарии
      `on_file_exists` (Unspecified/Rename/Overwrite/absent) сохранены

### 7. Сборка в `brookd`
- [x] `build_runtime` в `crates/brookd/src/app.rs`:
      `SharedDb::open(&paths.db)` → `file_repo` + `piece_repo`
      → `LocalPieceStorageFactory::new(inspect, defaults, piece_repo, file_repo)`
      → `DownloadManager::new(factory, file_repo as Arc<dyn TQueueStore>, fetch, cfg)`
- [x] Убрать ссылки на `.index.brook` в коде и логах
      (порт `TPieceStorage` переписан под piece-номера без
      упоминания sidecar; module-docs в `pieces.rs`/`local.rs`
      оставлены как исторический контекст перехода)

### 8. Документация
- [x] `docs/architecture.md`: «Раскладка на диске» и «Ресюм и сбои»
      — убрали двух-файловую логику; добавлен раздел
      «Схема `brook.db`» со ссылкой на [schema.dbml](schema.dbml)
- [x] `docs/features.md`: обновлён пункт про преаллокацию и индекс
      + семантика `Cancel` (piece-строки в `brook.db`, без sidecar)
- [x] `docs/api.md`: `Pause`/`Resume`/`Cancel` — переведены
      с `.index.brook` на piece-строки в `brook.db`
- [x] `CLAUDE.md`: «Runtime artifacts» + «DB access» — без sidecar

### 9. Verification
- [x] `just fmt-check && just clippy && just test`
- [x] Unit: `remove(file_id)` каскадно удаляет `file_settings`,
      `state_changes`, `pieces`
      (`files.rs::remove_cascades_to_children`)
- [x] Unit: две параллельные загрузки через shared db не пересекаются
      (`pieces.rs::two_downloads_do_not_interfere`)
- [x] Unit: `update_state(failed, reason)` атомарно пишет
      `files.state_id` + `state_changes.reason_code_id` + `reason_message`
      (`files.rs::update_state_failed_writes_reason_atomically`)
- [x] Unit: ошибка INSERT `state_changes` откатывает UPDATE `files.state_id`
      (`files.rs::update_state_rolls_back_on_state_changes_failure`)
- [x] E2E: `brookd` в tempdir → добавить загрузку → shutdown посреди
      → рядом с таргетом только `<name>.data.brook` (либо уже
      финализированный таргет), sidecar никогда не появляется;
      в `brook.db` есть piece-строки и хотя бы один переход
      в `state_changes` (`tests/shutdown.rs` round 1)
- [x] E2E: рестарт → дождаться `done` → `.data.brook` исчез,
      таргет на месте, piece-строки для `file_id` пусты, последняя
      запись `state_changes` — `done` (`tests/shutdown.rs` round 2)
- [x] `cargo tree -p brook-core` не тянет `rusqlite`

## Единый словарь статусов + персистентная идентичность воркера

- [x] Переименовать `states` → `statuses`, колонки `state_id` → `status_id`
      во всей схеме и во всех репозиториях; `pieces.status` тоже
      становится `status_id` c FK на `statuses.name`.
- [x] Proto: `DownloadState` → `DownloadStatus`, `QUEUED` → `PENDING`,
      поле `state` → `status`; `StateChangedEvent` → `StatusChangedEvent`.
      Переименование прошито через все крейты workspace'а.
- [x] Rust-enum'ы `FileStatus` / `WorkerStatus` / `PieceStatus` /
      `AttemptStatus` в `brook-core/src/domain/status.rs`; сериализация
      совпадает с natural-key в БД, компилятор страхует от
      «чужого» статуса.
- [x] Новые таблицы `workers` и `piece_attempts` (см. `schema.dbml`):
      per-сессионная identity воркера, журнал попыток piece'а.
- [x] Порты `TWorkerRepo` / `TPieceAttemptRepo` в `brook-core/ports`;
      SQLite-реализации `SqliteWorkerRepository` /
      `SqlitePieceAttemptRepository` в `brookd/storage`, зарегистрированы
      через mod.rs и покрыты unit-тестами (ensure_slots pause-sweep,
      глобальный recovery).
- [x] Startup recovery в `brookd::app::build_runtime`: под `.brook.lock`
      ровно один раз зовём `pause_all_running_globally` у обоих
      репозиториев до bootstrap'а менеджера.
- [x] `workers_repo` / `attempts_repo` прошиты через `DownloadManager`
      и `DownloadEngine`: `ensure_slots` на старте engine-сессии,
      `start` / `finish` / `fail` на каждый piece (через `worker_tx`
      в супервизор, чтобы не звать БД из горячего пути воркера),
      `mark_done` / `mark_failed` / `mark_cancelled` на финализации.
      Manager получил дефолтные generic-параметры `NoopWorkerRepo` /
      `NoopAttemptRepo`, чтобы тесты ядра и in-memory harness'ы
      оставались без изменений; brookd собирает `DownloadManager`
      через `with_tracking` с SQLite-репозиториями.
- [x] Engine-тесты покрывают: две сессии на один файл (первый набор
      воркеров → paused, второй → done); crash-recovery sweep перед
      новой сессией; piece retry создаёт вторую attempt-строку; cancel
      переводит worker-строки в cancelled и закрывает running-попытки;
      смена `max_workers` между сессиями (4 → 2) даёт свежий набор
      со `slot_index` 0, 1. In-memory `MemoryWorkerRepo` /
      `MemoryAttemptRepo` добавлены в `brook-core::testing`.
