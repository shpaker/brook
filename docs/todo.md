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
- [ ] Новый модуль `crates/brookd/src/storage/db.rs`: `SharedDb`
      с `open(path)`, `open_in_memory()`, `with_conn(f)` (через
      `spawn_blocking`)
- [ ] `open` применяет PRAGMA `journal_mode=WAL`,
      `synchronous=NORMAL`, `foreign_keys=ON`
- [ ] Миграция `user_version = 1` создаёт всю схему
      из [schema.dbml](schema.dbml) в одной транзакции
- [ ] Сид `states` и `reason_codes` через `INSERT OR IGNORE`
      с фиксированными UUID'ами
- [ ] Юнит-тесты: `open` идемпотентен, сид не дублируется

### 2. Расширение портов `brook-core`
- [ ] `TQueueStore::update_state(id, state, reason: Option<FailureReason>)` —
      reason обязателен при переходе в `failed`
- [ ] Доменный тип `FailureReason { code: ReasonCode, message: Option<String> }`
      + enum `ReasonCode` (по sideset'у из `reason_codes`)
- [ ] `TPieceStorageFactory::prepare(id: DownloadId, spec: &DownloadSpec)` —
      фабрика получает идентификатор, чтобы связать persisted state
      с piece-слоем
- [ ] Обновить in-memory реализации (`MemoryTQueueStore`,
      mock-фабрики) под новые сигнатуры
- [ ] Обновить все call-site'ы `update_state` в `service/manager.rs`
      и `service/engine.rs` — пробрасывать `FailureReason` на переходах
      в `failed`

### 3. `SqliteFileRepository` (переименование `queue.rs` → `files.rs`)
- [ ] Репозиторий живёт на `SharedDb`
- [ ] `insert(&Download)`: в одной транзакции INSERT `files`
      (`state_id` = начальный) + INSERT `file_settings` (inspect-поля NULL)
      + INSERT `state_changes` (initial transition)
- [ ] `update_state(id, state, reason?)`: транзакция UPDATE `files.state_id`
      + INSERT `state_changes` (с `reason_code_id`/`reason_message`, если
      есть). Откат при любой ошибке.
- [ ] `set_inspect_fields(id, total_size, piece_size, etag, last_modified)` —
      UPDATE `file_settings`. Внутренний (не в трейте).
- [ ] `get_inspect_fields(id)` — SELECT. Внутренний.
- [ ] `load_all`: JOIN `files` + `file_settings`, мапит в `Download`
- [ ] `remove(id)`: DELETE `files` — каскад в `file_settings`,
      `state_changes`, `pieces`
- [ ] Юнит-тесты: roundtrip insert/load/update_state/remove,
      `update_state(failed, reason)` атомарно пишет state + reason,
      cascade-delete, `set/get_inspect_fields`

### 4. `SqlitePieceRepository` (переименование `index.rs` → `pieces.rs`)
- [ ] API по номерам (`u32`), а не по `PieceLayout`:
      `init(file_id, count)`, `pending_numbers(file_id)`,
      `commit_done_batch(file_id, numbers)`, `is_initialized(file_id)`,
      `delete_all(file_id)`
- [ ] Статус в БД — `pending` / `done`; `in_progress` не персистится
- [ ] Юнит-тесты: init + pending + commit + resume на in-memory
      `SharedDb`; две загрузки не пересекаются
      (`pending_numbers(a)` не видит piece'ов `b`)

### 5. `LocalPieceStorage` — без sidecar
- [ ] Убрать `index_path`, `remove_index_files`, `Inner.index`
- [ ] Держит `Arc<SqlitePieceRepository>`, `Arc<SqliteFileRepository>`,
      `file_id`, `piece_size`, `total_size`
- [ ] `offset_for(number)` / `size_for(number)` — локальные хелперы
      (геометрия арифметикой)
- [ ] `open(id, ...)`: resume, если `get_inspect_fields` совпадает
      с текущими `total_size`/`piece_size` + `is_initialized`
      + `.data.brook` существует; иначе fresh (стереть `.data.brook`,
      `delete_all(id)`, `set_inspect_fields`, `init`, `preallocate`)
- [ ] `commit_batch(numbers)`: `sync_data()` **до**
      `commit_done_batch(id, numbers)`
- [ ] `finalize`: `sync_all` → `rename .data.brook → target`
      → `delete_all(id)`; переход в `done` делает engine
      через `TQueueStore::update_state`
- [ ] `abort`: `delete_all(id)` + `remove_file(.data.brook)`
- [ ] Обновить юнит-тесты под новые конструкторы и shared db

### 6. `LocalPieceStorageFactory`
- [ ] `new` принимает дополнительно `Arc<SqlitePieceRepository>`
      и `Arc<SqliteFileRepository>`
- [ ] `prepare(id, spec)`: после `inspect` пишет результат
      в `file_settings` (`set_inspect_fields`), затем открывает
      `LocalPieceStorage` с этими данными
- [ ] Юнит-тесты на фабрику: inspect-поля персистятся в `file_settings`,
      override'ы `on_file_exists` продолжают работать

### 7. Сборка в `brookd`
- [ ] `build_runtime` в `crates/brookd/src/app.rs`:
      `SharedDb::open(&paths.db)` → `file_repo` + `piece_repo`
      → `LocalPieceStorageFactory::new(inspect, defaults, piece_repo, file_repo)`
      → `DownloadManager::new(factory, file_repo as Arc<dyn TQueueStore>, fetch, cfg)`
- [ ] Убрать ссылки на `.index.brook` в коде и логах

### 8. Документация
- [ ] `docs/architecture.md`: «Раскладка на диске» и «Ресюм и сбои»
      — убрать двух-файловую логику; добавить раздел «Схема `brook.db`»
      со ссылкой на [schema.dbml](schema.dbml)
- [ ] `docs/features.md`: обновить пункт про преаллокацию и индекс

### 9. Verification
- [ ] `just fmt-check && just clippy && just test`
- [ ] Unit: `remove(file_id)` каскадно удаляет `file_settings`,
      `state_changes`, `pieces`
- [ ] Unit: две параллельные загрузки через shared db не пересекаются
- [ ] Unit: `update_state(failed, reason)` атомарно пишет
      `files.state_id` + `state_changes.reason_code_id` + `reason_message`
- [ ] Unit: ошибка INSERT `state_changes` откатывает UPDATE `files.state_id`
- [ ] E2E: `just run-d` в tempdir → добавить загрузку → SIGTERM посреди
      → в целевой папке только `<name>.data.brook`; в `brook.db` строки
      `pieces` со `status='pending'` и `state_changes` с переходами
      `queued → running → paused`
- [ ] E2E: рестарт → дождаться `done` → `.data.brook` исчез,
      таргет на месте, `pieces` для `file_id` пусты, `state_changes`
      содержит финальный переход
- [ ] `cargo tree -p brook-core` не тянет `rusqlite`
