# TODO — MVP

Порядок фиксирован ([roadmap.md](roadmap.md)). Не идти дальше, пока текущий этап не закрыт.

## 0. Workspace
- [x] Cargo workspace + 4 крейта: `brook-proto`, `brook-core`, `brook-api`, `brook`
- [ ] Editorsconfig для rust/markdown/toml
- [ ] prec config
- [x] `proto/brook/v1/brook.proto` — скелет из [api.md](api.md)
- [x] `brook-proto/build.rs` → `tonic-build`
- [x] CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`

## 1. `brook-core`

### HTTP-контракт
- [ ] `reqwest` + `rustls`
- [ ] HEAD → `Content-Length`, `Accept-Ranges`, `ETag`/`Last-Modified`, `Content-Disposition`
- [ ] HEAD fallback → `GET Range: bytes=0-0`
- [ ] Валидация Range-ответа: `206` + корректный `Content-Range` (иначе fallback на один сегмент)
- [ ] Guard мутации: `If-Match` / `If-Unmodified-Since` на каждый Range; `412` → `FAILED`
- [ ] Имя файла: `Content-Disposition` → URL-сегмент → промпт
- [ ] Таймауты: connect 10 s, read (idle) 30 s
- [ ] Retry: экспо-бэкофф + jitter, max 10 попыток, max delay 60 s
- [ ] Crash-loop guard: 5 одинаковых ошибок подряд → `FAILED`

### Диск и индекс
- [ ] Типы: `DownloadId`, `DownloadSpec`, `DownloadState`, `Progress`, `Download`
- [ ] `statvfs`-проверка свободного места перед пре-аллокацией
- [ ] Пре-аллокация `<filename>.data.brook` (`F_PREALLOCATE` + `ftruncate`)
- [ ] Нарезка на чанки 1–4 MB + расчёт offset'ов
- [ ] SQLite-индекс `<filename>.index.brook` (`rusqlite`, WAL, `synchronous=NORMAL`): `pending` / `done`
- [ ] `pwrite` / `read` wrapper: loop до полного слива, `EINTR`-safe
- [ ] Завершение: финальный `fsync` → `rename .data.brook` → `<filename>` → удалить индекс
- [ ] Потеря/повреждение `.index` или `.data` → рестарт с нуля

### Движок и очередь
- [ ] Work-stealing: общий atomic-счётчик «следующий `pending`»
- [ ] Сегмент: Range-запрос → потоковый `pwrite` буфером 64–256 KB
- [ ] Проверка: принято ровно `chunk_size` байт (иначе вернуть в `pending`)
- [ ] Батчевый commit: каждые 16 чанков → `fsync(.data)` + `UPDATE status='done'`
- [ ] `DownloadEngine`: mpsc команд (`pause`/`resume`/`cancel`), broadcast событий
- [ ] Ресюм: читаем индекс, докачиваем `pending`
- [ ] `Cancel`: статус `CANCELLED` + удалить `.data` и `.index`, запись остаётся в списке
- [ ] `Remove`: то же, что `Cancel`, плюс удаление записи из глобальной очереди
- [ ] Fallback: нет Range → один сегмент без чанков
- [ ] `DownloadManager`: реестр engines, очередь, `max_concurrent`
- [ ] `DownloadManager`: персистентность очереди в `./brook.db` (SQLite рядом с `brook.toml`)
- [ ] Progress-троттлинг в engine: агрегация в окне 200 ms → эмит не чаще 5 Hz per download
- [ ] State-changes / snapshot — мгновенный эмит, без троттлинга
- [ ] Центральный `broadcast::Sender<Event>` в `DownloadManager` (ring 1024)

### Тесты
- [ ] Без сети (`wiremock` / локальный HTTP)
- [ ] Fault-injection: обрыв, `500` на ретраях, отсутствие `Content-Length`, смена `ETag`
- [ ] Пиковый RSS ≤ 150 MB при 10 параллельных

## 2. `brook-proto` + `brook-api`
- [ ] Proto: `List`, `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `Watch`
- [ ] `brook-api`: реализация сервиса поверх `DownloadManager` (proto ↔ core)
- [ ] `Watch`: per-client fanout-задача (broadcast → tonic tx c `.await`)
- [ ] Initial snapshots при коннекте Watch — по одному на каждую известную загрузку
- [ ] Обработка `broadcast::RecvError::Lagged(n)` → синтетические snapshot'ы всех активных загрузок
- [ ] Bind: `127.0.0.1:<port из конфига>` (дефолт 7090)
- [ ] `session_id` / `download_id` / `request_id` в gRPC-метаданных
- [ ] Интеграционные тесты: tonic client ↔ server в одном процессе

## 3. Конфигурация (TOML)
- [ ] Структура `Config` (serde)
- [ ] Чтение `./brook.toml` из CWD
- [ ] Нет файла → создать с дефолтами + путь в stderr
- [ ] Unknown key → warning в лог, invalid value → ошибка старта с сообщением
- [ ] Применить в `DownloadManager` при старте

## 4. `brook` (ratatui)
- [ ] `main`: config → `DownloadManager` → `brook-api` → UI (всё в одном процессе)
- [ ] Single-instance lock: `flock` на `.brook.lock` в CWD; вторая копия → exit с сообщением
- [ ] Graceful shutdown: `SIGTERM` / `SIGINT` / `q` → пауза engines → batch-flush → exit
- [ ] tonic-client на `127.0.0.1:<port>`
- [ ] Фоновая задача: `Watch` → мутация view-model
- [ ] Список: имя, прогресс-бар, скорость, ETA, иконки `▶` / `❚❚` / `✓` / `✕`
- [ ] Сортировка: `RUNNING` → `RETRYING` → `QUEUED` → `PAUSED` → `DONE` → `FAILED` → `CANCELLED`, внутри — по `updated_at`
- [ ] Статус-бар сверху (активные/очередь/скорость), хинт-бар снизу
- [ ] Навигация: `↑↓` + `jk`, `gG`, `Enter` — разворот карточки
- [ ] Multi-select: `Space`, `Shift+↑↓` / `Shift+JK`
- [ ] Команды: `a` (модалка + clipboard prefill), `p`, `r`, `c` (confirm), `o`
- [ ] Фильтр `/`, `Esc` закрывает модалки/фильтр, `q` — quit
- [ ] Help overlay `?` — один экран
- [ ] Модалки дубликатов: URL-in-queue, file-exists
- [ ] Форматирование байт (humansize: `1.5 MB`, `532 KB`) и ETA (`2h 15m`, `15m 30s`, `<1s`)
- [ ] Минимальный размер терминала 60×15 + заглушка ниже
- [ ] `NO_COLOR` — рендер без ANSI-цветов
- [ ] Мышь игнорируем

## 5. Наблюдаемость
- [ ] `tracing` + JSON-форматтер во всех крейтах
- [ ] `session_id` (UUID на процесс), `download_id`, `request_id` в спанах
- [ ] `~/Library/Logs/brook/brook-<session_id>.jsonl` + stderr
- [ ] Ротация: 10 файлов × 50 MB, по размеру

## 6. Quality gate (ручная прогонка)
Сценарии из [open-questions.md](open-questions.md):
- [ ] Файл >1 GB + пауза + рестарт процесса → ресюм
- [ ] Отмена с очисткой частичного файла
- [ ] 10 задач одновременно — лимит параллельности соблюдается
- [ ] Потеря сети → автоматический ретрай → восстановление
- [ ] `brook` + `grpcurl` одновременно видят одно состояние
- [ ] Сервер без Range → fallback на 1 соединение
- [ ] `kill -9` посреди загрузки → рестарт → корректный ресюм
- [ ] Пиковый RSS при 10 параллельных укладывается в ≤ 150 MB
