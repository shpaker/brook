# TODO — MVP

Порядок фиксирован ([roadmap.md](roadmap.md)). Не идти дальше, пока текущий этап не закрыт.

## 0. Workspace
- [ ] Cargo workspace + 4 крейта: `brook-proto`, `brook-core`, `brook-api`, `brook`
- [ ] `proto/brook/v1/brook.proto` — скелет из [api.md](api.md)
- [ ] `brook-proto/build.rs` → `tonic-build`
- [ ] CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`

## 1. `brook-core`
- [ ] Типы: `DownloadId`, `DownloadSpec`, `DownloadState`, `Progress`, `Download`
- [ ] HTTP (`reqwest` + `rustls`): HEAD → `Content-Length` + `Accept-Ranges`
- [ ] Пре-аллокация `<filename>.data.brook` (`F_PREALLOCATE` + `ftruncate`)
- [ ] Нарезка на чанки 1–4 MB + расчёт offset'ов
- [ ] SQLite-индекс `<filename>.index.brook` (`rusqlite`, WAL, `synchronous=NORMAL`): схема `pending` / `done`
- [ ] Work-stealing: общий atomic-счётчик «следующий `pending`»
- [ ] Сегмент: Range-запрос → потоковый `pwrite` в `.data.brook` буфером 64–256 KB
- [ ] Проверка: принято ровно `chunk_size` байт (иначе вернуть в `pending`)
- [ ] Батчевый commit: каждые 16 чанков → `fsync(.data)` + SQLite-транзакция `UPDATE status='done'`
- [ ] Ретраи с экспоненциальным бэкофом
- [ ] `DownloadEngine`: mpsc команд (`pause`/`resume`/`cancel`), broadcast событий
- [ ] Ресюм: читаем индекс, докачиваем `pending`
- [ ] Потеря/повреждение `.index` или `.data` → рестарт с нуля
- [ ] Завершение: финальный `fsync` → `rename .data.brook` → `<filename>` → удалить индекс
- [ ] `Orchestrator`: реестр engines, очередь, `max_concurrent`
- [ ] Orchestrator: персистентность очереди в отдельной SQLite
- [ ] Fallback: нет Range → один сегмент
- [ ] Тесты без сети (`wiremock` / локальный HTTP)
- [ ] Тест: пиковый RSS ≤ 150 MB при 10 параллельных

## 2. `brook-proto` + `brook-api`
- [ ] Proto: `List`, `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `Watch`
- [ ] `brook-api`: реализация сервиса поверх `Orchestrator` (proto ↔ core)
- [ ] `Watch` — server-streaming из broadcast-канала Orchestrator
- [ ] Bind: `127.0.0.1:<port из конфига>` (дефолт 7090)
- [ ] `session_id` / `download_id` / `request_id` в gRPC-метаданных
- [ ] Интеграционные тесты: tonic client ↔ server в одном процессе

## 3. Конфигурация (TOML)
- [ ] Структура `Config` (serde)
- [ ] Чтение `./brook.toml` из CWD
- [ ] Нет файла → создать с дефолтами + путь в stderr
- [ ] Unknown key → warning в лог, invalid value → ошибка старта с сообщением
- [ ] Применить в `Orchestrator` при старте

## 4. `brook` (ratatui)
- [ ] `main`: config → `Orchestrator` → `brook-api` → UI (всё в одном процессе)
- [ ] tonic-client на `127.0.0.1:<port>`
- [ ] Фоновая задача: `Watch` → мутация view-model
- [ ] Список: имя, прогресс-бар, скорость, ETA, иконки `▶` / `❚❚` / `✓` / `✕`
- [ ] Статус-бар сверху (активные/очередь/скорость), хинт-бар снизу
- [ ] Навигация: `↑↓` + `jk`, `gG`, `Enter` — разворот карточки
- [ ] Multi-select: `Space`, `Shift+↑↓` / `Shift+JK`
- [ ] Команды: `a` (модалка + clipboard prefill), `p`, `r`, `c` (confirm), `o`
- [ ] Фильтр `/`, `Esc` закрывает модалки/фильтр, `q` — quit
- [ ] Help overlay `?` — один экран
- [ ] Модалки дубликатов: URL-in-queue, file-exists
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
