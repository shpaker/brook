# Стек

## Язык: Rust
Зафиксировано. Асинхронный I/O (tokio), строгая типизация для состояний задач, низкое потребление памяти, удобный вынос движка в библиотеку.

## UI: ratatui
`ratatui` + `crossterm` — TUI прямо в терминале. Естественно ложится на клавиатурное управление и TUI-эстетику из эскиза. Без нативных интеграций — быстрый путь к работающему UI, на котором проверяется API.

## API и сериализация: gRPC (`tonic` + `prost`)
Пользователь выбрал **proto**. Соответственно:
- `tonic` — gRPC-сервер и клиент.
- `prost` — protobuf-кодогенерация.
- `tonic-build` — в build.rs `brook-proto` для сборки `.proto` → Rust.
- Единый источник правды — `proto/brook/v1/brook.proto`.

Альтернативы, не выбрали:
- JSON-RPC (как у Transmission) — проще, но пользователь за proto.
- Cap'n Proto — слабее экосистема в Rust.

## Конфигурация: TOML
В MVP — `./brook.toml` в текущей рабочей директории (`brook` читает «из-под ног»). Если файла нет — создаётся с дефолтами, путь пишется в stderr.

Крейты:
- `serde` + `toml` — сериализация / десериализация.
- `directories` — для логов (см. «Наблюдаемость»); в MVP для конфига не используется.

Настройки читаются при старте и применяются. Правка — вручную. API-методов на settings нет. Пример структуры — [architecture.md#конфигурация](architecture.md#конфигурация).

## Персистентность очереди: SQLite через `rusqlite`
База — `~/Library/Application Support/brook/queue.db`. Миграции — `rusqlite_migration` или руками.

## Наблюдаемость
- `tracing` + `tracing-subscriber` с JSON-форматтером.
- **CorrelationId**: `session_id` (UUID на запуск процесса), `download_id` (на каждую загрузку). Прокидываются через `tracing::Span` — все нижестоящие спаны наследуют.
- Файловый sink в `~/Library/Logs/brook/` (JSONL) + stderr при запуске из терминала.

## Платформа
- macOS, **только самая свежая релизная версия**. Старые не поддерживаем.
- Архитектура кросс-платформенная, но билды и тесты — только под macOS.

## Ключевые крейты MVP

| Область | Крейт |
| --- | --- |
| HTTP-клиент | `reqwest` (rustls) |
| Async runtime | `tokio` |
| TUI | `ratatui` + `crossterm` |
| gRPC | `tonic` + `tonic-build` |
| API-сериализация | `prost` |
| Персистентность очереди | `rusqlite` |
| Конфиг (TOML) | `serde` + `toml` |
| Логи | `tracing`, `tracing-subscriber` (JSON) |
| Ошибки | `thiserror` (core/api), `anyhow` (bin) |
| Пути | `directories` |
| UUID | `uuid` (v4 для session_id / download_id) |
