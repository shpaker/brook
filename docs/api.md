# API

Контракт между движком и любыми клиентами. Транспорт — **gRPC**, схема — **protobuf**.

## Аналог
Концептуально как `transmission-daemon`:
- Сервис с API, который слушает порт.
- Разные клиенты говорят с ним одинаково.
- Локальный UI — не привилегированный: он такой же клиент, как удалённый.

Отличия:
- У нас gRPC + protobuf (а не JSON-RPC).
- Server-streaming для прогресса и лайфсайкла (вместо поллинга).
- В MVP **Settings через API не ходят** — конфиг живёт в таблице `settings` внутри `brook.db` (см. [architecture.md#конфигурация](architecture.md#конфигурация)).

## Расположение
- Схема: `proto/brook/v1/brook.proto` — единый источник правды.
- Крейт `brook-proto` генерирует Rust-код (`tonic-build` в build.rs).
- Все клиенты зависят от `brook-proto`.

## Схема

Все типы и методы — в `proto/brook/v1/brook.proto` (единый источник правды). Методы MVP: `List`, `Add`, `Remove`, `Pause`, `Resume`, `Cancel`, `PauseAll`, `ResumeAll`, `WatchFile`, `WatchProgress`. Ключевые типы: `FileId` (UUID), `FileSpec` (URL, путь, имя), `FileStatus` (`PENDING` / `RUNNING` / `PAUSED` / `RETRYING` / `DONE` / `FAILED` / `CANCELLED`), `File` (полный снимок лайфсайкла без прогресса), `FileEvent` (oneof: `snapshot`, `status_changed`, `completed`, `failed`), `ProgressTick` (ratio, байты, скорость, ETA).

## Семантика команд

- **`Pause`** — `RUNNING` → `PAUSED`. Inflight-куски доводятся до batch-границы, дальше воркеры останавливаются. `.data.brook` и piece-строки файла в `brook.db` сохраняются.
- **`Resume`** — `PAUSED` / `FAILED` → `RUNNING`. Для `FAILED` сбрасывается счётчик попыток, читаются piece-строки из `brook.db`, докачиваются `pending`.
- **`Cancel`** — из любого live-состояния (`PENDING` / `RUNNING` / `PAUSED` / `RETRYING` / `FAILED`): статус → `CANCELLED`, `.data.brook` и piece-строки файла в `brook.db` удаляются, **запись в `files` остаётся**. Это нужно, чтобы пользователь видел, что именно он отменил, и не добавил URL повторно по ошибке. На `DONE` — no-op (финальный файл уже у пользователя).
- **`Remove`** — сначала то же, что `Cancel` (если файл live), затем запись удаляется из глобальной очереди. На `DONE` — только удаление записи; финальный файл остаётся у пользователя.
- **`PauseAll` / `ResumeAll`** — массовое применение к live-файлам (`RUNNING` / `PENDING` / `RETRYING`).

## `WatchFile` и `WatchProgress`: события и реконсиляция

Скачивание разделено на два server-streaming RPC:

- **`WatchFile`** — лайфсайкл: добавление, смена статуса, завершение, ошибки. Клиент держит view-model и перезаписывает/мутирует запись по `file_id`.
- **`WatchProgress`** — поток `ProgressTick` для прогрессбаров, скорости, ETA.

### `WatchFile`

`FileEvent` — oneof из четырёх вариантов:

- **`snapshot`** (`File`) — полное состояние файла. Источник истины: клиент **перезаписывает** свою запись для `file_id` целиком.
- **`status_changed`** (`file_id`, `FileStatus`) — сменился статус, без сопутствующих полей.
- **`completed`** (`file_id`) — скачивание завершено успехом.
- **`failed`** (`file_id`, `error`) — скачивание упало фатально.

Когда сервер шлёт что:

| Событие           | Триггер                                                                                      |
|-------------------|----------------------------------------------------------------------------------------------|
| `snapshot`        | Initial-поток при коннекте — по одному на каждый известный файл                              |
| `snapshot`        | `Add`                                                                                        |
| `snapshot`        | Реконсиляция после desync (см. ниже)                                                         |
| `status_changed`  | Любая смена статуса, кроме терминальных `DONE` / `FAILED` (для них — `completed` / `failed`) |
| `completed`       | Успешное завершение                                                                          |
| `failed`          | Фатальная ошибка (после исчерпания ретраев)                                                  |

### `WatchProgress`

- Сервер агрегирует счётчики в движке и эмитит `ProgressTick` не чаще **5 раз/сек на файл** (окно 200 ms).
- `ProgressTick` несёт `file_id`, `progress` (ratio 0..=1), `bytes_done`, `bytes_total`, `speed_bps`, `eta_secs`.
- Стрим без initial-sync: актуальный `File` живёт в `WatchFile`, а прогресс начинается с первого тика от активных файлов.

### Backpressure и desync

Между движком и каждым watch-клиентом — tokio `broadcast::channel` (ring 1024). Под нагрузкой (медленный клиент, сетевая задержка) ring может переполниться: tonic tx на fanout-задаче ждёт транспорт, broadcast-receiver отстаёт.

- **Потеря `ProgressTick`-ов** безопасна: прогресс перепишется со следующим тиком.
- **Потеря `FileEvent`-ов** ловится через `broadcast::RecvError::Lagged(n)`. Сервер на этом событии шлёт **синтетическую реконсиляцию**: по одному свежему `snapshot` на каждый активный файл. Клиент перезаписывает записи и продолжает.

### Контракт клиента

- `snapshot` — overwrite записи для `file_id`.
- `status_changed` / `completed` / `failed` — дельта поверх существующей записи; без записи — drop.
- `ProgressTick` — дельта поверх существующей записи; без записи — drop.
- Разрыв стрима — переподключение; `WatchFile` снова начинает с initial-набора `snapshot`-ов.

## Решённое
- Транспорт: gRPC (`tonic`), формат — protobuf (`prost`).
- Локальный UI (`brook`, крейт `brook-tui`) — отдельный процесс, ходит в `brookd` только через gRPC. «Короткого пути» в обход API нет даже локально.
- Два server-streaming RPC (`WatchFile` + `WatchProgress`) — сервер шлёт релевантные события в каждый поток независимо.
- CorrelationId (`session_id`, `file_id`) прокидывается в gRPC-метаданных.
- Settings — не в API в MVP; правятся через SQL в `brook.db` и подхватываются при следующем старте `brookd`.
- Порт по умолчанию — `7090`.
- В MVP — все методы из схемы выше (включая `PauseAll`/`ResumeAll`).

## Открытое
- **Версионирование** — `brook.v1`. Breaking changes только в `brook.v2`.
