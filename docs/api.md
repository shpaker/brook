# API

Контракт между движком и любыми клиентами. Транспорт — **gRPC**, схема — **protobuf**.

## Аналог
Концептуально как `transmission-daemon`:
- Сервис с API, который слушает порт.
- Разные клиенты говорят с ним одинаково.
- Локальный UI — не привилегированный: он такой же клиент, как удалённый.

Отличия:
- У нас gRPC + protobuf (а не JSON-RPC).
- Server-streaming для прогресса (вместо поллинга).
- В MVP **Settings через API не ходят** — конфиг живёт в TOML (см. [architecture.md#конфигурация](architecture.md#конфигурация)).

## Расположение
- Схема: `proto/brook/v1/brook.proto` — единый источник правды.
- Крейт `brook-proto` генерирует Rust-код (`tonic-build` в build.rs).
- Все клиенты зависят от `brook-proto`.

## Эскиз схемы (draft)

```proto
syntax = "proto3";
package brook.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/empty.proto";

service DownloadService {
    rpc List       (ListRequest)    returns (ListResponse);
    rpc Add        (AddRequest)     returns (AddResponse);
    rpc Remove     (RemoveRequest)  returns (RemoveResponse);

    rpc Pause      (IdRequest)      returns (StatusResponse);
    rpc Resume     (IdRequest)      returns (StatusResponse);
    rpc Cancel     (IdRequest)      returns (StatusResponse);

    rpc PauseAll   (google.protobuf.Empty) returns (StatusResponse);
    rpc ResumeAll  (google.protobuf.Empty) returns (StatusResponse);

    // server-streaming — подписка на события (progress, state changes)
    rpc Watch      (WatchRequest)   returns (stream Event);
}

message DownloadId { string value = 1; }        // UUID как строка

message DownloadSpec {
    string url = 1;
    string target_path = 2;                     // абсолютный; либо префикс из TOML + имя
    uint32 segments = 3;                        // 0 = взять дефолт из конфига
    map<string, string> headers = 4;
}

enum DownloadState {
    STATE_UNSPECIFIED = 0;
    QUEUED = 1;
    RUNNING = 2;
    PAUSED = 3;
    DONE = 4;
    FAILED = 5;
    RETRYING = 6;
}

message Progress {
    uint64 downloaded_bytes = 1;
    uint64 total_bytes = 2;
    double speed_bps = 3;
    uint64 eta_seconds = 4;
}

message Download {
    DownloadId id = 1;
    DownloadSpec spec = 2;
    DownloadState state = 3;
    Progress progress = 4;
    uint32 attempt = 5;
    string error_message = 6;                   // только для FAILED / RETRYING
    google.protobuf.Timestamp created_at = 7;
    google.protobuf.Timestamp updated_at = 8;
}

message Event {
    DownloadId id = 1;
    oneof kind {
        Download snapshot = 2;                  // полный снимок (при подписке / смене состояния)
        Progress progress_tick = 3;             // лёгкий тик во время RUNNING
        string log_line = 4;                    // опционально — для дебаг-хвоста в UI
    }
}

// ... AddRequest / AddResponse / ListResponse / IdRequest / StatusResponse / WatchRequest ...
```

## Семантика команд

- **`Pause`** — `RUNNING` → `PAUSED`. Inflight-чанки доводятся до batch-границы, дальше сегменты останавливаются. `.data.brook` и `.index.brook` сохраняются.
- **`Resume`** — `PAUSED` / `FAILED` → `RUNNING`. Для `FAILED` сбрасывается счётчик попыток, читается `.index.brook`, докачиваются `pending`.
- **`Cancel`** — из любого live-состояния (`QUEUED` / `RUNNING` / `PAUSED` / `RETRYING` / `FAILED`): статус → `CANCELLED`, `.data.brook` и `.index.brook` удаляются, **запись в списке остаётся**. Это нужно, чтобы пользователь видел, что именно он отменил, и не добавил URL повторно по ошибке. На `DONE` — no-op (финальный файл уже у пользователя).
- **`Remove`** — сначала то же, что `Cancel` (если загрузка live), затем запись удаляется из глобальной очереди. На `DONE` — только удаление записи; финальный файл остаётся у пользователя.
- **`PauseAll` / `ResumeAll`** — массовое применение к live-загрузкам (`RUNNING` / `QUEUED` / `RETRYING`).

## `Watch`: события и реконсиляция

`Watch` — server-streaming RPC. Сервер шлёт поток `Event`-ов, клиент держит view-model и применяет события по мере прихода.

### Типы событий (из proto)

- **`snapshot`** (`Download`) — полное состояние загрузки. Источник истины: клиент **перезаписывает** свою запись для `download_id` целиком.
- **`progress_tick`** (`Progress`) — лёгкая дельта: байты, скорость, ETA. Применяется поверх существующей записи; если записи нет (snapshot ещё не пришёл) — игнорируется.
- **`log_line`** (`string`) — строка лога для debug-хвоста в карточке (разворот по `Enter`).

### Когда сервер шлёт что

| Событие         | Триггер                                                                                    |
|-----------------|--------------------------------------------------------------------------------------------|
| `snapshot`      | Initial-поток при коннекте — по одному на каждую известную загрузку                        |
| `snapshot`      | Любая смена состояния (`QUEUED` / `RUNNING` / `PAUSED` / `RETRYING` / `DONE` / `FAILED` / `CANCELLED`) |
| `snapshot`      | `Add` / `Remove` / `Cancel`                                                                |
| `snapshot`      | Реконсиляция после desync (см. ниже)                                                       |
| `progress_tick` | Во время `RUNNING` — **не чаще 5 Hz на загрузку**                                          |
| `log_line`      | Ретраи, ошибки, важные события жизненного цикла                                            |

### Троттлинг прогресса

- Сервер агрегирует счётчики в движке и эмитит `progress_tick` не чаще **5 раз/сек на загрузку** (окно 200 ms).
- `snapshot`-события **не троттлятся** — шлются сразу при смене состояния.

### Backpressure и desync

Между движком и каждым Watch-клиентом — tokio `broadcast::channel` (ring 1024). Под нагрузкой (медленный клиент, сетевая задержка) ring может переполниться: tonic tx на fanout-задаче ждёт транспорт, broadcast-receiver отстаёт.

- **Потеря `progress_tick`-ов** безопасна: прогресс восстановится со следующим тиком.
- **Потеря `snapshot`-ов** ловится через `broadcast::RecvError::Lagged(n)`. Сервер на этом событии шлёт **синтетическую реконсиляцию**: по одному свежему `snapshot` на каждую активную загрузку. Клиент перезаписывает записи и продолжает.

### Контракт клиента

- `snapshot` — overwrite записи для `download_id`.
- `progress_tick` — дельта поверх существующей записи; без записи — drop.
- Разрыв стрима — переподключение; сервер снова начинает с initial-набора `snapshot`-ов.

## Решённое
- Транспорт: gRPC (`tonic`), формат — protobuf (`prost`).
- Локальный UI (`brook`) ходит в API, а не дёргает `DownloadManager` напрямую.
- Server-streaming `Watch` — один стрим на клиента, сервер шлёт релевантные события.
- CorrelationId (`session_id`, `download_id`) прокидывается в gRPC-метаданных.
- Settings — не в API в MVP; правятся в TOML и подхватываются при следующем старте `brook`.
- Порт по умолчанию — `7090`.
- В MVP — все методы из схемы выше (включая `PauseAll`/`ResumeAll`).

## Открытое
- **Версионирование** — `brook.v1`. Breaking changes только в `brook.v2`.

Auth, TLS и Settings-методы — см. [post-mvp.md](post-mvp.md).
