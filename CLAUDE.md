# brook — agent rules

## 0. Workspace

**brook** is a macOS download manager: Rust async core (`brook-core`), HTTP adapter (`brook-http`), gRPC API (`brook-api`), server stack (`brook-daemon`), and ratatui TUI client (`brook-tui`), all driven by a single `brook` binary with clap subcommands. Product overview and architecture: [README.md](README.md).

### Crate layout

```
brook/
├── Cargo.toml                    # [workspace]
├── proto/brook/v1/brook.proto    # single source of truth for the API contract
├── crates/
│   ├── brook-proto/              # build.rs → prost + tonic stubs
│   ├── brook-core/               # hexagonal core: domain + ports (+ services); no network, no disk
│   ├── brook-http/               # HTTP adapter: reqwest + middleware, impls core's HTTP ports
│   ├── brook-api/                # gRPC server (tonic) + bearer-auth interceptor
│   ├── brook-runtime/            # shared daemon/TUI primitives: endpoint sidecar, constants
│   ├── brook-daemon/             # lib: server stack (config, storage, bootstrap, sandbox)
│   ├── brook-tui/                # lib: ratatui gRPC client
│   └── brook/                    # the only [[bin]]: `brook` dispatch (TUI by default; `brook server …` = daemon)
└── README.md
```

### Invocation modes

- `brook server --directory <DIR> [--host H] [--port P] [--client-pass …]` — daemon with mandatory sandbox root. Refuses non-loopback without `--client-pass` (also via `BROOK_CLIENT_PASS`).
- `brook` — TUI to local daemon. Auto-spawns `brook server` if `.brook.endpoint` is missing or unreachable; offers to stop that daemon on quit.
- `brook --remote HOST:PORT [--pass …]` — TUI to a remote daemon. Prompts for password on TTY when needed; never stops the remote daemon.

### Key commands

Все рутинные команды проекта живут в [justfile](justfile). Агент обязан
вызывать `just`, а не голые `cargo fmt` / `cargo clippy` / `cargo test` —
justfile знает про nightly-rustfmt, правильные флаги clippy и состав
проверок. Голый `cargo` допустим только когда нужного рецепта в `just`
нет.

```sh
just                         # список доступных рецептов
just build                   # сборка всего workspace
just run                     # brook (TUI-режим) с произвольными аргументами диспетчера
just run-server -- --directory ~/Downloads   # brook server ... c аргументами
just run-d                   # алиас на `brook server`
just run-tui                 # алиас на TUI-режим `brook`
just test                    # все тесты
just test-p brook-core       # тесты одного крейта
just fmt                     # nightly cargo fmt --all
just fmt-check               # проверка форматирования без правок
just clippy                  # clippy --all-targets -- -D warnings
just check                   # fmt-check + clippy + test (обязательный gate перед коммитом)
just fix                     # clippy --fix + fmt
```

**Правило для агента**: перед коммитом прогнать `just check`.
Валидация качества (fmt/clippy/test) всегда через `just`; прямые вызовы
`cargo fmt`/`cargo clippy` ведут к несогласованным флагам и срабатывают
не с тем rustfmt-тулчейном.

### Runtime artifacts

Пути резолвит `brook_runtime::AppPaths` через `directories::ProjectDirs`.
Для загрузки/выгрузки `BROOK_APP_DIR` перенаправляет все четыре файла в
один каталог (удобно для dev-запусков и изолированных инсталляций).

| File | macOS | Linux (XDG) | Windows | Purpose |
|---|---|---|---|---|
| `brook.yaml`      | `~/Library/Application Support/brook/` | `~/.config/brook/`      | `%APPDATA%\brook\config\`   | YAML config (global + per-download defaults) |
| `brook.db`        | `~/Library/Application Support/brook/` | `~/.local/share/brook/` | `%APPDATA%\brook\data\`     | Global download queue (SQLite, WAL) |
| `.brook.lock`     | `~/Library/Caches/brook/`              | `~/.cache/brook/`       | `%LOCALAPPDATA%\brook\cache\` | Single-instance flock (held by `brook server`) |
| `.brook.endpoint` | `~/Library/Caches/brook/`              | `~/.cache/brook/`       | `%LOCALAPPDATA%\brook\cache\` | Sidecar with actual `{host, port}` — lets TUI discover ephemeral ports; removed on graceful shutdown |

Per-download artefacts: only `<name>.data.brook` (preallocated) lives next to the target file. The piece index, per-download settings and state history are rows in the shared `brook.db` (tables `files`, `file_settings`, `state_changes`, `pieces`).

Integration-тесты обходят `AppPaths` и кладут все четыре файла в один tempdir через `brook_daemon::app::Paths::in_dir`.

## Git

- **Commit messages — English, Conventional Commits.**
  Format: `<type>(<scope>)?: <subject>`. Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `build`, `perf`.
  Subject in imperative mood, lowercase, no trailing period. Body optional, wrapped at 72 chars, explains *why*.
  Examples: `docs: pin global queue db path to ./brook.db`, `feat(core): add piece-based pwrite engine`.

- **PR title and body — English.** Title follows Conventional Commits too (it becomes the squash commit subject).

- **PR body must include:**
  - `## Summary` — 1–3 bullets, the *why*.
  - `## Release notes` — user-facing one-liner (or `_none_` for internal-only changes).
  - `## Changelog` — bullet list of notable changes for `CHANGELOG.md` (Keep a Changelog style: `Added` / `Changed` / `Fixed` / `Removed` / `Docs`).
  - `## Test plan` — checklist.

- **Merge strategy — squash & merge only.** No merge commits, no rebase-merge. The squash commit subject = PR title.

- Never push to `main` directly. Never force-push shared branches without explicit user ask.

## Coding conventions

- **Trait names** — prefix with `T`: `TPieceStorage`, `TQueueStore`, `TPieceStorageFactory`. Applies to all traits in every crate of this workspace.
- **`brook-core` layout — Hexagonal (Ports & Adapters).** New domain types go into `crates/brook-core/src/domain/` (pure, no I/O, no external-world dependencies). New outbound traits — into `crates/brook-core/src/ports/`. Application services (coordinators like `DownloadManager`, `DownloadEngine`) — into `crates/brook-core/src/service/` (to be created at stage 1.3). Concrete adapters (SQLite, HTTP clients, gRPC) **never** live in `brook-core` — they belong in `brook-daemon`, `brook-http`, `brook-api`, or other dedicated adapter crates. `brook-core` must not depend on `reqwest`, `rusqlite`, or any other I/O library; enforced by `cargo tree -p brook-core`. The public API of `brook-core` stays flat (`brook_core::DownloadId`, `brook_core::TPieceStorage`) — internal folders exist to keep layers from mixing, not to nest the API.
- **DB access — only through repository structs.** Any SQLite manipulation in `brook.db` lives inside a dedicated repository struct. SQL strings and `rusqlite::Connection` usage never leak past the repository boundary — callers get domain methods, not queries.
- **TUI hint bars — символ клавиши всегда выделяется цветом.** Любая подсказка о клавише рендерится минимум двумя `Span`'ами: сам ключевой символ — accent (`Color::Cyan`), остальное — dim (`Color::DarkGray`). Одним серым куском хинт класть нельзя — пользователь должен мгновенно видеть, какую клавишу нажать. В режиме `no_color` оба Span'а получают `Modifier::DIM`, но раскладка «клавиша отдельным span'ом» сохраняется. Допустимы две формы:
  - **Отдельный ключ + описание**: `<key> · <действие>` — для многобуквенных клавиш вроде `Tab`/`Enter`/`Esc`. Эталон — `hint_line` в [crates/brook-tui/src/ui/modal.rs](crates/brook-tui/src/ui/modal.rs).
  - **Клавиша встроена первой буквой слова**: `<a>dd`, `<q>uit`, `<r>eveal`, `<p>ause` — первая буква accent, остаток dim. Используется, когда клавиша — одиночный Char и совпадает с первой буквой слова действия (верхняя рамка главного окна, правый блок карточек). Эталон — `word_with_key` в [crates/brook-tui/src/ui/chrome.rs](crates/brook-tui/src/ui/chrome.rs).

  Новые хинты собирать через эти хелперы, а не через одиночный `Span::styled(string, …)`.

- **TUI модалки — структура и клавиатурный контракт.** Все модалки — `Rounded`-рамка 62×6, рамка `Color::DarkGray` (как у главного окна): заголовок `[  title  ]` в верхней рамке, действия в нижней; содержимое — только текст (4 строки: пустая · текст · текст · пустая). Хелперы: `bottom_yes_no`, `hint_line`, `modal_block` в [crates/brook-tui/src/ui/modal.rs](crates/brook-tui/src/ui/modal.rs).

  Кнопки `yes`/`no` в нижней рамке рендерятся через `bottom_yes_no(no_color)`: оба слова через `word_with_key` — первая буква Cyan (шоткат), остаток DarkGray. Фокуса нет, Tab не работает. Цветовой блок REVERSED не используется.

  Клавиатурный контракт, единый для всех `yes`/`no` модалок и **не указываемый явно в подсказках**:
  - `y`/`Y` и `Enter` — всегда `yes` (подтвердить основное действие).
  - `n`/`N` и `Esc` — всегда `no` (отмена / закрыть без действия).

## Formatting (Rust)

Конфиг — [rustfmt.toml](rustfmt.toml). Автоформат — `just fmt` (часть опций nightly-only, рецепт сам подтягивает nightly-rustfmt через `rustup which`; прямой `cargo +nightly fmt` через rustup-proxy обычно резолвится в stable и теряет unstable-правила).

Жёсткие правила, которые должны соблюдаться даже при ручной правке:

- **`use` и `pub use` — один item на строку.** Несколько имён из одного пути склеиваются в общий braced-блок с вертикальной раскладкой:
  ```rust
  pub use foo::{
      A,
      B,
  };
  ```
  Плоский `pub use foo::A; pub use foo::B;` или однострочный `pub use foo::{A, B};` запрещены — оба приводятся к форме выше (rustfmt с `imports_granularity = "Module"` + `imports_layout = "Vertical"` делает это автоматически).
  Исключение: re-export всего модуля через `pub use foo::*;` допустим, если это осознанный «фасад».
- **Группы импортов через пустую строку, в порядке:** `std` → внешние крейты → `crate` / `super` / `self`. Внутри группы — алфавитный порядок.
- **`max_width = 100`**, отступ — 4 пробела, переводы строк — `LF`.
- **Edition 2024** на весь workspace.
- **Никаких `use foo::bar::*`** внутри функций/модулей (только в явных фасадах, см. выше) — конкретные имена в импортах читаются лучше.
- Перед коммитом прогонять `just check` (он запускает `fmt-check` + `clippy` + `test`).

## Docs

- Product overview lives in [README.md](README.md) (Russian — product convention). Git artifacts (commit messages, PR titles/bodies, branch names, CHANGELOG) are English.
- README describes *what* and *why*, not implementation details. The code is the source of truth for how things are built; README references it in prose.
