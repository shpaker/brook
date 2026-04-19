# brook — agent rules

## 0. Workspace

**brook** is a macOS download manager: Rust async core (`brook-core`), gRPC API (`brook-api`), daemon (`brookd`), and ratatui TUI client (`brook`). Architecture: [docs/architecture.md](docs/architecture.md). Stack: [docs/stack.md](docs/stack.md).

### Crate layout

```
brook/
├── Cargo.toml                    # [workspace]
├── proto/brook/v1/brook.proto    # single source of truth for the API contract
├── crates/
│   ├── brook-proto/              # build.rs → prost + tonic stubs
│   ├── brook-core/               # DownloadManager + DownloadEngine
│   ├── brook-api/                # gRPC server (tonic), thin wrapper over core
│   ├── brookd/                   # daemon binary: boots core + api, holds .brook.lock
│   └── brook-tui/                # TUI binary (name: brook): ratatui gRPC client
└── docs/
```

### Key commands

```sh
cargo build                  # build all crates
cargo build -p brookd        # build daemon
cargo build -p brook-tui     # build TUI client
cargo run -p brookd          # run the daemon (CWD is the working directory)
cargo run -p brook-tui       # run the TUI client
cargo test                   # run all tests
cargo test -p brook-core     # core unit + integration tests (no network)
```

### Runtime artifacts in CWD

| File | Purpose |
|---|---|
| `brook.db` | Config (`settings` table) + global download queue (SQLite) |
| `.brook.lock` | Single-instance flock (held by `brookd`) |

Per-download artefacts live next to the target file: `<name>.data.brook` (preallocated) + `<name>.index.brook` (piece index, SQLite WAL).

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
- **DB access — only through repository structs.** Any SQLite manipulation (`brook.db`, `.index.brook`) lives inside a dedicated repository struct. SQL strings and `rusqlite::Connection` usage never leak past the repository boundary — callers get domain methods, not queries.

## Docs

- **Language**: product docs under `docs/` stay in Russian (current convention). Only git artifacts (commits, PR titles/bodies, branch names, CHANGELOG) are English.
- **Content**: docs describe *what* the system does and *why* — not how it's built. No code examples (Rust, SQL, proto snippets) and no implementation details in `docs/`. The code is the source of truth for implementation; docs reference it in prose.
