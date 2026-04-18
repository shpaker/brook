# brook — agent rules

## 0. Workspace

**brook** is a macOS download manager: Rust async core (`brook-core`), gRPC API (`brook-api`), and a ratatui TUI binary (`brook`). Architecture: [docs/architecture.md](docs/architecture.md). Stack: [docs/stack.md](docs/stack.md).

### Crate layout

```
brook/
├── Cargo.toml                    # [workspace]
├── proto/brook/v1/brook.proto    # single source of truth for the API contract
├── crates/
│   ├── brook-proto/              # build.rs → prost + tonic stubs
│   ├── brook-core/               # DownloadManager + DownloadEngine
│   ├── brook-api/                # gRPC server (tonic), thin wrapper over core
│   └── brook/                    # MVP binary: ratatui UI, boots core + api in-process
└── docs/
```

### Key commands

```sh
cargo build                # build all crates
cargo build -p brook       # build binary only
cargo run -p brook         # run the TUI (reads ./brook.toml from CWD)
cargo test                 # run all tests
cargo test -p brook-core   # core unit + integration tests (no network)
```

### Runtime artifacts in CWD

| File | Purpose |
|---|---|
| `brook.toml` | Config (created with defaults if absent) |
| `brook.db` | Global download queue (SQLite) |
| `.brook.lock` | Single-instance flock |

Per-download artefacts live next to the target file: `<name>.data.brook` (preallocated) + `<name>.index.brook` (chunk index, SQLite WAL).

## Git

- **Commit messages — English, Conventional Commits.**
  Format: `<type>(<scope>)?: <subject>`. Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `build`, `perf`.
  Subject in imperative mood, lowercase, no trailing period. Body optional, wrapped at 72 chars, explains *why*.
  Examples: `docs: pin global queue db path to ./brook.db`, `feat(core): add chunked pwrite engine`.

- **PR title and body — English.** Title follows Conventional Commits too (it becomes the squash commit subject).

- **PR body must include:**
  - `## Summary` — 1–3 bullets, the *why*.
  - `## Release notes` — user-facing one-liner (or `_none_` for internal-only changes).
  - `## Changelog` — bullet list of notable changes for `CHANGELOG.md` (Keep a Changelog style: `Added` / `Changed` / `Fixed` / `Removed` / `Docs`).
  - `## Test plan` — checklist.

- **Merge strategy — squash & merge only.** No merge commits, no rebase-merge. The squash commit subject = PR title.

- Never push to `main` directly. Never force-push shared branches without explicit user ask.

## Docs language

Product docs under `docs/` stay in Russian (current convention). Only git artifacts (commits, PR titles/bodies, branch names, CHANGELOG) are English.
