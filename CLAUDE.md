# brook — agent rules

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
