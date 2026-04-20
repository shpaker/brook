set shell := ["bash", "-cu"]

# Показать список рецептов
default:
    @just --list

# === build ===

# Собрать весь workspace
build:
    cargo build

# Собрать демон
build-d:
    cargo build -p brookd

# Собрать TUI-клиент
build-tui:
    cargo build -p brook-tui

# === run ===

# Запустить демон (CWD = рабочая директория; появятся brook.db и .brook.lock)
run-d *ARGS:
    cargo run -p brookd -- {{ARGS}}

# Запустить TUI-клиент
run-tui *ARGS:
    cargo run -p brook-tui -- {{ARGS}}

# === test ===

# Прогнать все тесты workspace
test:
    cargo test

# Тесты конкретного крейта: `just test-p brook-core`
test-p CRATE:
    cargo test -p {{CRATE}}

# === lint / format ===

# Применить nightly cargo fmt ко всему workspace
fmt:
    # rustfmt.toml содержит unstable опции (imports_granularity, imports_layout,
    # group_imports) — их понимает только nightly-rustfmt. rustup-proxy по
    # умолчанию резолвится в stable, поэтому прокидываем nightly-bin в PATH.
    NIGHTLY_BIN="$(dirname "$(rustup which --toolchain nightly rustfmt)")"; \
    PATH="$NIGHTLY_BIN:$PATH" cargo fmt --all

# Проверить форматирование без правок (для CI/pre-commit)
fmt-check:
    NIGHTLY_BIN="$(dirname "$(rustup which --toolchain nightly rustfmt)")"; \
    PATH="$NIGHTLY_BIN:$PATH" cargo fmt --all -- --check

# Clippy со всеми предупреждениями как ошибки
clippy:
    cargo clippy --all-targets -- -D warnings

# Полная проверка перед пушем: форматирование + clippy + тесты
check: fmt-check clippy test

# Починить то, что чинится автоматически: clippy --fix + fmt
fix:
    cargo clippy --all-targets --fix --allow-dirty --allow-staged -- -D warnings
    just fmt

# === housekeeping ===

# Удалить target/
clean:
    cargo clean

# Обновить зависимости в пределах семантического range
update:
    cargo update
