//! Константы, общие для демона и TUI.

/// Имя sidecar-файла, в который демон пишет актуальный host:port при
/// успешном `bind`. TUI читает его перед пробой коннекта. Живёт рядом с
/// `.brook.lock` в cache-каталоге (см. [`crate::AppPaths`]).
pub const ENDPOINT_FILENAME: &str = ".brook.endpoint";

/// Дефолтный TCP-порт gRPC, если явно не задан ни в конфиге, ни через CLI.
pub const DEFAULT_PORT: u16 = 7090;

/// gRPC-заголовок для bearer-авторизации клиента. Будет использоваться
/// в M5 (серверный interceptor + клиентский interceptor).
pub const AUTH_HEADER: &str = "authorization";

/// Префикс схемы авторизации в значении заголовка. Совпадает с
/// HTTP-конвенцией (`bearer <token>`). Значение регистронезависимое — в
/// проверке используем `strip_prefix` по обоим регистрам.
pub const AUTH_SCHEME: &str = "bearer ";
