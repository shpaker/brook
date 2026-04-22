//! Политика разрешённых `target_dir` для входящих `FileSpec`.
//!
//! Ядро не знает ни про sandbox, ни про канонизацию — это детали
//! адаптера (в проде — `ClampedPathPolicy` из `brook-daemon`). Но точку
//! врезки держим в ядре: любой путь, который прилетел через `FileSpec`,
//! должен пройти через [`TPathPolicy::check_target_dir`] до первого I/O.
//!
//! Зачем отдельный порт:
//! - даёт движку/фабрике единую ручку проверки независимо от того,
//!   включён sandbox или нет (в тестах — no-op реализация);
//! - даёт адаптеру свободу реализовать канонизацию как ему удобно
//!   (`fs::canonicalize` + `starts_with`, через `realpath`, и т.д.).
//!
//! Метод **синхронный**: реализация может дёргать `fs::canonicalize`
//! (syscall), но это одноразовая проверка на `prepare()`, не hot path.
//! Если станет узким местом — обернём вызов в `spawn_blocking` на
//! стороне адаптера, не ломая трейт.

use std::path::{
    Path,
    PathBuf,
};

use crate::error::Result;

/// Политика, которую адаптер применяет к `FileSpec.target_dir`.
///
/// Возвращает канонизированный путь (именно его фабрика отдаёт дальше в
/// I/O), либо [`Error::PathEscapesRoot`](crate::Error::PathEscapesRoot),
/// если путь после канонизации вышел за пределы разрешённого корня.
pub trait TPathPolicy: Send + Sync {
    fn check_target_dir(&self, target_dir: &Path) -> Result<PathBuf>;
}
