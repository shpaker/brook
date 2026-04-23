//! Общие примитивы, которые нужны и демону, и TUI, но не тянут на
//! полноценные крейты:
//!
//! - [`Endpoint`] — формат sidecar-файла `.brook.endpoint`, через который
//!   TUI находит запущенный локально демон (включая случай ephemeral
//!   порта).
//! - [`constants`] — строки и числа, которые обе стороны должны понимать
//!   одинаково (gRPC-заголовок auth, префикс схемы, имена файлов).
//!
//! Крейт сознательно лёгкий: `serde`, `serde_yaml`, `anyhow`, `tempfile`
//! — никакой async-среды, никакого tonic. Берём в зависимости и в
//! `brook-daemon`, и в `brook-tui` (они друг о друге не знают).

pub mod constants;
pub mod endpoint;
pub mod paths;

pub use endpoint::Endpoint;
pub use paths::{
    APP_DIR_ENV,
    AppPaths,
};
