//! Формат и atomic read/write для sidecar-файла `.brook.endpoint`.
//!
//! Демон пишет [`Endpoint`] после успешного `TcpListener::bind`, удаляет
//! при graceful shutdown. TUI читает файл перед первой пробой коннекта;
//! если файла нет или probe не прошёл — поднимает демона сам.
//!
//! Формат — YAML (уже тянем `serde_yaml` из-за `brook.yaml`):
//! ```yaml
//! host: 127.0.0.1
//! port: 54921
//! pid: 85421
//! ```

use std::path::Path;
use std::{
    fs,
    io,
};

use anyhow::{
    Context,
    Result,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub pid: u32,
}

impl Endpoint {
    /// Атомарно записать endpoint в `path`. Пишем во временный файл
    /// рядом и `rename` на место — это чинит ситуацию, когда TUI читает
    /// файл в момент старта демона.
    pub fn write_atomic(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let tmp = tempfile::Builder::new()
            .prefix(".brook.endpoint.")
            .tempfile_in(parent)
            .with_context(|| format!("create tempfile in {}", parent.display()))?;
        let body = serde_yaml::to_string(self).context("serialize endpoint")?;
        fs::write(tmp.path(), body).context("write endpoint tempfile")?;
        tmp.persist(path)
            .map_err(|e| anyhow::anyhow!("persist endpoint to {}: {}", path.display(), e.error))?;
        Ok(())
    }

    /// Прочитать endpoint из файла. `Ok(None)` — файла нет; `Err` —
    /// файл есть, но битый (повод для предупреждения, не для падения
    /// TUI).
    pub fn read(path: &Path) -> Result<Option<Self>> {
        match fs::read_to_string(path) {
            Ok(body) => {
                let ep: Endpoint = serde_yaml::from_str(&body)
                    .with_context(|| format!("parse endpoint {}", path.display()))?;
                Ok(Some(ep))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read endpoint {}", path.display())),
        }
    }

    /// Удалить endpoint-файл (idempotent; отсутствующий файл — не ошибка).
    pub fn remove(path: &Path) {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_tempdir() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(".brook.endpoint");
        let ep = Endpoint {
            host: "127.0.0.1".into(),
            port: 54921,
            pid: 4242,
        };
        ep.write_atomic(&path).unwrap();
        let back = Endpoint::read(&path).unwrap().expect("file must exist");
        assert_eq!(back.host, "127.0.0.1");
        assert_eq!(back.port, 54921);
        assert_eq!(back.pid, 4242);
    }

    #[test]
    fn read_missing_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nope.endpoint");
        assert!(Endpoint::read(&path).unwrap().is_none());
    }

    #[test]
    fn remove_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nope.endpoint");
        Endpoint::remove(&path); // файла нет — не должно падать
        let ep = Endpoint {
            host: "127.0.0.1".into(),
            port: 1,
            pid: 1,
        };
        ep.write_atomic(&path).unwrap();
        Endpoint::remove(&path);
        assert!(!path.exists());
    }
}
