//! Политики пути для входящих `FileSpec.target_dir`.
//!
//! Две реализации [`TPathPolicy`]:
//!
//! - [`ClampedPathPolicy`] — sandbox-режим (`brook server --directory <DIR>`).
//!   Канонизирует и `root`, и целевой путь, ловит `..` / симлинк-escape,
//!   запрещает выход за пределы корня.
//! - [`OpenPathPolicy`] — без sandbox. Доступна только при биндинге на
//!   loopback (см. `build_runtime`). Канонизирует абсолютные пути для
//!   разрешения симлинков и `..`, относительные — отвергает (CWD демона
//!   неинтуитивен для пользователя).
//!
//! Алгоритм канонизации общий: канонизируем самый длинный существующий
//! префикс пути и лексически доклеиваем несуществующий хвост. Этим мы
//! ловим симлинк или `..` в существующей части; несуществующий хвост сам
//! по себе ничего плохого сделать не может.

use std::path::{
    Path,
    PathBuf,
};
use std::{
    fs,
    io,
};

use brook_core::{
    Error,
    Result,
    TPathPolicy,
};

/// Политика-«песочница»: разрешает только пути под канонизированным
/// `root`. Корень фиксируется при создании.
#[derive(Debug, Clone)]
pub struct ClampedPathPolicy {
    root: PathBuf,
}

impl ClampedPathPolicy {
    /// Создать политику, канонизировав `root`. Если путь не существует
    /// или не читаем — ошибка I/O (это не sandbox-escape, а
    /// misconfiguration демона).
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = fs::canonicalize(root.as_ref())?;
        Ok(Self { root })
    }

    /// Канонизированный корень — нужен для диагностики и в интеграционных
    /// тестах, чтобы построить ожидаемый путь.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl TPathPolicy for ClampedPathPolicy {
    fn check_target_dir(&self, target_dir: &Path) -> Result<PathBuf> {
        // Относительные пути — относительно корня, не CWD демона.
        let joined = if target_dir.is_absolute() {
            target_dir.to_path_buf()
        } else {
            self.root.join(target_dir)
        };

        let canon = canonicalize_existing_prefix(&joined).map_err(|e| {
            Error::Other(format!("canonicalize {path}: {e}", path = joined.display()))
        })?;

        if !canon.starts_with(&self.root) {
            return Err(Error::PathEscapesRoot {
                attempted: canon.display().to_string(),
            });
        }
        Ok(canon)
    }
}

/// Политика без sandbox: канонизирует абсолютный путь и возвращает его как
/// есть. Используется только при биндинге на loopback — в `build_runtime`
/// её выбор гейтится `host.is_loopback()`.
///
/// Относительные пути отвергает: без `root` их пришлось бы резолвить
/// относительно CWD демона, что для клиента непредсказуемо (демон может
/// быть auto-spawn'нут TUI из произвольной директории).
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenPathPolicy;

impl OpenPathPolicy {
    pub fn new() -> Self {
        Self
    }
}

impl TPathPolicy for OpenPathPolicy {
    fn check_target_dir(&self, target_dir: &Path) -> Result<PathBuf> {
        if !target_dir.is_absolute() {
            return Err(Error::Other(format!(
                "relative target_dir {} is not allowed without --directory; \
                 pass an absolute path",
                target_dir.display()
            )));
        }
        canonicalize_existing_prefix(target_dir).map_err(|e| {
            Error::Other(format!(
                "canonicalize {path}: {e}",
                path = target_dir.display()
            ))
        })
    }
}

/// Канонизировать самый длинный существующий префикс `p` и применить
/// оставшийся (несуществующий) хвост лексически.
///
/// Зачем лексически: `..` в несуществующей части не может указывать на
/// симлинк (симлинков там нет по определению — путь не существует), так
/// что чистая composant-арифметика безопасна. Полагаться на
/// `file_name()`/`parent()` нельзя: для путей, оканчивающихся на `..`,
/// `file_name()` возвращает `None`, и цикл ломался.
fn canonicalize_existing_prefix(p: &Path) -> io::Result<PathBuf> {
    // Пустой путь — сразу ошибка, иначе первый `canonicalize("")`
    // вернёт IO-ошибку с невнятным сообщением.
    if p.as_os_str().is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
    }

    use std::path::Component;

    let components: Vec<Component<'_>> = p.components().collect();

    // Идём от полного пути к более короткому, пока canonicalize не
    // зацепится. `canonicalize` на пустой или некорневой prefix вернёт
    // ошибку — это мы трактуем как «существующего префикса нет».
    let mut end = components.len();
    let canon_base = loop {
        if end == 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no existing prefix under path",
            ));
        }
        let partial: PathBuf = components[..end].iter().collect();
        match fs::canonicalize(&partial) {
            Ok(c) => break c,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                end -= 1;
            }
            Err(e) => return Err(e),
        }
    };

    // Оставшиеся компоненты — не существуют на ФС, поэтому их можно
    // применить лексически: `.` пропускаем, `..` поп'аем, обычное имя
    // пуш'аем. Попытка «выскочить» из canon_base через `..` в хвосте
    // отловится на уровне `check_target_dir` через `starts_with(root)`.
    let mut result = canon_base;
    for comp in &components[end..] {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(name) => result.push(name),
            Component::Prefix(_) | Component::RootDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unexpected root-like component in non-existent suffix",
                ));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn accepts_root_itself() {
        let dir = tempdir().unwrap();
        let pol = ClampedPathPolicy::new(dir.path()).unwrap();
        let got = pol.check_target_dir(dir.path()).unwrap();
        assert_eq!(got, fs::canonicalize(dir.path()).unwrap());
    }

    #[test]
    fn accepts_subdir_under_root() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        let pol = ClampedPathPolicy::new(dir.path()).unwrap();
        let got = pol.check_target_dir(&sub).unwrap();
        assert!(got.starts_with(fs::canonicalize(dir.path()).unwrap()));
    }

    #[test]
    fn accepts_nonexistent_subdir_under_root() {
        let dir = tempdir().unwrap();
        let pol = ClampedPathPolicy::new(dir.path()).unwrap();
        let future = dir.path().join("not/yet/created");
        let got = pol.check_target_dir(&future).unwrap();
        assert!(got.starts_with(fs::canonicalize(dir.path()).unwrap()));
    }

    #[test]
    fn relative_path_is_resolved_against_root() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        let pol = ClampedPathPolicy::new(dir.path()).unwrap();
        let got = pol.check_target_dir(Path::new("sub")).unwrap();
        assert!(got.starts_with(fs::canonicalize(dir.path()).unwrap()));
        assert!(got.ends_with("sub"));
    }

    #[test]
    fn rejects_dotdot_escape() {
        let dir = tempdir().unwrap();
        let inner = dir.path().join("inner");
        fs::create_dir(&inner).unwrap();
        let pol = ClampedPathPolicy::new(&inner).unwrap();
        let err = pol.check_target_dir(&inner.join("../outside")).unwrap_err();
        assert!(
            matches!(err, Error::PathEscapesRoot { .. }),
            "expected PathEscapesRoot, got {err:?}"
        );
    }

    #[test]
    fn rejects_absolute_outside_root() {
        let dir = tempdir().unwrap();
        let pol = ClampedPathPolicy::new(dir.path()).unwrap();
        let err = pol.check_target_dir(Path::new("/tmp")).unwrap_err();
        assert!(matches!(err, Error::PathEscapesRoot { .. }), "{err:?}");
    }

    #[test]
    fn rejects_symlink_escaping_root() {
        let outer = tempdir().unwrap();
        let root = tempdir().unwrap();
        // root/link -> outer (outer лежит вне root).
        let link = root.path().join("link");
        symlink(outer.path(), &link).unwrap();
        let pol = ClampedPathPolicy::new(root.path()).unwrap();
        let err = pol.check_target_dir(&link).unwrap_err();
        assert!(matches!(err, Error::PathEscapesRoot { .. }), "{err:?}");
    }

    #[test]
    fn open_policy_accepts_existing_absolute_path() {
        let dir = tempdir().unwrap();
        let pol = OpenPathPolicy::new();
        let got = pol.check_target_dir(dir.path()).unwrap();
        assert_eq!(got, fs::canonicalize(dir.path()).unwrap());
    }

    #[test]
    fn open_policy_accepts_nonexistent_subdir() {
        let dir = tempdir().unwrap();
        let pol = OpenPathPolicy::new();
        let future = dir.path().join("not/yet/created");
        let got = pol.check_target_dir(&future).unwrap();
        assert!(got.starts_with(fs::canonicalize(dir.path()).unwrap()));
        assert!(got.ends_with("not/yet/created"));
    }

    #[test]
    fn open_policy_rejects_relative_path() {
        let pol = OpenPathPolicy::new();
        let err = pol.check_target_dir(Path::new("relative/sub")).unwrap_err();
        match err {
            Error::Other(msg) => assert!(msg.contains("relative"), "{msg}"),
            other => panic!("expected Error::Other for relative path, got {other:?}"),
        }
    }

    #[test]
    fn open_policy_resolves_symlinks() {
        let target = tempdir().unwrap();
        let host = tempdir().unwrap();
        let link = host.path().join("link");
        symlink(target.path(), &link).unwrap();
        let pol = OpenPathPolicy::new();
        let got = pol.check_target_dir(&link).unwrap();
        assert_eq!(got, fs::canonicalize(target.path()).unwrap());
    }
}
