//! Sandbox: принудительный клэмп `FileSpec.target_dir` под корень,
//! заданный `--directory`.
//!
//! Правила:
//! 1. Относительный путь трактуем относительно `root` (а не CWD демона —
//!    иначе `target_dir = "."` ссылалось бы не туда, где пользователь
//!    ожидает).
//! 2. Канонизируем и корень, и целевой путь. Канонизация разрешает `..`
//!    и симлинки на каждой позиции цепочки — после неё `starts_with`
//!    превращается в надёжный prefix-check.
//! 3. Если целевой путь ещё не существует (типично: подпапка, которую
//!    клиент хочет, чтобы демон создал), канонизируем самый длинный
//!    *существующий* префикс и доклеиваем хвост. Этим мы ловим симлинк
//!    или `..` в существующей части — а несуществующий хвост сам по
//!    себе ничего плохого сделать не может.
//! 4. Канонизированный путь обязан `starts_with(root)`. Иначе —
//!    [`Error::PathEscapesRoot`].
//!
//! Корень канонизируется один раз в `build_runtime` и хранится в
//! [`ClampedPathPolicy`] — `fs::canonicalize` на каждый `prepare()` был
//! бы лишним syscall.

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
}
