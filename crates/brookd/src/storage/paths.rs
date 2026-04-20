//! Защита от path traversal при выборе имени файла загрузки.
//!
//! `filename` может прилетать из недоверенных источников — `Content-Disposition`
//! заголовка или последнего сегмента URL. Без проверки злоумышленник
//! мог бы подсунуть `../../etc/passwd` и заставить нас писать за пределы
//! целевой директории.
//!
//! Стратегия — самая строгая и потому простая: `filename` должен быть
//! **ровно одним «нормальным» компонентом пути** (`Component::Normal`).
//! Это автоматически отсекает:
//! - пустую строку,
//! - `.` и `..`,
//! - любые разделители (`/` на Unix, `\` на Windows),
//! - абсолютные пути, префиксы `C:\`, UNC-пути на Windows.

use std::path::{
    Component,
    Path,
    PathBuf,
};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("filename is empty")]
    Empty,
    /// Имя содержит разделитель, `.`, `..`, NUL или является абсолютным.
    #[error("filename is unsafe: {0:?}")]
    Unsafe(String),
}

/// Собрать безопасный целевой путь `target_dir / filename`, проверив
/// что `filename` — один безопасный компонент.
///
/// Директория `target_dir` считается доверенной (её задаёт оператор
/// через settings или клиент явно); она может быть относительной или
/// абсолютной — оба варианта допустимы по правилу этапа 1.6.
pub fn resolve_target(target_dir: &Path, filename: &str) -> Result<PathBuf, PathError> {
    validate_filename(filename)?;
    Ok(target_dir.join(filename))
}

/// Проверить, что `filename` безопасен как один компонент пути.
///
/// Публично, т.к. полезно до того, как `target_dir` известна (например,
/// в валидации `DownloadSpec`).
pub fn validate_filename(filename: &str) -> Result<(), PathError> {
    if filename.is_empty() {
        return Err(PathError::Empty);
    }
    if filename.contains('\0') {
        return Err(PathError::Unsafe(filename.to_owned()));
    }

    let path = Path::new(filename);
    let mut components = path.components();
    let only = components.next();
    let extra = components.next();

    match (only, extra) {
        (Some(Component::Normal(name)), None) if name == filename => Ok(()),
        _ => Err(PathError::Unsafe(filename.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_filename() {
        assert!(validate_filename("ubuntu.iso").is_ok());
        assert!(validate_filename("archive.tar.gz").is_ok());
        assert!(validate_filename(".hidden").is_ok());
        assert!(validate_filename("имя с пробелом.bin").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(validate_filename(""), Err(PathError::Empty));
    }

    #[test]
    fn rejects_dot_and_dotdot() {
        assert!(matches!(validate_filename("."), Err(PathError::Unsafe(_))));
        assert!(matches!(validate_filename(".."), Err(PathError::Unsafe(_))));
    }

    #[test]
    fn rejects_path_separators() {
        assert!(matches!(
            validate_filename("../etc/passwd"),
            Err(PathError::Unsafe(_))
        ));
        assert!(matches!(
            validate_filename("sub/file.bin"),
            Err(PathError::Unsafe(_))
        ));
        assert!(matches!(
            validate_filename("/etc/passwd"),
            Err(PathError::Unsafe(_))
        ));
    }

    #[test]
    fn rejects_nul_byte() {
        assert!(matches!(
            validate_filename("bad\0name"),
            Err(PathError::Unsafe(_))
        ));
    }

    #[test]
    fn resolve_joins_under_target() {
        let target = Path::new("/downloads");
        let resolved = resolve_target(target, "file.bin").unwrap();
        assert_eq!(resolved, PathBuf::from("/downloads/file.bin"));
    }

    #[test]
    fn resolve_rejects_traversal() {
        let target = Path::new("/downloads");
        assert!(resolve_target(target, "../evil").is_err());
    }
}
