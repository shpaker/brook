//! Фабрика [`TPieceStorageFactory`] — composition-layer между HTTP-inspect,
//! расчётом `piece_size` и файловым [`LocalPieceStorage`].
//!
//! `DownloadManager` требует `Arc<impl TPieceStorageFactory>`; без этого
//! звена он не может «подготовить» загрузку перед запуском engine. Сама
//! фабрика — чистый клей:
//!
//! ```text
//! inspect(url)
//!   → resolve_filename + on_file_exists
//!   → effective_plan_config + plan_pieces → piece_size
//!   → files_repo.set_inspect_fields(id, total_size, piece_size, etag, last_modified)
//!   → LocalPieceStorage::open(id, total_size, piece_size, …)
//! ```
//!
//! ## Политика `on_file_exists`
//!
//! `ask`/`rename`/`overwrite` здесь не живёт. [`LocalPieceStorage::open`]
//! уже умеет два легальных сценария (инициализация на пустом месте и
//! resume поверх валидного `.data.brook`); конфликт именно с существующим
//! *готовым* файлом (целевым) — задача TUI-модалки §6.6, которая до
//! `prepare` договаривается с пользователем и правит `spec.filename`.
//! Фабрика не делает предположений — смотрит только политику
//! `on_file_exists_override`.

use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use brook_core::{
    DownloadId,
    DownloadSpec,
    Error,
    OnFileExistsOverride,
    PreparedDownload,
    RangeGuard,
    Result,
    THttpInspect,
    TPieceStorageFactory,
};

use super::files::SqliteFileRepository;
use super::local::LocalPieceStorage;
use super::paths::validate_filename;
use super::pieces::SqlitePieceRepository;
use super::plan::{
    effective_plan_config,
    plan_pieces,
};
use crate::config::DownloadDefaults;

/// Фабрика локальных piece-хранилищ.
///
/// Параметризуется реализацией [`THttpInspect`] — в тестах это мок, в
/// бинаре — `brook_http::HttpInspectClient`. Держит [`Arc`]-ссылки на
/// общие SQLite-репозитории; клонирование фабрики дешёвое.
pub struct LocalPieceStorageFactory<I: THttpInspect + ?Sized> {
    inspect: Arc<I>,
    defaults: DownloadDefaults,
    pieces_repo: Arc<SqlitePieceRepository>,
    files_repo: Arc<SqliteFileRepository>,
}

impl<I: THttpInspect + ?Sized> LocalPieceStorageFactory<I> {
    pub fn new(
        inspect: Arc<I>,
        defaults: DownloadDefaults,
        pieces_repo: Arc<SqlitePieceRepository>,
        files_repo: Arc<SqliteFileRepository>,
    ) -> Self {
        Self {
            inspect,
            defaults,
            pieces_repo,
            files_repo,
        }
    }
}

impl<I> TPieceStorageFactory for LocalPieceStorageFactory<I>
where
    I: THttpInspect + ?Sized + Send + Sync + 'static,
{
    type Storage = LocalPieceStorage;

    fn prepare(
        &self,
        id: DownloadId,
        spec: &DownloadSpec,
    ) -> impl std::future::Future<Output = Result<PreparedDownload<Self::Storage>>> + Send {
        // Клонируем всё, что нужно для async-блока: фабрика может быть
        // живее отдельного `prepare`-вызова.
        let inspect = Arc::clone(&self.inspect);
        let defaults = self.defaults;
        let pieces_repo = Arc::clone(&self.pieces_repo);
        let files_repo = Arc::clone(&self.files_repo);
        let spec = spec.clone();
        async move {
            let report = inspect
                .inspect(&spec.url)
                .await
                .map_err(|e| Error::Other(format!("inspect: {e}")))?;

            let total_size = report.total_size.ok_or_else(|| {
                Error::Other("inspect: server did not report Content-Length".into())
            })?;

            let filename = resolve_filename(&spec, report.filename.as_deref())?;
            validate_filename(&filename).map_err(|e| Error::Other(format!("filename: {e}")))?;
            let filename =
                apply_on_file_exists(&spec.target_dir, filename, spec.on_file_exists_override)?;

            let cfg = effective_plan_config(&spec, &defaults)
                .map_err(|e| Error::Other(format!("plan config: {e}")))?;
            // `plan_pieces` считает и раскладку, но геометрия piece'ов
            // теперь арифметическая (см. LocalPieceStorage): нам нужен
            // только `piece_size`. Список кусков `plan.pieces`
            // не используется и дропается.
            let piece_size = plan_pieces(total_size, cfg).piece_size;
            let guard = RangeGuard::from_report(&report);
            let accepts_ranges = report.accepts_ranges;

            // Персистим inspect-поля в `file_settings` ДО открытия
            // хранилища: `LocalPieceStorage::open` сверит их через
            // `get_inspect_fields` при принятии решения «resume vs fresh».
            files_repo
                .set_inspect_fields(
                    id,
                    total_size,
                    piece_size,
                    report.etag.clone(),
                    report.last_modified.clone(),
                )
                .await?;

            let storage = LocalPieceStorage::open(
                &spec.target_dir,
                &filename,
                id,
                total_size,
                piece_size,
                pieces_repo,
                files_repo,
            )
            .await?;

            Ok(PreparedDownload {
                storage,
                total_size,
                piece_size,
                accepts_ranges,
                guard,
                resolved_filename: filename,
            })
        }
    }
}

/// Определить имя файла: `spec.filename` → `InspectReport.filename` →
/// последний сегмент URL-пути.
fn resolve_filename(spec: &DownloadSpec, from_report: Option<&str>) -> Result<String> {
    if let Some(name) = &spec.filename {
        return Ok(name.clone());
    }
    if let Some(name) = from_report {
        return Ok(name.to_owned());
    }
    filename_from_url(&spec.url).ok_or_else(|| {
        Error::Other(format!(
            "cannot derive filename from URL {url}",
            url = spec.url
        ))
    })
}

/// Применить политику `on_file_exists_override`: если целевой файл уже
/// лежит в `target_dir`, либо подобрать свободное имя (Rename), либо
/// удалить его (Overwrite), либо вернуть `Error::FileExists` (Unspecified).
fn apply_on_file_exists(
    target_dir: &Path,
    filename: String,
    policy: OnFileExistsOverride,
) -> Result<String> {
    let target = target_dir.join(&filename);
    if !target.exists() {
        return Ok(filename);
    }
    match policy {
        OnFileExistsOverride::Unspecified => Err(Error::FileExists { path: target }),
        OnFileExistsOverride::Overwrite => {
            std::fs::remove_file(&target)
                .map_err(|e| Error::Other(format!("overwrite {}: {e}", target.display())))?;
            Ok(filename)
        }
        OnFileExistsOverride::Rename => pick_free_name(target_dir, &filename),
    }
}

/// Подобрать `<stem> (N).<ext>` (или `<name> (N)` без расширения),
/// начиная с `N = 1`, пока не найдётся несуществующее имя.
fn pick_free_name(target_dir: &Path, original: &str) -> Result<String> {
    let as_path = PathBuf::from(original);
    let stem = as_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(original)
        .to_owned();
    let ext = as_path
        .extension()
        .and_then(|s| s.to_str())
        .map(str::to_owned);
    for n in 1u32..=10_000 {
        let candidate = match &ext {
            Some(e) => format!("{stem} ({n}).{e}"),
            None => format!("{stem} ({n})"),
        };
        if !target_dir.join(&candidate).exists() {
            return Ok(candidate);
        }
    }
    Err(Error::Other(format!(
        "cannot pick free filename for {original} in {}",
        target_dir.display()
    )))
}

fn filename_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = after_scheme.split_once('/').map(|(_, rest)| rest)?;
    let path = path.split(['?', '#']).next().unwrap_or("");
    let last = path.rsplit('/').find(|s| !s.is_empty())?;
    Some(last.to_owned())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use async_trait::async_trait;
    use brook_core::{
        Download,
        DownloadSpec,
        InspectError,
        InspectReport,
        THttpInspect,
        TPieceStorageFactory,
        TQueueStore,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::storage::db::SharedDb;

    struct MockInspect {
        report: InspectReport,
    }

    #[async_trait]
    impl THttpInspect for MockInspect {
        async fn inspect(&self, _url: &str) -> std::result::Result<InspectReport, InspectError> {
            Ok(self.report.clone())
        }
    }

    fn defaults() -> DownloadDefaults {
        DownloadDefaults {
            workers: 4,
            piece_target_count: 128,
            piece_size_min: 16 * 1024 * 1024,
            piece_size_max: 128 * 1024 * 1024,
        }
    }

    fn inspect_with(total: Option<u64>, filename: Option<&str>) -> Arc<MockInspect> {
        Arc::new(MockInspect {
            report: InspectReport {
                total_size: total,
                accepts_ranges: true,
                etag: Some("\"abc\"".into()),
                last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
                filename: filename.map(str::to_owned),
                effective_url: None,
            },
        })
    }

    /// Собирает фабрику поверх in-memory `SharedDb` и заранее регистрирует
    /// одну загрузку в `files` (FK требует, чтобы строка существовала
    /// до `set_inspect_fields`).
    async fn build<I: THttpInspect + ?Sized + Send + Sync + 'static>(
        inspect: Arc<I>,
        spec: &DownloadSpec,
    ) -> (
        SharedDb,
        Arc<SqliteFileRepository>,
        Arc<SqlitePieceRepository>,
        DownloadId,
        LocalPieceStorageFactory<I>,
    ) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = Arc::new(SqliteFileRepository::new(db.clone()));
        let pieces = Arc::new(SqlitePieceRepository::new(db.clone()));
        let d = Download::new(DownloadId::new(), spec.clone());
        let id = d.id;
        files.insert(&d).await.unwrap();
        let factory = LocalPieceStorageFactory::new(
            inspect,
            defaults(),
            Arc::clone(&pieces),
            Arc::clone(&files),
        );
        (db, files, pieces, id, factory)
    }

    #[tokio::test]
    async fn prepare_uses_spec_filename_first() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/path/server.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("explicit.bin".into()),
            workers: 2,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        };
        let (_db, _files, _pieces, id, factory) = build(
            inspect_with(Some(1024 * 1024), Some("from-header.bin")),
            &spec,
        )
        .await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "explicit.bin");
        assert_eq!(prepared.total_size, 1024 * 1024);
        assert!(prepared.accepts_ranges);
        assert_eq!(prepared.guard, Some(RangeGuard::Etag("\"abc\"".into())));
    }

    #[tokio::test]
    async fn prepare_falls_back_to_report_filename() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/path/server.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: None,
            workers: 2,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(2048), Some("from-header.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "from-header.bin");
    }

    #[tokio::test]
    async fn prepare_falls_back_to_url_tail() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/path/server.bin?x=1".into(),
            target_dir: dir.path().to_path_buf(),
            filename: None,
            workers: 2,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(2048), None), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "server.bin");
    }

    #[tokio::test]
    async fn prepare_errors_without_content_length() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec::new("https://host/f.bin", dir.path());
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(None, Some("f.bin")), &spec).await;
        let err = match factory.prepare(id, &spec).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("Content-Length"), "{err}");
    }

    #[tokio::test]
    async fn prepare_respects_piece_size_override() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("f.bin".into()),
            workers: 1,
            piece_target_count: Some(64),
            piece_size_min: Some(1024 * 1024),
            piece_size_max: Some(1024 * 1024),
            on_file_exists_override: Default::default(),
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(64 * 1024 * 1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.piece_size, 1024 * 1024);
    }

    #[tokio::test]
    async fn prepare_rejects_invalid_power_of_two() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("f.bin".into()),
            workers: 1,
            piece_target_count: Some(8),
            piece_size_min: Some(3 * 1024 * 1024),
            piece_size_max: Some(16 * 1024 * 1024),
            on_file_exists_override: Default::default(),
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(4 * 1024 * 1024), Some("f.bin")), &spec).await;
        let err = match factory.prepare(id, &spec).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("power of two"), "{err}");
    }

    #[tokio::test]
    async fn prepare_persists_inspect_fields() {
        let dir = tempdir().unwrap();
        let spec = DownloadSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("f.bin".into()),
            workers: 1,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: Default::default(),
        };
        let (_db, files, _pieces, id, factory) =
            build(inspect_with(Some(64 * 1024 * 1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();

        let got = files.get_inspect_fields(id).await.unwrap().unwrap();
        assert_eq!(got.total_size, 64 * 1024 * 1024);
        assert_eq!(got.piece_size, prepared.piece_size);
        assert_eq!(got.etag.as_deref(), Some("\"abc\""));
        assert_eq!(
            got.last_modified.as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
    }

    #[test]
    fn filename_from_url_strips_query_and_fragment() {
        assert_eq!(
            filename_from_url("https://a/b/c/d.bin?x=1#y").as_deref(),
            Some("d.bin")
        );
        assert_eq!(
            filename_from_url("https://a/b/c/d.bin").as_deref(),
            Some("d.bin")
        );
        assert_eq!(filename_from_url("https://a/").as_deref(), None);
        assert_eq!(filename_from_url("https://a").as_deref(), None);
    }

    // Use PathBuf to silence unused-import lints across platforms.
    #[allow(dead_code)]
    fn _unused(_: PathBuf) {}

    fn spec_for(dir: &Path, filename: &str, policy: OnFileExistsOverride) -> DownloadSpec {
        DownloadSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.to_path_buf(),
            filename: Some(filename.into()),
            workers: 1,
            piece_target_count: None,
            piece_size_min: None,
            piece_size_max: None,
            on_file_exists_override: policy,
        }
    }

    #[tokio::test]
    async fn on_file_exists_unspecified_errors_when_target_present() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("f.bin"), b"old").unwrap();
        let spec = spec_for(dir.path(), "f.bin", OnFileExistsOverride::Unspecified);
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let err = match factory.prepare(id, &spec).await {
            Err(e) => e,
            Ok(_) => panic!("expected FileExists"),
        };
        assert!(
            matches!(err, brook_core::Error::FileExists { .. }),
            "expected FileExists, got {err:?}"
        );
        assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), b"old");
    }

    #[tokio::test]
    async fn on_file_exists_rename_picks_free_candidate() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("f.bin"), b"old").unwrap();
        std::fs::write(dir.path().join("f (1).bin"), b"other").unwrap();
        let spec = spec_for(dir.path(), "f.bin", OnFileExistsOverride::Rename);
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "f (2).bin");
        assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), b"old");
        assert_eq!(
            std::fs::read(dir.path().join("f (1).bin")).unwrap(),
            b"other"
        );
    }

    #[tokio::test]
    async fn on_file_exists_overwrite_removes_existing_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("f.bin"), b"old").unwrap();
        let spec = spec_for(dir.path(), "f.bin", OnFileExistsOverride::Overwrite);
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "f.bin");
        assert!(!dir.path().join("f.bin").exists());
        assert!(dir.path().join("f.bin.data.brook").exists());
    }

    #[tokio::test]
    async fn on_file_exists_unspecified_passes_when_target_absent() {
        let dir = tempdir().unwrap();
        let spec = spec_for(dir.path(), "f.bin", OnFileExistsOverride::Unspecified);
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "f.bin");
    }

    #[test]
    fn pick_free_name_handles_no_extension() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("foo"), b"").unwrap();
        assert_eq!(pick_free_name(dir.path(), "foo").unwrap(), "foo (1)");
    }

    #[test]
    fn pick_free_name_uses_last_extension() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("a.tar.gz"), b"").unwrap();
        assert_eq!(
            pick_free_name(dir.path(), "a.tar.gz").unwrap(),
            "a.tar (1).gz"
        );
    }
}
