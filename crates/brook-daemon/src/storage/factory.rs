//! Фабрика [`TPieceStorageFactory`] — composition-layer между HTTP-inspect,
//! расчётом `piece_size` и файловым [`LocalPieceStorage`].
//!
//! `DownloadManager` требует `Arc<impl TPieceStorageFactory>`; без этого
//! звена он не может «подготовить» загрузку перед запуском engine. Сама
//! фабрика — чистый клей:
//!
//! ```text
//! inspect(url)
//!   → resolve_filename (+ проверка конфликта имени)
//!   → effective_plan_config + plan_pieces → piece_size
//!   → files_repo.set_inspect_fields(id, total_size, piece_size, etag, last_modified)
//!   → LocalPieceStorage::open(id, total_size, piece_size, …)
//! ```
//!
//! ## Политика конфликта имени файла
//!
//! Хардкодится как **ошибка**: если целевой файл уже лежит в
//! `target_dir`, фабрика возвращает [`Error::FileExists`]. Подбор
//! свободного имени — ответственность клиента (TUI видит ошибку через
//! `tonic::Code::AlreadyExists` и сам ретраит `Add` с явным
//! `filename`). Настройки в `brook.yaml` для этого нет.

use std::sync::Arc;

use brook_core::{
    Error,
    FileId,
    FileSpec,
    PreparedDownload,
    PreparedMode,
    RangeGuard,
    Result,
    THttpInspect,
    TPathPolicy,
    TPieceStorageFactory,
};

use super::files::SqliteFileRepository;
use super::local::{
    LocalPieceStorage,
    LocalStreamStorage,
};
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
///
/// Политика путей ([`TPathPolicy`]) обязательна: любой `spec.target_dir`
/// проходит через неё до первого I/O. В тестах подставляется no-op-
/// реализация (`AllowAnyPath`), в проде — `ClampedPathPolicy` с
/// корнем из `--directory`.
pub struct LocalPieceStorageFactory<I: THttpInspect + ?Sized, P: TPathPolicy + ?Sized> {
    inspect: Arc<I>,
    defaults: DownloadDefaults,
    pieces_repo: Arc<SqlitePieceRepository>,
    files_repo: Arc<SqliteFileRepository>,
    policy: Arc<P>,
}

impl<I: THttpInspect + ?Sized, P: TPathPolicy + ?Sized> LocalPieceStorageFactory<I, P> {
    pub fn new(
        inspect: Arc<I>,
        defaults: DownloadDefaults,
        pieces_repo: Arc<SqlitePieceRepository>,
        files_repo: Arc<SqliteFileRepository>,
        policy: Arc<P>,
    ) -> Self {
        Self {
            inspect,
            defaults,
            pieces_repo,
            files_repo,
            policy,
        }
    }
}

impl<I, P> TPieceStorageFactory for LocalPieceStorageFactory<I, P>
where
    I: THttpInspect + ?Sized + Send + Sync + 'static,
    P: TPathPolicy + ?Sized + 'static,
{
    type Storage = LocalPieceStorage;
    type StreamStorage = LocalStreamStorage;

    fn prepare(
        &self,
        id: FileId,
        spec: &FileSpec,
    ) -> impl std::future::Future<
        Output = Result<PreparedDownload<Self::Storage, Self::StreamStorage>>,
    > + Send {
        // Клонируем всё, что нужно для async-блока: фабрика может быть
        // живее отдельного `prepare`-вызова.
        let inspect = Arc::clone(&self.inspect);
        let defaults = self.defaults;
        let pieces_repo = Arc::clone(&self.pieces_repo);
        let files_repo = Arc::clone(&self.files_repo);
        let policy = Arc::clone(&self.policy);
        let spec = spec.clone();
        async move {
            // Клэмп target_dir первым — до любого сетевого/дискового I/O.
            // Escape — ошибка, которая маппится в PermissionDenied.
            let target_dir = policy.check_target_dir(&spec.target_dir)?;

            let report = inspect
                .inspect(&spec.url)
                .await
                .map_err(|e| Error::Other(format!("inspect: {e}")))?;

            let filename = resolve_filename(&spec, report.filename.as_deref())?;
            validate_filename(&filename).map_err(|e| Error::Other(format!("filename: {e}")))?;
            if target_dir.join(&filename).exists() {
                return Err(Error::FileExists { filename });
            }

            let guard = RangeGuard::from_report(&report);
            let effective_url = report.effective_url.clone();

            match report.total_size {
                Some(total_size) => {
                    let cfg = effective_plan_config(&defaults)
                        .map_err(|e| Error::Other(format!("plan config: {e}")))?;
                    // `plan_pieces` считает раскладку, но геометрия piece'ов
                    // арифметическая: нужен только `piece_size`.
                    let piece_size = plan_pieces(total_size, cfg).piece_size;
                    let accepts_ranges = report.accepts_ranges;

                    files_repo
                        .set_inspect_fields(
                            id,
                            Some(total_size),
                            Some(piece_size),
                            report.etag.clone(),
                            report.last_modified.clone(),
                            effective_url.clone(),
                        )
                        .await?;

                    let storage = LocalPieceStorage::open(
                        &target_dir,
                        &filename,
                        id,
                        total_size,
                        piece_size,
                        pieces_repo,
                        files_repo,
                    )
                    .await?;

                    Ok(PreparedDownload {
                        mode: PreparedMode::Known {
                            total_size,
                            piece_size,
                            accepts_ranges,
                            guard,
                        },
                        piece_storage: Some(storage),
                        stream_storage: None,
                        resolved_filename: filename,
                        effective_url,
                    })
                }
                None => {
                    // Streaming-режим: ни размера, ни piece-нарезки. Один
                    // воркер, append-mode. Resume не поддерживается.
                    files_repo
                        .set_inspect_fields(
                            id,
                            None,
                            None,
                            report.etag.clone(),
                            report.last_modified.clone(),
                            effective_url.clone(),
                        )
                        .await?;
                    let stream = LocalStreamStorage::open_streaming(&target_dir, &filename).await?;
                    Ok(PreparedDownload {
                        mode: PreparedMode::Streaming,
                        piece_storage: None,
                        stream_storage: Some(stream),
                        resolved_filename: filename,
                        effective_url,
                    })
                }
            }
        }
    }
}

/// Определить имя файла: `spec.filename` → `InspectReport.filename` →
/// последний сегмент URL-пути.
fn resolve_filename(spec: &FileSpec, from_report: Option<&str>) -> Result<String> {
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

fn filename_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = after_scheme.split_once('/').map(|(_, rest)| rest)?;
    let path = path.split(['?', '#']).next().unwrap_or("");
    let last = path.rsplit('/').find(|s| !s.is_empty())?;
    Some(last.to_owned())
}

#[cfg(test)]
mod tests {
    use std::path::{
        Path,
        PathBuf,
    };

    use async_trait::async_trait;
    use brook_core::{
        File,
        FileSpec,
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

    /// No-op политика для юнит-тестов: отдаёт путь как есть, без sandbox-
    /// проверок. Прод использует `ClampedPathPolicy`, но для проверки
    /// именно фабричной логики sandbox только мешает.
    struct AllowAnyPath;
    impl brook_core::TPathPolicy for AllowAnyPath {
        fn check_target_dir(&self, target_dir: &Path) -> brook_core::Result<PathBuf> {
            Ok(target_dir.to_path_buf())
        }
    }

    fn defaults() -> DownloadDefaults {
        DownloadDefaults {
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
        spec: &FileSpec,
    ) -> (
        SharedDb,
        Arc<SqliteFileRepository>,
        Arc<SqlitePieceRepository>,
        FileId,
        LocalPieceStorageFactory<I, AllowAnyPath>,
    ) {
        let db = SharedDb::open_in_memory().unwrap();
        let files = Arc::new(SqliteFileRepository::new(db.clone()));
        let pieces = Arc::new(SqlitePieceRepository::new(db.clone()));
        let d = File::new(FileId::new(), spec.clone());
        let id = d.id;
        files.insert(&d).await.unwrap();
        let factory = LocalPieceStorageFactory::new(
            inspect,
            defaults(),
            Arc::clone(&pieces),
            Arc::clone(&files),
            Arc::new(AllowAnyPath),
        );
        (db, files, pieces, id, factory)
    }

    #[tokio::test]
    async fn prepare_uses_spec_filename_first() {
        let dir = tempdir().unwrap();
        let spec = FileSpec {
            url: "https://host/path/server.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("explicit.bin".into()),
        };
        let (_db, _files, _pieces, id, factory) = build(
            inspect_with(Some(1024 * 1024), Some("from-header.bin")),
            &spec,
        )
        .await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "explicit.bin");
        match prepared.mode {
            PreparedMode::Known {
                total_size,
                accepts_ranges,
                guard,
                ..
            } => {
                assert_eq!(total_size, 1024 * 1024);
                assert!(accepts_ranges);
                assert_eq!(guard, Some(RangeGuard::Etag("\"abc\"".into())));
            }
            PreparedMode::Streaming => panic!("expected Known"),
        }
    }

    #[tokio::test]
    async fn prepare_falls_back_to_report_filename() {
        let dir = tempdir().unwrap();
        let spec = FileSpec {
            url: "https://host/path/server.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: None,
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(2048), Some("from-header.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "from-header.bin");
    }

    #[tokio::test]
    async fn prepare_falls_back_to_url_tail() {
        let dir = tempdir().unwrap();
        let spec = FileSpec {
            url: "https://host/path/server.bin?x=1".into(),
            target_dir: dir.path().to_path_buf(),
            filename: None,
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(2048), None), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "server.bin");
    }

    #[tokio::test]
    async fn prepare_allows_unknown_size() {
        let dir = tempdir().unwrap();
        let spec = FileSpec::new("https://host/f.bin", dir.path());
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(None, Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert!(
            matches!(prepared.mode, PreparedMode::Streaming),
            "expected Streaming mode, got {:?}",
            prepared.mode
        );
        assert!(prepared.piece_storage.is_none());
        assert!(prepared.stream_storage.is_some());
        assert_eq!(prepared.resolved_filename, "f.bin");
    }

    #[tokio::test]
    async fn prepare_uses_daemon_piece_size() {
        let dir = tempdir().unwrap();
        let spec = FileSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("f.bin".into()),
        };
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(64 * 1024 * 1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        // 64 MiB / 128 = 512 KiB → clamp(min=16 MiB) = 16 MiB.
        match prepared.mode {
            PreparedMode::Known { piece_size, .. } => assert_eq!(piece_size, 16 * 1024 * 1024),
            PreparedMode::Streaming => panic!("expected Known"),
        }
    }

    #[tokio::test]
    async fn prepare_persists_inspect_fields() {
        let dir = tempdir().unwrap();
        let spec = FileSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.path().to_path_buf(),
            filename: Some("f.bin".into()),
        };
        let (_db, files, _pieces, id, factory) =
            build(inspect_with(Some(64 * 1024 * 1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        let prepared_piece_size = match prepared.mode {
            PreparedMode::Known { piece_size, .. } => piece_size,
            PreparedMode::Streaming => panic!("expected Known"),
        };

        let got = files.get_inspect_fields(id).await.unwrap().unwrap();
        assert_eq!(got.total_size, Some(64 * 1024 * 1024));
        assert_eq!(got.piece_size, Some(prepared_piece_size));
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

    fn spec_for(dir: &Path, filename: &str) -> FileSpec {
        FileSpec {
            url: "https://host/f.bin".into(),
            target_dir: dir.to_path_buf(),
            filename: Some(filename.into()),
        }
    }

    #[tokio::test]
    async fn errors_with_file_exists_when_target_present() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("f.bin"), b"old").unwrap();
        let spec = spec_for(dir.path(), "f.bin");
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let err = match factory.prepare(id, &spec).await {
            Err(e) => e,
            Ok(_) => panic!("expected FileExists error"),
        };
        assert!(
            matches!(&err, Error::FileExists { filename } if filename == "f.bin"),
            "got {err:?}"
        );
        // Существующий файл не трогаем — политика строго «ошибка».
        assert_eq!(std::fs::read(dir.path().join("f.bin")).unwrap(), b"old");
    }

    #[tokio::test]
    async fn keeps_filename_when_target_absent() {
        let dir = tempdir().unwrap();
        let spec = spec_for(dir.path(), "f.bin");
        let (_db, _files, _pieces, id, factory) =
            build(inspect_with(Some(1024), Some("f.bin")), &spec).await;
        let prepared = factory.prepare(id, &spec).await.unwrap();
        assert_eq!(prepared.resolved_filename, "f.bin");
    }
}
