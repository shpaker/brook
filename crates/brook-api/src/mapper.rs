//! Бидиректные мапперы между proto-типами (`brook.v1.*`) и доменом
//! `brook-core`.
//!
//! Никакой бизнес-логики — только перекладывание полей. Валидация
//! проходит на границе (например, парсинг UUID из строки). Ошибки
//! возвращаем как `tonic::Status::invalid_argument`, чтобы любой
//! невалидный proto-запрос сразу упирался в 400-like отказ, а не
//! утекал в ядро.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::SystemTime;

use brook_core::{
    Download,
    DownloadEvent,
    DownloadId,
    DownloadSpec,
    FileStatus,
    OnFileExistsOverride,
    Progress,
    default_workers,
};
use brook_proto::brook::v1 as proto;
use tonic::Status;

// ─── DownloadId ──────────────────────────────────────────────────────────

/// Core → proto: сериализуем UUID в строку.
pub fn id_to_proto(id: DownloadId) -> proto::DownloadId {
    proto::DownloadId {
        value: id.to_string(),
    }
}

/// Proto → core: парсим UUID из строки; пустая или невалидная → 400.
pub fn id_from_proto(id: &proto::DownloadId) -> Result<DownloadId, Status> {
    DownloadId::from_str(&id.value)
        .map_err(|e| Status::invalid_argument(format!("invalid download id: {e}")))
}

/// Опционально распакованный id — для запросов, где `id` обёрнут в Option
/// на стороне prost'а (все message-поля optional по-умолчанию в proto3).
pub fn id_from_proto_opt(id: Option<&proto::DownloadId>) -> Result<DownloadId, Status> {
    let id = id.ok_or_else(|| Status::invalid_argument("download id is required"))?;
    id_from_proto(id)
}

// ─── DownloadSpec ────────────────────────────────────────────────────────

pub fn spec_from_proto(s: proto::DownloadSpec) -> Result<DownloadSpec, Status> {
    if s.url.is_empty() {
        return Err(Status::invalid_argument("spec.url is required"));
    }
    if s.target_dir.is_empty() {
        return Err(Status::invalid_argument("spec.target_dir is required"));
    }
    // `0` у `workers` — «подставь дефолт». Выделенный флаг не нужен:
    // запустить загрузку с нулём воркеров — бессмысленно.
    let workers = if s.workers == 0 {
        default_workers()
    } else {
        s.workers
    };
    let filename = s.filename.filter(|f| !f.is_empty());
    let on_file_exists_override =
        match proto::OnFileExistsOverride::try_from(s.on_file_exists_override) {
            Ok(proto::OnFileExistsOverride::Rename) => OnFileExistsOverride::Rename,
            Ok(proto::OnFileExistsOverride::Overwrite) => OnFileExistsOverride::Overwrite,
            // Неизвестный/Unspecified — дефолт.
            _ => OnFileExistsOverride::Unspecified,
        };
    Ok(DownloadSpec {
        url: s.url,
        target_dir: PathBuf::from(s.target_dir),
        filename,
        workers,
        piece_target_count: s.piece_target_count,
        piece_size_min: s.piece_size_min,
        piece_size_max: s.piece_size_max,
        on_file_exists_override,
    })
}

fn override_to_proto(o: OnFileExistsOverride) -> proto::OnFileExistsOverride {
    match o {
        OnFileExistsOverride::Unspecified => proto::OnFileExistsOverride::Unspecified,
        OnFileExistsOverride::Rename => proto::OnFileExistsOverride::Rename,
        OnFileExistsOverride::Overwrite => proto::OnFileExistsOverride::Overwrite,
    }
}

pub fn spec_to_proto(s: &DownloadSpec) -> proto::DownloadSpec {
    proto::DownloadSpec {
        url: s.url.clone(),
        // `to_string_lossy`: на macOS/Linux пути обычно UTF-8; если вдруг
        // нет — теряем невалидные байты. Для MVP (только macOS) приемлемо.
        target_dir: s.target_dir.to_string_lossy().into_owned(),
        filename: s.filename.clone(),
        workers: s.workers,
        piece_target_count: s.piece_target_count,
        piece_size_min: s.piece_size_min,
        piece_size_max: s.piece_size_max,
        on_file_exists_override: override_to_proto(s.on_file_exists_override) as i32,
    }
}

// ─── Status ───────────────────────────────────────────────────────────────

pub fn status_to_proto(s: FileStatus) -> proto::DownloadStatus {
    match s {
        FileStatus::Pending => proto::DownloadStatus::Pending,
        FileStatus::Running => proto::DownloadStatus::Running,
        FileStatus::Paused => proto::DownloadStatus::Paused,
        FileStatus::Retrying => proto::DownloadStatus::Retrying,
        FileStatus::Done => proto::DownloadStatus::Done,
        FileStatus::Failed => proto::DownloadStatus::Failed,
        FileStatus::Cancelled => proto::DownloadStatus::Cancelled,
    }
}

// ─── Progress ────────────────────────────────────────────────────────────

pub fn progress_to_proto(p: &Progress) -> proto::Progress {
    proto::Progress {
        bytes_done: p.bytes_done,
        bytes_total: p.bytes_total,
        pieces_done: p.pieces_done,
        pieces_total: p.pieces_total,
        speed_bps: p.speed_bps,
        eta_secs: p.eta_secs,
    }
}

// ─── Timestamps ──────────────────────────────────────────────────────────

/// `SystemTime` → `prost_types::Timestamp`. Времена до UNIX-эпохи не
/// ожидаются (наши записи создаются `SystemTime::now()`), но защищаемся:
/// на ошибке отдаём «нулевой» timestamp.
pub fn systime_to_proto(t: SystemTime) -> prost_types::Timestamp {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        },
    }
}

// ─── Download ────────────────────────────────────────────────────────────

pub fn download_to_proto(d: &Download) -> proto::Download {
    proto::Download {
        id: Some(id_to_proto(d.id)),
        spec: Some(spec_to_proto(&d.spec)),
        status: status_to_proto(d.status) as i32,
        progress: Some(progress_to_proto(&d.progress)),
        attempt: d.attempt,
        error: d.error.clone(),
        created_at: Some(systime_to_proto(d.created_at)),
        updated_at: Some(systime_to_proto(d.updated_at)),
    }
}

// ─── DownloadEvent ───────────────────────────────────────────────────────

pub fn event_to_proto(ev: &DownloadEvent) -> proto::Event {
    use proto::event::Kind;
    let kind = match ev {
        DownloadEvent::Progress { id, progress } => Kind::Progress(proto::ProgressEvent {
            id: Some(id_to_proto(*id)),
            progress: Some(progress_to_proto(progress)),
        }),
        DownloadEvent::StatusChanged { id, status } => {
            Kind::StatusChanged(proto::StatusChangedEvent {
                id: Some(id_to_proto(*id)),
                status: status_to_proto(*status) as i32,
            })
        }
        DownloadEvent::WorkerUpdate {
            id,
            piece_index,
            fraction,
        } => Kind::WorkerUpdate(proto::WorkerUpdateEvent {
            id: Some(id_to_proto(*id)),
            piece_index: *piece_index,
            fraction: *fraction,
        }),
        DownloadEvent::Completed { id } => Kind::Completed(proto::CompletedEvent {
            id: Some(id_to_proto(*id)),
        }),
        DownloadEvent::Failed { id, error } => Kind::Failed(proto::FailedEvent {
            id: Some(id_to_proto(*id)),
            error: error.clone(),
        }),
        DownloadEvent::Snapshot { download } => Kind::Snapshot(proto::SnapshotEvent {
            download: Some(download_to_proto(download)),
        }),
    };
    proto::Event { kind: Some(kind) }
}

/// Обёртка: синтетический `Snapshot` поверх уже имеющегося `Download`.
/// Используется для initial-stream в `Watch` и для реконсиляции при
/// `broadcast::RecvError::Lagged`.
pub fn snapshot_event(d: &Download) -> proto::Event {
    proto::Event {
        kind: Some(proto::event::Kind::Snapshot(proto::SnapshotEvent {
            download: Some(download_to_proto(d)),
        })),
    }
}

// ─── Error mapping ───────────────────────────────────────────────────────

/// `brook_core::Error` → `tonic::Status`. Отдельные варианты получают
/// осмысленные коды, «разное остальное» падает в `internal`.
pub fn core_err_to_status(e: brook_core::Error) -> Status {
    use brook_core::Error as E;
    match e {
        E::NotFound => Status::not_found("download not found"),
        E::FileExists { path } => {
            Status::already_exists(format!("target file already exists: {}", path.display()))
        }
        E::SourceMutated => Status::aborted("source changed while downloading"),
        E::TruncatedResponse => Status::data_loss("truncated response from source"),
        E::Io(ref io) => Status::internal(format!("io error: {io}")),
        E::Other(msg) => {
            // Частный случай: manager.remove(id) для активной загрузки
            // возвращает Error::Other("download is active, cancel before remove").
            // Это пользовательская ошибка — `failed_precondition`.
            if msg.contains("active") || msg.contains("terminal") {
                Status::failed_precondition(msg)
            } else {
                Status::internal(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use brook_core::DownloadSpec as CoreSpec;

    use super::*;

    #[test]
    fn id_roundtrip() {
        let id = DownloadId::new();
        let p = id_to_proto(id);
        let back = id_from_proto(&p).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn id_from_empty_is_error() {
        let p = proto::DownloadId {
            value: String::new(),
        };
        assert!(id_from_proto(&p).is_err());
    }

    #[test]
    fn id_from_garbage_is_error() {
        let p = proto::DownloadId {
            value: "not-a-uuid".into(),
        };
        assert!(id_from_proto(&p).is_err());
    }

    #[test]
    fn spec_from_proto_validates_and_fills_defaults() {
        let p = proto::DownloadSpec {
            url: "https://example.com/f".into(),
            target_dir: "/tmp".into(),
            workers: 0,
            ..Default::default()
        };
        let s = spec_from_proto(p).unwrap();
        assert_eq!(s.url, "https://example.com/f");
        assert_eq!(s.target_dir, PathBuf::from("/tmp"));
        assert_eq!(s.filename, None);
        assert_eq!(s.workers, default_workers());
    }

    #[test]
    fn spec_from_proto_rejects_empty() {
        let p = proto::DownloadSpec {
            target_dir: "/tmp".into(),
            workers: 2,
            ..Default::default()
        };
        assert_eq!(
            spec_from_proto(p).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn spec_from_proto_drops_empty_filename() {
        let p = proto::DownloadSpec {
            url: "https://x".into(),
            target_dir: "/tmp".into(),
            filename: Some(String::new()),
            workers: 1,
            ..Default::default()
        };
        let s = spec_from_proto(p).unwrap();
        assert_eq!(s.filename, None);
    }

    #[test]
    fn spec_piece_overrides_roundtrip() {
        let p = proto::DownloadSpec {
            url: "https://x".into(),
            target_dir: "/tmp".into(),
            workers: 1,
            piece_target_count: Some(64),
            piece_size_min: Some(8 * 1024 * 1024),
            piece_size_max: Some(256 * 1024 * 1024),
            ..Default::default()
        };
        let s = spec_from_proto(p.clone()).unwrap();
        assert_eq!(s.piece_target_count, Some(64));
        assert_eq!(s.piece_size_min, Some(8 * 1024 * 1024));
        assert_eq!(s.piece_size_max, Some(256 * 1024 * 1024));
        let back = spec_to_proto(&s);
        assert_eq!(back.piece_target_count, p.piece_target_count);
        assert_eq!(back.piece_size_min, p.piece_size_min);
        assert_eq!(back.piece_size_max, p.piece_size_max);
    }

    #[test]
    fn spec_piece_overrides_absent_by_default() {
        let p = proto::DownloadSpec {
            url: "https://x".into(),
            target_dir: "/tmp".into(),
            workers: 1,
            ..Default::default()
        };
        let s = spec_from_proto(p).unwrap();
        assert_eq!(s.piece_target_count, None);
        assert_eq!(s.piece_size_min, None);
        assert_eq!(s.piece_size_max, None);
    }

    #[test]
    fn download_roundtrip_through_proto_keeps_fields() {
        let id = DownloadId::new();
        let mut d = Download::new(id, CoreSpec::new("https://x", "/tmp"));
        d.status = FileStatus::Running;
        d.progress = Progress {
            bytes_done: 50,
            bytes_total: 100,
            pieces_done: 1,
            pieces_total: 2,
            speed_bps: 123.4,
            eta_secs: Some(42),
        };
        d.attempt = 3;
        d.error = Some("boom".into());
        let p = download_to_proto(&d);
        assert_eq!(p.id.unwrap().value, id.to_string());
        assert_eq!(p.status, proto::DownloadStatus::Running as i32);
        let pg = p.progress.unwrap();
        assert_eq!(pg.bytes_done, 50);
        assert_eq!(pg.pieces_total, 2);
        assert_eq!(pg.eta_secs, Some(42));
        assert_eq!(p.attempt, 3);
        assert_eq!(p.error.as_deref(), Some("boom"));
    }

    #[test]
    fn event_mapper_covers_all_variants() {
        let id = DownloadId::new();
        let d = Download::new(id, CoreSpec::new("https://x", "/tmp"));
        let variants = [
            DownloadEvent::Progress {
                id,
                progress: Progress::default(),
            },
            DownloadEvent::StatusChanged {
                id,
                status: FileStatus::Paused,
            },
            DownloadEvent::WorkerUpdate {
                id,
                piece_index: 1,
                fraction: 0.5,
            },
            DownloadEvent::Completed { id },
            DownloadEvent::Failed {
                id,
                error: "e".into(),
            },
            DownloadEvent::Snapshot {
                download: Box::new(d),
            },
        ];
        for ev in &variants {
            let p = event_to_proto(ev);
            assert!(p.kind.is_some(), "kind must be set for {ev:?}");
        }
    }

    #[test]
    fn state_enum_parity() {
        // Все 7 core-вариантов отображаются в не-Unspecified proto-значения.
        for s in [
            FileStatus::Pending,
            FileStatus::Running,
            FileStatus::Paused,
            FileStatus::Retrying,
            FileStatus::Done,
            FileStatus::Failed,
            FileStatus::Cancelled,
        ] {
            assert_ne!(status_to_proto(s), proto::DownloadStatus::Unspecified);
        }
    }

    #[test]
    fn core_err_not_found_to_status() {
        let st = core_err_to_status(brook_core::Error::NotFound);
        assert_eq!(st.code(), tonic::Code::NotFound);
    }

    #[test]
    fn core_err_active_to_failed_precondition() {
        let st = core_err_to_status(brook_core::Error::Other(
            "download is active, cancel before remove".into(),
        ));
        assert_eq!(st.code(), tonic::Code::FailedPrecondition);
    }
}
