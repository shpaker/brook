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
    File,
    FileId,
    FileLifecycleEvent,
    FileSpec,
    FileStatus,
    Progress,
    ProgressEvent,
};
use brook_proto::brook::v1 as proto;
use tonic::Status;

// ─── FileId ──────────────────────────────────────────────────────────────

/// Core → proto: сериализуем UUID в строку.
pub fn id_to_proto(id: FileId) -> proto::FileId {
    proto::FileId {
        value: id.to_string(),
    }
}

/// Proto → core: парсим UUID из строки; пустая или невалидная → 400.
pub fn id_from_proto(id: &proto::FileId) -> Result<FileId, Status> {
    FileId::from_str(&id.value)
        .map_err(|e| Status::invalid_argument(format!("invalid file id: {e}")))
}

/// Опционально распакованный id — для запросов, где `id` обёрнут в Option
/// на стороне prost'а (все message-поля optional по-умолчанию в proto3).
pub fn id_from_proto_opt(id: Option<&proto::FileId>) -> Result<FileId, Status> {
    let id = id.ok_or_else(|| Status::invalid_argument("file id is required"))?;
    id_from_proto(id)
}

// ─── FileSpec ────────────────────────────────────────────────────────────

pub fn spec_from_proto(s: proto::FileSpec) -> Result<FileSpec, Status> {
    if s.url.is_empty() {
        return Err(Status::invalid_argument("spec.url is required"));
    }
    if s.target_dir.is_empty() {
        return Err(Status::invalid_argument("spec.target_dir is required"));
    }
    let filename = s.filename.filter(|f| !f.is_empty());
    Ok(FileSpec {
        url: s.url,
        target_dir: PathBuf::from(s.target_dir),
        filename,
    })
}

pub fn spec_to_proto(s: &FileSpec) -> proto::FileSpec {
    proto::FileSpec {
        url: s.url.clone(),
        // `to_string_lossy`: на macOS/Linux пути обычно UTF-8; если вдруг
        // нет — теряем невалидные байты. Для MVP (только macOS) приемлемо.
        target_dir: s.target_dir.to_string_lossy().into_owned(),
        filename: s.filename.clone(),
    }
}

// ─── Status ──────────────────────────────────────────────────────────────

pub fn status_to_proto(s: FileStatus) -> proto::FileStatus {
    match s {
        FileStatus::Pending => proto::FileStatus::Pending,
        FileStatus::Running => proto::FileStatus::Running,
        FileStatus::Paused => proto::FileStatus::Paused,
        FileStatus::Retrying => proto::FileStatus::Retrying,
        FileStatus::Done => proto::FileStatus::Done,
        FileStatus::Failed => proto::FileStatus::Failed,
        FileStatus::Cancelled => proto::FileStatus::Cancelled,
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

// ─── File ────────────────────────────────────────────────────────────────

pub fn file_to_proto(d: &File) -> proto::File {
    proto::File {
        id: Some(id_to_proto(d.id)),
        spec: Some(spec_to_proto(&d.spec)),
        status: status_to_proto(d.status) as i32,
        attempt: d.attempt,
        error: d.error.clone(),
        created_at: Some(systime_to_proto(d.created_at)),
        updated_at: Some(systime_to_proto(d.updated_at)),
    }
}

// ─── Events ──────────────────────────────────────────────────────────────

pub fn lifecycle_event_to_proto(ev: &FileLifecycleEvent) -> proto::FileEvent {
    use proto::file_event::Kind;
    let kind = match ev {
        FileLifecycleEvent::StatusChanged { id, status } => {
            Kind::StatusChanged(proto::StatusChangedEvent {
                id: Some(id_to_proto(*id)),
                status: status_to_proto(*status) as i32,
            })
        }
        FileLifecycleEvent::Completed { id } => Kind::Completed(proto::CompletedEvent {
            id: Some(id_to_proto(*id)),
        }),
        FileLifecycleEvent::Failed { id, error } => Kind::Failed(proto::FailedEvent {
            id: Some(id_to_proto(*id)),
            error: error.clone(),
        }),
        FileLifecycleEvent::Snapshot { file } => Kind::Snapshot(proto::SnapshotEvent {
            file: Some(file_to_proto(file)),
        }),
    };
    proto::FileEvent { kind: Some(kind) }
}

/// Обёртка: синтетический `Snapshot` поверх уже имеющегося `File`.
/// Используется для initial-stream в `WatchFile` и для реконсиляции при
/// `broadcast::RecvError::Lagged`.
pub fn snapshot_event(d: &File) -> proto::FileEvent {
    proto::FileEvent {
        kind: Some(proto::file_event::Kind::Snapshot(proto::SnapshotEvent {
            file: Some(file_to_proto(d)),
        })),
    }
}

pub fn progress_event_to_proto(ev: &ProgressEvent) -> proto::ProgressTick {
    match ev {
        ProgressEvent::Tick { id, progress } => progress_tick_from(*id, progress),
    }
}

fn progress_tick_from(id: FileId, p: &Progress) -> proto::ProgressTick {
    proto::ProgressTick {
        file_id: Some(id_to_proto(id)),
        progress: progress_ratio(p),
        bytes_done: p.bytes_done,
        bytes_total: p.bytes_total,
        speed_bps: p.speed_bps,
        eta_secs: p.eta_secs,
    }
}

fn progress_ratio(p: &Progress) -> f64 {
    if p.bytes_total == 0 {
        0.0
    } else {
        (p.bytes_done as f64 / p.bytes_total as f64).clamp(0.0, 1.0)
    }
}

// ─── Error mapping ───────────────────────────────────────────────────────

/// `brook_core::Error` → `tonic::Status`. Отдельные варианты получают
/// осмысленные коды, «разное остальное» падает в `internal`.
pub fn core_err_to_status(e: brook_core::Error) -> Status {
    use brook_core::Error as E;
    match e {
        E::NotFound => Status::not_found("file not found"),
        E::SourceMutated => Status::aborted("source changed while downloading"),
        E::TruncatedResponse => Status::data_loss("truncated response from source"),
        E::FileExists { filename } => {
            Status::already_exists(format!("file already exists: {filename}"))
        }
        E::Io(ref io) => Status::internal(format!("io error: {io}")),
        E::Other(msg) => {
            // Пользовательские precondition-ошибки (`pause/resume` у
            // терминальной загрузки и подобные) — `failed_precondition`,
            // остальное — `internal`.
            if msg.contains("terminal") {
                Status::failed_precondition(msg)
            } else {
                Status::internal(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use brook_core::FileSpec as CoreSpec;

    use super::*;

    #[test]
    fn id_roundtrip() {
        let id = FileId::new();
        let p = id_to_proto(id);
        let back = id_from_proto(&p).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn id_from_empty_is_error() {
        let p = proto::FileId {
            value: String::new(),
        };
        assert!(id_from_proto(&p).is_err());
    }

    #[test]
    fn id_from_garbage_is_error() {
        let p = proto::FileId {
            value: "not-a-uuid".into(),
        };
        assert!(id_from_proto(&p).is_err());
    }

    #[test]
    fn spec_from_proto_validates_and_fills_defaults() {
        let p = proto::FileSpec {
            url: "https://example.com/f".into(),
            target_dir: "/tmp".into(),
            ..Default::default()
        };
        let s = spec_from_proto(p).unwrap();
        assert_eq!(s.url, "https://example.com/f");
        assert_eq!(s.target_dir, PathBuf::from("/tmp"));
        assert_eq!(s.filename, None);
    }

    #[test]
    fn spec_from_proto_rejects_empty() {
        let p = proto::FileSpec {
            target_dir: "/tmp".into(),
            ..Default::default()
        };
        assert_eq!(
            spec_from_proto(p).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn spec_from_proto_drops_empty_filename() {
        let p = proto::FileSpec {
            url: "https://x".into(),
            target_dir: "/tmp".into(),
            filename: Some(String::new()),
        };
        let s = spec_from_proto(p).unwrap();
        assert_eq!(s.filename, None);
    }

    #[test]
    fn file_roundtrip_through_proto_keeps_fields() {
        let id = FileId::new();
        let mut d = File::new(id, CoreSpec::new("https://x", "/tmp"));
        d.status = FileStatus::Running;
        d.attempt = 3;
        d.error = Some("boom".into());
        let p = file_to_proto(&d);
        assert_eq!(p.id.unwrap().value, id.to_string());
        assert_eq!(p.status, proto::FileStatus::Running as i32);
        assert_eq!(p.attempt, 3);
        assert_eq!(p.error.as_deref(), Some("boom"));
    }

    #[test]
    fn lifecycle_event_mapper_covers_all_variants() {
        let id = FileId::new();
        let d = File::new(id, CoreSpec::new("https://x", "/tmp"));
        let variants = [
            FileLifecycleEvent::StatusChanged {
                id,
                status: FileStatus::Paused,
            },
            FileLifecycleEvent::Completed { id },
            FileLifecycleEvent::Failed {
                id,
                error: "e".into(),
            },
            FileLifecycleEvent::Snapshot { file: Box::new(d) },
        ];
        for ev in &variants {
            let p = lifecycle_event_to_proto(ev);
            assert!(p.kind.is_some(), "kind must be set for {ev:?}");
        }
    }

    #[test]
    fn progress_tick_maps_ratio_and_fields() {
        let id = FileId::new();
        let tick = progress_event_to_proto(&ProgressEvent::Tick {
            id,
            progress: Progress {
                bytes_done: 50,
                bytes_total: 100,
                pieces_done: 1,
                pieces_total: 2,
                speed_bps: 123.4,
                eta_secs: Some(42),
            },
        });
        assert_eq!(tick.file_id.unwrap().value, id.to_string());
        assert!((tick.progress - 0.5).abs() < 1e-9);
        assert_eq!(tick.bytes_done, 50);
        assert_eq!(tick.bytes_total, 100);
        assert_eq!(tick.eta_secs, Some(42));
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
            assert_ne!(status_to_proto(s), proto::FileStatus::Unspecified);
        }
    }

    #[test]
    fn core_err_not_found_to_status() {
        let st = core_err_to_status(brook_core::Error::NotFound);
        assert_eq!(st.code(), tonic::Code::NotFound);
    }

    #[test]
    fn core_err_terminal_to_failed_precondition() {
        let st = core_err_to_status(brook_core::Error::Other("download is terminal".into()));
        assert_eq!(st.code(), tonic::Code::FailedPrecondition);
    }
}
