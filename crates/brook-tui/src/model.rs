//! ViewModel клиента и связанные доменные структуры.
//!
//! ViewModel мутируется только UI-task'ом (см. `events.rs`). Все поля
//! публичные — ViewModel живёт целиком внутри крейта, инкапсуляция тут
//! только раздувала бы код.

use std::collections::HashMap;
use std::time::Instant;

use brook_proto::brook::v1 as proto;
use indexmap::IndexMap;

/// Снимок одного воркера для прогрессбара. Храним последний известный
/// `fraction` по piece_index — §6.3 рисует по одному сегменту на
/// активный кусок.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // `fraction` читается §6.3+ вариантом с прогрессом по сегменту
pub struct WorkerSegment {
    pub piece_index: u32,
    pub fraction: f32,
}

#[derive(Debug, Clone)]
pub struct DownloadRow {
    pub id: String,
    pub url: String,
    pub target_dir: String,
    pub filename: String,
    pub state: proto::DownloadState,
    pub progress: proto::Progress,
    pub attempt: u32,
    pub max_attempts: u32,
    pub error: Option<String>,
    /// Секунды с эпохи — достаточно для сортировки, без Timestamp-церемоний.
    pub updated_at: i64,
    /// Активные куски по piece_index. Завершившиеся piece'ы
    /// выкидываются (fraction >= 1.0), поэтому размер карты
    /// ограничен числом воркеров.
    pub workers: HashMap<u32, WorkerSegment>,
}

impl DownloadRow {
    pub fn from_snapshot(d: &proto::Download) -> Self {
        let spec = d.spec.clone().unwrap_or_default();
        let filename = spec.filename.clone().unwrap_or_default();
        Self {
            id: d.id.as_ref().map(|i| i.value.clone()).unwrap_or_default(),
            url: spec.url,
            target_dir: spec.target_dir,
            filename,
            state: proto::DownloadState::try_from(d.state)
                .unwrap_or(proto::DownloadState::Unspecified),
            progress: d.progress.unwrap_or_default(),
            attempt: d.attempt,
            max_attempts: 0, // brook-proto не передаёт max; подтянем при бэкенд-расширении
            error: d.error.clone(),
            updated_at: d.updated_at.as_ref().map(|t| t.seconds).unwrap_or(0),
            workers: HashMap::new(),
        }
    }

    /// Итоговое имя для списка — spec.filename, либо хвост URL при пустом filename.
    pub fn display_name(&self) -> &str {
        if !self.filename.is_empty() {
            &self.filename
        } else if let Some(tail) = self.url.rsplit('/').next() {
            if tail.is_empty() { &self.url } else { tail }
        } else {
            &self.url
        }
    }
}

/// Состояние коннекта (строка 1 статус-бара).
#[derive(Debug, Clone)]
#[allow(dead_code)] // `reason` пойдёт в toast из §6.6
pub enum ConnectionState {
    Connected,
    Reconnecting { attempt: u32 },
    Disconnected { reason: String },
}

#[derive(Debug, Clone)]
pub struct Toast {
    pub message: String,
    pub expires_at: Instant,
}

pub struct ViewModel {
    pub downloads: IndexMap<String, DownloadRow>,
    pub cursor: usize,
    pub detail_visible: bool,
    pub connection: ConnectionState,
    pub toast: Option<Toast>,
    pub port: u16,
    pub settings: proto::GetSettingsResponse,
}

impl ViewModel {
    pub fn new(port: u16, settings: proto::GetSettingsResponse) -> Self {
        Self {
            downloads: IndexMap::new(),
            cursor: 0,
            detail_visible: true,
            connection: ConnectionState::Reconnecting { attempt: 1 },
            toast: None,
            port,
            settings,
        }
    }

    /// Применить стримовое событие к модели. Снимки добавляют или
    /// перезаписывают запись; частные события обновляют подмножество
    /// полей. Если события приходят для незнакомого id (например,
    /// Progress до Snapshot), запись молча создаётся из пустого —
    /// последующий Snapshot её перезальёт.
    pub fn apply_stream(&mut self, ev: crate::events::StreamEvent) {
        use crate::events::StreamEvent as E;
        match ev {
            E::Snapshot(d) => {
                let row = DownloadRow::from_snapshot(&d);
                self.downloads.insert(row.id.clone(), row);
            }
            E::Progress(id, p) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.progress = p;
                }
            }
            E::StateChanged(id, st) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.state = proto::DownloadState::try_from(st)
                        .unwrap_or(proto::DownloadState::Unspecified);
                }
            }
            E::WorkerUpdate(id, piece, frac) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    if frac >= 1.0 {
                        row.workers.remove(&piece);
                    } else {
                        row.workers.insert(
                            piece,
                            WorkerSegment {
                                piece_index: piece,
                                fraction: frac,
                            },
                        );
                    }
                }
            }
            E::Completed(id) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.state = proto::DownloadState::Done;
                    row.workers.clear();
                }
            }
            E::Failed(id, err) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.state = proto::DownloadState::Failed;
                    row.error = Some(err);
                    row.workers.clear();
                }
            }
        }
    }

    pub fn reset(&mut self) {
        self.downloads.clear();
        self.cursor = 0;
    }

    /// Идентификаторы в отображаемом порядке с учётом сортировки и
    /// фильтрации CANCELLED. Пересчитывается каждый кадр — загрузок
    /// мало (лимит параллельности 3, плюс очередь; десятки максимум),
    /// сортировать их O(n log n) в кадре дешевле, чем поддерживать
    /// инкрементальный индекс.
    pub fn visible_ids(&self) -> Vec<String> {
        let mut ids: Vec<&DownloadRow> = self
            .downloads
            .values()
            .filter(|r| r.state != proto::DownloadState::Cancelled)
            .collect();
        ids.sort_by(|a, b| {
            state_rank(a.state)
                .cmp(&state_rank(b.state))
                .then(b.updated_at.cmp(&a.updated_at))
        });
        ids.into_iter().map(|r| r.id.clone()).collect()
    }

    #[allow(dead_code)] // §6.5 вызовет явно при Remove/Cancel
    pub fn clamp_cursor(&mut self, visible_len: usize) {
        if visible_len == 0 {
            self.cursor = 0;
        } else if self.cursor >= visible_len {
            self.cursor = visible_len - 1;
        }
    }
}

/// Порядок групп по §6.2: RUNNING → RETRYING → QUEUED → PAUSED → DONE → FAILED.
/// CANCELLED отфильтрован выше и здесь не появляется.
fn state_rank(s: proto::DownloadState) -> u8 {
    use proto::DownloadState as S;
    match s {
        S::Running => 0,
        S::Retrying => 1,
        S::Queued => 2,
        S::Paused => 3,
        S::Done => 4,
        S::Failed => 5,
        S::Cancelled | S::Unspecified => 6,
    }
}
