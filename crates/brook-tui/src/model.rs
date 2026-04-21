//! ViewModel клиента и связанные доменные структуры.
//!
//! ViewModel мутируется только UI-task'ом (см. `events.rs`). Все поля
//! публичные — ViewModel живёт целиком внутри крейта, инкапсуляция тут
//! только раздувала бы код.

use std::collections::{
    HashMap,
    HashSet,
};
use std::time::{
    Duration,
    Instant,
};

use brook_proto::brook::v1 as proto;
use indexmap::IndexMap;

use crate::events::AddForm;

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

/// Что сейчас перехватывает ввод поверх списка. `Normal` = ничего.
/// Остальные варианты — открытая модалка / overlay; клавиатура роутится
/// в обработчик модалки, команды списка не срабатывают.
#[derive(Debug, Clone)]
pub enum Mode {
    Normal,
    Add(AddModal),
    Duplicate {
        form: AddForm,
        existing_id: String,
    },
    FileExists {
        form: AddForm,
    },
    ConfirmCancel {
        ids: Vec<String>,
    },
    Help {
        scroll: u16,
    },
    /// На выходе из TUI: гасить `brookd`, которого мы же подняли, или
    /// оставить крутиться в фоне.
    QuitConfirm,
}

#[derive(Debug, Clone)]
pub struct AddModal {
    pub url: String,
    pub folder: String,
    pub field: AddField,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddField {
    Url,
    Folder,
}

impl AddModal {
    pub fn new(url: String, folder: String) -> Self {
        Self {
            url,
            folder,
            field: AddField::Url,
            error: None,
        }
    }

    pub fn toggle_field(&mut self) {
        self.field = match self.field {
            AddField::Url => AddField::Folder,
            AddField::Folder => AddField::Url,
        };
    }

    pub fn insert_str(&mut self, s: &str) {
        match self.field {
            AddField::Url => self.url.push_str(s),
            AddField::Folder => self.folder.push_str(s),
        }
    }

    pub fn insert_char(&mut self, c: char) {
        match self.field {
            AddField::Url => self.url.push(c),
            AddField::Folder => self.folder.push(c),
        }
    }

    pub fn backspace(&mut self) {
        match self.field {
            AddField::Url => {
                self.url.pop();
            }
            AddField::Folder => {
                self.folder.pop();
            }
        }
    }
}

pub struct ViewModel {
    pub downloads: IndexMap<String, DownloadRow>,
    pub cursor: usize,
    /// Якорь для Shift-расширения диапазона.
    pub anchor: Option<usize>,
    /// Id выделенных строк (stable при переупорядочивании списка).
    pub selected: HashSet<String>,
    pub detail_visible: bool,
    pub connection: ConnectionState,
    pub toast: Option<Toast>,
    pub port: u16,
    pub settings: proto::GetSettingsResponse,
    pub mode: Mode,
    /// Поднимали ли мы `brookd` в этом процессе. Если да — при выходе
    /// спрашиваем, гасить ли его.
    pub spawned_daemon: bool,
}

impl ViewModel {
    pub fn new(port: u16, settings: proto::GetSettingsResponse, spawned_daemon: bool) -> Self {
        Self {
            downloads: IndexMap::new(),
            cursor: 0,
            anchor: None,
            selected: HashSet::new(),
            detail_visible: true,
            connection: ConnectionState::Reconnecting { attempt: 1 },
            toast: None,
            port,
            settings,
            mode: Mode::Normal,
            spawned_daemon,
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
        self.anchor = None;
        self.selected.clear();
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

    pub fn clamp_cursor(&mut self, visible_len: usize) {
        if visible_len == 0 {
            self.cursor = 0;
        } else if self.cursor >= visible_len {
            self.cursor = visible_len - 1;
        }
    }

    /// Список id, на которые действуют bulk-команды (`p`/`r`/`c`): если
    /// есть выделенные — они, иначе одна строка под курсором.
    pub fn action_targets(&self) -> Vec<String> {
        let visible = self.visible_ids();
        if !self.selected.is_empty() {
            return visible
                .into_iter()
                .filter(|id| self.selected.contains(id))
                .collect();
        }
        let idx = self.cursor.min(visible.len().saturating_sub(1));
        visible.get(idx).cloned().into_iter().collect()
    }

    /// `open`-таргет: если DONE → полный путь к файлу; иначе — target_dir.
    pub fn open_target(&self) -> Option<String> {
        let visible = self.visible_ids();
        let idx = self.cursor.min(visible.len().saturating_sub(1));
        let id = visible.get(idx)?;
        let row = self.downloads.get(id)?;
        if row.state == proto::DownloadState::Done && !row.filename.is_empty() {
            let mut p = std::path::PathBuf::from(&row.target_dir);
            p.push(&row.filename);
            Some(p.display().to_string())
        } else {
            Some(row.target_dir.clone())
        }
    }

    pub fn set_toast(&mut self, msg: impl Into<String>) {
        self.toast = Some(Toast {
            message: msg.into(),
            expires_at: Instant::now() + Duration::from_secs(3),
        });
    }

    pub fn find_by_url(&self, url: &str) -> Option<String> {
        self.downloads
            .values()
            .find(|r| r.url == url && r.state != proto::DownloadState::Cancelled)
            .map(|r| r.id.clone())
    }

    /// Расширяет выделение от `anchor` до `cursor`. Если якоря нет —
    /// ставит якорь в текущую позицию и помечает её.
    pub fn extend_selection(&mut self, visible_len: usize) {
        if visible_len == 0 {
            return;
        }
        let visible = self.visible_ids();
        let cur = self.cursor.min(visible_len - 1);
        let anchor = *self.anchor.get_or_insert(cur);
        let (lo, hi) = if anchor <= cur {
            (anchor, cur)
        } else {
            (cur, anchor)
        };
        self.selected.clear();
        for id in visible.iter().take(hi + 1).skip(lo) {
            self.selected.insert(id.clone());
        }
    }

    /// Toggle выделения текущей строки, фиксирует anchor на ней.
    pub fn toggle_select_here(&mut self) {
        let visible = self.visible_ids();
        if visible.is_empty() {
            return;
        }
        let idx = self.cursor.min(visible.len() - 1);
        self.anchor = Some(idx);
        let id = &visible[idx];
        if !self.selected.remove(id) {
            self.selected.insert(id.clone());
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
