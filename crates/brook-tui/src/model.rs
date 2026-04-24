//! ViewModel клиента и связанные доменные структуры.
//!
//! ViewModel мутируется только UI-task'ом (см. `events.rs`). Все поля
//! публичные — ViewModel живёт целиком внутри крейта, инкапсуляция тут
//! только раздувала бы код.

use std::collections::HashMap;
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

/// Снимок прогресса для одной загрузки. Обновляется `WatchProgress`-тиком;
/// не живёт в `proto::File` (лайфсайкл и прогресс — разные стримы).
#[derive(Debug, Clone, Default)]
pub struct ProgressSnapshot {
    /// Отношение 0..=1; UI рисует шкалу по `bytes_done / bytes_total`,
    /// но сервер уже считает clamped ratio — держим для диагностики и
    /// будущего использования в detail-панели.
    #[allow(dead_code)]
    pub progress: f64,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub speed_bps: f64,
    pub eta_secs: Option<u64>,
}

impl ProgressSnapshot {
    pub fn from_tick(t: &proto::ProgressTick) -> Self {
        Self {
            progress: t.progress,
            bytes_done: t.bytes_done,
            bytes_total: t.bytes_total,
            speed_bps: t.speed_bps,
            eta_secs: t.eta_secs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadRow {
    pub id: String,
    pub url: String,
    pub target_dir: String,
    pub filename: String,
    pub status: proto::FileStatus,
    pub progress: ProgressSnapshot,
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
    pub fn from_snapshot(d: &proto::File) -> Self {
        let spec = d.spec.clone().unwrap_or_default();
        let filename = spec.filename.clone().unwrap_or_default();
        Self {
            id: d.id.as_ref().map(|i| i.value.clone()).unwrap_or_default(),
            url: spec.url,
            target_dir: spec.target_dir,
            filename,
            status: proto::FileStatus::try_from(d.status).unwrap_or(proto::FileStatus::Unspecified),
            progress: ProgressSnapshot::default(),
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
    /// URL уже в очереди. y = добавить дубль, n/Esc = отмена.
    Duplicate {
        form: AddForm,
        existing_id: String,
    },
    /// Демон вернул `AlreadyExists`: в target-каталоге лежит файл с
    /// таким же именем. Открываем модалку, чтобы пользователь выбрал
    /// имя (префилл — `<base> (N)` по конвенции Windows/Finder).
    RenameOnConflict {
        modal: RenameModal,
    },
    /// y = удалить, n/Esc = отмена.
    ConfirmDelete {
        ids: Vec<String>,
    },
    /// Подтверждение перезапуска упавшей загрузки. `r` на Failed
    /// не дёргает retry молча — сначала спрашиваем. y = повторить, n/Esc = отмена.
    ConfirmRetry {
        ids: Vec<String>,
    },
    /// Демон не знает id, по которому TUI пытался pause/resume.
    /// y = перекачать, n/Esc = убрать запись из списка.
    Ghost {
        ids: Vec<String>,
    },
    /// Выход из TUI. y = выйти (демон останавливается если TUI его запускал),
    /// n/Esc = отмена.
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

/// Состояние rename-модалки, которая открывается при `AlreadyExists`.
/// `form` сохраняется, чтобы повторить `Add` под выбранным именем;
/// `base` — имя, о которое споткнулся демон, нужно для инкремента
/// `counter` при повторных конфликтах.
#[derive(Debug, Clone)]
pub struct RenameModal {
    pub form: AddForm,
    pub base: String,
    pub name: String,
    pub counter: u32,
    pub error: Option<String>,
}

impl RenameModal {
    pub fn new(base: String, form: AddForm) -> Self {
        let name = crate::command::apply_counter(&base, 1);
        Self {
            form,
            base,
            name,
            counter: 1,
            error: None,
        }
    }

    /// Повторный конфликт: подставить следующий счётчик, сбросить
    /// возможные правки пользователя на автоматический кандидат.
    pub fn bump(&mut self) {
        self.counter = self.counter.saturating_add(1);
        self.name = crate::command::apply_counter(&self.base, self.counter);
        self.error = None;
    }

    pub fn insert_char(&mut self, c: char) {
        self.name.push(c);
        self.error = None;
    }

    pub fn backspace(&mut self) {
        self.name.pop();
        self.error = None;
    }
}

pub struct ViewModel {
    pub downloads: IndexMap<String, DownloadRow>,
    pub cursor: usize,
    pub connection: ConnectionState,
    pub toast: Option<Toast>,
    pub port: u16,
    pub settings: proto::GetSettingsResponse,
    pub mode: Mode,
    /// Можем ли мы предложить «остановить демон» в `QuitConfirm`.
    /// `true` — только если TUI сам запустил локального демона; для
    /// remote-сессий и случаев, когда демон уже крутился — `false`.
    pub can_stop_daemon: bool,
}

impl ViewModel {
    pub fn new(port: u16, settings: proto::GetSettingsResponse, can_stop_daemon: bool) -> Self {
        Self {
            downloads: IndexMap::new(),
            cursor: 0,
            connection: ConnectionState::Reconnecting { attempt: 1 },
            toast: None,
            port,
            settings,
            mode: Mode::Normal,
            can_stop_daemon,
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
            E::Progress(tick) => {
                if let Some(id) = tick.file_id.as_ref()
                    && let Some(row) = self.downloads.get_mut(&id.value)
                {
                    row.progress = ProgressSnapshot::from_tick(&tick);
                }
            }
            E::StatusChanged(id, st) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.status =
                        proto::FileStatus::try_from(st).unwrap_or(proto::FileStatus::Unspecified);
                }
            }
            E::Completed(id) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.status = proto::FileStatus::Done;
                    row.workers.clear();
                }
            }
            E::Failed(id, err) => {
                if let Some(row) = self.downloads.get_mut(&id.value) {
                    row.status = proto::FileStatus::Failed;
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
            .filter(|r| r.status != proto::FileStatus::Cancelled)
            .collect();
        ids.sort_by(|a, b| {
            state_rank(a.status)
                .cmp(&state_rank(b.status))
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

    /// Строка под курсором — цель одиночных команд (pause/resume/retry/delete).
    /// Мультиселекшн удалён: операция всегда работает над одной записью.
    pub fn action_targets(&self) -> Vec<String> {
        let visible = self.visible_ids();
        let idx = self.cursor.min(visible.len().saturating_sub(1));
        visible.get(idx).cloned().into_iter().collect()
    }

    /// Выкинуть строки из ViewModel локально. Нужно, когда удаление
    /// произошло на стороне демона (или по ghost-алерту): демон не
    /// шлёт явного `Removed`-события, и без ручной чистки запись висит.
    pub fn drop_rows(&mut self, ids: &[String]) {
        for id in ids {
            self.downloads.shift_remove(id);
        }
        let visible_len = self.visible_ids().len();
        self.clamp_cursor(visible_len);
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
            .find(|r| r.url == url && r.status != proto::FileStatus::Cancelled)
            .map(|r| r.id.clone())
    }
}

/// Порядок групп по §6.2: RUNNING → RETRYING → QUEUED → PAUSED → DONE → FAILED.
/// CANCELLED отфильтрован выше и здесь не появляется.
fn state_rank(s: proto::FileStatus) -> u8 {
    use proto::FileStatus as S;
    match s {
        S::Running => 0,
        S::Retrying => 1,
        S::Pending => 2,
        S::Paused => 3,
        S::Done => 4,
        S::Failed => 5,
        S::Cancelled | S::Unspecified => 6,
    }
}
