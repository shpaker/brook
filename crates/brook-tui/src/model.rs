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
    /// Сколько воркеров крутится над файлом сейчас. 0 — значение ещё не
    /// пришло (пустой Default до первого тика); UI рисует «—» в этом случае.
    pub workers_count: u32,
}

impl ProgressSnapshot {
    pub fn from_tick(t: &proto::ProgressTick) -> Self {
        Self {
            progress: t.progress,
            bytes_done: t.bytes_done,
            bytes_total: t.bytes_total,
            speed_bps: t.speed_bps,
            eta_secs: t.eta_secs,
            workers_count: t.workers_count,
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
    pub created_at: i64,
    /// Активные куски по piece_index. Завершившиеся piece'ы
    /// выкидываются (fraction >= 1.0), поэтому размер карты
    /// ограничен числом воркеров.
    pub workers: HashMap<u32, WorkerSegment>,
    /// Итоговая средняя скорость (байт/сек). Заполняется только для
    /// `Done`-файлов (демон вычисляет on-the-fly при чтении).
    pub avg_speed_bps: Option<f64>,
    /// Кол-во разных воркеров за время загрузки. Только для `Done`.
    pub workers_count: Option<u32>,
    /// Итоговый размер файла в байтах (из inspect-полей демона).
    /// `None` до завершения inspect-зонда и в streaming-режиме.
    pub total_size: Option<u64>,
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
            created_at: d.created_at.as_ref().map(|t| t.seconds).unwrap_or(0),
            workers: HashMap::new(),
            avg_speed_bps: d.avg_speed_bps,
            workers_count: d.workers_count,
            total_size: d.total_size,
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

/// Какой экран сейчас активен. `Mode` (модалки) поверх любого `Screen`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    /// Главный экран — recently (активность за 24ч).
    Main,
    /// Экран «история» — пагинированный список всех загрузок.
    History,
}

/// Состояние экрана истории. Порядок строк строго от сервера, никакой
/// клиентской пере-сортировки. `ids` ссылаются на `vm.downloads` по
/// строковому id.
#[derive(Debug, Clone, Default)]
pub struct HistoryState {
    pub ids: Vec<String>,
    pub cursor: usize,
    pub has_more: bool,
    /// Идёт ли сейчас фоновый запрос следующей страницы — UI рисует
    /// `loading next page…` и блокирует повторный запрос.
    pub loading: bool,
    /// Сколько записей уже загружено — следующий `GetFiles` идёт с этим
    /// offset'ом.
    pub next_offset: u32,
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
    /// Активный экран. По умолчанию — `Main`.
    pub screen: Screen,
    /// Состояние экрана истории.
    pub history: HistoryState,
    /// `true` — после первого успешного `GetRecently`. Empty-state
    /// `"No activity in the last 24 hours."` показывается только когда
    /// этот флаг `true` и `visible_ids().is_empty()` — иначе мог бы
    /// мигать в момент между connect'ом и приходом ответа.
    pub recently_loaded: bool,
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
            screen: Screen::Main,
            history: HistoryState::default(),
            recently_loaded: false,
            can_stop_daemon,
        }
    }

    /// Применить стримовое событие к модели.
    ///
    /// `Status`-event обновляет статус (и, при FAILED, текст ошибки)
    /// уже известной записи. Незнакомый id молча игнорируется — стрим
    /// не создаёт новых строк (это делают только ответы `Add` через
    /// `CmdOutcome::AddAccepted`); записи от других клиентов появятся
    /// при следующем `GetRecently`/reconnect.
    pub fn apply_stream(&mut self, ev: crate::events::StreamEvent) {
        use crate::events::StreamEvent as E;
        match ev {
            E::Status(s) => {
                let Some(id) = s.id.as_ref() else { return };
                let Some(row) = self.downloads.get_mut(&id.value) else {
                    return;
                };
                let status =
                    proto::FileStatus::try_from(s.status).unwrap_or(proto::FileStatus::Unspecified);
                row.status = status;
                if matches!(status, proto::FileStatus::Failed) {
                    row.error = s.description.clone();
                }
                if matches!(status, proto::FileStatus::Done | proto::FileStatus::Failed) {
                    row.workers.clear();
                }
            }
            E::Progress(tick) => {
                if let Some(id) = tick.file_id.as_ref()
                    && let Some(row) = self.downloads.get_mut(&id.value)
                {
                    row.progress = ProgressSnapshot::from_tick(&tick);
                }
            }
        }
    }

    pub fn reset(&mut self) {
        self.downloads.clear();
        self.cursor = 0;
        self.history = HistoryState::default();
        self.recently_loaded = false;
        self.screen = Screen::Main;
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
        ids.sort_by_key(|r| std::cmp::Reverse(r.created_at));
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
    /// Чистим и `downloads`, и `history.ids`, чтобы id не оставался
    /// «призраком» в истории и не рендерился пустой карточкой.
    pub fn drop_rows(&mut self, ids: &[String]) {
        for id in ids {
            self.downloads.shift_remove(id);
        }
        if !ids.is_empty() {
            self.history.ids.retain(|x| !ids.contains(x));
            let len = self.history.ids.len();
            if self.history.cursor >= len {
                self.history.cursor = len.saturating_sub(1);
            }
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
