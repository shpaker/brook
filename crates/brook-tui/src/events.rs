//! События, по которым крутится UI-task.
//!
//! `UiEvent` — единственный путь, через который мутируется ViewModel.
//! Ключевое решение: UI-task не дергает ни tonic, ни crossterm напрямую
//! из своего цикла — он только `recv`-ит из `mpsc<UiEvent>`. Ввод и
//! watch-стрим живут в фоновых задачах, которые конвертят сырые
//! события в `UiEvent` и шлют по каналу.

use brook_proto::brook::v1 as proto;
use crossterm::event::{
    KeyEvent,
    MouseEvent,
};

use crate::model::DownloadRow;

/// Обёртки над proto-событиями из `WatchStatus` / `WatchProgress`-стримов.
/// Держим сырой proto, потому что §6.2 целиком живёт на нём, а свой
/// доменный тип породил бы лишний слой перевода без пользы.
///
/// `WatchStatus` — это flat-стрим статусных переходов. Стартовое
/// состояние клиент берёт через RPC `GetRecently`/`GetFiles`.
/// Создание новых записей и физическое удаление через стрим не
/// приходят: создающая сторона видит результат через `AddResponse.file`,
/// удалившая — через ответ `Remove`. Призраки у наблюдателей лечит
/// ghost-режим TUI.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Status(proto::StatusEvent),
    Progress(proto::ProgressTick),
}

#[derive(Debug)]
#[allow(dead_code)] // Resize/Mouse полезны для диагностики, мутации нет
pub enum UiEvent {
    Key(KeyEvent),
    Resize(u16, u16),
    Paste(String),
    Mouse(MouseEvent),
    Stream(Box<StreamEvent>),
    StreamConnected,
    /// Стрим оборвался; watch-task уже ушёл в backoff и попробует
    /// переподключиться сам.
    StreamDisconnected(String),
    /// 250 ms-тик: протухание toast'ов, пересчёт ETA.
    Tick,
    /// Результат команды, запущенной с UI. Передаёт ok/err-контекст.
    CmdResult(CmdOutcome),
    /// Ответ на `GetRecently` для главного экрана.
    RecentlyLoaded(Vec<DownloadRow>),
    /// Очередная страница `GetFiles` для экрана истории. `append=true` —
    /// дописать к концу `history.ids` (infinite-scroll), `false` — заменить
    /// (первый запрос).
    HistoryPage {
        rows: Vec<DownloadRow>,
        has_more: bool,
        append: bool,
    },
    /// Внешний сигнал (SIGTERM/SIGINT из `tokio::signal`): чисто
    /// завершаем цикл.
    Quit,
}

/// Итог команды для UI-task'а.
#[derive(Debug)]
pub enum CmdOutcome {
    /// Успех без побочных UI-эффектов.
    Ok,
    /// Ошибка — показываем toast'ом.
    Error(String),
    /// Сервер успешно принял `Add`. `row` — свежий снимок созданной
    /// записи (из ответа `AddResponse`); UI вставляет его в model сразу,
    /// не дожидаясь `Created`-события из стрима. Если клиент переименовал
    /// файл после конфликта (`AlreadyExists`), в `renamed_to` — финальное
    /// имя для toast'а «Saved as …».
    AddAccepted {
        row: Box<DownloadRow>,
        renamed_to: Option<String>,
    },
    /// Демон вернул `AlreadyExists`: в `target_dir` уже лежит файл
    /// `base_name`. UI открывает rename-модалку с префиллом
    /// `<base_name> (1)` (по конвенции Windows/Finder — перед последней
    /// точкой) и даёт пользователю выбрать итоговое имя.
    AddConflict { base_name: String, form: AddForm },
    /// `Remove` прошёл успешно (или же демон ответил `NotFound`, но
    /// `Remove` идемпотентен — см. `manager::remove`). UI должен выкинуть
    /// перечисленные id из ViewModel, потому что демон про них больше
    /// событий слать не будет.
    Removed { ids: Vec<String> },
    /// Pause/Resume упёрлись в ghost-запись: id есть в ViewModel, но
    /// демон отвечает `NotFound`. UI открывает алерт с предложением
    /// пере-загрузить или удалить призрак.
    NotFound { ids: Vec<String> },
    /// Retry на сервере прошёл (remove + add): старый id уехал, для
    /// того же `FileSpec` создана новая запись. UI должен выкинуть
    /// `old_id` и вставить `row` под её новым id.
    RetryAccepted {
        old_id: String,
        row: Box<DownloadRow>,
    },
}

/// Форма Add-модалки.
#[derive(Debug, Clone)]
pub struct AddForm {
    pub url: String,
    pub folder: String,
}
