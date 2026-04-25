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

/// Обёртки над proto-событиями из `WatchFile` / `WatchProgress`-стримов.
/// Держим сырой proto, потому что §6.2 целиком живёт на нём, а свой
/// доменный тип породил бы лишний слой перевода без пользы.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Snapshot(proto::File),
    Progress(proto::ProgressTick),
    StatusChanged(proto::FileId, i32),
    Completed(proto::FileId),
    Failed(proto::FileId, String),
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
    /// Сервер успешно принял `Add`. Если клиент переименовал файл
    /// после конфликта (`AlreadyExists`), в `renamed_to` — финальное имя
    /// для toast'а «Saved as …».
    AddAccepted { renamed_to: Option<String> },
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
}

/// Форма Add-модалки.
#[derive(Debug, Clone)]
pub struct AddForm {
    pub url: String,
    pub folder: String,
}
