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

/// Обёртки над proto-событиями из `Watch`-стрима. Держим сырой proto,
/// потому что §6.2 целиком живёт на нём, а свой доменный тип породил
/// бы лишний слой перевода без пользы.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Snapshot(proto::Download),
    Progress(proto::DownloadId, proto::Progress),
    StateChanged(proto::DownloadId, i32),
    WorkerUpdate(proto::DownloadId, u32, f32),
    Completed(proto::DownloadId),
    Failed(proto::DownloadId, String),
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
    /// Результат команды, запущенной с UI. Передаёт ok/err-контекст,
    /// и для `Add` — `FileExists`-хук, чтобы UI мог показать модалку
    /// выбора rename/overwrite без лишнего кода.
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
    /// Сервер отказал в `Add` из-за существующего файла. UI должен
    /// открыть модалку выбора rename/overwrite и повторить `Add` с
    /// нужным override. Чтобы не терять введённое, возвращаем исходный
    /// `AddForm`.
    AddFileExists { form: AddForm },
    /// Сервер успешно принял `Add` — вернул новый `DownloadId`.
    AddAccepted,
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

/// Форма Add-модалки. Используется и при первом `Add`, и при повторе
/// после `FileExists`.
#[derive(Debug, Clone)]
pub struct AddForm {
    pub url: String,
    pub folder: String,
}
