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
#[allow(dead_code)] // Resize/Paste/Mouse полезны только с §6.5-6.6
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
}
