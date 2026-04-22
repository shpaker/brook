//! Главный event-loop клиента: поднимает input- и watch-задачи, рулит
//! ViewModel'ом и прогоняет рендер через ratatui.

use std::io;
use std::time::{
    Duration,
    Instant,
};

use anyhow::{
    Context,
    Result,
};
use brook_proto::brook::v1::GetSettingsResponse;
use crossterm::event::{
    Event as CEvent,
    KeyCode,
    KeyEvent,
    KeyEventKind,
    KeyModifiers,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use crate::connect::AuthedChannel;
use crate::events::{
    AddForm,
    CmdOutcome,
    UiEvent,
};
use crate::model::{
    AddModal,
    ConnectionState,
    Mode,
    RenameModal,
    ViewModel,
};
use crate::{
    command,
    ui,
    watch,
};

const TICK: Duration = Duration::from_millis(250);

pub async fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    channel: AuthedChannel,
    settings: GetSettingsResponse,
    port: u16,
    can_stop_daemon: bool,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<UiEvent>();

    // Поток ввода специально не джойним на выходе: crossterm::event::read()
    // блокирующий, без следующей клавиши он не отпустит. Раз нам нечего
    // флашить из этого потока — отпускаем его жить до конца процесса.
    let input_tx = tx.clone();
    std::thread::spawn(move || input_loop(input_tx));

    let (watch_file_handle, watch_progress_handle) = watch::spawn(channel.clone(), tx.clone());

    let tick_tx = tx.clone();
    let tick_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if tick_tx.send(UiEvent::Tick).is_err() {
                return;
            }
        }
    });

    // SIGTERM/SIGINT → Quit. Raw mode перехватывает Ctrl+C в виде Key(c,
    // CONTROL), но внешний SIGTERM должен закрыть UI тоже корректно.
    let signal_tx = tx.clone();
    let signal_handle = tokio::spawn(async move {
        use tokio::signal::unix::{
            SignalKind,
            signal,
        };
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let _ = term.recv().await;
        let _ = signal_tx.send(UiEvent::Quit);
    });

    let mut vm = ViewModel::new(port, settings, can_stop_daemon);
    terminal.draw(|f| ui::draw(f, &vm)).context("draw")?;

    while let Some(ev) = rx.recv().await {
        if handle_event(&mut vm, ev, &channel, &tx) {
            break;
        }
        if let Some(t) = &vm.toast
            && t.expires_at <= Instant::now()
        {
            vm.toast = None;
        }
        terminal.draw(|f| ui::draw(f, &vm)).context("draw")?;
    }

    watch_file_handle.abort();
    watch_progress_handle.abort();
    tick_handle.abort();
    signal_handle.abort();
    drop(rx);

    Ok(())
}

/// Возвращает `true`, если пора выходить из цикла.
fn handle_event(
    vm: &mut ViewModel,
    ev: UiEvent,
    channel: &AuthedChannel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match ev {
        UiEvent::Key(k) => {
            if k.kind != KeyEventKind::Press {
                return false;
            }
            return handle_key(vm, k, channel, tx);
        }
        UiEvent::Paste(s) => {
            if let Mode::Add(m) = &mut vm.mode {
                m.insert_str(&s);
            }
        }
        UiEvent::Resize(_, _) | UiEvent::Mouse(_) => {}
        UiEvent::Stream(ev) => {
            vm.apply_stream(*ev);
            let visible_len = vm.visible_ids().len();
            vm.clamp_cursor(visible_len);
        }
        UiEvent::StreamConnected => {
            vm.connection = ConnectionState::Connected;
            vm.reset();
        }
        UiEvent::StreamDisconnected(reason) => {
            let next_attempt = match vm.connection {
                ConnectionState::Reconnecting { attempt } => attempt + 1,
                _ => 1,
            };
            vm.connection = if next_attempt == 1 {
                ConnectionState::Disconnected { reason }
            } else {
                ConnectionState::Reconnecting {
                    attempt: next_attempt,
                }
            };
        }
        UiEvent::CmdResult(o) => handle_cmd_result(vm, o),
        UiEvent::Tick => {}
        UiEvent::Quit => return true,
    }
    false
}

fn handle_cmd_result(vm: &mut ViewModel, outcome: CmdOutcome) {
    match outcome {
        CmdOutcome::Ok => {
            // Тихий успех. Watch-стрим сам отобразит новое состояние.
            if matches!(vm.mode, Mode::Add(_)) {
                vm.mode = Mode::Normal;
            }
        }
        CmdOutcome::AddAccepted { renamed_to } => {
            if matches!(vm.mode, Mode::Add(_) | Mode::RenameOnConflict { .. }) {
                vm.mode = Mode::Normal;
            }
            if let Some(name) = renamed_to {
                vm.set_toast(format!("saved as {name}"));
            }
        }
        CmdOutcome::AddConflict { base_name, form } => {
            // Если rename-модалка уже открыта (повторный конфликт после
            // редактирования пользователем) — просто бампим счётчик. Иначе
            // открываем новую поверх Add-модалки.
            if let Mode::RenameOnConflict { modal } = &mut vm.mode {
                modal.bump();
                modal.error = Some(format!("name is taken, bumped to ({})", modal.counter));
            } else {
                vm.mode = Mode::RenameOnConflict {
                    modal: RenameModal::new(base_name, form),
                };
            }
        }
        CmdOutcome::Error(msg) => {
            if let Mode::Add(m) = &mut vm.mode {
                m.error = Some(msg);
            } else if let Mode::RenameOnConflict { modal } = &mut vm.mode {
                modal.error = Some(msg);
            } else {
                vm.set_toast(msg);
            }
        }
        CmdOutcome::Removed { ids } => {
            vm.drop_rows(&ids);
        }
        CmdOutcome::NotFound { ids } => {
            // Не перетираем другую модалку (напр., Add) — в этом случае
            // просто тост, без алерта: призраки всплывают только по
            // нормальным pause/resume, где Mode::Normal.
            if matches!(vm.mode, Mode::Normal) {
                vm.mode = Mode::Ghost { ids };
            } else {
                vm.set_toast("download not found on daemon");
            }
        }
    }
}

fn handle_key(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: &AuthedChannel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    // Глобальный Ctrl+C — выход.
    if k.modifiers.contains(KeyModifiers::CONTROL) && k.code == KeyCode::Char('c') {
        return true;
    }

    match &vm.mode {
        Mode::Normal => handle_key_normal(vm, k, channel, tx),
        Mode::Add(_) => {
            handle_key_add(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::Duplicate { .. } => {
            handle_key_duplicate(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::ConfirmDelete { .. } => {
            handle_key_confirm(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::ConfirmRetry { .. } => {
            handle_key_confirm_retry(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::Ghost { .. } => {
            handle_key_ghost(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::RenameOnConflict { .. } => {
            handle_key_rename(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::QuitConfirm => handle_key_quit_confirm(vm, k, channel.clone(), tx.clone()),
    }
}

fn handle_key_normal(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: &AuthedChannel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    let visible_len = vm.visible_ids().len();
    match k.code {
        KeyCode::Char('q') => {
            vm.mode = Mode::QuitConfirm;
            return false;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            vm.cursor = vm.cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            vm.cursor = vm.cursor.saturating_add(1);
            vm.clamp_cursor(visible_len);
        }
        KeyCode::Char(' ') => primary_action(vm, channel, tx),
        KeyCode::Enter => reveal_cursor(vm),
        KeyCode::Char('a') => {
            let default_dir = vm.settings.default_dir.clone();
            let clipboard_url = command::clipboard_url().unwrap_or_default();
            vm.mode = Mode::Add(AddModal::new(clipboard_url, default_dir));
        }
        KeyCode::Char('d') => {
            let ids = vm.action_targets();
            if !ids.is_empty() {
                vm.mode = Mode::ConfirmDelete { ids };
            }
        }
        _ => {}
    }
    false
}

/// Space на курсоре: действие зависит от статуса строки (зеркалит
/// `<action>`-глиф из карточки). Для Failed — открывает модалку
/// подтверждения вместо молчаливого retry.
fn primary_action(
    vm: &mut ViewModel,
    channel: &AuthedChannel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) {
    use brook_proto::brook::v1::FileStatus as S;
    let visible = vm.visible_ids();
    let Some(id) = visible.get(vm.cursor.min(visible.len().saturating_sub(1))) else {
        return;
    };
    let id = id.clone();
    let Some(row) = vm.downloads.get(&id) else {
        return;
    };
    match row.status {
        S::Running | S::Retrying | S::Pending => {
            command::pause(channel.clone(), tx.clone(), vec![id]);
        }
        S::Paused => {
            command::resume(channel.clone(), tx.clone(), vec![id]);
        }
        S::Failed => {
            vm.mode = Mode::ConfirmRetry { ids: vec![id] };
        }
        S::Done => {
            command::reveal_in_finder(&row.target_dir, &row.filename);
        }
        S::Cancelled | S::Unspecified => {}
    }
}

fn reveal_cursor(vm: &ViewModel) {
    let visible = vm.visible_ids();
    let Some(id) = visible.get(vm.cursor.min(visible.len().saturating_sub(1))) else {
        return;
    };
    if let Some(row) = vm.downloads.get(id) {
        command::reveal_in_finder(&row.target_dir, &row.filename);
    }
}

fn handle_key_add(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::Add(m) = &mut vm.mode else {
        return;
    };
    match k.code {
        KeyCode::Esc => vm.mode = Mode::Normal,
        KeyCode::Tab => m.toggle_field(),
        KeyCode::Backspace => m.backspace(),
        KeyCode::Char(c) => m.insert_char(c),
        KeyCode::Enter => {
            let url = m.url.trim().to_string();
            let folder = m.folder.trim().to_string();
            if url.is_empty() {
                m.error = Some("url is required".into());
                return;
            }
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                m.error = Some("url must be http(s)://".into());
                return;
            }
            if folder.is_empty() {
                m.error = Some("folder is required".into());
                return;
            }
            // Клиентская проверка URL-дубля.
            if let Some(existing_id) = vm.find_by_url(&url) {
                vm.mode = Mode::Duplicate {
                    form: AddForm { url, folder },
                    existing_id,
                };
                return;
            }
            let form = AddForm { url, folder };
            command::add(channel, tx, form, None);
        }
        _ => {}
    }
}

fn handle_key_rename(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::RenameOnConflict { modal } = &mut vm.mode else {
        return;
    };
    match k.code {
        KeyCode::Esc => vm.mode = Mode::Normal,
        KeyCode::Backspace => modal.backspace(),
        KeyCode::Char(c) => modal.insert_char(c),
        KeyCode::Enter => {
            let name = modal.name.trim().to_string();
            if name.is_empty() {
                modal.error = Some("name is required".into());
                return;
            }
            let form = modal.form.clone();
            modal.error = None;
            command::add(channel, tx, form, Some(name));
        }
        _ => {}
    }
}

fn handle_key_duplicate(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::Duplicate { form, existing_id } = &vm.mode else {
        return;
    };
    let form = form.clone();
    let existing_id = existing_id.clone();
    match k.code {
        KeyCode::Esc => vm.mode = Mode::Normal,
        KeyCode::Char('o') => {
            // Прыжок курсора на существующую запись.
            let visible = vm.visible_ids();
            if let Some(pos) = visible.iter().position(|id| id == &existing_id) {
                vm.cursor = pos;
            }
            vm.mode = Mode::Normal;
        }
        KeyCode::Char('a') => {
            vm.mode = Mode::Normal;
            command::add(channel, tx, form, None);
        }
        _ => {}
    }
}

fn handle_key_confirm(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::ConfirmDelete { ids } = &vm.mode else {
        return;
    };
    let ids = ids.clone();
    match k.code {
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => vm.mode = Mode::Normal,
        KeyCode::Enter | KeyCode::Char('y') | KeyCode::Char('Y') => {
            vm.mode = Mode::Normal;
            command::remove(channel, tx, ids);
        }
        _ => {}
    }
}

fn handle_key_confirm_retry(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::ConfirmRetry { ids } = &vm.mode else {
        return;
    };
    let ids = ids.clone();
    match k.code {
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => vm.mode = Mode::Normal,
        KeyCode::Enter | KeyCode::Char('y') | KeyCode::Char('Y') => {
            vm.mode = Mode::Normal;
            command::retry(channel, tx, ids);
        }
        _ => {}
    }
}

fn handle_key_ghost(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::Ghost { ids } = &vm.mode else {
        return;
    };
    let ids = ids.clone();
    match k.code {
        KeyCode::Esc => vm.mode = Mode::Normal,
        KeyCode::Char('d') | KeyCode::Char('D') => {
            // Выкинуть призраки из ViewModel + дёрнуть Remove (идемпотентен,
            // если запись уже не числится — просто подчищает очередь на
            // стороне демона).
            vm.drop_rows(&ids);
            vm.mode = Mode::Normal;
            command::remove(channel, tx, ids);
        }
        KeyCode::Char('r') | KeyCode::Char('R') => {
            // Пере-загрузка: снимаем призраков, перезапускаем Add по
            // сохранённым url/folder. Делаем по всем id из алерта — если
            // их несколько, улетит пачка Add'ов.
            let forms: Vec<AddForm> = ids
                .iter()
                .filter_map(|id| vm.downloads.get(id))
                .map(|row| AddForm {
                    url: row.url.clone(),
                    folder: row.target_dir.clone(),
                })
                .collect();
            vm.drop_rows(&ids);
            vm.mode = Mode::Normal;
            for form in forms {
                command::add(channel.clone(), tx.clone(), form, None);
            }
        }
        _ => {}
    }
}

fn handle_key_quit_confirm(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) -> bool {
    // Три варианта quit-модалки:
    //   s        — выход с остановкой демона (Shutdown RPC → Quit)
    //   k / Enter — выход без остановки демона (демон продолжает работать)
    //   Esc      — отмена, остаёмся в TUI
    // Пункт «s» скрыт, если `can_stop_daemon = false` (remote или
    // внешне запущенный локальный демон) — мы его не поднимали, нам
    // его и не гасить.
    match k.code {
        KeyCode::Esc => {
            vm.mode = Mode::Normal;
            false
        }
        KeyCode::Char('s') | KeyCode::Char('S') if vm.can_stop_daemon => {
            // Сам RPC уходит в фон; по его завершении в канал падает
            // `UiEvent::Quit` и мы выйдем из цикла. Даже если Shutdown
            // ответил ошибкой — закрываемся, чтобы не зависнуть в
            // модалке.
            command::shutdown_daemon(channel, tx);
            vm.mode = Mode::Normal;
            false
        }
        KeyCode::Enter | KeyCode::Char('k') | KeyCode::Char('K') => true,
        _ => false,
    }
}

fn input_loop(tx: mpsc::UnboundedSender<UiEvent>) {
    loop {
        let ev = match crossterm::event::read() {
            Ok(e) => e,
            Err(_) => return,
        };
        let ui_ev = match ev {
            CEvent::Key(k) => UiEvent::Key(k),
            CEvent::Resize(w, h) => UiEvent::Resize(w, h),
            CEvent::Paste(s) => UiEvent::Paste(s),
            CEvent::Mouse(m) => UiEvent::Mouse(m),
            CEvent::FocusGained | CEvent::FocusLost => continue,
        };
        if tx.send(ui_ev).is_err() {
            return;
        }
    }
}
