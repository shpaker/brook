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
use brook_proto::brook::v1::{
    GetSettingsResponse,
    OnFileExistsOverride,
};
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
use tonic::transport::Channel;

use crate::events::{
    AddForm,
    CmdOutcome,
    UiEvent,
};
use crate::model::{
    AddModal,
    ConnectionState,
    Mode,
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
    channel: Channel,
    settings: GetSettingsResponse,
    port: u16,
    spawned_daemon: bool,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<UiEvent>();

    let input_tx = tx.clone();
    let input_handle = std::thread::spawn(move || input_loop(input_tx));

    let watch_handle = watch::spawn(channel.clone(), tx.clone());

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

    let mut vm = ViewModel::new(port, settings, spawned_daemon);
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

    watch_handle.abort();
    tick_handle.abort();
    signal_handle.abort();
    drop(rx);
    let _ = input_handle.join();

    Ok(())
}

/// Возвращает `true`, если пора выходить из цикла.
fn handle_event(
    vm: &mut ViewModel,
    ev: UiEvent,
    channel: &Channel,
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
        CmdOutcome::Ok | CmdOutcome::AddAccepted => {
            // Тихий успех. Watch-стрим сам отобразит новое состояние.
            if matches!(vm.mode, Mode::Add(_)) {
                vm.mode = Mode::Normal;
            }
        }
        CmdOutcome::Error(msg) => {
            if let Mode::Add(m) = &mut vm.mode {
                m.error = Some(msg);
            } else {
                vm.set_toast(msg);
            }
        }
        CmdOutcome::AddFileExists { form } => {
            vm.mode = Mode::FileExists { form };
        }
    }
}

fn handle_key(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: &Channel,
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
        Mode::FileExists { .. } => {
            handle_key_file_exists(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::ConfirmCancel { .. } => {
            handle_key_confirm(vm, k, channel.clone(), tx.clone());
            false
        }
        Mode::Help { .. } => {
            handle_key_help(vm, k);
            false
        }
        Mode::QuitConfirm => handle_key_quit_confirm(vm, k, channel.clone(), tx.clone()),
    }
}

fn handle_key_normal(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: &Channel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    let shift = k.modifiers.contains(KeyModifiers::SHIFT);
    let visible_len = vm.visible_ids().len();
    match k.code {
        KeyCode::Char('q') => {
            if vm.spawned_daemon {
                vm.mode = Mode::QuitConfirm;
                return false;
            }
            return true;
        }
        KeyCode::Tab => vm.detail_visible = !vm.detail_visible,
        KeyCode::Char('?') => vm.mode = Mode::Help { scroll: 0 },
        KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K') => {
            vm.cursor = vm.cursor.saturating_sub(1);
            if shift {
                vm.extend_selection(visible_len);
            }
        }
        KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J') => {
            vm.cursor = vm.cursor.saturating_add(1);
            vm.clamp_cursor(visible_len);
            if shift {
                vm.extend_selection(visible_len);
            }
        }
        KeyCode::Char('g') => vm.cursor = 0,
        KeyCode::Char('G') => {
            vm.cursor = visible_len.saturating_sub(1);
        }
        KeyCode::Char(' ') => vm.toggle_select_here(),
        KeyCode::Char('a') => {
            let default_dir = vm.settings.default_dir.clone();
            let clipboard_url = command::clipboard_url().unwrap_or_default();
            vm.mode = Mode::Add(AddModal::new(clipboard_url, default_dir));
        }
        KeyCode::Char('p') => {
            let ids = vm.action_targets();
            if !ids.is_empty() {
                command::pause(channel.clone(), tx.clone(), ids);
            }
        }
        KeyCode::Char('r') => {
            let ids = vm.action_targets();
            if !ids.is_empty() {
                command::resume(channel.clone(), tx.clone(), ids);
            }
        }
        KeyCode::Char('c') => {
            let ids = vm.action_targets();
            if !ids.is_empty() {
                vm.mode = Mode::ConfirmCancel { ids };
            }
        }
        KeyCode::Char('o') => {
            if let Some(path) = vm.open_target() {
                command::open_path(tx.clone(), path);
            }
        }
        _ => {}
    }
    false
}

fn handle_key_add(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: Channel,
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
            command::add(channel, tx, form, OnFileExistsOverride::Unspecified);
        }
        _ => {}
    }
}

fn handle_key_duplicate(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: Channel,
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
            command::add(channel, tx, form, OnFileExistsOverride::Unspecified);
        }
        _ => {}
    }
}

fn handle_key_file_exists(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: Channel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::FileExists { form } = &vm.mode else {
        return;
    };
    let form = form.clone();
    match k.code {
        KeyCode::Esc => vm.mode = Mode::Normal,
        KeyCode::Char('r') => {
            vm.mode = Mode::Normal;
            command::add(channel, tx, form, OnFileExistsOverride::Rename);
        }
        KeyCode::Char('o') => {
            vm.mode = Mode::Normal;
            command::add(channel, tx, form, OnFileExistsOverride::Overwrite);
        }
        _ => {}
    }
}

fn handle_key_confirm(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: Channel,
    tx: mpsc::UnboundedSender<UiEvent>,
) {
    let Mode::ConfirmCancel { ids } = &vm.mode else {
        return;
    };
    let ids = ids.clone();
    match k.code {
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => vm.mode = Mode::Normal,
        KeyCode::Enter | KeyCode::Char('y') | KeyCode::Char('Y') => {
            vm.mode = Mode::Normal;
            command::cancel(channel, tx, ids);
        }
        _ => {}
    }
}

fn handle_key_quit_confirm(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: Channel,
    tx: mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match k.code {
        KeyCode::Esc => {
            vm.mode = Mode::Normal;
            false
        }
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            // Сам RPC уходит в фон; по его завершении в канал падает
            // `UiEvent::Quit` и мы выйдем из цикла. Даже если Shutdown
            // ответил ошибкой — закрываемся, чтобы не зависнуть в
            // модалке; демон всё равно уже получил сигнал или не
            // получил — но TUI здесь бесполезен.
            command::shutdown_daemon(channel, tx);
            vm.mode = Mode::Normal;
            false
        }
        KeyCode::Char('n') | KeyCode::Char('N') => true,
        _ => false,
    }
}

fn handle_key_help(vm: &mut ViewModel, k: KeyEvent) {
    let Mode::Help { scroll } = &mut vm.mode else {
        return;
    };
    // §6.7: Esc/?/q всегда закрывают; стрелки листают при маленьком
    // экране (контент всё равно влезает — скролл будет безобидным); любая
    // другая клавиша закрывает без побочного эффекта на список.
    match k.code {
        KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => vm.mode = Mode::Normal,
        KeyCode::Up => *scroll = scroll.saturating_sub(1),
        KeyCode::Down => *scroll = scroll.saturating_add(1),
        _ => vm.mode = Mode::Normal,
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
