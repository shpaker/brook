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
    HistoryState,
    Mode,
    RenameModal,
    Screen,
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
            // Стрим больше не делает initial-sync — стартовое состояние
            // главного экрана берём через `GetRecently` (последние 24ч).
            command::refresh_recently(channel.clone(), tx.clone());
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
        UiEvent::RecentlyLoaded(rows) => {
            for row in rows {
                vm.downloads.insert(row.id.clone(), row);
            }
            vm.recently_loaded = true;
            let visible_len = vm.visible_ids().len();
            vm.clamp_cursor(visible_len);
        }
        UiEvent::HistoryPage {
            rows,
            has_more,
            append,
        } => {
            if !append {
                vm.history.ids.clear();
                vm.history.cursor = 0;
                vm.history.next_offset = 0;
            }
            let mut added: u32 = 0;
            for row in rows {
                let id = row.id.clone();
                vm.downloads.insert(id.clone(), row);
                vm.history.ids.push(id);
                added += 1;
            }
            vm.history.has_more = has_more;
            vm.history.next_offset = vm.history.next_offset.saturating_add(added);
            vm.history.loading = false;
            let history_len = vm.history.ids.len();
            if vm.history.cursor >= history_len {
                vm.history.cursor = history_len.saturating_sub(1);
            }
        }
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
        CmdOutcome::AddAccepted { row, renamed_to } => {
            if matches!(vm.mode, Mode::Add(_) | Mode::RenameOnConflict { .. }) {
                vm.mode = Mode::Normal;
            }
            // Вставляем row сразу — клиент видит карточку без задержки
            // на дельту из стрима. Created-event прилетит следом и
            // повторит вставку (idempotent upsert).
            let id = row.id.clone();
            vm.downloads.insert(id, *row);
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
        CmdOutcome::RetryAccepted { old_id, row } => {
            // Hard-restart на сервере: старая запись ушла, появилась
            // новая под другим id. Симметрично AddAccepted, но
            // дополнительно выкидываем старый id из ViewModel, чтобы
            // карточка не задвоилась.
            vm.drop_rows(&[old_id]);
            let id = row.id.clone();
            vm.downloads.insert(id, *row);
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

    // Сначала роутим по экрану (History ловит свои клавиши целиком,
    // если поверх него нет модалки), потом — по модалке.
    if matches!(vm.mode, Mode::Normal) && matches!(vm.screen, Screen::History) {
        return handle_key_history(vm, k, channel, tx);
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
        // WASD-движение курсора. Стрелки тоже работают как универсальные.
        KeyCode::Up | KeyCode::Char('w') => {
            vm.cursor = vm.cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('s') => {
            vm.cursor = vm.cursor.saturating_add(1);
            vm.clamp_cursor(visible_len);
        }
        // `r` — первая буква primary-действий группы resume/retry
        // (соответственно Paused/Failed). Для Done действия нет.
        KeyCode::Char('r') => r_action_cursor(vm, channel, tx),
        // `p` — первая буква `pause` (Running/Retrying/Pending).
        KeyCode::Char('p') => p_action_cursor(vm, channel, tx),
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
        // Перейти на экран «история». Сбрасываем существующее состояние
        // истории и сразу запрашиваем первую страницу.
        KeyCode::Char('h') => {
            vm.screen = Screen::History;
            vm.history = HistoryState::default();
            vm.history.loading = true;
            command::history_load(channel.clone(), tx.clone(), 0);
        }
        _ => {}
    }
    false
}

/// Клавиатура экрана истории. Esc — назад на главный, w/s — движение
/// курсора, q — quit, d — delete (открывает Mode::ConfirmDelete для
/// записи под курсором). При скролле к концу страницы запускается
/// фоновая подгрузка следующей.
fn handle_key_history(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: &AuthedChannel,
    tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match k.code {
        KeyCode::Esc => {
            vm.screen = Screen::Main;
            return false;
        }
        KeyCode::Char('q') => {
            vm.mode = Mode::QuitConfirm;
            return false;
        }
        KeyCode::Up | KeyCode::Char('w') => {
            vm.history.cursor = vm.history.cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('s') => {
            let len = vm.history.ids.len();
            vm.history.cursor = vm.history.cursor.saturating_add(1);
            if vm.history.cursor >= len {
                vm.history.cursor = len.saturating_sub(1);
            }
            // Подгружаем следующую страницу, если курсор подошёл к концу
            // и сервер сказал, что есть ещё.
            const PREFETCH_TAIL: usize = 5;
            if vm.history.has_more
                && !vm.history.loading
                && vm.history.cursor + PREFETCH_TAIL >= len
            {
                vm.history.loading = true;
                command::history_load(channel.clone(), tx.clone(), vm.history.next_offset);
            }
        }
        KeyCode::Char('d') => {
            if let Some(id) = vm.history.ids.get(vm.history.cursor).cloned() {
                vm.mode = Mode::ConfirmDelete { ids: vec![id] };
            }
        }
        _ => {}
    }
    false
}

/// `r` на курсоре: resume для Paused, retry (через ConfirmRetry) для
/// Failed. Для остальных статусов — no-op; на таких строках в карточке
/// справа показан `pause  delete` (или пусто), так что `r` там не
/// должна вызывать никаких действий.
fn r_action_cursor(
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
        S::Paused => {
            command::resume(channel.clone(), tx.clone(), vec![id]);
        }
        S::Failed => {
            vm.mode = Mode::ConfirmRetry { ids: vec![id] };
        }
        S::Running | S::Retrying | S::Pending | S::Done | S::Cancelled | S::Unspecified => {}
    }
}

/// `p` на курсоре: pause для Running/Retrying/Pending. Остальные
/// статусы — no-op.
fn p_action_cursor(
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
        S::Paused | S::Failed | S::Done | S::Cancelled | S::Unspecified => {}
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
            // Сбрасываем режим до отправки команды: повторный Enter до ответа
            // сервера иначе добавил бы тот же URL ещё раз.
            vm.mode = Mode::Normal;
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
            vm.mode = Mode::Normal;
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
    let Mode::Duplicate { form, .. } = &vm.mode else {
        return;
    };
    let form = form.clone();
    match k.code {
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => vm.mode = Mode::Normal,
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
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
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
            // Убираем из ViewModel немедленно — не ждём gRPC-ответа. Отмена
            // работающей загрузки на сервере может занять секунду (дожидается
            // воркеров), оптимистичное удаление делает UI мгновенным.
            vm.mode = Mode::Normal;
            vm.drop_rows(&ids);
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
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
            vm.mode = Mode::Normal;
            // ConfirmRetry открывается из `r_action_cursor` всегда c
            // одним id — берём первый, остальные (если когда-то будут
            // bulk-retry) можно подцепить позже. Hard-restart требует
            // отдельной RPC на каждую запись, у нас — на одну.
            if let Some(id) = ids.into_iter().next() {
                command::retry(channel, tx, id);
            }
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
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
            vm.drop_rows(&ids);
            vm.mode = Mode::Normal;
            command::remove(channel, tx, ids);
        }
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
            let forms = ghost_redownload_forms(vm, &ids);
            vm.drop_rows(&ids);
            vm.mode = Mode::Normal;
            for form in forms {
                command::add(channel.clone(), tx.clone(), form, None);
            }
        }
        _ => {}
    }
}

fn ghost_redownload_forms(vm: &ViewModel, ids: &[String]) -> Vec<AddForm> {
    ids.iter()
        .filter_map(|id| vm.downloads.get(id))
        .map(|row| AddForm {
            url: row.url.clone(),
            folder: row.target_dir.clone(),
        })
        .collect()
}

fn handle_key_quit_confirm(
    vm: &mut ViewModel,
    k: KeyEvent,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match k.code {
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
            vm.mode = Mode::Normal;
            false
        }
        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => quit_yes(vm, channel, tx),
        _ => false,
    }
}

fn quit_yes(
    vm: &mut ViewModel,
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) -> bool {
    if vm.can_stop_daemon {
        // RPC уходит в фон; по завершении падает `UiEvent::Quit`.
        command::shutdown_daemon(channel, tx);
        vm.mode = Mode::Normal;
        false
    } else {
        true
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
