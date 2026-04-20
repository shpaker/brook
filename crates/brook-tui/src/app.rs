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
    KeyEventKind,
    KeyModifiers,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::events::UiEvent;
use crate::model::{
    ConnectionState,
    ViewModel,
};
use crate::{
    ui,
    watch,
};

const TICK: Duration = Duration::from_millis(250);

pub async fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    channel: Channel,
    settings: GetSettingsResponse,
    port: u16,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<UiEvent>();

    // Input-task: блокирующий crossterm::event::read в отдельном
    // spawn_blocking-потоке. Переводим сырые crossterm-события в UiEvent.
    let input_tx = tx.clone();
    let input_handle = std::thread::spawn(move || input_loop(input_tx));

    // Watch-task.
    let watch_handle = watch::spawn(channel.clone(), tx.clone());

    // Tick-task: 250 ms-пинг для toast'ов и ETA.
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

    let mut vm = ViewModel::new(port, settings);

    // Первый рендер до первых событий.
    terminal.draw(|f| ui::draw(f, &vm)).context("draw")?;

    while let Some(ev) = rx.recv().await {
        if handle_event(&mut vm, ev) {
            break;
        }
        if let Some(t) = &vm.toast
            && t.expires_at <= Instant::now()
        {
            vm.toast = None;
        }
        terminal.draw(|f| ui::draw(f, &vm)).context("draw")?;
    }

    // Прибираем фоновые задачи. input_loop остановится сам после exit из
    // цикла (его tx умрёт последним, когда упадёт receiver).
    watch_handle.abort();
    tick_handle.abort();
    drop(rx);
    let _ = input_handle.join();

    Ok(())
}

/// Возвращает `true`, если пора выходить из цикла.
fn handle_event(vm: &mut ViewModel, ev: UiEvent) -> bool {
    match ev {
        UiEvent::Key(k) => {
            if k.kind != KeyEventKind::Press {
                return false;
            }
            match k.code {
                KeyCode::Char('q') => return true,
                KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => return true,
                KeyCode::Tab => vm.detail_visible = !vm.detail_visible,
                KeyCode::Up | KeyCode::Char('k') => {
                    vm.cursor = vm.cursor.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    vm.cursor = vm.cursor.saturating_add(1);
                }
                KeyCode::Char('g') => vm.cursor = 0,
                KeyCode::Char('G') => vm.cursor = usize::MAX, // clamp в draw/visible_ids
                _ => {}
            }
        }
        UiEvent::Resize(_, _) => {}
        UiEvent::Paste(_) => {}
        UiEvent::Mouse(_) => {}
        UiEvent::Stream(ev) => vm.apply_stream(*ev),
        UiEvent::StreamConnected => {
            vm.connection = ConnectionState::Connected;
            vm.reset(); // §6.2: initial-Snapshot'ы зальют заново.
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
        UiEvent::Tick => {}
    }
    false
}

/// Синхронный input-loop — блокирует поток на `crossterm::event::read`
/// и перекладывает в mpsc. Tокio-рантайму дешевле держать такой в
/// отдельном `std::thread`, чем в `spawn_blocking`, потому что поток
/// живёт всё время приложения.
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
