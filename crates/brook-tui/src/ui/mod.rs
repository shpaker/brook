//! Рендер TUI. Корневой `draw` режет экран на §6.1-слои
//! `status(2) · list(Min) · detail(5) · hint(1)` и делегирует
//! отрисовку подмодулям.

use ratatui::Frame;
use ratatui::layout::{
    Constraint,
    Direction,
    Layout,
    Rect,
};
use ratatui::style::{
    Modifier,
    Style,
};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use crate::model::ViewModel;

mod detail;
mod hint;
mod list;
mod progress;
mod status;

/// Минимальный размер терминала. Меньше — заглушка без попыток рендера.
const MIN_WIDTH: u16 = 60;
const MIN_HEIGHT: u16 = 15;
/// Порог авто-скрытия detail-панели.
const DETAIL_AUTO_HIDE_HEIGHT: u16 = 20;

pub fn draw(f: &mut Frame, vm: &ViewModel) {
    let area = f.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        draw_too_small(f, area);
        return;
    }

    // Detail скрывается автоматически при height < 20, но toggle
    // (`Tab`) работает поверх — пользователь может и раскрыть, и закрыть.
    let show_detail = vm.detail_visible && area.height >= DETAIL_AUTO_HIDE_HEIGHT;

    let constraints: Vec<Constraint> = if show_detail {
        vec![
            Constraint::Length(2), // status
            Constraint::Min(0),    // list
            Constraint::Length(5), // detail
            Constraint::Length(1), // hint
        ]
    } else {
        vec![
            Constraint::Length(2),
            Constraint::Min(0),
            Constraint::Length(1),
        ]
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    status::draw(f, chunks[0], vm);
    list::draw(f, chunks[1], vm);
    if show_detail {
        detail::draw(f, chunks[2], vm);
        hint::draw(f, chunks[3]);
    } else {
        hint::draw(f, chunks[2]);
    }

    // Toast рендерится поверх последней строки списка — простая
    // однострочная плашка.
    if let Some(t) = &vm.toast {
        let toast_area = Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(2),
            width: area.width,
            height: 1,
        };
        f.render_widget(
            Paragraph::new(Line::from(t.message.clone()))
                .style(Style::default().add_modifier(Modifier::REVERSED)),
            toast_area,
        );
    }
}

fn draw_too_small(f: &mut Frame, area: Rect) {
    let msg = Paragraph::new("brook needs at least 60×15");
    f.render_widget(msg, area);
}
