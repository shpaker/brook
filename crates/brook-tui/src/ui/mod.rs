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
pub mod modal;
mod progress;
mod status;

/// Минимальный размер терминала. Меньше — заглушка без попыток рендера.
const MIN_WIDTH: u16 = 60;
const MIN_HEIGHT: u16 = 15;

pub fn draw(f: &mut Frame, vm: &ViewModel) {
    let area = f.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        draw_too_small(f, area);
        return;
    }
    let no_color = std::env::var_os("NO_COLOR").is_some();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(2), // status
            Constraint::Min(0),    // list
            Constraint::Length(5), // detail
            Constraint::Length(1), // hint
        ])
        .split(area);

    status::draw(f, chunks[0], vm, no_color);
    list::draw(f, chunks[1], vm, no_color);
    detail::draw(f, chunks[2], vm, no_color);
    hint::draw(f, chunks[3], no_color);

    modal::draw_overlay(f, vm, no_color);

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
