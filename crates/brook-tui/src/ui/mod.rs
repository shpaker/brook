//! Рендер TUI. Одна внешняя rounded-рамка с двумя bracket-сегментами
//! (`[ brook | addr ]` сверху, `[ ␣ action | a add | d delete | ? help
//! ]` снизу), внутри — плоский список карточек. Модалки и overlay'и
//! идут поверх.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{
    Color,
    Style,
};
use ratatui::widgets::{
    Block,
    BorderType,
    Paragraph,
};

use crate::model::ViewModel;

mod card;
mod chrome;
mod list;
pub mod modal;
mod progress;

/// Минимальный размер терминала. Ниже — заглушка без попыток рендера.
const MIN_WIDTH: u16 = 60;
const MIN_HEIGHT: u16 = 15;

pub fn draw(f: &mut Frame, vm: &ViewModel) {
    let area = f.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        draw_too_small(f, area);
        return;
    }
    let no_color = std::env::var_os("NO_COLOR").is_some();

    let border_style = if no_color {
        Style::default()
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let mut block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border_style)
        .title(chrome::top_title(vm))
        .title_bottom(chrome::hints_bar(chrome::action_word(vm)));
    if let Some(toast) = chrome::toast_line(vm) {
        block = block.title_bottom(toast);
    }

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Внутри — только список карточек с паддингом 1 по краям.
    let list_area = Rect {
        x: inner.x.saturating_add(1),
        y: inner.y.saturating_add(1),
        width: inner.width.saturating_sub(2),
        height: inner.height.saturating_sub(2),
    };
    list::draw(f, list_area, vm, no_color);

    modal::draw_overlay(f, vm, no_color);
}

fn draw_too_small(f: &mut Frame, area: Rect) {
    let msg = Paragraph::new("brook needs at least 60×15");
    f.render_widget(msg, area);
}
