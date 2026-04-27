//! Рендер TUI. Одна внешняя rounded-рамка: сверху по центру —
//! `[ brook · addr · <screen> ]`; внутри — плоский список карточек,
//! содержимое зависит от `vm.screen` (`Main` → recently / `History` →
//! пагинированный список); снизу — `hints_bar` со всеми действиями
//! текущего экрана плюс тост слева, когда активен. Модалки и overlay'и
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

use crate::model::{
    Screen,
    ViewModel,
};

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
        .title(chrome::top_brand(vm))
        .title_bottom(chrome::hints_bar(vm, no_color));
    if let Some(toast) = chrome::toast_line(vm) {
        block = block.title_bottom(toast);
    }

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Внутри — только список карточек с паддингом 1 по краям.
    // Крайнюю правую колонку внутри `list_area` забирает всегда
    // видимый скроллбар из `list::draw` — так между ним и рамкой
    // остаётся 1-колонка воздуха, симметричная левому паддингу.
    let list_area = Rect {
        x: inner.x.saturating_add(1),
        y: inner.y.saturating_add(1),
        width: inner.width.saturating_sub(2),
        height: inner.height.saturating_sub(2),
    };
    match vm.screen {
        Screen::Main => list::draw_main(f, list_area, vm, no_color),
        Screen::History => list::draw_history(f, list_area, vm, no_color),
    }

    modal::draw_overlay(f, vm, no_color);
}

fn draw_too_small(f: &mut Frame, area: Rect) {
    let msg = Paragraph::new("brook needs at least 60×15");
    f.render_widget(msg, area);
}
