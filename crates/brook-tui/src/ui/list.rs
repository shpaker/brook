//! Вертикальный список карточек. Скролл — курсор держим в видимой
//! области; каждая карточка фиксированной высоты (`CARD_HEIGHT`).
//! Крайний правый столбец области отведён под всегда видимый
//! скроллбар: даже когда весь список помещается во viewport или
//! пуст — трек рисуется, чтобы фрейм оставался визуально стабильным.
//!
//! `draw_main` / `draw_history` отличаются только источником id'ов и
//! строкой empty-state. Сама карточка не меняется.

use ratatui::Frame;
use ratatui::layout::{
    Alignment,
    Rect,
};
use ratatui::style::{
    Color,
    Modifier,
    Style,
};
use ratatui::widgets::{
    Paragraph,
    Scrollbar,
    ScrollbarOrientation,
    ScrollbarState,
};

use crate::model::ViewModel;
use crate::ui::card::{
    self,
    CARD_HEIGHT,
};

/// Ширина колонки под всегда видимый скроллбар справа.
const SCROLLBAR_WIDTH: u16 = 1;
/// Зазор между контентом карточек и скроллбаром — две колонки воздуха,
/// чтобы текст/прогресс-бар не «лип» к треку.
const SCROLLBAR_GAP: u16 = 2;
/// Сколько колонок суммарно забираем справа у области списка:
/// сам скроллбар + зазор перед ним.
const RIGHT_RESERVED: u16 = SCROLLBAR_WIDTH + SCROLLBAR_GAP;

/// Главный экран — список «recently» (фильтр на сервере). Empty-state
/// `"No activity in the last 24 hours."` показывается только после
/// первого ответа `GetRecently`.
pub fn draw_main(f: &mut Frame, area: Rect, vm: &ViewModel, no_color: bool) {
    let ids = vm.visible_ids();
    let cursor = vm.cursor;
    let empty_msg = if vm.recently_loaded {
        Some("No activity in the last 24 hours.")
    } else {
        None
    };
    draw_list(f, area, vm, &ids, cursor, empty_msg, None, no_color);
}

/// Экран истории — рендер по `vm.history.ids` (порядок от сервера, без
/// клиентской пере-сортировки). Empty-state `"History is empty."`. Пока
/// идёт фоновая подгрузка следующей страницы — внизу `cards_area`
/// рисуется `loading next page…` тонкой dim-строкой.
pub fn draw_history(f: &mut Frame, area: Rect, vm: &ViewModel, no_color: bool) {
    let ids = vm.history.ids.clone();
    let cursor = vm.history.cursor;
    let empty_msg = if vm.history.loading && ids.is_empty() {
        None
    } else {
        Some("History is empty.")
    };
    let footer = if vm.history.loading && !ids.is_empty() {
        Some("loading next page…")
    } else {
        None
    };
    draw_list(f, area, vm, &ids, cursor, empty_msg, footer, no_color);
}

#[allow(clippy::too_many_arguments)]
fn draw_list(
    f: &mut Frame,
    area: Rect,
    vm: &ViewModel,
    ids: &[String],
    cursor: usize,
    empty_msg: Option<&'static str>,
    footer: Option<&'static str>,
    no_color: bool,
) {
    if area.width <= RIGHT_RESERVED {
        return;
    }
    let cards_area = Rect {
        x: area.x,
        y: area.y,
        width: area.width - RIGHT_RESERVED,
        height: area.height,
    };
    let scrollbar_area = Rect {
        x: area.x + area.width - SCROLLBAR_WIDTH,
        y: area.y,
        width: SCROLLBAR_WIDTH,
        height: area.height,
    };

    // Под `loading next page…` забираем нижнюю строку cards_area, чтобы
    // не накладываться на последнюю карточку. Если высоты не хватает —
    // пропускаем footer.
    let footer_h: u16 = if footer.is_some() && cards_area.height > CARD_HEIGHT {
        1
    } else {
        0
    };
    let cards_inner = Rect {
        height: cards_area.height.saturating_sub(footer_h),
        ..cards_area
    };

    let viewport_cards = (cards_inner.height / CARD_HEIGHT) as usize;

    let (scroll, total) = if ids.is_empty() || viewport_cards == 0 {
        (0usize, ids.len())
    } else {
        let cursor_clamped = cursor.min(ids.len() - 1);
        (
            compute_scroll(cursor_clamped, viewport_cards, ids.len()),
            ids.len(),
        )
    };

    if cards_inner.height >= CARD_HEIGHT && !ids.is_empty() && viewport_cards > 0 {
        let cursor_clamped = cursor.min(ids.len() - 1);
        for slot in 0..viewport_cards {
            let idx = scroll + slot;
            if idx >= ids.len() {
                break;
            }
            let Some(row) = vm.downloads.get(&ids[idx]) else {
                continue;
            };
            let card_area = Rect {
                x: cards_inner.x,
                y: cards_inner.y + slot as u16 * CARD_HEIGHT,
                width: cards_inner.width,
                height: CARD_HEIGHT,
            };
            card::draw(f, card_area, row, idx == cursor_clamped, no_color);
        }
    } else if let Some(msg) = empty_msg
        && cards_inner.height >= 1
    {
        let style = if no_color {
            Style::default().add_modifier(Modifier::DIM)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let para = Paragraph::new(msg)
            .alignment(Alignment::Center)
            .style(style);
        let line_y = cards_inner.y + cards_inner.height / 2;
        let line_area = Rect {
            x: cards_inner.x,
            y: line_y,
            width: cards_inner.width,
            height: 1,
        };
        f.render_widget(para, line_area);
    }

    if footer_h > 0
        && let Some(msg) = footer
    {
        let style = if no_color {
            Style::default().add_modifier(Modifier::DIM)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let para = Paragraph::new(msg).alignment(Alignment::Left).style(style);
        let footer_area = Rect {
            x: cards_area.x,
            y: cards_area.y + cards_area.height - 1,
            width: cards_area.width,
            height: 1,
        };
        f.render_widget(para, footer_area);
    }

    draw_scrollbar(f, scrollbar_area, scroll, viewport_cards, total, no_color);
}

fn draw_scrollbar(
    f: &mut Frame,
    area: Rect,
    scroll: usize,
    viewport: usize,
    total: usize,
    no_color: bool,
) {
    if area.height == 0 {
        return;
    }

    let (track_style, thumb_style) = if no_color {
        (
            Style::default().add_modifier(Modifier::DIM),
            Style::default().add_modifier(Modifier::DIM),
        )
    } else {
        (
            Style::default().fg(Color::DarkGray),
            Style::default().fg(Color::Gray),
        )
    };

    let widget = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("│"))
        .thumb_symbol("┃")
        .track_style(track_style)
        .thumb_style(thumb_style);

    // `ratatui::Scrollbar` трактует `position` как индекс верхней
    // строки в «скроллируемом пространстве» длиной `content_length`
    // и клампит его к `[0, content_length - 1]`. В нашей модели
    // `scroll ∈ [0, total - viewport]` — это количество возможных
    // верхних позиций, поэтому виджету нужно отдавать
    // `content_length = total - viewport + 1` (а `viewport_length`
    // оставляем настоящим, чтобы сохранить пропорцию тамба
    // `viewport / total`). Иначе при скролле в самый низ тамб
    // упирается в ~середину трека, а не в его конец.
    //
    // Плюс особый случай «скроллить некуда» (пусто или всё влезает):
    // ratatui не рисует скроллбар при `content_length == 0`, а при
    // целиком помещающемся контенте тамб занимал бы лишь часть
    // трека — обе ситуации ломают «всегда видимый» скроллбар. Для
    // них подставляем (1, 1, 0): полный трек, тамб во всю высоту —
    // визуальный сигнал «двигаться некуда».
    let (content_len, viewport_len, pos) = if total == 0 || total <= viewport {
        (1, 1, 0)
    } else {
        (total - viewport + 1, viewport, scroll)
    };

    let mut state = ScrollbarState::new(content_len)
        .viewport_content_length(viewport_len)
        .position(pos);

    f.render_stateful_widget(widget, area, &mut state);
}

fn compute_scroll(cursor: usize, viewport: usize, total: usize) -> usize {
    if total <= viewport {
        return 0;
    }
    let max = total - viewport;
    // Держим курсор примерно в центре.
    let desired = cursor.saturating_sub(viewport / 2);
    desired.min(max)
}
