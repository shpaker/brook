//! Вертикальный список карточек. Скролл — курсор держим в видимой
//! области; каждая карточка фиксированной высоты (`CARD_HEIGHT`).

use ratatui::Frame;
use ratatui::layout::Rect;

use crate::model::ViewModel;
use crate::ui::card::{
    self,
    CARD_HEIGHT,
};

pub fn draw(f: &mut Frame, area: Rect, vm: &ViewModel, no_color: bool) {
    if area.height < CARD_HEIGHT {
        return;
    }
    let ids = vm.visible_ids();
    if ids.is_empty() {
        return;
    }
    let cursor = vm.cursor.min(ids.len() - 1);

    let viewport_cards = (area.height / CARD_HEIGHT) as usize;
    if viewport_cards == 0 {
        return;
    }
    let scroll = compute_scroll(cursor, viewport_cards, ids.len());

    for slot in 0..viewport_cards {
        let idx = scroll + slot;
        if idx >= ids.len() {
            break;
        }
        let Some(row) = vm.downloads.get(&ids[idx]) else {
            continue;
        };
        let card_area = Rect {
            x: area.x,
            y: area.y + slot as u16 * CARD_HEIGHT,
            width: area.width,
            height: CARD_HEIGHT,
        };
        card::draw(
            f,
            card_area,
            row,
            idx == cursor,
            vm.selected.contains(&ids[idx]),
            no_color,
        );
    }
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
