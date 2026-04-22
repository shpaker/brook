//! Прогресс-бар как фоновая заливка строки с именем файла (§6.3).
//!
//! Бар не рисует собственные символы — только красит `bg` ячеек поверх
//! уже отрисованного текста. Done-зона получает акцентный фон, pending —
//! приглушённый. Таким образом имя файла, префикс и правая колонка
//! остаются читаемыми, а заполнение прогресса виден как «подсветка»
//! строки слева направо.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{
    Color,
    Style,
};
use ratatui::widgets::Widget;

use crate::model::{
    ProgressSnapshot,
    WorkerSegment,
};

pub struct ProgressBar<'a> {
    pub progress: &'a ProgressSnapshot,
    pub workers: &'a [WorkerSegment],
    pub no_color: bool,
}

impl<'a> Widget for ProgressBar<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 || self.no_color {
            // В no-color режиме бар-фон невидим; оставляем строку без заливки.
            let _ = self.workers;
            return;
        }
        let width = area.width as usize;
        // `bytes_total == 0` — размер неизвестен (streaming/unknown-size).
        // Рисуем indeterminate-бар: сплошная заливка `▒` без «готовой» зоны.
        if self.progress.bytes_total == 0 {
            let style = if self.no_color {
                Style::default()
            } else {
                Style::default().fg(Color::Cyan)
            };
            for x in 0..width {
                let cell = buf
                    .cell_mut((area.x + x as u16, area.y))
                    .expect("cell in area");
                cell.set_char('▒');
                cell.set_style(style);
            }
            return;
        }
        let total = self.progress.bytes_total as f64;
        let done = (self.progress.bytes_done as f64 / total).clamp(0.0, 1.0);
        let done_cells = (done * width as f64).round() as usize;
        let done_cells = done_cells.min(width);

        // Приглушённый фон для pending (чуть темнее фона терминала —
        // визуально бар «дышит», не перебивая текст).
        let pending_bg = Color::Rgb(0x22, 0x22, 0x22);
        // Тёмно-зелёный для done — контрастный, но не режет глаз.
        let done_bg = Color::Rgb(0x1e, 0x4d, 0x1e);

        for x in 0..width {
            let cell = buf
                .cell_mut((area.x + x as u16, area.y))
                .expect("cell in area");
            let bg = if x < done_cells { done_bg } else { pending_bg };
            cell.set_bg(bg);
        }

        let _ = self.workers;
    }
}
