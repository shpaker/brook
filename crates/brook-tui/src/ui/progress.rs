//! Кастомный прогресс-бар с сегментами воркеров (§6.3).
//!
//! Зоны слева направо:
//!
//! 1. **Done** — `█`, accent-цвет.
//! 2. **Активные куски** — по одному сегменту на воркера, ширина
//!    `piece_size / total × bar_width`. Цвет из циклической палитры по
//!    `worker_id % N`. Под `NO_COLOR` все активные — `▓`, без цвета.
//! 3. **Pending** — `░`.
//!
//! No-Range fallback (`pieces_total = 1`) — бар = done + один активный
//! сегмент, pending-дырок нет (сама модель в этом случае не хранит
//! больше одного worker-сегмента).

use brook_proto::brook::v1::Progress;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{
    Color,
    Style,
};
use ratatui::widgets::Widget;

use crate::model::WorkerSegment;

/// Палитра цветов активных сегментов — 6 штук, выбираем по
/// `worker_id % 6`. Подобраны так, чтобы на тёмной теме все читались.
const WORKER_PALETTE: [Color; 6] = [
    Color::Cyan,
    Color::Magenta,
    Color::Yellow,
    Color::Blue,
    Color::Green,
    Color::LightRed,
];

pub struct ProgressBar<'a> {
    pub progress: &'a Progress,
    pub workers: &'a [WorkerSegment],
    pub no_color: bool,
}

impl<'a> Widget for ProgressBar<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let width = area.width as usize;
        let total = self.progress.bytes_total.max(1) as f64;
        let done = (self.progress.bytes_done as f64 / total).clamp(0.0, 1.0);
        let done_cells = (done * width as f64).round() as usize;
        let done_cells = done_cells.min(width);

        // Заполняем pending по всей длине, потом затираем done + активные.
        for x in 0..width {
            let cell = buf
                .cell_mut((area.x + x as u16, area.y))
                .expect("cell in area");
            cell.set_char('░');
            if !self.no_color {
                cell.set_style(Style::default().fg(Color::DarkGray));
            }
        }

        let done_color = if self.no_color {
            Style::default()
        } else {
            Style::default().fg(Color::Green)
        };
        for x in 0..done_cells {
            let cell = buf
                .cell_mut((area.x + x as u16, area.y))
                .expect("cell in done zone");
            cell.set_char('█');
            cell.set_style(done_color);
        }

        if self.progress.pieces_total == 0 {
            return;
        }
        let piece_size = total / self.progress.pieces_total as f64;
        for seg in self.workers {
            let piece_start = (seg.piece_index as f64 * piece_size) / total;
            let piece_end = ((seg.piece_index as f64 + 1.0) * piece_size) / total;
            let start_cell = (piece_start * width as f64).floor() as usize;
            let end_cell = (piece_end * width as f64).ceil() as usize;
            let start_cell = start_cell.max(done_cells);
            let end_cell = end_cell.min(width);
            if start_cell >= end_cell {
                continue;
            }
            let (glyph, style) = if self.no_color {
                ('▓', Style::default())
            } else {
                let color = WORKER_PALETTE[(seg.piece_index as usize) % WORKER_PALETTE.len()];
                ('█', Style::default().fg(color))
            };
            for x in start_cell..end_cell {
                let cell = buf
                    .cell_mut((area.x + x as u16, area.y))
                    .expect("cell in worker segment");
                cell.set_char(glyph);
                cell.set_style(style);
            }
        }
    }
}
