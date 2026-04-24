//! Двухслойный прогресс: dim track `─` + accent fill `━`, варианты по
//! статусу. Возвращает `Line` фиксированной длины `width`; рендерится
//! как третья строка карточки.
//!
//! Когда бэкенд присылает `BarState` (загрузка не линейная, размер известен),
//! рендерим chunked bar из трёх состояний ячейки:
//! - **Active** `◆` Cyan — воркер качает прямо сейчас.
//! - **Done**   `━` White (в фокусе) / DarkGray — кусок завершён.
//! - **Pending** `─` White (в фокусе) / DarkGray — ещё не начат.
//!
//! TUI масштабирует N≤100 бэкендных сегментов к реальной ширине бара:
//! несколько сегментов на ячейку → агрегация по приоритету Active > Done > Pending.

use brook_proto::brook::v1::{
    BarState,
    FileStatus,
};
use ratatui::style::{
    Color,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};

use crate::model::DownloadRow;

pub fn progress_line(row: &DownloadRow, width: u16, is_cursor: bool) -> Line<'static> {
    let width = width as usize;
    if width == 0 {
        return Line::from("");
    }

    match row.status {
        FileStatus::Done => full_bar(width, accent_style(is_cursor)),
        FileStatus::Pending | FileStatus::Cancelled | FileStatus::Unspecified => {
            full_track(width, is_cursor)
        }
        FileStatus::Failed => {
            if let Some(bar) = &row.progress.bar
                && !bar.segments.is_empty()
            {
                chunked_bar(bar, width, is_cursor)
            } else {
                failed_bar(row, width, is_cursor)
            }
        }
        FileStatus::Paused | FileStatus::Retrying => {
            if let Some(bar) = &row.progress.bar
                && !bar.segments.is_empty()
            {
                chunked_bar(bar, width, is_cursor)
            } else {
                filled_bar(row, width, Color::Yellow, is_cursor)
            }
        }
        FileStatus::Running => {
            if let Some(bar) = &row.progress.bar
                && !bar.segments.is_empty()
            {
                chunked_bar(bar, width, is_cursor)
            } else {
                filled_bar(row, width, Color::Cyan, is_cursor)
            }
        }
    }
}

/// Chunked bar: N≤100 сегментов бэкенда → `width` ячеек.
///
/// Приоритет ячейки: Active (`◆` Cyan) > Done (`━`) > Pending (`─`).
/// В фокусе оба символа White, иначе DarkGray.
/// Один сегмент может растягиваться на несколько ячеек (W > N) или
/// несколько сегментов могут попасть в одну ячейку (W < N) — агрегация
/// одинакова.
fn chunked_bar(bar: &BarState, width: usize, is_cursor: bool) -> Line<'static> {
    let s = bar.segments.len();
    if s == 0 || width == 0 {
        return full_track(width, is_cursor);
    }

    let mut spans: Vec<Span<'static>> = Vec::with_capacity(width);
    let done_style = accent_style(is_cursor);
    let pending_style = accent_style(is_cursor);

    for cell in 0..width {
        // Диапазон сегментов, которые попадают в эту ячейку.
        let seg_from = cell * s / width;
        let seg_to = (((cell + 1) * s / width).min(s)).max(seg_from + 1);

        // Ячейка показывает ━, если хотя бы один кусок в диапазоне завершён.
        let any_done = bar.segments[seg_from..seg_to].iter().any(|&f| f > 0.0);

        spans.push(if any_done {
            Span::styled("━", done_style)
        } else {
            Span::styled("─", pending_style)
        });
    }

    Line::from(spans)
}

fn accent_style(is_cursor: bool) -> Style {
    let color = if is_cursor {
        Color::White
    } else {
        Color::DarkGray
    };
    Style::default().fg(color)
}

fn full_bar(width: usize, style: Style) -> Line<'static> {
    Line::from(Span::styled("━".repeat(width), style))
}

fn full_track(width: usize, is_cursor: bool) -> Line<'static> {
    Line::from(Span::styled("─".repeat(width), accent_style(is_cursor)))
}

fn filled_bar(
    row: &DownloadRow,
    width: usize,
    fill_color: Color,
    is_cursor: bool,
) -> Line<'static> {
    let ratio = ratio(row);
    let fill_cells = ((ratio * width as f64).round() as usize).min(width);
    Line::from(vec![
        Span::styled("━".repeat(fill_cells), Style::default().fg(fill_color)),
        Span::styled("─".repeat(width - fill_cells), accent_style(is_cursor)),
    ])
}

fn failed_bar(row: &DownloadRow, width: usize, is_cursor: bool) -> Line<'static> {
    let ratio = ratio(row);
    let fill_cells = ((ratio * width as f64).round() as usize).min(width);
    Line::from(vec![
        Span::styled("━".repeat(fill_cells), Style::default().fg(Color::Red)),
        Span::styled("─".repeat(width - fill_cells), accent_style(is_cursor)),
    ])
}

pub(crate) fn ratio(row: &DownloadRow) -> f64 {
    if row.progress.bytes_total == 0 {
        0.0
    } else {
        (row.progress.bytes_done as f64 / row.progress.bytes_total as f64).clamp(0.0, 1.0)
    }
}
