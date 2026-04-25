//! Двухслойный прогресс: dim track `─` + accent fill `━`, варианты по
//! статусу. Возвращает `Line` фиксированной длины `width`; рендерится
//! как третья строка карточки.

use brook_proto::brook::v1::FileStatus;
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
        FileStatus::Failed => failed_bar(row, width, is_cursor),
        FileStatus::Paused | FileStatus::Retrying => {
            filled_bar(row, width, Color::Yellow, is_cursor)
        }
        FileStatus::Running => filled_bar(row, width, Color::Cyan, is_cursor),
    }
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
