//! Двухслойный прогресс: dim track `─` + accent fill `━`, варианты по
//! статусу. Возвращает `Line` фиксированной длины `width`; рендерится
//! как третья строка карточки.
//!
//! Искры `◆` на позициях активных piece'ов планировались, но
//! `WorkerSegment`-данные до proto не проброшены — пока сегменты не
//! появятся в `WatchProgress`, искры остаются визуальным TODO.

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

pub fn progress_line(row: &DownloadRow, width: u16) -> Line<'static> {
    let width = width as usize;
    if width == 0 {
        return Line::from("");
    }

    match row.status {
        FileStatus::Done => full_bar(width, accent_dim()),
        FileStatus::Pending | FileStatus::Cancelled | FileStatus::Unspecified => full_track(width),
        FileStatus::Failed => failed_bar(row, width),
        FileStatus::Paused => filled_bar(row, width, Color::Yellow),
        FileStatus::Retrying => filled_bar(row, width, Color::Yellow),
        FileStatus::Running => filled_bar(row, width, Color::Cyan),
    }
}

fn accent_dim() -> Style {
    Style::default().fg(Color::DarkGray)
}

fn track_style() -> Style {
    Style::default().fg(Color::DarkGray)
}

fn full_bar(width: usize, style: Style) -> Line<'static> {
    Line::from(Span::styled("━".repeat(width), style))
}

fn full_track(width: usize) -> Line<'static> {
    Line::from(Span::styled("─".repeat(width), track_style()))
}

fn filled_bar(row: &DownloadRow, width: usize, fill_color: Color) -> Line<'static> {
    let ratio = ratio(row);
    let fill_cells = ((ratio * width as f64).round() as usize).min(width);
    Line::from(vec![
        Span::styled("━".repeat(fill_cells), Style::default().fg(fill_color)),
        Span::styled("─".repeat(width - fill_cells), track_style()),
    ])
}

fn failed_bar(row: &DownloadRow, width: usize) -> Line<'static> {
    let ratio = ratio(row);
    let fill_cells = ((ratio * width as f64).round() as usize).min(width);
    Line::from(vec![
        Span::styled("━".repeat(fill_cells), Style::default().fg(Color::Red)),
        Span::styled("─".repeat(width - fill_cells), track_style()),
    ])
}

pub(crate) fn ratio(row: &DownloadRow) -> f64 {
    if row.progress.bytes_total == 0 {
        0.0
    } else {
        (row.progress.bytes_done as f64 / row.progress.bytes_total as f64).clamp(0.0, 1.0)
    }
}
