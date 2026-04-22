//! Рендер одной карточки (3 строки контента + blank). Фиксированная
//! высота избавляет от «прыжков» при Running → Done / Pending → Running.

use brook_proto::brook::v1::FileStatus;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{
    Color,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};
use ratatui::widgets::Paragraph;

use crate::format;
use crate::model::DownloadRow;
use crate::ui::progress::progress_line;

/// Высота одной карточки: 3 строки содержимого + 1 пустой разделитель.
pub const CARD_HEIGHT: u16 = 4;

/// Ширина левого «желоба» перед контентом: ticker(2) + glyph(1) +
/// spaces(3) = 6. Для 2-й и 3-й строк glyph заменяется пробелами.
const GUTTER: u16 = 6;
/// Правый margin: action(1) + spaces(2) = 3. Используется только для
/// первой строки; meta/progress растягиваются до (area.width − GUTTER).
const RIGHT_MARGIN: u16 = 3;

pub fn draw(
    f: &mut Frame,
    area: Rect,
    row: &DownloadRow,
    is_cursor: bool,
    is_selected: bool,
    no_color: bool,
) {
    if area.height == 0 || area.width < GUTTER + RIGHT_MARGIN + 4 {
        return;
    }
    let ticker = ticker_span(is_cursor, is_selected, no_color);

    let line1 = title_line(row, ticker.clone(), area.width, no_color);
    let line2 = meta_line(row, ticker.clone(), area.width);
    let content_width = area.width.saturating_sub(GUTTER);
    let line3_bar = progress_line(row, content_width);
    let line3 = prefix_with_ticker(ticker, line3_bar);

    let lines = vec![line1, line2, line3];
    f.render_widget(
        Paragraph::new(lines),
        Rect {
            x: area.x,
            y: area.y,
            width: area.width,
            height: area.height.min(3),
        },
    );
}

fn ticker_span(is_cursor: bool, is_selected: bool, no_color: bool) -> Span<'static> {
    let (ch, color) = if is_cursor {
        ('▌', Color::Cyan)
    } else if is_selected {
        ('▌', Color::DarkGray)
    } else {
        (' ', Color::Reset)
    };
    let mut style = Style::default();
    if !no_color && ch != ' ' {
        style = style.fg(color);
    }
    Span::styled(format!("{ch} "), style)
}

fn dim<S: Into<String>>(s: S) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::DarkGray))
}

fn err_span<S: Into<String>>(s: S) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::Red))
}

fn title_line(
    row: &DownloadRow,
    ticker: Span<'static>,
    width: u16,
    _no_color: bool,
) -> Line<'static> {
    let glyph = status_glyph(row.status);
    let action = action_glyph(row.status);

    let name_is_dim = matches!(
        row.status,
        FileStatus::Done | FileStatus::Failed | FileStatus::Cancelled
    );

    // Доступная ширина под имя: width - ticker(2) - glyph-block(4) - action-block(3).
    let name_space = width
        .saturating_sub(2) // ticker
        .saturating_sub(4) // glyph + 3 spaces
        .saturating_sub(3) as usize; // action + 2 spaces right margin
    let name = format::right_ellipsis(row.display_name(), name_space);
    let name_pad = name_space.saturating_sub(name.chars().count());

    let name_style = if name_is_dim {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    Line::from(vec![
        ticker,
        dim(format!("{glyph}   ")),
        Span::styled(name, name_style),
        Span::raw(" ".repeat(name_pad + 1)),
        dim(action.to_string()),
        dim("  "),
    ])
}

fn meta_line(row: &DownloadRow, ticker: Span<'static>, width: u16) -> Line<'static> {
    let content_width = width.saturating_sub(GUTTER) as usize;
    let (text, is_err) = meta_text(row);
    let trimmed = format::right_ellipsis(&text, content_width);
    let span = if is_err {
        err_span(trimmed)
    } else {
        dim(trimmed)
    };
    Line::from(vec![ticker, Span::raw("    "), span])
}

fn prefix_with_ticker(ticker: Span<'static>, inner: Line<'static>) -> Line<'static> {
    let mut spans = Vec::with_capacity(inner.spans.len() + 2);
    spans.push(ticker);
    spans.push(Span::raw("    "));
    spans.extend(inner.spans);
    Line::from(spans)
}

fn status_glyph(s: FileStatus) -> &'static str {
    match s {
        FileStatus::Running => "⏵",
        FileStatus::Paused => "⏸",
        FileStatus::Retrying => "↻",
        FileStatus::Pending => "⏳",
        FileStatus::Done => "✓",
        FileStatus::Failed => "✕",
        FileStatus::Cancelled | FileStatus::Unspecified => "·",
    }
}

fn action_glyph(s: FileStatus) -> &'static str {
    match s {
        FileStatus::Running | FileStatus::Retrying | FileStatus::Pending => "⏸",
        FileStatus::Paused => "⏵",
        FileStatus::Failed => "↻",
        FileStatus::Done => "→",
        FileStatus::Cancelled | FileStatus::Unspecified => " ",
    }
}

/// Возвращает (meta-text, is_error_style).
fn meta_text(row: &DownloadRow) -> (String, bool) {
    let p = &row.progress;
    match row.status {
        FileStatus::Running => {
            let eta = p.eta_secs.map(format::eta).unwrap_or_else(|| "—".into());
            let done = format::bytes(p.bytes_done);
            let total = if p.bytes_total > 0 {
                format::bytes(p.bytes_total)
            } else {
                "—".into()
            };
            let speed = format::speed(p.speed_bps);
            (
                format!("{eta} left  ·  {done} of {total}  ·  {speed}"),
                false,
            )
        }
        FileStatus::Paused => {
            let done = format::bytes(p.bytes_done);
            let total = if p.bytes_total > 0 {
                format::bytes(p.bytes_total)
            } else {
                "—".into()
            };
            (format!("paused  ·  {done} of {total}"), false)
        }
        FileStatus::Retrying => {
            let err = row.error.as_deref().unwrap_or("transient error");
            if row.max_attempts > 0 {
                (
                    format!("retrying {}/{}  ·  {}", row.attempt, row.max_attempts, err),
                    false,
                )
            } else {
                (format!("retrying {}  ·  {}", row.attempt, err), false)
            }
        }
        FileStatus::Pending => (format!("queued  ·  {}", url_host(&row.url)), false),
        FileStatus::Done => {
            let size = if p.bytes_total > 0 {
                format::bytes(p.bytes_total)
            } else {
                "—".into()
            };
            (format!("done  ·  {}  ·  {size}", url_host(&row.url)), false)
        }
        FileStatus::Failed => {
            let err = row.error.as_deref().unwrap_or("unknown error");
            (format!("failed  ·  {err}"), true)
        }
        FileStatus::Cancelled => ("cancelled".into(), false),
        FileStatus::Unspecified => (String::new(), false),
    }
}

fn url_host(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let host = after_scheme
        .split_once('/')
        .map(|(h, _)| h)
        .unwrap_or(after_scheme);
    if host.is_empty() {
        url.to_string()
    } else {
        host.to_string()
    }
}
