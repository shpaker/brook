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

/// Ширина левого «желоба» перед контентом: ticker(1) + 2 пробела = 3.
/// Контент всех трёх строк начинается с одной колонки; на 3-й строке
/// первым символом контента идёт status-glyph, а сама подпись — через
/// один пробел после него.
const GUTTER: u16 = 3;
/// Правый блок первой строки: `␣  <action>  ` = 6 колонок. Для Cancelled
/// рисуются шесть пробелов — чтобы правый край не «прыгал».
const RIGHT_MARGIN: u16 = 6;

pub fn draw(f: &mut Frame, area: Rect, row: &DownloadRow, is_cursor: bool, no_color: bool) {
    if area.height == 0 || area.width < GUTTER + RIGHT_MARGIN + 4 {
        return;
    }
    let ticker = ticker_span(is_cursor, no_color);

    let content_width = area.width.saturating_sub(GUTTER);
    let line1 = title_line(row, ticker.clone(), area.width, no_color);
    let line2_bar = progress_line(row, content_width);
    let line2 = prefix_with_ticker(ticker.clone(), line2_bar);
    let line3 = meta_line(row, ticker, area.width);

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

fn ticker_span(is_cursor: bool, no_color: bool) -> Span<'static> {
    let (ch, color) = if is_cursor {
        ('▌', Color::Cyan)
    } else {
        (' ', Color::Reset)
    };
    let mut style = Style::default();
    if !no_color && ch != ' ' {
        style = style.fg(color);
    }
    Span::styled(ch.to_string(), style)
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
    let action = action_glyph(row.status);

    let name_is_dim = matches!(
        row.status,
        FileStatus::Done | FileStatus::Failed | FileStatus::Cancelled
    );

    // ticker(1) + gutter(2) + name + pad + right(6) = width.
    let name_space = width.saturating_sub(GUTTER).saturating_sub(RIGHT_MARGIN) as usize;
    let name = format::right_ellipsis(row.display_name(), name_space);
    let name_pad = name_space.saturating_sub(name.chars().count());

    let name_style = if name_is_dim {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let mut spans = vec![
        ticker,
        Span::raw("  "),
        Span::styled(name, name_style),
        Span::raw(" ".repeat(name_pad)),
    ];
    spans.extend(right_hint(action));
    Line::from(spans)
}

/// Правый хинт 1-й строки: `␣  <action>  ` (6 колонок). Для статусов без
/// действия (Cancelled/Unspecified) — шесть пробелов, чтобы ширина
/// осталась фиксированной и список не дёргался.
fn right_hint(action: &'static str) -> Vec<Span<'static>> {
    if action == " " {
        vec![Span::raw("      ")]
    } else {
        vec![
            dim("␣  ".to_string()),
            dim(action.to_string()),
            dim("  ".to_string()),
        ]
    }
}

fn meta_line(row: &DownloadRow, ticker: Span<'static>, width: u16) -> Line<'static> {
    // После gutter идут glyph(1) + space(1), затем текст. Под сам текст
    // остаётся content_width - 2.
    let text_width = width.saturating_sub(GUTTER).saturating_sub(2) as usize;
    let (text, is_err) = meta_text(row);
    let trimmed = format::right_ellipsis(&text, text_width);
    let value = if is_err {
        err_span(trimmed)
    } else {
        dim(trimmed)
    };
    let glyph = status_glyph(row.status);
    Line::from(vec![
        ticker,
        Span::raw("  "),
        dim(glyph.to_string()),
        Span::raw(" "),
        value,
    ])
}

fn prefix_with_ticker(ticker: Span<'static>, inner: Line<'static>) -> Line<'static> {
    let mut spans = Vec::with_capacity(inner.spans.len() + 2);
    spans.push(ticker);
    spans.push(Span::raw("  "));
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
