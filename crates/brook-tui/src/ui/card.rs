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
use crate::ui::progress::{
    progress_line,
    ratio,
};

/// Высота одной карточки: 3 строки содержимого + 1 пустой разделитель.
pub const CARD_HEIGHT: u16 = 4;

/// Ширина левого «желоба» перед контентом: ticker(1) + 2 пробела = 3.
/// Контент всех трёх строк начинается с одной колонки; на 3-й строке
/// первым символом контента идёт status-glyph, а сама подпись — через
/// один пробел после него.
const GUTTER: u16 = 3;

pub fn draw(f: &mut Frame, area: Rect, row: &DownloadRow, is_cursor: bool, no_color: bool) {
    if area.height == 0 || area.width < GUTTER + 4 {
        return;
    }
    let ticker = ticker_span(is_cursor, no_color);

    let content_width = area.width.saturating_sub(GUTTER);
    let line1 = title_line(row, ticker.clone(), area.width);
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

fn title_line(row: &DownloadRow, ticker: Span<'static>, width: u16) -> Line<'static> {
    // Fixed-width `"  0.0%"`..`"100.0%"` — чтобы правая кромка `%` не
    // «прыгала» при переходе 9.9 → 10.0 → 99.9 → 100.0.
    const PERCENT_WIDTH: u16 = 6;
    const GAP_MIN: u16 = 1;

    let name_is_dim = matches!(
        row.status,
        FileStatus::Done | FileStatus::Failed | FileStatus::Cancelled
    );
    let base_style = if name_is_dim {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let available = width.saturating_sub(GUTTER) as usize;
    let show_percent = row.progress.bytes_total > 0 && width >= GUTTER + PERCENT_WIDTH + GAP_MIN;

    if !show_percent {
        // Размер неизвестен (streaming без Content-Length, свежий
        // Pending без тиков) — либо окно слишком узкое. Имя во всю
        // ширину, как до появления процента.
        let name = format::right_ellipsis(row.display_name(), available);
        return Line::from(vec![
            ticker,
            Span::raw("  "),
            Span::styled(name, base_style),
        ]);
    }

    let name_space = available
        .saturating_sub(PERCENT_WIDTH as usize)
        .saturating_sub(GAP_MIN as usize);
    let name = format::right_ellipsis(row.display_name(), name_space);
    let pad = available
        .saturating_sub(name.chars().count())
        .saturating_sub(PERCENT_WIDTH as usize);
    let percent = format::percent(ratio(row));

    Line::from(vec![
        ticker,
        Span::raw("  "),
        Span::styled(name, base_style),
        Span::raw(" ".repeat(pad)),
        Span::styled(percent, base_style),
    ])
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

/// Возвращает (meta-text, is_error_style).
fn meta_text(row: &DownloadRow) -> (String, bool) {
    let p = &row.progress;
    match row.status {
        FileStatus::Running => {
            let done = format::bytes(p.bytes_done);
            let speed = format::speed(p.speed_bps);
            if p.bytes_total > 0 {
                let eta = p.eta_secs.map(format::eta).unwrap_or_else(|| "—".into());
                let total = format::bytes(p.bytes_total);
                (
                    format!("{eta} left  ·  {done} of {total}  ·  {speed}"),
                    false,
                )
            } else {
                // Размер неизвестен (streaming без Content-Length): ETA
                // посчитать нечем — `remaining` неизвестен.
                (format!("unknown time left  ·  {done}  ·  {speed}"), false)
            }
        }
        FileStatus::Paused => {
            let done = format::bytes(p.bytes_done);
            if p.bytes_total > 0 {
                let total = format::bytes(p.bytes_total);
                (format!("paused  ·  {done} of {total}"), false)
            } else {
                (format!("paused  ·  {done}"), false)
            }
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
            // Для стриминга bytes_total = 0 — общий размер известен
            // только по факту скачивания, берём bytes_done как итог.
            let final_size = if p.bytes_total > 0 {
                p.bytes_total
            } else {
                p.bytes_done
            };
            let size = if final_size > 0 {
                format::bytes(final_size)
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
