//! Статус-бар: две строки, §6.4.
//!
//! Строка 1: `brook · ●/◐/○ 127.0.0.1:<port> [attempt N]`.
//! Строка 2: счётчики + `↓ <speed>`, с адаптивным сужением.

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
use crate::model::{
    ConnectionState,
    ViewModel,
};

pub fn draw(f: &mut Frame, area: Rect, vm: &ViewModel, no_color: bool) {
    let width = area.width as usize;
    let line1 = line_identity(vm, width, no_color);
    let line2 = line_counters(vm, width);
    f.render_widget(Paragraph::new(vec![line1, line2]), area);
}

fn line_identity(vm: &ViewModel, width: usize, no_color: bool) -> Line<'static> {
    let (glyph, color, attempt_suffix) = match &vm.connection {
        ConnectionState::Connected => ("●", Color::Green, String::new()),
        ConnectionState::Reconnecting { attempt } => {
            ("◐", Color::Yellow, format!(" [attempt {attempt}]"))
        }
        ConnectionState::Disconnected { .. } => ("○", Color::Red, String::new()),
    };
    let glyph_style = if no_color {
        Style::default()
    } else {
        Style::default().fg(color)
    };
    let addr = format!("127.0.0.1:{}", vm.port);
    let right = format!("{glyph} {addr}{attempt_suffix}");

    // Если ширина позволяет — «brook · <right>»; иначе дропаем заголовок (§6.4).
    let header = "brook · ";
    let mut spans = Vec::with_capacity(3);
    if header.len() + right.len() <= width {
        spans.push(Span::raw(header));
    }
    spans.push(Span::styled(glyph.to_string(), glyph_style));
    spans.push(Span::raw(format!(" {addr}{attempt_suffix}")));
    Line::from(spans)
}

fn line_counters(vm: &ViewModel, width: usize) -> Line<'static> {
    let mut running = 0u32;
    let mut queued = 0u32;
    let mut paused = 0u32;
    let mut retrying = 0u32;
    let mut speed = 0.0_f64;
    for r in vm.downloads.values() {
        match r.status {
            FileStatus::Running => {
                running += 1;
                speed += r.progress.speed_bps;
            }
            FileStatus::Pending => queued += 1,
            FileStatus::Paused => paused += 1,
            FileStatus::Retrying => retrying += 1,
            _ => {}
        }
    }
    let max = vm.settings.max_concurrent.max(1);

    let mut left_parts: Vec<String> = Vec::new();
    left_parts.push(format!("active {running}/{max}"));
    if queued > 0 {
        left_parts.push(format!("queued {queued}"));
    }
    if paused > 0 {
        left_parts.push(format!("paused {paused}"));
    }
    if retrying > 0 {
        left_parts.push(format!("retrying {retrying}"));
    }
    let mut left = left_parts.join(" · ");
    let stale = matches!(vm.connection, ConnectionState::Disconnected { .. });
    if stale {
        left.push_str(" · stale");
    }

    let right_full = format!("↓ {}", format::speed(speed));

    // Адаптивное сужение: сначала пытаемся вместить `left` + пробелы + `right`.
    // Если не вмещается — выкидываем speed целиком.
    let gap_min = 2usize;
    let total_with_speed = left.chars().count() + gap_min + right_full.chars().count();
    if total_with_speed <= width {
        let gap = width - left.chars().count() - right_full.chars().count();
        Line::from(vec![
            Span::raw(left),
            Span::raw(" ".repeat(gap)),
            Span::raw(right_full),
        ])
    } else {
        Line::from(vec![Span::raw(left)])
    }
}
