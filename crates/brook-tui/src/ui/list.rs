//! Рендер списка загрузок (§6.3).
//!
//! Каждая загрузка — 3 строки (шапка + прогрессбар + пустой разделитель).
//! Скроллинг — попиксельный (по строкам буфера), а не «по элементам».
//! Scrollbar справа появляется только при переполнении.

use brook_proto::brook::v1::DownloadState;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{
    Modifier,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};
use ratatui::widgets::{
    Paragraph,
    Scrollbar,
    ScrollbarOrientation,
    ScrollbarState,
};

use crate::format;
use crate::model::{
    DownloadRow,
    ViewModel,
    WorkerSegment,
};
use crate::ui::progress::ProgressBar;

/// Каждой загрузке — 2 строки содержимого + 1 строка-разделитель снизу.
const ROWS_PER_ENTRY: u16 = 3;
/// Ширина префикса `[cursor][select][icon]` + запас на пробел.
const PREFIX_WIDTH: u16 = 4;
/// Фиксированная ширина правой метрика-колонки.
const RIGHT_COL_WIDTH: u16 = 34;

pub fn draw(f: &mut Frame, area: Rect, vm: &ViewModel) {
    if area.width < PREFIX_WIDTH + 10 || area.height == 0 {
        return;
    }
    let no_color = std::env::var_os("NO_COLOR").is_some();

    let ids = vm.visible_ids();
    let visible_len = ids.len();
    let cursor = cursor_index(vm, visible_len);

    let total_rows = visible_len as u16 * ROWS_PER_ENTRY;
    let needs_scrollbar = total_rows > area.height;
    let list_area = if needs_scrollbar {
        Rect {
            width: area.width.saturating_sub(1),
            ..area
        }
    } else {
        area
    };

    // Рисуем с учётом скролла: держим курсор в поле зрения.
    let viewport_rows = list_area.height;
    let scroll_offset = compute_scroll(cursor, viewport_rows, total_rows);

    for (i, id) in ids.iter().enumerate() {
        let row_top = i as u16 * ROWS_PER_ENTRY;
        if row_top + ROWS_PER_ENTRY <= scroll_offset {
            continue;
        }
        if row_top >= scroll_offset + viewport_rows {
            break;
        }
        let Some(row) = vm.downloads.get(id) else {
            continue;
        };
        let rendered_top = row_top.saturating_sub(scroll_offset);
        draw_entry(
            f,
            Rect {
                x: list_area.x,
                y: list_area.y + rendered_top,
                width: list_area.width,
                height: (ROWS_PER_ENTRY).min(list_area.height - rendered_top),
            },
            row,
            i == cursor,
            no_color,
        );
    }

    if needs_scrollbar {
        let sb = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut state = ScrollbarState::new(total_rows.saturating_sub(viewport_rows) as usize)
            .position(scroll_offset as usize);
        f.render_stateful_widget(sb, area, &mut state);
    }
}

fn cursor_index(vm: &ViewModel, visible_len: usize) -> usize {
    if visible_len == 0 {
        0
    } else {
        vm.cursor.min(visible_len - 1)
    }
}

fn compute_scroll(cursor: usize, viewport: u16, total: u16) -> u16 {
    if total <= viewport {
        return 0;
    }
    // Держим курсор-шапку в видимой области. Каждая запись = ROWS_PER_ENTRY.
    let cursor_row = cursor as u16 * ROWS_PER_ENTRY;
    let max_offset = total.saturating_sub(viewport);
    // Показываем чуть контекста сверху-снизу.
    let desired = cursor_row.saturating_sub(viewport / 2);
    desired.min(max_offset)
}

fn draw_entry(f: &mut Frame, area: Rect, row: &DownloadRow, is_cursor: bool, no_color: bool) {
    // Строка 1 — шапка.
    if area.height >= 1 {
        let header = header_line(row, is_cursor, area.width);
        f.render_widget(
            Paragraph::new(header),
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: 1,
            },
        );
    }
    // Строка 2 — прогрессбар (выровнен с именем файла).
    if area.height >= 2 {
        let bar_x = area.x + PREFIX_WIDTH;
        let bar_width = area.width.saturating_sub(PREFIX_WIDTH);
        if bar_width > 0 {
            let workers: Vec<WorkerSegment> = row.workers.values().copied().collect();
            let widget = ProgressBar {
                progress: &row.progress,
                workers: &workers,
                no_color,
            };
            f.render_widget(
                widget,
                Rect {
                    x: bar_x,
                    y: area.y + 1,
                    width: bar_width,
                    height: 1,
                },
            );
            // Справа от бара — процент в пару колонок. Накладывается
            // поверх последних символов бара: §6.3 допускает это.
            let pct = if row.progress.bytes_total > 0 {
                ((row.progress.bytes_done as f64 / row.progress.bytes_total as f64) * 100.0).round()
                    as u32
            } else {
                0
            };
            let pct_text = format!("  {pct:>3}%");
            let pct_w = pct_text.chars().count() as u16;
            if bar_width > pct_w + 2 {
                f.render_widget(
                    Paragraph::new(pct_text),
                    Rect {
                        x: bar_x + bar_width - pct_w,
                        y: area.y + 1,
                        width: pct_w,
                        height: 1,
                    },
                );
            }
        }
    }
    // Строка 3 — пустой разделитель (само собой, просто ничего не рисуем).
}

fn header_line(row: &DownloadRow, is_cursor: bool, width: u16) -> Line<'static> {
    let cursor_ch = if is_cursor { '›' } else { ' ' };
    let select_ch = ' '; // multi-select появится в §6.5
    let icon = state_icon(row.state);

    let prefix = format!("{cursor_ch} {select_ch} {icon} ");
    let right = right_column(row);
    let right_w = right.chars().count() as u16;
    let prefix_w = prefix.chars().count() as u16;

    let name_space = width.saturating_sub(prefix_w + right_w + 1);
    let name = format::right_ellipsis(row.display_name(), name_space as usize);
    let name_pad = (name_space as usize).saturating_sub(name.chars().count());

    let mut style = Style::default();
    if is_cursor {
        style = style.add_modifier(Modifier::BOLD);
    }
    Line::from(vec![
        Span::styled(prefix, style),
        Span::styled(name, style),
        Span::raw(" ".repeat(name_pad + 1)),
        Span::raw(right),
    ])
}

fn state_icon(s: DownloadState) -> &'static str {
    match s {
        DownloadState::Running => "▶",
        DownloadState::Paused => "❚❚",
        DownloadState::Retrying => "↻",
        DownloadState::Queued => "⏳",
        DownloadState::Done => "✓",
        DownloadState::Failed | DownloadState::Cancelled => "✕",
        DownloadState::Unspecified => "·",
    }
}

fn right_column(row: &DownloadRow) -> String {
    let done = format::bytes(row.progress.bytes_done);
    let total = if row.progress.bytes_total > 0 {
        format::bytes(row.progress.bytes_total)
    } else {
        "—".to_string()
    };
    let state_field: String = match row.state {
        DownloadState::Running => {
            let eta = row
                .progress
                .eta_secs
                .map(format::eta)
                .unwrap_or_else(|| "—".to_string());
            format!("{} · {}", format::speed(row.progress.speed_bps), eta)
        }
        DownloadState::Paused => "— · paused".into(),
        DownloadState::Queued => "— · queued".into(),
        DownloadState::Retrying => {
            if row.max_attempts > 0 {
                format!("— · {}/{}", row.attempt, row.max_attempts)
            } else {
                format!("— · retry {}", row.attempt)
            }
        }
        DownloadState::Done => "— · done".into(),
        DownloadState::Failed => "— · failed".into(),
        DownloadState::Cancelled => "— · cancelled".into(),
        DownloadState::Unspecified => "—".into(),
    };
    // Обрезаем до фиксированной ширины, чтобы все правые колонки
    // совпали по границе.
    let raw = format!("{done} / {total} · {state_field}");
    let w = RIGHT_COL_WIDTH as usize;
    if raw.chars().count() >= w {
        format::right_ellipsis(&raw, w)
    } else {
        format!("{:>width$}", raw, width = w)
    }
}
