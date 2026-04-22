//! Модалки (§6.6) и help-overlay (§6.7).
//!
//! Рендер-инфраструктура: `centered` возвращает центрированный Rect
//! под заданные размеры; `draw_*` функции печатают конкретные модалки
//! поверх уже нарисованного UI (Clear + Block + Paragraph).

use brook_proto::brook::v1::FileStatus;
use ratatui::Frame;
use ratatui::layout::{
    Alignment,
    Rect,
};
use ratatui::style::{
    Color,
    Modifier,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};
use ratatui::widgets::{
    Block,
    Borders,
    Clear,
    Paragraph,
};

use crate::model::{
    AddField,
    AddModal,
    Mode,
    ViewModel,
};

const HELP_LINES: &[&str] = &[
    "navigation",
    "  ↑ ↓ / j k       — move cursor",
    "  g / G           — first / last",
    "  Tab             — toggle selection",
    "  Shift+↑↓ / J K  — extend selection",
    "",
    "actions",
    "  Space           — primary action on cursor (pause/resume/retry/reveal)",
    "  Enter           — reveal in Finder",
    "  a               — add download",
    "  d               — delete (with confirm)",
    "",
    "view",
    "  ?               — this help",
    "",
    "misc",
    "  q / Ctrl+C      — quit (choose: stop daemon / keep / cancel)",
    "  Esc             — close modal / help",
];

pub fn draw_overlay(f: &mut Frame, vm: &ViewModel, no_color: bool) {
    match &vm.mode {
        Mode::Normal => {}
        Mode::Add(m) => draw_add(f, m, no_color),
        Mode::Duplicate { form, existing_id } => draw_duplicate(f, vm, form, existing_id, no_color),
        Mode::ConfirmDelete { ids } => draw_confirm_delete(f, vm, ids, no_color),
        Mode::Ghost { ids } => draw_ghost(f, vm, ids, no_color),
        Mode::Help { scroll } => draw_help(f, *scroll, no_color),
        Mode::QuitConfirm => draw_quit_confirm(f, vm, no_color),
    }
}

fn centered(area: Rect, w: u16, h: u16) -> Rect {
    let w = w.min(area.width);
    let h = h.min(area.height);
    Rect {
        x: area.x + (area.width - w) / 2,
        y: area.y + (area.height - h) / 2,
        width: w,
        height: h,
    }
}

fn block<'a>(title: &'a str, no_color: bool) -> Block<'a> {
    let mut b = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {title} "));
    if !no_color {
        b = b.border_style(Style::default().fg(Color::Cyan));
    }
    b
}

fn draw_add(f: &mut Frame, m: &AddModal, no_color: bool) {
    let area = centered(f.area(), 60, 9);
    f.render_widget(Clear, area);
    let block = block("add download", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![
        field_line("url   ", &m.url, m.field == AddField::Url, no_color),
        field_line("folder", &m.folder, m.field == AddField::Folder, no_color),
        Line::from(""),
    ];
    if let Some(err) = &m.error {
        let style = if no_color {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Red)
        };
        lines.push(Line::from(Span::styled(err.clone(), style)));
    } else {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(Span::styled(
        " Tab · switch   Enter · add   Esc · cancel ",
        hint_style(no_color),
    )));

    f.render_widget(Paragraph::new(lines), inner);
}

fn field_line(label: &'static str, value: &str, focused: bool, no_color: bool) -> Line<'static> {
    let caret = if focused { "▌" } else { " " };
    let value_style = if focused && !no_color {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    Line::from(vec![
        Span::styled(
            format!(" {label} "),
            Style::default().add_modifier(Modifier::DIM),
        ),
        Span::styled(value.to_string(), value_style),
        Span::styled(caret.to_string(), value_style),
    ])
}

fn hint_style(no_color: bool) -> Style {
    if no_color {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

fn draw_duplicate(
    f: &mut Frame,
    vm: &ViewModel,
    _form: &crate::events::AddForm,
    existing_id: &str,
    no_color: bool,
) {
    let area = centered(f.area(), 60, 7);
    f.render_widget(Clear, area);
    let block = block("duplicate url", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let existing_label = vm
        .downloads
        .get(existing_id)
        .map(|r| format!("{} ({:?})", r.display_name(), r.status))
        .unwrap_or_else(|| "(unknown)".into());

    let lines = vec![
        Line::from(""),
        Line::from(" this url is already in the queue."),
        Line::from(format!(" existing: {existing_label}")),
        Line::from(""),
        Line::from(Span::styled(
            " o · open existing    a · add anyway    Esc · cancel ",
            hint_style(no_color),
        )),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_confirm_delete(f: &mut Frame, vm: &ViewModel, ids: &[String], no_color: bool) {
    let area = centered(f.area(), 60, 7);
    f.render_widget(Clear, area);
    let block = block("delete download?", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let summary = if ids.len() == 1 {
        vm.downloads
            .get(&ids[0])
            .map(|r| r.display_name().to_string())
            .unwrap_or_else(|| ids[0].clone())
    } else {
        format!("{} downloads will be deleted.", ids.len())
    };

    let lines = vec![
        Line::from(""),
        Line::from(format!(" {summary}")),
        Line::from(" partial files will be removed."),
        Line::from(""),
        Line::from(Span::styled(
            "                    y · yes    n · no    Esc ",
            hint_style(no_color),
        ))
        .alignment(Alignment::Left),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_ghost(f: &mut Frame, vm: &ViewModel, ids: &[String], no_color: bool) {
    let area = centered(f.area(), 60, 8);
    f.render_widget(Clear, area);
    let block = block("download not found", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let summary = if ids.len() == 1 {
        vm.downloads
            .get(&ids[0])
            .map(|r| r.display_name().to_string())
            .unwrap_or_else(|| ids[0].clone())
    } else {
        format!("{} downloads are not known to brookd.", ids.len())
    };

    let lines = vec![
        Line::from(""),
        Line::from(format!(" {summary}")),
        Line::from(" brookd has no record of it anymore."),
        Line::from(""),
        Line::from(Span::styled(
            " r · redownload    d · delete    Esc · cancel ",
            hint_style(no_color),
        )),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_quit_confirm(f: &mut Frame, vm: &ViewModel, no_color: bool) {
    let running = vm
        .downloads
        .values()
        .filter(|r| r.status == FileStatus::Running)
        .count();

    let mut lines: Vec<Line> = Vec::with_capacity(8);
    lines.push(Line::from(""));
    if running > 0 {
        lines.push(Line::from(format!(" {running} downloads are running.")));
    } else {
        lines.push(Line::from(" no active downloads."));
    }
    lines.push(Line::from(" [ quit + stop daemon ]"));
    lines.push(Line::from(" [ quit, keep daemon ⏎ ]"));
    lines.push(Line::from(" [ cancel ␛ ]"));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        " s · stop daemon    k / Enter · keep daemon    Esc · cancel ",
        hint_style(no_color),
    )));

    let h = lines.len() as u16 + 2;
    let area = centered(f.area(), 62, h);
    f.render_widget(Clear, area);
    let block = block("quit brook", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_help(f: &mut Frame, scroll: u16, no_color: bool) {
    let full = f.area();
    f.render_widget(Clear, full);
    let block = block("help", no_color);
    let inner = block.inner(full);
    f.render_widget(block, full);

    let lines: Vec<Line> = HELP_LINES
        .iter()
        .map(|s| {
            if s.is_empty() {
                Line::from("")
            } else if !s.starts_with(' ') {
                Line::from(Span::styled(
                    (*s).to_string(),
                    Style::default().add_modifier(Modifier::BOLD),
                ))
            } else {
                Line::from((*s).to_string())
            }
        })
        .collect();
    f.render_widget(Paragraph::new(lines).scroll((scroll, 0)), inner);

    // Подсказка выхода — в последней строке overlay'я.
    let footer_area = Rect {
        x: inner.x,
        y: inner.y + inner.height.saturating_sub(1),
        width: inner.width,
        height: 1,
    };
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            if f.area().height < 25 {
                " ↑↓ scroll   Esc / ? / q · close "
            } else {
                " any key · close "
            },
            hint_style(no_color),
        ))),
        footer_area,
    );
}

// Чтобы Mode::Duplicate можно было рендерить без лишнего заимствования.
pub fn _state_name(s: FileStatus) -> &'static str {
    match s {
        FileStatus::Running => "RUNNING",
        FileStatus::Paused => "PAUSED",
        FileStatus::Retrying => "RETRYING",
        FileStatus::Pending => "QUEUED",
        FileStatus::Done => "DONE",
        FileStatus::Failed => "FAILED",
        FileStatus::Cancelled => "CANCELLED",
        FileStatus::Unspecified => "—",
    }
}
