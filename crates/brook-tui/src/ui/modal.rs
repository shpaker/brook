//! Модалки (§6.6).
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
    RenameModal,
    ViewModel,
};

pub fn draw_overlay(f: &mut Frame, vm: &ViewModel, no_color: bool) {
    match &vm.mode {
        Mode::Normal => {}
        Mode::Add(m) => draw_add(f, m, no_color),
        Mode::Duplicate { form, existing_id } => draw_duplicate(f, vm, form, existing_id, no_color),
        Mode::ConfirmDelete { ids } => draw_confirm_delete(f, vm, ids, no_color),
        Mode::ConfirmRetry { ids } => draw_confirm_retry(f, vm, ids, no_color),
        Mode::Ghost { ids } => draw_ghost(f, vm, ids, no_color),
        Mode::RenameOnConflict { modal } => draw_rename(f, modal, no_color),
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
    lines.push(hint_line(
        &[
            ("Tab", Some("switch")),
            ("Enter", Some("add")),
            ("Esc", Some("cancel")),
        ],
        "   ",
        no_color,
    ));

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

/// Пункт хинта: клавиша и опциональное описание (None — ключ без
/// глагола, как одинокий `Esc` в confirm-модалках).
type HintItem = (&'static str, Option<&'static str>);

/// Собирает хинт-бар из пар (клавиша, описание). Клавиша рисуется
/// accent-цветом, описание — dim; в no_color всё — `Modifier::DIM`,
/// но остаётся раскладка по Span'ам. См. правило
/// «TUI hint bars — символ клавиши всегда выделяется цветом» в
/// CLAUDE.md.
fn hint_line(items: &[HintItem], separator: &'static str, no_color: bool) -> Line<'static> {
    let (key_style, desc_style) = if no_color {
        let dim = Style::default().add_modifier(Modifier::DIM);
        (dim, dim)
    } else {
        (
            Style::default().fg(Color::Cyan),
            Style::default().fg(Color::DarkGray),
        )
    };

    let mut spans: Vec<Span<'static>> = Vec::with_capacity(items.len() * 3 + 2);
    spans.push(Span::styled(" ", desc_style));
    for (i, (key, desc)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(separator, desc_style));
        }
        spans.push(Span::styled(*key, key_style));
        if let Some(text) = desc {
            spans.push(Span::styled(format!(" · {text}"), desc_style));
        }
    }
    spans.push(Span::styled(" ", desc_style));
    Line::from(spans)
}

fn draw_rename(f: &mut Frame, m: &RenameModal, no_color: bool) {
    let area = centered(f.area(), 60, 8);
    f.render_widget(Clear, area);
    let block = block("file exists — pick a name", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let caret = "▌";
    let value_style = if no_color {
        Style::default()
    } else {
        Style::default().fg(Color::Yellow)
    };
    let mut lines = vec![
        Line::from(format!(" already in folder: {}", m.base)),
        Line::from(vec![
            Span::styled(" name   ", Style::default().add_modifier(Modifier::DIM)),
            Span::styled(m.name.clone(), value_style),
            Span::styled(caret, value_style),
        ]),
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
    lines.push(hint_line(
        &[("Enter", Some("save")), ("Esc", Some("cancel"))],
        "    ",
        no_color,
    ));

    f.render_widget(Paragraph::new(lines), inner);
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
        hint_line(
            &[
                ("o", Some("open existing")),
                ("a", Some("add anyway")),
                ("Esc", Some("cancel")),
            ],
            "    ",
            no_color,
        ),
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
        hint_line(
            &[("y", Some("yes")), ("n", Some("no")), ("Esc", None)],
            "    ",
            no_color,
        )
        .alignment(Alignment::Right),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_confirm_retry(f: &mut Frame, vm: &ViewModel, ids: &[String], no_color: bool) {
    let area = centered(f.area(), 60, 7);
    f.render_widget(Clear, area);
    let block = block("retry download?", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let summary = if ids.len() == 1 {
        vm.downloads
            .get(&ids[0])
            .map(|r| r.display_name().to_string())
            .unwrap_or_else(|| ids[0].clone())
    } else {
        format!("{} downloads will be retried.", ids.len())
    };

    let lines = vec![
        Line::from(""),
        Line::from(format!(" {summary}")),
        Line::from(" download will resume from where it stopped."),
        Line::from(""),
        hint_line(
            &[("y", Some("yes")), ("n", Some("no")), ("Esc", None)],
            "    ",
            no_color,
        )
        .alignment(Alignment::Right),
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
        format!("{} downloads are not known to the daemon.", ids.len())
    };

    let lines = vec![
        Line::from(""),
        Line::from(format!(" {summary}")),
        Line::from(" the daemon has no record of it anymore."),
        Line::from(""),
        hint_line(
            &[
                ("r", Some("redownload")),
                ("d", Some("delete")),
                ("Esc", Some("cancel")),
            ],
            "    ",
            no_color,
        ),
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
    // Пункт «остановить демон» и соответствующий хинт скрываем, если
    // TUI не поднимал демон сам (remote-сессия или внешне запущенный
    // локальный процесс) — гасить чужой демон мы не имеем права.
    if vm.can_stop_daemon {
        lines.push(Line::from(" [ quit + stop daemon ]"));
    }
    lines.push(Line::from(" [ quit, keep daemon ⏎ ]"));
    lines.push(Line::from(" [ cancel ␛ ]"));
    lines.push(Line::from(""));
    let hint = if vm.can_stop_daemon {
        hint_line(
            &[
                ("s", Some("stop daemon")),
                ("k / Enter", Some("keep daemon")),
                ("Esc", Some("cancel")),
            ],
            "    ",
            no_color,
        )
    } else {
        hint_line(
            &[("k / Enter", Some("quit")), ("Esc", Some("cancel"))],
            "    ",
            no_color,
        )
    };
    lines.push(hint);

    let h = lines.len() as u16 + 2;
    let area = centered(f.area(), 62, h);
    f.render_widget(Clear, area);
    let block = block("quit brook", no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines), inner);
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
