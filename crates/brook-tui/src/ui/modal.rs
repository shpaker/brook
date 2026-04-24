//! Модалки (§6.6).
//!
//! Каждая модалка — `Rounded`-рамка шириной 62 и высотой 6:
//!   - верхняя рамка: `[  title  ]` по центру
//!   - нижняя рамка: `[  action  |  action  ]` по центру (кнопки или хинты)
//!   - содержимое: 4 строки чистого текста (пустая · текст · текст · пустая)
//!
//! Дизайн согласован с chrome главного окна: скруглённые углы, cyan border,
//! тот же формат `[  …  |  …  ]` для нижней подсказки.

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
    BorderType,
    Borders,
    Clear,
    Paragraph,
};

use crate::model::{
    AddField,
    AddModal,
    Mode,
    RenameModal,
    TwoButtonFocus,
    ViewModel,
};

pub fn draw_overlay(f: &mut Frame, vm: &ViewModel, no_color: bool) {
    match &vm.mode {
        Mode::Normal => {}
        Mode::Add(m) => draw_add(f, m, no_color),
        Mode::Duplicate {
            form,
            existing_id,
            focus,
        } => draw_duplicate(f, vm, form, existing_id, *focus, no_color),
        Mode::ConfirmDelete { ids, focus } => draw_confirm_delete(f, vm, ids, *focus, no_color),
        Mode::ConfirmRetry { ids, focus } => draw_confirm_retry(f, vm, ids, *focus, no_color),
        Mode::Ghost { ids, focus } => draw_ghost(f, vm, ids, *focus, no_color),
        Mode::RenameOnConflict { modal } => draw_rename(f, modal, no_color),
        Mode::QuitConfirm { focus } => draw_quit_confirm(f, vm, *focus, no_color),
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

/// Rounded-рамка с `[  title  ]` сверху и произвольной `bottom`-строкой снизу.
/// Оба элемента выровнены по центру. Цвет рамки — Cyan (кроме no_color).
fn modal_block<'a>(title: &'a str, bottom: Line<'static>, no_color: bool) -> Block<'a> {
    let bracket_style = if no_color {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let top = Line::from(vec![
        Span::styled("[  ", bracket_style),
        Span::raw(title.to_owned()),
        Span::styled("  ]", bracket_style),
    ])
    .alignment(Alignment::Center);

    let mut b = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(top)
        .title_bottom(bottom);
    if !no_color {
        b = b.border_style(Style::default().fg(Color::Cyan));
    }
    b
}

fn draw_add(f: &mut Frame, m: &AddModal, no_color: bool) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let bottom = hint_line(
        &[
            ("Tab", Some("switch")),
            ("Enter", Some("add")),
            ("Esc", Some("cancel")),
        ],
        no_color,
    );
    let block = modal_block("add download", bottom, no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![
        Line::from(""),
        field_line("url   ", &m.url, m.field == AddField::Url, no_color),
        field_line("folder", &m.folder, m.field == AddField::Folder, no_color),
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

/// Пункт хинта: клавиша и опциональное описание.
type HintItem = (&'static str, Option<&'static str>);

/// Строка хинтов в формате `[  Key · desc  |  Key · desc  ]`,
/// согласованном с chrome главного окна. Выровнена по центру.
fn hint_line(items: &[HintItem], no_color: bool) -> Line<'static> {
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
    spans.push(Span::styled("[  ", desc_style));
    for (i, (key, desc)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  |  ", desc_style));
        }
        spans.push(Span::styled(*key, key_style));
        if let Some(text) = desc {
            spans.push(Span::styled(format!(" · {text}"), desc_style));
        }
    }
    spans.push(Span::styled("  ]", desc_style));
    Line::from(spans).alignment(Alignment::Center)
}

/// Кнопки `[  yes  |  no  ]` для нижней рамки. Сфокусированная —
/// Cyan REVERSED, другая — dim.
fn bottom_yes_no(focus: TwoButtonFocus, no_color: bool) -> Line<'static> {
    let focused_style = if no_color {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::REVERSED)
    };
    let idle_style = Style::default().add_modifier(Modifier::DIM);
    let desc_style = if no_color {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let yes_style = if focus == TwoButtonFocus::Yes {
        focused_style
    } else {
        idle_style
    };
    let no_style = if focus == TwoButtonFocus::No {
        focused_style
    } else {
        idle_style
    };

    Line::from(vec![
        Span::styled("[  ", desc_style),
        Span::styled("yes", yes_style),
        Span::styled("  |  ", desc_style),
        Span::styled("no", no_style),
        Span::styled("  ]", desc_style),
    ])
    .alignment(Alignment::Center)
}

fn draw_rename(f: &mut Frame, m: &RenameModal, no_color: bool) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let bottom = hint_line(
        &[("Enter", Some("save")), ("Esc", Some("cancel"))],
        no_color,
    );
    let block = modal_block("file exists — pick a name", bottom, no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![
        Line::from(""),
        Line::from(format!(" already in folder: {}", m.base)),
        field_line("name   ", &m.name, true, no_color),
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

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_duplicate(
    f: &mut Frame,
    vm: &ViewModel,
    _form: &crate::events::AddForm,
    existing_id: &str,
    focus: TwoButtonFocus,
    no_color: bool,
) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let block = modal_block("duplicate url", bottom_yes_no(focus, no_color), no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let existing_label = vm
        .downloads
        .get(existing_id)
        .map(|r| format!("{} ({:?})", r.display_name(), r.status))
        .unwrap_or_else(|| "(unknown)".into());

    let lines = vec![
        Line::from(""),
        Line::from(" url is already in the queue — add anyway?"),
        Line::from(format!(" existing: {existing_label}")),
        Line::from(""),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_confirm_delete(
    f: &mut Frame,
    vm: &ViewModel,
    ids: &[String],
    focus: TwoButtonFocus,
    no_color: bool,
) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let block = modal_block("delete download?", bottom_yes_no(focus, no_color), no_color);
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
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_confirm_retry(
    f: &mut Frame,
    vm: &ViewModel,
    ids: &[String],
    focus: TwoButtonFocus,
    no_color: bool,
) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let block = modal_block("retry download?", bottom_yes_no(focus, no_color), no_color);
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
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_ghost(
    f: &mut Frame,
    vm: &ViewModel,
    ids: &[String],
    focus: TwoButtonFocus,
    no_color: bool,
) {
    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let block = modal_block(
        "download not found",
        bottom_yes_no(focus, no_color),
        no_color,
    );
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
        Line::from(" the daemon has no record of it — redownload?"),
        Line::from(""),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_quit_confirm(f: &mut Frame, vm: &ViewModel, focus: TwoButtonFocus, no_color: bool) {
    let running = vm
        .downloads
        .values()
        .filter(|r| r.status == FileStatus::Running)
        .count();

    let (line1, line2) = if vm.can_stop_daemon {
        if running > 0 {
            (
                format!(" {running} downloads are running."),
                " they will be interrupted on quit.".to_string(),
            )
        } else {
            (
                " no active downloads.".to_string(),
                " daemon will stop on quit.".to_string(),
            )
        }
    } else if running > 0 {
        (
            format!(" {running} downloads are running."),
            " they will keep running after exit.".to_string(),
        )
    } else {
        (
            " no active downloads.".to_string(),
            " daemon will keep running after exit.".to_string(),
        )
    };

    let area = centered(f.area(), 62, 6);
    f.render_widget(Clear, area);
    let block = modal_block("quit brook", bottom_yes_no(focus, no_color), no_color);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines = vec![
        Line::from(""),
        Line::from(line1),
        Line::from(line2),
        Line::from(""),
    ];
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
