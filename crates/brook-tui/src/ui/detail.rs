//! Detail-панель (§6.4): 5 строк с url / path / pieces / error под курсором.

use std::path::PathBuf;

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
use ratatui::widgets::{
    Block,
    Borders,
    Paragraph,
};

use crate::format;
use crate::model::{
    DownloadRow,
    ViewModel,
};

pub fn draw(f: &mut Frame, area: Rect, vm: &ViewModel, _no_color: bool) {
    let block = Block::default().borders(Borders::TOP);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.height == 0 {
        return;
    }
    let ids = vm.visible_ids();
    let cursor = vm.cursor.min(ids.len().saturating_sub(1));
    let Some(row) = ids.get(cursor).and_then(|id| vm.downloads.get(id)) else {
        return;
    };

    let lines = build_lines(row, inner.width as usize);
    f.render_widget(Paragraph::new(lines), inner);
}

fn build_lines(row: &DownloadRow, width: usize) -> Vec<Line<'static>> {
    let url_max = width.saturating_sub(10);
    let url = format::middle_ellipsis(&row.url, url_max.max(4));
    let path = {
        let mut p = PathBuf::from(&row.target_dir);
        if !row.filename.is_empty() {
            p.push(&row.filename);
        }
        p.display().to_string()
    };
    // Piece-агрегаты в ProgressTick больше не едут — UI показывает
    // только байтовый прогресс; строчку `pieces` временно опускаем.
    let mut lines = vec![labelled("url    ", url), labelled("path   ", path)];
    if row.status == FileStatus::Failed {
        let err = row.error.clone().unwrap_or_default();
        lines.push(Line::from(vec![
            Span::styled(" error  ", Style::default().fg(Color::Red)),
            Span::styled(err, Style::default().fg(Color::Red)),
        ]));
    }
    lines
}

fn labelled(label: &'static str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!(" {label}"), Style::default().fg(Color::DarkGray)),
        Span::raw(value),
    ])
}
