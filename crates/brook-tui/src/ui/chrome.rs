//! Top/bottom titles для внешней rounded-рамки.
//!
//! Обе «шапки» — `Line`-титулы `Block`'а (`title` / `title_bottom`).
//! Верх: `[ brook | 127.0.0.1:<port> ]`, низ: `[ ␣ action | a add | d
//! delete | q quit ]`. Toast подменяет нижний хинт-бар своей строкой.

use brook_proto::brook::v1::FileStatus;
use ratatui::layout::Alignment;
use ratatui::style::{
    Color,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};

use crate::model::{
    ConnectionState,
    ViewModel,
};

fn dim(s: impl Into<String>) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::DarkGray))
}

fn accent(s: impl Into<String>) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::Cyan))
}

/// Верхний титул: `[ brook | 127.0.0.1:<port> ]`. При reconnect/offline
/// во втором сегменте показывается причина, а не адрес.
pub fn top_title(vm: &ViewModel) -> Line<'static> {
    let (tail, tail_style): (String, Style) = match &vm.connection {
        ConnectionState::Connected => (
            format!("127.0.0.1:{}", vm.port),
            Style::default().fg(Color::DarkGray),
        ),
        ConnectionState::Reconnecting { attempt } => (
            format!("reconnecting · #{attempt}"),
            Style::default().fg(Color::Yellow),
        ),
        ConnectionState::Disconnected { reason } => (
            format!("offline · {}", short_reason(reason)),
            Style::default().fg(Color::Red),
        ),
    };

    Line::from(vec![
        dim("[ "),
        accent("brook"),
        dim(" | "),
        Span::styled(tail, tail_style),
        dim(" ]"),
    ])
    .alignment(Alignment::Center)
}

/// Нижний хинт-бар: `[ ␣ <verb> | a add | d delete | q quit ]`.
/// `<verb>` зависит от статуса строки под курсором — pause/resume/
/// retry/reveal; если действия нет (Cancelled или пустой список) —
/// рисуем `—`, чтобы ширина бара оставалась предсказуемой.
pub fn hints_bar(action_word: &str) -> Line<'static> {
    Line::from(vec![
        dim("[ "),
        accent("␣"),
        dim(format!(" {action_word} ")),
        dim("| "),
        accent("a"),
        dim(" add "),
        dim("| "),
        accent("d"),
        dim(" delete "),
        dim("| "),
        accent("q"),
        dim(" quit "),
        dim("]"),
    ])
    .alignment(Alignment::Center)
}

/// Конкретный глагол для `␣ <verb>` в хинт-баре. Берётся по статусу
/// строки под курсором, чтобы пользователь видел что именно сейчас
/// сделает Space.
pub fn action_word(vm: &ViewModel) -> &'static str {
    let visible = vm.visible_ids();
    let Some(id) = visible.get(vm.cursor.min(visible.len().saturating_sub(1))) else {
        return "—";
    };
    let Some(row) = vm.downloads.get(id) else {
        return "—";
    };
    match row.status {
        FileStatus::Running | FileStatus::Retrying | FileStatus::Pending => "pause",
        FileStatus::Paused => "resume",
        FileStatus::Failed => "retry",
        FileStatus::Done => "reveal",
        FileStatus::Cancelled | FileStatus::Unspecified => "—",
    }
}

/// Toast-подпись слева в нижней рамке. Показывается, пока `vm.toast`
/// активен (3 сек с момента set_toast).
pub fn toast_line(vm: &ViewModel) -> Option<Line<'static>> {
    let t = vm.toast.as_ref()?;
    Some(
        Line::from(vec![
            dim(" "),
            accent("✓"),
            Span::raw(" "),
            Span::raw(t.message.clone()),
            dim(" "),
        ])
        .alignment(Alignment::Left),
    )
}

fn short_reason(s: &str) -> String {
    const MAX: usize = 32;
    if s.chars().count() <= MAX {
        s.to_string()
    } else {
        let head: String = s.chars().take(MAX - 1).collect();
        format!("{head}…")
    }
}
