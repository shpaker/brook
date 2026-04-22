//! Top/bottom titles для внешней rounded-рамки.
//!
//! Обе «шапки» живут как `Line`-титулы `Block`'а — через `title` и
//! `title_bottom`. Формат — pipe-tab'ы `| brook |` / `| ? help |`,
//! визуально «нашитые» на рамку. Toast подменяет нижний титул на
//! время своей жизни.

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

/// Верхний титул: `| brook |  ...  127.0.0.1:<port>  <●|◐|○>`.
/// Во время reconnect адрес подменяется на `reconnecting · #N`,
/// при offline — на `offline · <reason>`.
pub fn top_title(vm: &ViewModel) -> Line<'static> {
    let (glyph, glyph_style, tail): (&str, Style, String) = match &vm.connection {
        ConnectionState::Connected => (
            "●",
            Style::default().fg(Color::Green),
            format!("127.0.0.1:{}", vm.port),
        ),
        ConnectionState::Reconnecting { attempt } => (
            "◐",
            Style::default().fg(Color::Yellow),
            format!("reconnecting · #{attempt}"),
        ),
        ConnectionState::Disconnected { reason } => (
            "○",
            Style::default().fg(Color::Red),
            format!("offline · {}", short_reason(reason)),
        ),
    };

    Line::from(vec![
        dim("| "),
        accent("brook"),
        dim(" |  "),
        dim(tail),
        Span::raw(" "),
        Span::styled(glyph.to_string(), glyph_style),
        dim(" "),
    ])
    .alignment(Alignment::Left)
}

/// `| ? help |` tab — всегда справа в нижней рамке.
pub fn help_tab() -> Line<'static> {
    Line::from(vec![dim(" | "), accent("?"), dim(" help"), dim(" | ")]).alignment(Alignment::Right)
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
