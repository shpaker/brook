//! Hint-bar (§6.4): одна строка с шорткатами внизу.

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

const HINTS: &[(&str, &str)] = &[
    ("a", "add"),
    ("p", "pause"),
    ("r", "resume"),
    ("c", "cancel"),
    ("o", "open"),
    ("Tab", "details"),
    ("?", "help"),
    ("q", "quit"),
];

pub fn draw(f: &mut Frame, area: Rect) {
    let mut spans = Vec::with_capacity(HINTS.len() * 3);
    for (i, (key, label)) in HINTS.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" · ", Style::default().fg(Color::DarkGray)));
        }
        spans.push(Span::styled(
            (*key).to_string(),
            Style::default().fg(Color::Cyan),
        ));
        spans.push(Span::raw(format!(" {label}")));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}
