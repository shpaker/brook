//! Титулы внешней rounded-рамки.
//!
//! Верх рамки — три сегмента через ` · `:
//! `[  brook  ·  127.0.0.1:<port>  ·  <screen>  ]`. `<screen>` —
//! `recently` для главного, `history` для экрана истории. При
//! reconnect/offline вместо адреса показывается причина (`reconnecting
//! #N` / `offline …`). Разделитель внутри статуса убран, чтобы не
//! сталкиваться с разделителем top-bar'а.
//!
//! Низ рамки — хинт-бар со всеми действиями нормального режима. На
//! главном экране: `[ add | <primary> | delete | history | quit ]`,
//! `<primary>` зависит от строки под курсором (pause/resume/retry);
//! для Done primary пуст (терминальное состояние без действия), для
//! пустого списка primary и delete пропускаются — `[ add | history |
//! quit ]`. На экране истории: `[ Esc · back | delete | quit ]`. Тост
//! (`toast_line`, align Left) при активном `vm.toast` рисуется на той
//! же нижней линии слева — ratatui поддерживает несколько titles на
//! одной рамке.
//!
//! Подсказки клавиш встроены прямо в слова действий: первая буква
//! accent (`Color::Cyan`), остаток dim (`Color::DarkGray`). В `no_color`
//! оба span'а получают `Modifier::DIM`, но раскладка «клавиша отдельным
//! span'ом» сохраняется (см. CLAUDE.md, §TUI hint bars).

use brook_proto::brook::v1::FileStatus;
use ratatui::layout::Alignment;
use ratatui::style::{
    Color,
    Modifier,
    Style,
};
use ratatui::text::{
    Line,
    Span,
};

use crate::model::{
    ConnectionState,
    Screen,
    ViewModel,
};

fn dim(s: impl Into<String>) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::DarkGray))
}

fn accent(s: impl Into<String>) -> Span<'static> {
    Span::styled(s.into(), Style::default().fg(Color::Cyan))
}

/// Подсветка клавиши первой буквой слова.
///
/// Возвращает два span'а: первую букву (accent, клавиша) и остаток слова
/// (dim, описание). В `no_color` оба получают `Modifier::DIM`, но
/// структура spans сохраняется — контракт §TUI hint bars в CLAUDE.md.
pub fn word_with_key(word: &str, no_color: bool) -> [Span<'static>; 2] {
    let mut chars = word.chars();
    let head: String = chars.next().map(|c| c.to_string()).unwrap_or_default();
    let tail: String = chars.collect();
    let (key_style, desc_style) = if no_color {
        let d = Style::default().add_modifier(Modifier::DIM);
        (d, d)
    } else {
        (
            Style::default().fg(Color::Cyan),
            Style::default().fg(Color::DarkGray),
        )
    };
    [
        Span::styled(head, key_style),
        Span::styled(tail, desc_style),
    ]
}

/// Верхний brand-титул: `[  brook  ·  127.0.0.1:<port>  ·  <screen>  ]`
/// по центру. Третий сегмент — текущий экран (`recently` для главного,
/// `history` для истории) — рисуется accent (Cyan), как `brook`. При
/// reconnect/offline во втором сегменте показывается причина вместо
/// адреса (без внутренних ` · `, чтобы не сталкивать с разделителем
/// top-bar'а).
pub fn top_brand(vm: &ViewModel) -> Line<'static> {
    let (middle, middle_style): (String, Style) = match &vm.connection {
        ConnectionState::Connected => (
            format!("127.0.0.1:{}", vm.port),
            Style::default().fg(Color::DarkGray),
        ),
        ConnectionState::Reconnecting { attempt } => (
            format!("reconnecting #{attempt}"),
            Style::default().fg(Color::Yellow),
        ),
        ConnectionState::Disconnected { reason } => (
            format!("offline {}", short_reason(reason)),
            Style::default().fg(Color::Red),
        ),
    };

    let screen_label = match vm.screen {
        Screen::Main => "recently",
        Screen::History => "history",
    };

    Line::from(vec![
        dim("[  "),
        accent("brook"),
        dim("  ·  "),
        Span::styled(middle, middle_style),
        dim("  ·  "),
        accent(screen_label),
        dim("  ]"),
    ])
    .alignment(Alignment::Center)
}

/// Нижний хинт-бар: набор зависит от активного экрана.
///
/// **Main:** `[ add | <primary> | delete | history | quit ]`. `add`,
/// `history` и `quit` — глобальные клавиши нормального режима, всегда
/// видимы. `<primary>` выбирается по статусу строки под курсором
/// (pause/resume/retry); для Done primary пуст (терминал без действия);
/// если строки нет или Unspecified — primary и delete пропускаются.
///
/// **History:** `[ Esc · back | delete | quit ]`. `Esc · back` — мульти-
/// символьная клавиша, рендерится через раздельные key + " · " + label
/// (см. CLAUDE.md, §TUI hint bars). `add` тут нет — добавление логично
/// делать на главной.
///
/// Разделитель сегментов — `  |  ` (dim). Первая буква каждого слова
/// accent (клавиша), остаток dim; в `no_color` оба получают
/// `Modifier::DIM`.
pub fn hints_bar(vm: &ViewModel, no_color: bool) -> Line<'static> {
    match vm.screen {
        Screen::Main => main_hints_bar(vm, no_color),
        Screen::History => history_hints_bar(no_color),
    }
}

fn main_hints_bar(vm: &ViewModel, no_color: bool) -> Line<'static> {
    let (primary, secondary) = cursor_actions(vm);
    let words: Vec<&'static str> = ["add", primary, secondary, "history", "quit"]
        .into_iter()
        .filter(|w| !w.is_empty())
        .collect();

    let mut spans: Vec<Span<'static>> = Vec::with_capacity(words.len() * 3 + 2);
    spans.push(dim("[  "));
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            spans.push(dim("  |  "));
        }
        let [key, tail] = word_with_key(word, no_color);
        spans.push(key);
        spans.push(tail);
    }
    spans.push(dim("  ]"));
    Line::from(spans).alignment(Alignment::Center)
}

fn history_hints_bar(no_color: bool) -> Line<'static> {
    let (key_style, label_style) = if no_color {
        let d = Style::default().add_modifier(Modifier::DIM);
        (d, d)
    } else {
        (
            Style::default().fg(Color::Cyan),
            Style::default().fg(Color::DarkGray),
        )
    };
    let mut spans: Vec<Span<'static>> = Vec::with_capacity(12);
    spans.push(dim("[  "));
    // `Esc · back` — раздельные spans (мульти-символьная клавиша).
    spans.push(Span::styled("Esc", key_style));
    spans.push(Span::styled(" · ", label_style));
    spans.push(Span::styled("back", label_style));

    spans.push(dim("  |  "));
    let [k_d, t_d] = word_with_key("delete", no_color);
    spans.push(k_d);
    spans.push(t_d);

    spans.push(dim("  |  "));
    let [k_q, t_q] = word_with_key("quit", no_color);
    spans.push(k_q);
    spans.push(t_q);

    spans.push(dim("  ]"));
    Line::from(spans).alignment(Alignment::Center)
}

/// Действия для строки под курсором: (primary, secondary).
///
/// Primary зависит от статуса: pause для активных, resume для Paused,
/// retry для Failed. Для Done, Cancelled и Unspecified primary пусто.
/// Secondary всегда `delete`, кроме Unspecified. Если видимый список
/// пуст — оба слова пусты.
fn cursor_actions(vm: &ViewModel) -> (&'static str, &'static str) {
    let visible = vm.visible_ids();
    let Some(id) = visible.get(vm.cursor.min(visible.len().saturating_sub(1))) else {
        return ("", "");
    };
    let Some(row) = vm.downloads.get(id) else {
        return ("", "");
    };
    let primary = match row.status {
        FileStatus::Running | FileStatus::Retrying | FileStatus::Pending => "pause",
        FileStatus::Paused => "resume",
        FileStatus::Failed => "retry",
        FileStatus::Done | FileStatus::Cancelled | FileStatus::Unspecified => "",
    };
    let secondary = if matches!(row.status, FileStatus::Unspecified) {
        ""
    } else {
        "delete"
    };
    (primary, secondary)
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
