//! Форматтеры для humansize-значений, скорости и ETA (§6.8, используем уже в §6.3).

/// Байты в humansize-строку: `15 B`, `532 KB`, `1.5 MB`, `1.8 GB`.
pub fn bytes(n: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    let x = n as f64;
    if x < KB {
        format!("{n} B")
    } else if x < MB {
        format!("{:.0} KB", x / KB)
    } else if x < GB {
        format!("{:.1} MB", x / MB)
    } else if x < TB {
        format!("{:.1} GB", x / GB)
    } else {
        format!("{:.1} TB", x / TB)
    }
}

/// Скорость в humansize/s.
pub fn speed(bps: f64) -> String {
    if bps <= 0.0 {
        return "—".to_string();
    }
    let bytes = bytes(bps as u64);
    format!("{bytes}/s")
}

/// ETA в коротком формате: `2h 15m`, `15m 30s`, `45s`, `<1s`.
pub fn eta(secs: u64) -> String {
    if secs == 0 {
        return "<1s".to_string();
    }
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Обрезка `…` **справа** — для имён файлов.
pub fn right_ellipsis(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        return s.to_string();
    }
    if max == 0 {
        return String::new();
    }
    let head: String = chars.iter().take(max.saturating_sub(1)).collect();
    format!("{head}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_formatting() {
        assert_eq!(bytes(15), "15 B");
        assert_eq!(bytes(1024), "1 KB");
        assert_eq!(bytes(532 * 1024), "532 KB");
        assert!(bytes(1_500_000).starts_with("1.4")); // 1.43 MB
    }

    #[test]
    fn eta_formatting() {
        assert_eq!(eta(0), "<1s");
        assert_eq!(eta(45), "45s");
        assert_eq!(eta(930), "15m 30s");
        assert_eq!(eta(8100), "2h 15m");
    }

    #[test]
    fn right_ellipsis_truncates() {
        assert_eq!(right_ellipsis("hello world", 8), "hello w…");
        assert_eq!(right_ellipsis("short", 10), "short");
    }
}
