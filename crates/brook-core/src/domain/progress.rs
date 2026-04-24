//! Снимок прогресса загрузки — то, что клиент рендерит в прогрессбаре.
//!
//! Почему нет `Eq`: поля `speed_bps: f64` и `eta_secs: f64` — плавающая
//! точка. В Rust `f32`/`f64` реализуют `PartialEq` (NaN != NaN → отношение
//! не полное), но не `Eq`. Поэтому `#[derive(Eq)]` компилятор отвергнет.
//! Автоматом деривить `Hash` на `Progress` тоже нельзя по той же причине.

/// Максимальное число сегментов в [`BarState`].
///
/// Бэкенд агрегирует произвольное количество piece'ов (7…800) в не более чем
/// `BAR_SEGMENTS` сегментов. Это держит размер proto-payload постоянным
/// независимо от размера файла.
pub const BAR_SEGMENTS: usize = 100;

/// Чанкованное состояние прогрессбара.
///
/// Передаётся в [`ProgressEvent::Tick`] как опциональный спутник [`Progress`].
/// Не хранится в `Progress` напрямую, чтобы тот оставался `Copy`.
#[derive(Debug, Clone)]
pub struct BarState {
    /// ≤ [`BAR_SEGMENTS`] флоатов, каждый в `0.0..=1.0`.
    /// `0.0` = pending, `1.0` = done, промежуточное = resume.
    pub segments: Vec<f32>,
    /// Индексы в `segments`, где сейчас работает воркер.
    pub worker_positions: Vec<u32>,
}

/// Мгновенное состояние прогресса.
///
/// `Copy` — структура маленькая (несколько полей POD-типов), дешевле копировать,
/// чем передавать по ссылке. Плюс не надо думать про borrow checker.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Progress {
    /// Сколько байт уже загружено (сумма по всем завершённым + текущие piece'ы).
    pub bytes_done: u64,
    /// Общий размер в байтах. `0` означает «размер ещё не известен»
    /// (когда сервер не прислал `Content-Length`).
    pub bytes_total: u64,
    /// Сколько piece'ов завершено.
    pub pieces_done: u32,
    /// Сколько всего piece'ов будет (0, если размер ещё не известен).
    pub pieces_total: u32,
    /// Текущая скорость, байт/с. EMA со стороны движка.
    pub speed_bps: f64,
    /// Оценка оставшегося времени, секунды. `None` — неизвестно.
    pub eta_secs: Option<u64>,
}

impl Progress {
    /// Доля завершённого в `[0.0, 1.0]`. `None`, если общий размер ещё неизвестен.
    /// Отдельный метод (а не поле) — чтобы не хранить производное значение
    /// и не рисковать рассинхроном с `bytes_done/bytes_total`.
    pub fn fraction(&self) -> Option<f64> {
        if self.bytes_total == 0 {
            None
        } else {
            // `as f64` у `u64` — потеря точности возможна для гигантских чисел,
            // но для прогрессбара (нужна относительная точность ~1/пикс) хватает.
            Some(self.bytes_done as f64 / self.bytes_total as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zeroed() {
        let p = Progress::default();
        assert_eq!(p.bytes_done, 0);
        assert_eq!(p.bytes_total, 0);
        assert_eq!(p.pieces_done, 0);
        assert_eq!(p.pieces_total, 0);
        assert_eq!(p.speed_bps, 0.0);
        assert_eq!(p.eta_secs, None);
    }

    #[test]
    fn fraction_unknown_when_total_is_zero() {
        let p = Progress::default();
        assert_eq!(p.fraction(), None);
    }

    #[test]
    fn fraction_half() {
        let p = Progress {
            bytes_done: 50,
            bytes_total: 100,
            ..Progress::default() // «остальное — как в Default». Паттерн update.
        };
        assert_eq!(p.fraction(), Some(0.5));
    }
}
