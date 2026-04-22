//! Политика повторов (retry) и защита от crash-loop.
//!
//! Поведение:
//! - экспоненциальный бэкофф: `base × 2^attempt`, ограниченный сверху `max_delay`;
//! - jitter ±`jitter_ratio` (по умолчанию ±20%) на финальную задержку;
//! - до `max_attempts` попыток; не-транзиентные ошибки не ретраятся;
//! - `CrashLoopGuard`: N одинаковых подряд ошибок → считать задачу FAILED.
//!
//! Модуль — **чистая логика без I/O**: ни сети, ни диска, ни таймеров.
//! Решение применяет вызывающий код (например, `DownloadEngine`).

use std::time::Duration;

use rand::Rng;

/// Политика расчёта задержки и решения «ретраить / сдаваться».
///
/// Создаётся один раз и переиспользуется; внутреннего состояния нет —
/// `attempt` приходит снаружи (его хранит владелец, обычно воркер).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Базовая задержка первой попытки (attempt=0 → ~`base`).
    pub base: Duration,
    /// Жёсткий потолок после clamp экспоненты (jitter может выйти за него
    /// на `±jitter_ratio`).
    pub max_delay: Duration,
    /// Сколько всего попыток делать (включая первую неуспешную).
    /// `attempt >= max_attempts` ⇒ `GiveUp`.
    pub max_attempts: u32,
    /// Доля случайного разброса: `1 ± jitter_ratio`. Для 0.2 — это ±20%.
    /// Значение ≥ 0; `0.0` отключает jitter.
    pub jitter_ratio: f64,
}

/// Результат классификации ошибки по политике.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Подождать указанное время и сделать ещё одну попытку.
    Retry(Duration),
    /// Больше не пробовать — превышен лимит попыток или ошибка не транзиентная.
    GiveUp,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: 10,
            jitter_ratio: 0.2,
        }
    }
}

impl RetryPolicy {
    /// Принять решение: ретраить (и когда) или сдаваться.
    ///
    /// `attempt` — номер **уже сделанной** попытки, начиная с 0. То есть
    /// первая попытка → `attempt=0`, после её провала вызывается
    /// `classify(0, transient)`.
    pub fn classify(&self, attempt: u32, transient: bool) -> RetryDecision {
        if !transient || attempt + 1 >= self.max_attempts {
            return RetryDecision::GiveUp;
        }
        RetryDecision::Retry(self.delay_for(attempt))
    }

    /// Чистый расчёт задержки перед `attempt+1`-й попыткой.
    ///
    /// Формула: `min(base × 2^attempt, max_delay) × (1 ± jitter_ratio)`.
    /// Clamp применяется **до** jitter — иначе верх превышал бы `max_delay`
    /// предсказуемым образом, а мы хотим жёсткий потолок экспоненты.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let base_secs = self.base.as_secs_f64();
        let max_secs = self.max_delay.as_secs_f64();
        // `2^attempt` считаем в f64 — u32 переполнялся бы на attempt>=64,
        // а f64 просто уедет в inf и после min даст `max_secs`.
        let raw = base_secs * (attempt as f64).exp2();
        let capped = raw.min(max_secs);
        let factor = if self.jitter_ratio > 0.0 {
            let lo = 1.0 - self.jitter_ratio;
            let hi = 1.0 + self.jitter_ratio;
            rand::thread_rng().gen_range(lo..=hi)
        } else {
            1.0
        };
        Duration::from_secs_f64(capped * factor)
    }
}

/// Состояние счётчика «одинаковых подряд ошибок».
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashLoopState {
    /// Порог ещё не достигнут — можно продолжать ретраить.
    Ok,
    /// Порог достигнут — пора помечать загрузку как FAILED.
    Triggered,
}

/// Отслеживает последовательные ошибки с одним и тем же «ключом».
///
/// Ключ — строка, которую формирует вызывающий (обычно `err.to_string()`
/// или имя варианта enum). Совпадение считается по `==` на строках.
/// Смена ключа (другая причина отказа) сбрасывает streak на 1.
#[derive(Debug, Clone)]
pub struct CrashLoopGuard {
    limit: u32,
    last: Option<String>,
    streak: u32,
}

impl CrashLoopGuard {
    /// `limit` — сколько одинаковых подряд ошибок допустимо, прежде чем
    /// `observe` вернёт `Triggered`. По умолчанию 5.
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            last: None,
            streak: 0,
        }
    }

    /// Зарегистрировать новую ошибку. Возвращает текущее состояние guard'а
    /// **после** учёта этой ошибки.
    pub fn observe(&mut self, err_key: &str) -> CrashLoopState {
        match self.last.as_deref() {
            Some(prev) if prev == err_key => self.streak += 1,
            _ => {
                self.last = Some(err_key.to_owned());
                self.streak = 1;
            }
        }
        if self.streak >= self.limit {
            CrashLoopState::Triggered
        } else {
            CrashLoopState::Ok
        }
    }

    /// Сбросить счётчик после успеха или ручного вмешательства.
    pub fn reset(&mut self) {
        self.last = None;
        self.streak = 0;
    }
}

impl Default for CrashLoopGuard {
    fn default() -> Self {
        Self::new(5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Погрешность для диапазонных проверок — миллисекунды теряются при
    // `Duration::from_secs_f64`, плюс граничный gen_range(..=hi) возможен.
    const EPS: f64 = 0.01;

    fn policy() -> RetryPolicy {
        RetryPolicy::default()
    }

    #[test]
    fn delay_first_attempt_in_jitter_band() {
        let p = policy();
        for _ in 0..64 {
            let d = p.delay_for(0).as_secs_f64();
            assert!((0.8 - EPS..=1.2 + EPS).contains(&d), "unexpected: {d}s");
        }
    }

    #[test]
    fn delay_third_attempt_is_eight_seconds_plus_jitter() {
        // base × 2^3 = 8s, после jitter — в [6.4, 9.6].
        let p = policy();
        for _ in 0..64 {
            let d = p.delay_for(3).as_secs_f64();
            assert!((6.4 - EPS..=9.6 + EPS).contains(&d), "unexpected: {d}s");
        }
    }

    #[test]
    fn delay_clamped_to_max() {
        // 2^30 секунд базы >> max_delay (60s). Ожидаем 60 × [0.8, 1.2].
        let p = policy();
        for _ in 0..64 {
            let d = p.delay_for(30).as_secs_f64();
            assert!((48.0 - EPS..=72.0 + EPS).contains(&d), "unexpected: {d}s");
        }
    }

    #[test]
    fn delay_without_jitter_is_exact() {
        let p = RetryPolicy {
            jitter_ratio: 0.0,
            ..policy()
        };
        assert_eq!(p.delay_for(0), Duration::from_secs(1));
        assert_eq!(p.delay_for(2), Duration::from_secs(4));
        assert_eq!(p.delay_for(100), Duration::from_secs(60));
    }

    #[test]
    fn classify_non_transient_gives_up() {
        assert_eq!(policy().classify(0, false), RetryDecision::GiveUp);
        assert_eq!(policy().classify(3, false), RetryDecision::GiveUp);
    }

    #[test]
    fn classify_last_attempt_gives_up() {
        let p = policy();
        // attempt=max-1 — это 10-я попытка (счёт с 0), лимит исчерпан.
        assert_eq!(p.classify(p.max_attempts - 1, true), RetryDecision::GiveUp);
    }

    #[test]
    fn classify_penultimate_retries() {
        let p = policy();
        match p.classify(p.max_attempts - 2, true) {
            RetryDecision::Retry(_) => {}
            RetryDecision::GiveUp => panic!("expected Retry on penultimate attempt"),
        }
    }

    #[test]
    fn crash_loop_triggers_on_fifth_identical_error() {
        let mut g = CrashLoopGuard::new(5);
        for _ in 0..4 {
            assert_eq!(g.observe("network"), CrashLoopState::Ok);
        }
        assert_eq!(g.observe("network"), CrashLoopState::Triggered);
    }

    #[test]
    fn crash_loop_resets_on_different_key() {
        let mut g = CrashLoopGuard::new(5);
        for _ in 0..4 {
            assert_eq!(g.observe("network"), CrashLoopState::Ok);
        }
        // Смена причины — streak обнуляется до 1.
        assert_eq!(g.observe("timeout"), CrashLoopState::Ok);
        // После сброса — ещё 4 того же ключа не должны триггерить.
        for _ in 0..3 {
            assert_eq!(g.observe("timeout"), CrashLoopState::Ok);
        }
        assert_eq!(g.observe("timeout"), CrashLoopState::Triggered);
    }

    #[test]
    fn crash_loop_reset_clears_state() {
        let mut g = CrashLoopGuard::new(3);
        g.observe("x");
        g.observe("x");
        g.reset();
        assert_eq!(g.observe("x"), CrashLoopState::Ok);
        assert_eq!(g.observe("x"), CrashLoopState::Ok);
        assert_eq!(g.observe("x"), CrashLoopState::Triggered);
    }
}
