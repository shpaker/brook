//! Расчёт раскладки piece'ов для одной загрузки.
//!
//! Чистая арифметика: на входе — размер файла и конфиг, на выходе —
//! выбранный `piece_size` и список кусков `(idx, offset, size)`. Никакого
//! I/O здесь нет, поэтому модуль легко покрывается юнит-тестами.
//!
//! Формула выбора `piece_size`:
//! ```text
//! piece_size = clamp(next_pow2(size / target_count), min, max)
//! ```
//! где `target_count` — желаемое число кусков (по умолчанию 128). Идея —
//! держать количество piece'ов в разумном диапазоне независимо от размера
//! файла: маленькие файлы → один-два больших куска, огромные файлы →
//! упираемся в `piece_size_max` и пишем больше кусков.

/// Желаемое число piece'ов на загрузку (верхняя оценка).
///
/// TODO(stage 3.x): читать из `settings`; пока хардкод.
pub const DEFAULT_TARGET_COUNT: u32 = 128;

/// Минимальный размер piece'а — 16 MiB.
///
/// TODO(stage 3.x): читать из `settings`; пока хардкод.
pub const DEFAULT_PIECE_SIZE_MIN: u64 = 16 * 1024 * 1024;

/// Максимальный размер piece'а — 128 MiB.
///
/// TODO(stage 3.x): читать из `settings`; пока хардкод.
pub const DEFAULT_PIECE_SIZE_MAX: u64 = 128 * 1024 * 1024;

/// Параметры нарезки.
///
/// Собран отдельной структурой, чтобы позже (этап 3.x) легко подставить
/// значения из `Settings` — без изменения сигнатуры `plan_pieces`.
#[derive(Debug, Clone, Copy)]
pub struct PiecePlanConfig {
    pub target_count: u32,
    pub piece_size_min: u64,
    pub piece_size_max: u64,
}

impl Default for PiecePlanConfig {
    fn default() -> Self {
        Self {
            target_count: DEFAULT_TARGET_COUNT,
            piece_size_min: DEFAULT_PIECE_SIZE_MIN,
            piece_size_max: DEFAULT_PIECE_SIZE_MAX,
        }
    }
}

/// Расположение одного piece'а в файле.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceLayout {
    pub idx: u32,
    pub offset: u64,
    pub size: u64,
}

/// Итоговая раскладка: выбранный `piece_size` + список кусков.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiecePlan {
    pub piece_size: u64,
    pub pieces: Vec<PieceLayout>,
}

/// Посчитать раскладку для файла размером `size` байт.
///
/// Пустой файл (`size == 0`) → пустой план: 0 кусков. На практике такой
/// загрузки быть не должно, но возвращать ошибку здесь излишне — это
/// чистая арифметика, а проверку «size > 0» сделает вызывающий.
pub fn plan_pieces(size: u64, cfg: PiecePlanConfig) -> PiecePlan {
    if size == 0 {
        return PiecePlan {
            piece_size: cfg.piece_size_min,
            pieces: Vec::new(),
        };
    }

    let target = cfg.target_count.max(1) as u64;
    // Округление вверх: если `size < target`, raw будет 0 → clamp поднимет до min.
    let raw = size.div_ceil(target);
    let rounded = next_pow2_u64(raw);
    let piece_size = rounded.clamp(cfg.piece_size_min, cfg.piece_size_max);

    let n = size.div_ceil(piece_size);
    let mut pieces = Vec::with_capacity(n as usize);
    for i in 0..n {
        let offset = i * piece_size;
        let remaining = size - offset;
        let this_size = remaining.min(piece_size);
        pieces.push(PieceLayout {
            idx: i as u32,
            offset,
            size: this_size,
        });
    }

    PiecePlan { piece_size, pieces }
}

/// Округлить вверх до ближайшей степени двойки.
///
/// `n == 0 → 1`. При переполнении u64 (`n > 2^63`) возвращаем `u64::MAX`,
/// но это защитная ветка — в реальности размеры файлов до таких величин
/// не доходят, да и `piece_size_max` всё равно прижмёт результат вниз.
fn next_pow2_u64(n: u64) -> u64 {
    if n <= 1 {
        return 1;
    }
    // `next_power_of_two` паникует при переполнении; checked-вариант вернёт
    // None только для очень больших n — тогда отдаём u64::MAX как «заведомо
    // больше max', clamp дальше прижмёт к piece_size_max».
    n.checked_next_power_of_two().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = DEFAULT_PIECE_SIZE_MIN; // 16 MiB
    const MAX: u64 = DEFAULT_PIECE_SIZE_MAX; // 128 MiB

    fn cfg() -> PiecePlanConfig {
        PiecePlanConfig::default()
    }

    #[test]
    fn zero_size_yields_empty_plan() {
        let plan = plan_pieces(0, cfg());
        assert!(plan.pieces.is_empty());
    }

    #[test]
    fn small_file_clamps_to_min_piece_size() {
        // 10 MiB / 128 = 80 KiB → next_pow2 = 128 KiB → clamp(MIN) = 16 MiB.
        // Файл меньше min'а → один кусок размером с файл.
        let plan = plan_pieces(10 * 1024 * 1024, cfg());
        assert_eq!(plan.piece_size, MIN);
        assert_eq!(plan.pieces.len(), 1);
        assert_eq!(plan.pieces[0].size, 10 * 1024 * 1024);
        assert_eq!(plan.pieces[0].offset, 0);
    }

    #[test]
    fn huge_file_clamps_to_max_piece_size() {
        // 100 GiB / 128 = 800 MiB → next_pow2 = 1 GiB → clamp(MAX) = 128 MiB.
        let size = 100 * 1024 * 1024 * 1024u64;
        let plan = plan_pieces(size, cfg());
        assert_eq!(plan.piece_size, MAX);
        assert_eq!(plan.pieces.len() as u64, size.div_ceil(MAX));
    }

    #[test]
    fn piece_size_is_always_power_of_two_within_bounds() {
        for size in [
            MIN,
            MIN * 3,
            MIN * 128,
            MAX,
            MAX * 64,
            10 * 1024 * 1024 * 1024u64,
        ] {
            let plan = plan_pieces(size, cfg());
            assert!(plan.piece_size.is_power_of_two());
            assert!(plan.piece_size >= MIN);
            assert!(plan.piece_size <= MAX);
        }
    }

    #[test]
    fn last_piece_can_be_smaller() {
        // Выбираем размер, который точно не кратен piece_size.
        let size = MIN * 3 + 12345;
        let plan = plan_pieces(size, cfg());
        let total: u64 = plan.pieces.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
        let last = plan.pieces.last().unwrap();
        assert!(last.size <= plan.piece_size);
        assert!(last.size < plan.piece_size); // именно меньше
    }

    #[test]
    fn offsets_are_contiguous_and_idx_monotonic() {
        let plan = plan_pieces(MIN * 5 + 7, cfg());
        let mut expected_offset = 0u64;
        for (i, p) in plan.pieces.iter().enumerate() {
            assert_eq!(p.idx, i as u32);
            assert_eq!(p.offset, expected_offset);
            expected_offset += p.size;
        }
    }

    #[test]
    fn exact_multiple_all_full_pieces() {
        let size = MIN * 4;
        let plan = plan_pieces(size, cfg());
        assert_eq!(plan.piece_size, MIN);
        assert_eq!(plan.pieces.len(), 4);
        for p in &plan.pieces {
            assert_eq!(p.size, MIN);
        }
    }

    #[test]
    fn next_pow2_edges() {
        assert_eq!(next_pow2_u64(0), 1);
        assert_eq!(next_pow2_u64(1), 1);
        assert_eq!(next_pow2_u64(2), 2);
        assert_eq!(next_pow2_u64(3), 4);
        assert_eq!(next_pow2_u64(5), 8);
        assert_eq!(next_pow2_u64(1024), 1024);
    }
}
