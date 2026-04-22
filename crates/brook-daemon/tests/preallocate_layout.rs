//! Интеграционный тест этапа 1.6: связываем `fs::preallocate` и `plan_pieces`
//! на одном реалистичном размере (100 MiB).

use std::fs::OpenOptions;

use brook_daemon::storage::fs::{
    available_space,
    preallocate,
};
use brook_daemon::storage::plan::{
    PiecePlanConfig,
    plan_pieces,
};
use tempfile::tempdir;

const MIB: u64 = 1024 * 1024;

#[test]
fn preallocate_100mib_and_layout() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.data.brook");

    let size = 100 * MIB;

    // Сначала проверим, что на целевой ФС есть место — это предохранитель
    // для CI-окружений с маленькими tmpfs.
    let free = available_space(dir.path()).unwrap();
    assert!(free > size, "not enough free space for test: {free} bytes");

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    preallocate(&file, size).unwrap();

    let on_disk = file.metadata().unwrap().len();
    assert_eq!(on_disk, size);

    let plan = plan_pieces(size, PiecePlanConfig::default());

    // 100 MiB / 128 = 800 KiB → next_pow2 = 1 MiB → clamp по MIN = 16 MiB.
    assert_eq!(plan.piece_size, 16 * MIB);

    // ceil(100 / 16) = 7 кусков, последний = 4 MiB.
    assert_eq!(plan.pieces.len(), 7);
    let last = plan.pieces.last().unwrap();
    assert_eq!(last.size, 4 * MIB);

    // Сумма размеров кусков = размер файла; offset'ы непрерывны.
    let total: u64 = plan.pieces.iter().map(|p| p.size).sum();
    assert_eq!(total, size);

    let mut expected_offset = 0u64;
    for (i, p) in plan.pieces.iter().enumerate() {
        assert_eq!(p.idx, i as u32);
        assert_eq!(p.offset, expected_offset);
        expected_offset += p.size;
    }
}
