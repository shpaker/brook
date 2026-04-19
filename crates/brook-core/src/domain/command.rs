//! Команды, которые менеджер отдаёт движку (или движок — самому себе).
//!
//! Отдельный тип, чтобы сигнатура канала была самодокументированной:
//! `mpsc::Sender<DownloadCommand>` читается как «канал для команд», а
//! не как «канал со строками, которые мы договорились интерпретировать».

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownloadCommand {
    /// Приостановить активную загрузку: останавливаем воркеры, но
    /// сохраняем прогресс — возобновим позже с того же места.
    Pause,
    /// Снять паузу / запустить из `Queued`.
    Resume,
    /// Отменить и удалить частично загруженные файлы.
    Cancel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_is_implemented() {
        // `Copy` — не просто декоратор: без него вот этот код
        // не скомпилируется, потому что `c` «переехал» в `take`.
        fn take(_c: DownloadCommand) {}
        let c = DownloadCommand::Pause;
        take(c);
        // Если бы `DownloadCommand` не был `Copy`, эта строка упала бы с
        // "value used after move".
        assert_eq!(c, DownloadCommand::Pause);
    }
}
