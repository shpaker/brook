//! Упрощённый in-memory `TRangeFetch` для тестов верхних слоёв.
//!
//! Нужен `DownloadManager`-тестам, которым подробности piece-level retry
//! и усечённых ответов неинтересны: достаточно «загрузка либо доходит,
//! либо падает с контролируемой ошибкой». Более богатый мок с планом
//! исходов по каждому piece'у живёт в `service::engine` рядом со своими
//! тестами — там он заточен под engine-level сценарии.

use std::sync::Mutex;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;

use crate::ports::{
    ByteStream,
    RangeError,
    RangeGuard,
    TRangeFetch,
};

/// Режим мок-ответа на конкретный вызов `fetch_range` / `fetch_full`.
#[derive(Clone)]
pub enum FetchOutcome {
    /// Вернуть запрошенный срез из `bytes`.
    Ok,
    /// Вернуть 503 — транзиентная ошибка, движок должен ретрайнуть.
    Transient,
    /// Вернуть 401 — не-транзиентная ошибка.
    Permanent,
    /// `412 Precondition Failed` — guard не совпал.
    SourceMutated,
}

/// Мок, отдающий заранее заданные байты. По умолчанию — `Ok` на все запросы.
pub struct MockRangeFetch {
    bytes: Vec<u8>,
    /// Очередь исходов — применяется по одному на вызов, пока не опустеет;
    /// после этого — `default_outcome`.
    queue: Mutex<Vec<FetchOutcome>>,
    default_outcome: FetchOutcome,
    calls: AtomicUsize,
    delay: Duration,
}

impl MockRangeFetch {
    /// Всегда успешно отдаёт срез из `bytes`.
    pub fn always_ok(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            queue: Mutex::new(Vec::new()),
            default_outcome: FetchOutcome::Ok,
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
        }
    }

    /// Искусственная задержка на каждый вызов — даёт тестам время
    /// отправить `Pause`/`Cancel` до завершения загрузки.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Подменить исход первых N вызовов (FIFO).
    pub fn push_outcomes(&self, outcomes: impl IntoIterator<Item = FetchOutcome>) {
        let mut q = self.queue.lock().expect("mutex poisoned");
        q.extend(outcomes);
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn next_outcome(&self) -> FetchOutcome {
        let mut q = self.queue.lock().expect("mutex poisoned");
        if q.is_empty() {
            self.default_outcome.clone()
        } else {
            q.remove(0)
        }
    }
}

#[async_trait]
impl TRangeFetch for MockRangeFetch {
    async fn fetch_range(
        &self,
        _url: &str,
        offset: u64,
        len: u64,
        _guard: Option<&RangeGuard>,
    ) -> Result<ByteStream, RangeError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        match self.next_outcome() {
            FetchOutcome::Ok => {
                let start = offset as usize;
                let end = start + len as usize;
                let slice = Bytes::copy_from_slice(&self.bytes[start..end]);
                Ok(Box::pin(stream::iter(vec![Ok(slice)])))
            }
            FetchOutcome::Transient => Err(RangeError::UnexpectedStatus { code: 503 }),
            FetchOutcome::Permanent => Err(RangeError::UnexpectedStatus { code: 401 }),
            FetchOutcome::SourceMutated => Err(RangeError::SourceMutated),
        }
    }

    async fn fetch_full(&self, _url: &str) -> Result<ByteStream, RangeError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        match self.next_outcome() {
            FetchOutcome::Ok => {
                let all = Bytes::copy_from_slice(&self.bytes);
                Ok(Box::pin(stream::iter(vec![Ok(all)])))
            }
            FetchOutcome::Transient => Err(RangeError::UnexpectedStatus { code: 503 }),
            FetchOutcome::Permanent => Err(RangeError::UnexpectedStatus { code: 401 }),
            FetchOutcome::SourceMutated => Err(RangeError::SourceMutated),
        }
    }
}

/// Удобный конструктор «побайтного» мока: байты заполняются `idx -> idx mod 251`.
pub fn sequential_bytes(total: u64) -> Vec<u8> {
    (0..total).map(|i| (i % 251) as u8).collect()
}
