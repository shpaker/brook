//! `brook-http` — HTTP-адаптер для портов `brook-core`.
//!
//! Реализует [`brook_core::THttpInspect`] и [`brook_core::TRangeFetch`] поверх
//! `reqwest` + `reqwest-middleware`. Всё, что связано с сетью, TLS и парсингом
//! HTTP-заголовков, живёт здесь; `brook-core` не знает о существовании
//! `reqwest`.
//!
//! Публичный API:
//! - [`HttpClientBuilder`] — единственная точка сборки `reqwest::Client`
//!   (rustls, таймауты, отключённая автокомпрессия, лимит редиректов,
//!   зашитый middleware логирования).
//! - [`HttpInspectClient`] — реализация `THttpInspect`.
//! - [`RangeFetchClient`] — реализация `TRangeFetch`.

mod builder;
mod inspect;
mod logging;
mod range;
mod url_scheme;

pub use builder::HttpClientBuilder;
pub use inspect::HttpInspectClient;
pub use range::RangeFetchClient;
