#![allow(clippy::result_large_err)]
// tonic::Status крупный, но это возвращаемый тип, зашитый в трейт tonic — boxing ничего не даёт.

//! `brook-api` — тонкая gRPC-обёртка над `brook_core::DownloadManager`.
//!
//! Слой `brook-api` не содержит бизнес-логики: он транслирует proto-запросы
//! в вызовы `DownloadManager` и обратно. Вся реальная работа (очередь,
//! движки, broadcast событий) живёт в `brook-core`. Биндинг сокета и
//! настройка `tonic::transport::Server` — дело `brookd`.
//!
//! Публичный API:
//! - [`BrookService`] — реализация proto-сервиса `brook.v1.BrookService`.
//! - Re-export [`BrookServiceServer`] — чтобы `brookd` подключил сервис
//!   одной строчкой (`BrookServiceServer::new(BrookService::new(manager))`).
//! - [`trace::trace_interceptor`] — tonic-интерцептор, прокидывающий
//!   `session_id`/`request_id` в `tracing::Span`.

pub mod mapper;
pub mod service;
pub mod trace;

pub use brook_proto::brook::v1::brook_service_server::BrookServiceServer;
pub use service::BrookService;
pub use trace::trace_interceptor;
