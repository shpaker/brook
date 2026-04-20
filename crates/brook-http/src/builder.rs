//! `HttpClientBuilder` — единственная точка сборки `reqwest::Client` для
//! всех `*Client`-ов крейта.
//!
//! Что фиксирует билдер:
//! - rustls (без native-tls);
//! - connect timeout 10 s, pool idle timeout 90 s;
//! - User-Agent `brook-http/<CARGO_PKG_VERSION>`;
//! - автокомпрессия (gzip/brotli/deflate) **выключена** — иначе ломается
//!   байт-точность `Content-Length` и валидация `Content-Range`;
//! - максимум 10 редиректов (дефолт reqwest, вынесен явно).
//!
//! Билдер возвращает [`reqwest_middleware::ClientWithMiddleware`] с уже
//! зашитым [`RequestResponseLoggingMiddleware`]. Read-timeout (per-request
//! idle) проставляют конкретные клиенты сами — он зависит от типа запроса.

use std::time::Duration;

use reqwest::redirect;
use reqwest_middleware::{
    ClientBuilder as MwBuilder,
    ClientWithMiddleware,
};

use crate::logging::RequestResponseLoggingMiddleware;

const USER_AGENT: &str = concat!("brook-http/", env!("CARGO_PKG_VERSION"));
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_REDIRECTS: usize = 10;

/// Сборщик HTTP-клиентов с общими для всего `brook-http` настройками.
pub struct HttpClientBuilder {
    connect_timeout: Duration,
    pool_idle_timeout: Duration,
    user_agent: String,
    max_redirects: usize,
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            connect_timeout: CONNECT_TIMEOUT,
            pool_idle_timeout: POOL_IDLE_TIMEOUT,
            user_agent: USER_AGENT.to_string(),
            max_redirects: MAX_REDIRECTS,
        }
    }
}

impl HttpClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    pub fn with_user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = value.into();
        self
    }

    /// Собрать `ClientWithMiddleware` с зашитым логирующим middleware.
    pub fn build(self) -> ClientWithMiddleware {
        let client = reqwest::Client::builder()
            .user_agent(self.user_agent)
            .connect_timeout(self.connect_timeout)
            .pool_idle_timeout(self.pool_idle_timeout)
            .redirect(redirect::Policy::limited(self.max_redirects))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .build()
            .expect("reqwest::Client build: rustls is always available in this workspace");

        MwBuilder::new(client)
            .with(RequestResponseLoggingMiddleware)
            .build()
    }
}
