//! `RequestResponseLoggingMiddleware` — одна пара `tracing`-событий
//! (`http.request` / `http.response`) на каждый HTTP-вызов.
//!
//! Логируются только метаданные: метод, хост, путь, статус, `Content-Length`,
//! длительность. **Тело никогда не пишется в лог.**
//!
//! Корреляция: если текущий `tracing`-span содержит поля `download_id` /
//! `request_id`, они подхватятся автоматически (через родительский span у
//! события). Middleware ничего не читает из span'а сам — он только создаёт
//! событие внутри активного контекста вызывающего.

use std::time::Instant;

use reqwest::{
    Request,
    Response,
};
use reqwest_middleware::{
    Middleware,
    Next,
    Result as MwResult,
};
use tracing::{
    Instrument,
    debug,
    info_span,
};

pub(crate) struct RequestResponseLoggingMiddleware;

#[async_trait::async_trait]
impl Middleware for RequestResponseLoggingMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> MwResult<Response> {
        let method = req.method().clone();
        let url = req.url().clone();
        let host = url.host_str().unwrap_or("").to_string();
        let path = url.path().to_string();

        // Дочерний span на один HTTP-вызов; поля корреляции из родителя
        // (`download_id`, `request_id`) — наследуются автоматически.
        let span = info_span!(
            "http.call",
            method = %method,
            url.host = %host,
            url.path = %path,
        );

        async move {
            debug!(
                target: "brook_http",
                event = "http.request",
                method = %method,
                url.host = %host,
                url.path = %path,
                "outgoing HTTP request",
            );

            let started = Instant::now();
            let result = next.run(req, extensions).await;
            let elapsed_ms = started.elapsed().as_millis() as u64;

            match &result {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let content_length = resp.content_length();
                    debug!(
                        target: "brook_http",
                        event = "http.response",
                        method = %method,
                        url.host = %host,
                        url.path = %path,
                        status,
                        content_length,
                        elapsed_ms,
                        "HTTP response received",
                    );
                }
                Err(err) => {
                    debug!(
                        target: "brook_http",
                        event = "http.response",
                        method = %method,
                        url.host = %host,
                        url.path = %path,
                        elapsed_ms,
                        error = %err,
                        "HTTP request failed",
                    );
                }
            }

            result
        }
        .instrument(span)
        .await
    }
}
